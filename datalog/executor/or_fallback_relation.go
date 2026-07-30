package executor

import (
	"fmt"
	"strings"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Note: fmt is used for String() method

// OrFallbackRelation is a streaming relation that evaluates OR branches
// lazily per outer tuple. When shortCircuit=true (or-default), it tries
// branches in order until one produces results. When shortCircuit=false
// (correlated union), it evaluates ALL branches and unions the results.
type OrFallbackRelation struct {
	executor       *DefaultQueryExecutor
	ctx            *Context
	branches       [][]query.Clause
	outerRel       Relation
	symbols        []query.Symbol // Determined lazily from first result
	options        ExecutorOptions
	properties     RelationProperties
	deduplicate    bool
	iteratorCount  int            // Track how many iterators have been created (for debugging)
	joinSyms       []query.Symbol // From or-join: explicit join variables for cache keying
	prefetched     bool           // True when PrefetchEntities has warmed the EA cache
	shortCircuit   bool           // true = fallback (first match wins), false = correlated union (all branches)
	consumedGroups []int          // Input relation groups already incorporated into outerRel

	// scope is the clause's canonical interface (query.ScopeOf) for
	// explicit-header forms (or-join, or-default-join). Branch evaluation
	// may see only the outer bindings of Provides ∪ Correlates: a branch
	// variable outside that interface is a local, and a name collision with
	// an outer symbol must not bind (alpha-equivalence). nil means an
	// inference form (plain or / or-default), which unifies on every shared
	// free variable by language rule.
	scope *query.ClauseScope
}

// NewOrFallbackRelation creates a streaming OR relation.
// When shortCircuit=true, uses fallback semantics (first match wins).
// When shortCircuit=false, uses correlated union (all branches contribute).
func NewOrFallbackRelation(
	executor *DefaultQueryExecutor,
	ctx *Context,
	branches [][]query.Clause,
	outerRel Relation,
	options ExecutorOptions,
	shortCircuit bool,
) *OrFallbackRelation {
	// Compute output symbols statically:
	// Output = outer symbols + symbols produced by branches (that aren't in outer)
	outerSyms := outerRel.Symbols()
	outerSymSet := make(map[query.Symbol]bool)
	for _, sym := range outerSyms {
		outerSymSet[sym] = true
	}

	// Collect symbols that branches produce (common across all branches)
	branchOutputs := computeOrBranchOutputSymbols(branches)

	// Build output symbols: outer symbols first, then new symbols from branches
	symbols := make([]query.Symbol, len(outerSyms))
	copy(symbols, outerSyms)
	for _, sym := range branchOutputs {
		if !outerSymSet[sym] {
			symbols = append(symbols, sym)
		}
	}
	producedSymbols := collectAllBranchOutputSymbols(branches)
	branchesAtMostOne := orBranchesEmitAtMostOne(branches, outerSyms)
	properties, deduplicate := orProperties(
		outerRel.Properties(),
		producedSymbols,
		collectAllBranchOverwrittenSymbols(branches, outerSyms),
		symbols,
		shortCircuit,
		branchesAtMostOne,
	)
	if options.Handler != nil {
		options.Handler(annotations.Event{
			Name: annotations.OrPropertiesDerived,
			Data: map[string]interface{}{
				"short_circuit":           shortCircuit,
				"branches_at_most_one":    branchesAtMostOne,
				"deduplicate":             deduplicate,
				"outer_candidate_keys":    outerRel.Properties().clone().Keys,
				"result_candidate_keys":   properties.clone().Keys,
				"result_ordering":         properties.clone().Ordering,
				"branch_output_symbols":   append([]query.Symbol(nil), producedSymbols...),
				"relation_output_symbols": append([]query.Symbol(nil), symbols...),
			},
		})
	}

	return &OrFallbackRelation{
		executor:     executor,
		ctx:          ctx,
		branches:     branches,
		outerRel:     outerRel,
		symbols:      symbols,
		options:      options,
		properties:   properties,
		deduplicate:  deduplicate,
		shortCircuit: shortCircuit,
	}
}

// computeOrBranchOutputSymbols computes symbols that OR branches produce.
// Returns symbols that are common to all branches (intersection).
func computeOrBranchOutputSymbols(branches [][]query.Clause) []query.Symbol {
	if len(branches) == 0 {
		return nil
	}

	// Collect outputs from first branch
	firstBranchOutputs := collectBranchOutputSymbols(branches[0])
	if len(branches) == 1 {
		return firstBranchOutputs
	}

	// Convert to set for intersection
	commonSet := make(map[query.Symbol]bool)
	for _, sym := range firstBranchOutputs {
		commonSet[sym] = true
	}

	// Intersect with other branches
	for i := 1; i < len(branches); i++ {
		branchOutputs := collectBranchOutputSymbols(branches[i])
		branchSet := make(map[query.Symbol]bool)
		for _, sym := range branchOutputs {
			branchSet[sym] = true
		}
		// Keep only symbols in both
		for sym := range commonSet {
			if !branchSet[sym] {
				delete(commonSet, sym)
			}
		}
	}

	// Preserve order from first branch
	var result []query.Symbol
	for _, sym := range firstBranchOutputs {
		if commonSet[sym] {
			result = append(result, sym)
		}
	}
	return result
}

func collectAllBranchOutputSymbols(branches [][]query.Clause) []query.Symbol {
	var result []query.Symbol
	seen := make(map[query.Symbol]bool)
	for _, branch := range branches {
		for _, symbol := range collectBranchOutputSymbols(branch) {
			if !seen[symbol] {
				seen[symbol] = true
				result = append(result, symbol)
			}
		}
	}
	return result
}

func collectAllBranchOverwrittenSymbols(
	branches [][]query.Clause,
	outerSymbols []query.Symbol,
) []query.Symbol {
	var result []query.Symbol
	seen := make(map[query.Symbol]bool)
	outerSet := make(map[query.Symbol]bool, len(outerSymbols))
	for _, symbol := range outerSymbols {
		outerSet[symbol] = true
	}
	add := func(symbol query.Symbol) {
		if symbol != nil && !seen[symbol] {
			seen[symbol] = true
			result = append(result, symbol)
		}
	}
	var collectClause func(query.Clause)
	collectClause = func(clause query.Clause) {
		switch typed := clause.(type) {
		case *query.Expression:
			switch binding := typed.Binding.(type) {
			case query.Symbol:
				add(binding)
			case query.TupleBinding:
				for _, symbol := range binding.Variables {
					add(symbol)
				}
			}
		case *query.SubqueryPattern:
			switch binding := typed.Binding.(type) {
			case query.ScalarBinding:
				add(binding.Variable)
			case query.TupleBinding:
				for _, symbol := range binding.Variables {
					add(symbol)
				}
			case query.RelationBinding:
				for i, symbol := range binding.Variables {
					if !subqueryFindVariablePassesOuter(typed.Query, i, symbol, outerSet) {
						add(symbol)
					}
				}
			}
		case *query.OrClause:
			for _, nested := range typed.Branches {
				for _, nestedClause := range nested {
					collectClause(nestedClause)
				}
			}
		case *query.OrDefaultClause:
			for _, nested := range typed.Branches {
				for _, nestedClause := range nested {
					collectClause(nestedClause)
				}
			}
		}
	}
	for _, branch := range branches {
		for _, clause := range branch {
			collectClause(clause)
		}
	}
	return result
}

func orBranchesEmitAtMostOne(
	branches [][]query.Clause,
	outerSymbols []query.Symbol,
) bool {
	if len(branches) == 0 {
		return false
	}
	outerSet := make(map[query.Symbol]bool, len(outerSymbols))
	for _, symbol := range outerSymbols {
		outerSet[symbol] = true
	}
	for _, branch := range branches {
		if len(branch) == 0 {
			return false
		}
		for _, clause := range branch {
			switch typed := clause.(type) {
			case *query.Expression:
				if typed.Function == nil || typed.Function.ReturnType() == "tuples" {
					return false
				}
			case *query.SubqueryPattern:
				switch binding := typed.Binding.(type) {
				case query.ScalarBinding, query.TupleBinding:
				case query.RelationBinding:
					if !relationBindingGroupsAreOuterBound(typed.Query, binding.Variables, outerSet) {
						return false
					}
				default:
					return false
				}
			default:
				return false
			}
		}
	}
	return true
}

func relationBindingGroupsAreOuterBound(
	subquery *query.Query,
	bindingVariables []query.Symbol,
	outerSymbols map[query.Symbol]bool,
) bool {
	if subquery == nil || len(subquery.Find) != len(bindingVariables) {
		return false
	}
	for i, element := range subquery.Find {
		switch element.(type) {
		case query.FindVariable:
			if !outerSymbols[bindingVariables[i]] {
				return false
			}
		case query.FindAggregate:
		default:
			return false
		}
	}
	return true
}

func subqueryFindVariablePassesOuter(
	subquery *query.Query,
	findIndex int,
	bindingVariable query.Symbol,
	outerSymbols map[query.Symbol]bool,
) bool {
	if subquery == nil || findIndex >= len(subquery.Find) || !outerSymbols[bindingVariable] {
		return false
	}
	_, isVariable := subquery.Find[findIndex].(query.FindVariable)
	return isVariable
}

// collectBranchOutputSymbols collects symbols that a single branch produces.
func collectBranchOutputSymbols(branch []query.Clause) []query.Symbol {
	var outputs []query.Symbol
	seen := make(map[query.Symbol]bool)

	for _, c := range branch {
		switch clause := c.(type) {
		case *query.DataPattern:
			for _, elem := range clause.Elements {
				if v, ok := elem.(query.Variable); ok && !seen[v.Name] {
					seen[v.Name] = true
					outputs = append(outputs, v.Name)
				}
			}
		case *query.Expression:
			switch b := clause.Binding.(type) {
			case query.Symbol:
				if b != nil && !seen[b] {
					seen[b] = true
					outputs = append(outputs, b)
				}
			case query.TupleBinding:
				for _, v := range b.Variables {
					if !seen[v] {
						seen[v] = true
						outputs = append(outputs, v)
					}
				}
			}
		case *query.SubqueryPattern:
			// Subquery provides its binding variables
			for _, v := range clause.Binding.BoundVariables() {
				if !seen[v] {
					seen[v] = true
					outputs = append(outputs, v)
				}
			}
		case *query.OrJoinClause:
			// or-join declares its output symbols as join vars
			for _, v := range clause.JoinVars {
				if !seen[v] {
					seen[v] = true
					outputs = append(outputs, v)
				}
			}
		case *query.OrClause:
			// plain or: output symbols are the intersection across branches
			orOutputs := computeOrBranchOutputSymbols(clause.Branches)
			for _, v := range orOutputs {
				if !seen[v] {
					seen[v] = true
					outputs = append(outputs, v)
				}
			}
		case *query.OrDefaultClause:
			orOutputs := computeOrBranchOutputSymbols(clause.Branches)
			for _, v := range orOutputs {
				if !seen[v] {
					seen[v] = true
					outputs = append(outputs, v)
				}
			}
		}
	}
	return outputs
}

func (r *OrFallbackRelation) Iterator() Iterator {
	r.iteratorCount++

	// When a cacheable DataPattern-only or-join branch will be narrowed to the
	// outer join keys (see outerJoinKeys), the outer must be re-iterable: once to
	// extract the join keys and again to drive per-tuple probing. A streaming
	// outer can only be iterated once, so drain it to a MaterializedRelation up
	// front. OR-fallback iterates the whole outer per-tuple regardless, so this
	// buffers the driving relation rather than adding work.
	// See docs/bugs/BUG_GETELSE_SCAN_REWRITE_NOT_NARROWED_BY_BOUND_CHILD.md.
	outer := r.outerRel
	var setupErr error
	if r.shortCircuit && len(r.joinSyms) > 0 && hasNarrowableBranch(r.branches) {
		if _, isMat := outer.(*MaterializedRelation); !isMat {
			var tuples []Tuple
			setupErr = collectTuplesInto(&tuples, outer)
			materialized := newMaterializedRelationFromSet(
				outer.Symbols(),
				tuples,
				r.options,
				outer.Properties(),
			)
			materialized.err = setupErr
			outer = materialized
			if r.options.Handler != nil {
				r.options.Handler(annotations.Event{
					Name: annotations.OrFallbackOuterMaterialized,
					Data: map[string]interface{}{
						"reason":      "join-key-narrowing",
						"tuple_count": len(tuples),
					},
				})
			}
		}
	}

	outerIter := outer.Iterator()
	var seen *TupleKeyMap
	if r.deduplicate {
		seen = NewTupleKeyMap()
	}

	// Branch-visible outer bindings, derived from the canon. Explicit-header
	// forms (scope != nil) see the outer bindings of the clause's declared
	// interface — Provides ∪ Correlates per query.ScopeOf; a branch variable
	// outside that interface is a local, and a name collision with an outer
	// symbol must not bind (alpha-equivalence). Inference forms unify on
	// every shared free variable by language rule.
	branchFree := branchFreeVariables(r.branches)
	interfaceSyms := branchFree
	if r.scope != nil {
		interfaceSyms = append(append([]query.Symbol(nil), r.scope.Provides...), r.scope.Correlates...)
	}
	var branchVisibleSyms []query.Symbol
	var branchVisibleIdx []int
	for i, sym := range outer.Symbols() {
		if query.ContainsSymbol(interfaceSyms, sym) {
			branchVisibleSyms = append(branchVisibleSyms, sym)
			branchVisibleIdx = append(branchVisibleIdx, i)
		}
	}

	// Environment bindings the branches consume. The locals rule above
	// governs relation data only: an :in-bound single-valued parameter is
	// the query's formal parameter, ambient in every clause scope, so its
	// relation joins into the branch input at this scope boundary — exactly
	// as the top-level and subquery boundaries bind it into their input
	// relations. Restricted to symbols some branch actually uses, minus
	// those already branch-visible from the outer tuple. Nested compound
	// clauses join their own consumption when they execute.
	var envRel Relation
	if env := r.ctx.Environment(); env != nil {
		var consumed []query.Symbol
		for _, sym := range branchFree {
			if query.ContainsSymbol(env.Symbols(), sym) && !query.ContainsSymbol(branchVisibleSyms, sym) {
				consumed = append(consumed, sym)
			}
		}
		if len(consumed) > 0 {
			projected, err := env.Project(consumed)
			if err != nil {
				if setupErr == nil {
					setupErr = err
				}
			} else {
				envRel = projected
			}
		}
	}

	// Emit annotation for iterator creation
	// Note: Don't call outer.Size() - it may block for streaming relations
	if r.options.Handler != nil {
		r.options.Handler(annotations.Event{
			Name:  annotations.OrFallbackIteratorCreated,
			Start: time.Now(),
			Data: map[string]interface{}{
				"iterator_count": r.iteratorCount,
				"outer_symbols":  outer.Symbols(),
				"outer_type":     fmt.Sprintf("%T", outer),
			},
		})
	}

	return &OrFallbackIterator{
		executor:          r.executor,
		ctx:               r.ctx,
		branches:          r.branches,
		shortCircuit:      r.shortCircuit,
		outerIter:         outerIter,
		outerRel:          outer, // re-iterable: drives join-key narrowing & EA cache branch building
		outerSyms:         outer.Symbols(),
		outputSyms:        r.symbols, // Use pre-computed symbols
		options:           r.options,
		joinSyms:          r.joinSyms,
		branchVisibleSyms: branchVisibleSyms,
		envRel:            envRel,
		branchInput: makeBranchInput(
			branchVisibleSyms,
			branchVisibleIdx,
			len(branchVisibleIdx) == len(outer.Symbols()),
			envRel,
			r.options,
		),
		prefetched: r.prefetched,
		seen:       seen,
		err:        setupErr,
		done:       setupErr != nil,
	}
}

func (r *OrFallbackRelation) Symbols() []query.Symbol {
	// Symbols are computed at construction time - no peeking needed
	return r.symbols
}

func (r *OrFallbackRelation) Properties() RelationProperties {
	return r.properties
}

func (r *OrFallbackRelation) Size() int {
	return -1 // Streaming - unknown size
}

func (r *OrFallbackRelation) Get(i int) Tuple {
	return nil // Not supported for streaming
}

func (r *OrFallbackRelation) String() string {
	var symStrs []string
	for _, sym := range r.Symbols() {
		symStrs = append(symStrs, sym.String())
	}
	return fmt.Sprintf("OrFallbackRelation([%s], streaming)", strings.Join(symStrs, " "))
}

func (r *OrFallbackRelation) Table() string {
	return r.Materialize().Table()
}

func (r *OrFallbackRelation) ProjectFromPattern(pattern *query.DataPattern) Relation {
	return r.Materialize().ProjectFromPattern(pattern)
}

func (r *OrFallbackRelation) Sorted() ([]Tuple, error) {
	return r.Materialize().Sorted()
}

func (r *OrFallbackRelation) Project(symbols []query.Symbol) (Relation, error) {
	return r.Materialize().Project(symbols)
}

func (r *OrFallbackRelation) Materialize() Relation {
	var tuples []Tuple
	err := collectTuplesInto(&tuples, r)

	syms := r.symbols
	if syms == nil {
		syms = r.Symbols()
	}
	mat := newMaterializedRelationFromSet(syms, tuples, r.options, r.properties)
	if err != nil {
		mat.err = err
	}
	return mat
}

func (r *OrFallbackRelation) Sort(orderBy []query.OrderByClause) Relation {
	return r.Materialize().Sort(orderBy)
}

func (r *OrFallbackRelation) FilterWithPredicate(pred query.Predicate) Relation {
	return r.Materialize().FilterWithPredicate(pred)
}

func (r *OrFallbackRelation) EvaluateFunction(fn query.Function, outputSymbol query.Symbol) Relation {
	return r.Materialize().EvaluateFunction(fn, outputSymbol)
}

func (r *OrFallbackRelation) Select(pred func(Tuple) bool) Relation {
	return Select(r, pred)
}

func (r *OrFallbackRelation) Join(other Relation) Relation {
	common := CommonSymbols(r, other)
	if len(common) == 0 {
		return crossProduct(r, other)
	}
	return r.HashJoin(other, common)
}

func (r *OrFallbackRelation) HashJoin(other Relation, joinSyms []query.Symbol) Relation {
	return HashJoin(r, other, joinSyms)
}

func (r *OrFallbackRelation) SemiJoin(other Relation, joinSyms []query.Symbol) Relation {
	return SemiJoin(r, other, joinSyms)
}

func (r *OrFallbackRelation) AntiJoin(other Relation, joinSyms []query.Symbol) Relation {
	return AntiJoin(r, other, joinSyms)
}

func (r *OrFallbackRelation) Aggregate(findElements []query.FindElement) Relation {
	return ExecuteAggregations(r, findElements)
}

func (r *OrFallbackRelation) Options() ExecutorOptions {
	return r.options
}

// RequiresCopy returns false because OrFallbackIterator copies tuples from
// sources that have RequiresCopy() = true at the boundary.
func (r *OrFallbackRelation) RequiresCopy() bool {
	return false
}

// OrFallbackIterator lazily evaluates OR branches per outer tuple.
type OrFallbackIterator struct {
	executor     *DefaultQueryExecutor
	ctx          *Context
	branches     [][]query.Clause
	shortCircuit bool // true = fallback, false = correlated union
	outerIter    Iterator
	outerRel     Relation // Materialized outer relation (for EA cache branch building)
	outerSyms    []query.Symbol
	options      ExecutorOptions
	joinSyms     []query.Symbol // From or-join: used as cache key (not all shared symbols)

	// Branch-visible outer bindings: the outer symbols of the clause's
	// canonical interface (query.ScopeOf Provides ∪ Correlates for
	// explicit-header forms; every shared branch free variable for inference
	// forms). Shared by filterBranchToOuterTuple and the EA-cache E check.
	branchVisibleSyms []query.Symbol

	// envRel is the single retained source of the environment bindings the
	// branches consume: the context's environment relation projected onto
	// the symbols some branch actually uses, minus those branch-visible from
	// the outer. Constant for the query's lifetime; consumers read
	// Symbols()/Get(0) from it — no scalar copies.
	envRel Relation

	// branchInput builds one outer tuple's branch input — the join of the
	// branch-visible projection with envRel — and the visible tuple itself
	// (for shared-symbol filtering of relation data). A closure: the join's
	// loop-invariant sides (projection indices, output schema, envRel) are
	// captures, scoped to this single consumer.
	branchInput func(Tuple) (Relation, Tuple)

	// Current state
	currentBranchIter Iterator
	currentTuple      Tuple
	outputSyms        []query.Symbol
	seen              *TupleKeyMap
	done              bool
	err               error

	// Cached-branch emit state: a probe result being emitted directly from
	// the branch cache's shared backing — no wrapper relation, no
	// re-deduplication (the tuples are a contiguous span of a relation that is
	// already a set), no per-probe iterator.
	cachedMatches  []Tuple
	cachedMatchPos int
	cachedPlan     projectionPlan
	cachedOuter    Tuple

	// branchPlans memoizes each branch's output projection, keyed by branch
	// index with the branch's result symbols as the plan's cache key. Per
	// branch the symbols are fixed for the iterator's lifetime (same clauses,
	// same input shape), so the projection is planned once instead of
	// building symbol maps per emitted tuple.
	branchPlans map[int]branchPlan

	// Cache for uncorrelated branch results. Key: branch index.
	// See ALGEBRA.md "OR-Fallback Branch Caching" for specification.
	branchCache map[int]*cachedBranch
	prefetched  bool // True when PrefetchEntities has warmed the EA cache for outer entities

	// Narrowed join keys for cacheable DataPattern-only or-join branches: the
	// outer relation projected onto joinSyms (deduplicated), computed once.
	// Passing these as the cached branch's bindings narrows its scan to the
	// outer entities instead of the whole attribute extent — without them, a
	// get-else on a single bound entity full-scans the attribute.
	// See docs/bugs/BUG_GETELSE_SCAN_REWRITE_NOT_NARROWED_BY_BOUND_CHILD.md.
	joinKeyRel       Relation
	joinKeysComputed bool

	// cacheableInput memoizes outerJoinKeys ⋈ envRel: the single evaluation
	// of a cacheable branch is narrowed by the outer join keys AND the
	// environment bindings the branches consume.
	cacheableInput         Relation
	cacheableInputComputed bool

	// Correlated union state: track which branch we're iterating within the current outer tuple
	unionBranchIdx  int      // next branch to try for current outer tuple
	unionOuterTuple Tuple    // current outer tuple being processed
	unionInputRel   Relation // inputRel for current outer tuple
}

// makeBranchInput builds one iterator's branch-input construction: per outer
// tuple, the join of the branch-visible outer projection with the environment
// relation, plus the visible tuple itself (for shared-symbol filtering of
// relation data — env is enforced at evaluation, not there). The join's
// loop-invariant sides are captured once: the projection indices, the output
// schema, and envRel — whose single tuple is read by reference (Get(0)) per
// call, never copied. passThrough marks a projection covering the whole outer
// tuple (indices are built in outer order, so equal lengths mean identity).
// A nil returned relation means no bindings at all — branches evaluate with
// no input.
func makeBranchInput(
	visibleSyms []query.Symbol,
	visibleIdx []int,
	passThrough bool,
	envRel Relation,
	opts ExecutorOptions,
) func(Tuple) (Relation, Tuple) {
	inputSyms := visibleSyms
	if envRel != nil {
		inputSyms = append(append([]query.Symbol(nil), visibleSyms...), envRel.Symbols()...)
	}
	return func(outerTuple Tuple) (Relation, Tuple) {
		visible := outerTuple
		if !passThrough {
			visible = make(Tuple, len(visibleIdx))
			for i, idx := range visibleIdx {
				visible[i] = outerTuple[idx]
			}
		}
		if len(inputSyms) == 0 {
			return nil, visible
		}
		input := visible
		if envRel != nil {
			envTuple := envRel.Get(0)
			input = make(Tuple, 0, len(visible)+len(envTuple))
			input = append(input, visible...)
			input = append(input, envTuple...)
		}
		return NewMaterializedRelationWithOptions(
			inputSyms,
			[]Tuple{input},
			opts,
		), visible
	}
}

// branchFreeVariables returns the union of every branch's free variables
// (query.FreeVariables), deduplicated in first-appearance order — the
// canonical answer to "which symbols do these branches consume or produce
// at their interface."
func branchFreeVariables(branches [][]query.Clause) []query.Symbol {
	var free []query.Symbol
	for _, branch := range branches {
		for _, sym := range query.FreeVariables(branch) {
			if !query.ContainsSymbol(free, sym) {
				free = append(free, sym)
			}
		}
	}
	return free
}

// cachedBranch is an uncorrelated branch result grouped for per-outer-tuple
// probing: a groupedTupleIndex keyed probe-side by the outer tuple's join
// positions and stored-side by the branch tuple's, carrying the branch's
// result symbols for output projection.
type cachedBranch struct {
	*groupedTupleIndex
	branchSyms []query.Symbol
}

// buildBranchFromEACache builds a cached branch for a DataPattern-only or-join
// branch using LookupAttribute (EA cache) instead of a full storage scan.
// Iterates the materialized outerRel to collect entity IDs, looks up each
// entity's attribute value from the EA cache, and builds a TupleKeyMap index.
// Returns nil if prerequisites aren't met (wrong branch shape, no LookupAttribute).
func (it *OrFallbackIterator) buildBranchFromEACache(branch []query.Clause) *cachedBranch {
	if len(branch) != 1 || it.outerRel == nil {
		return nil
	}
	// outerRel must be re-iterable (materialized). StreamingRelation panics
	// on second Iterator() call.
	if _, isStreaming := it.outerRel.(*StreamingRelation); isStreaming {
		return nil
	}
	dp, ok := branch[0].(*query.DataPattern)
	if !ok {
		return nil
	}

	// Need E as variable (join key) and A as constant keyword. The E
	// variable must be branch-visible: outside an explicit header it is a
	// branch local and must not bind to an outer symbol of the same name.
	eVar, eIsVar := dp.GetE().(query.Variable)
	if !eIsVar {
		return nil
	}
	if !query.ContainsSymbol(it.branchVisibleSyms, eVar.Name) {
		return nil
	}
	var aKw datalog.Keyword
	if aConst, ok := dp.GetA().(query.Constant); ok {
		aKw, _ = aConst.Value.(datalog.Keyword)
	}
	if aKw == nil {
		return nil
	}

	// Need V as variable
	vVar, vIsVar := dp.GetV().(query.Variable)
	if !vIsVar {
		return nil
	}

	// Need matcher to support LookupAttribute
	lookupMatcher, ok := it.executor.matcher.(EntityLookupMatcher)
	if !ok {
		return nil
	}

	// Find E position in outer relation symbols
	eIdx := query.SymbolIndex(it.outerSyms, eVar.Name)
	if eIdx < 0 {
		return nil
	}

	// Build branch tuples from EA cache lookups.
	// Iterate the materialized outer relation (safe to create second iterator).
	branchSyms := []query.Symbol{eVar.Name, vVar.Name}
	bIdx := []int{0} // E is at position 0 in branch tuples

	var collected []Tuple
	if n := it.outerRel.Size(); n >= 0 {
		collected = make([]Tuple, 0, n)
	}
	// Outer tuples are distinct but the value at their entity position can
	// repeat; one lookup and one tuple per distinct entity keeps the collected
	// tuples a set.
	seenEntities := make(map[datalog.Identity]struct{})
	outerIt := it.outerRel.Iterator()
	for outerIt.Next() {
		t := outerIt.Tuple()
		if eIdx >= len(t) {
			continue
		}
		entity, ok := t[eIdx].(datalog.Identity)
		if !ok {
			continue
		}
		if _, dup := seenEntities[entity]; dup {
			continue
		}
		seenEntities[entity] = struct{}{}
		value, found, err := lookupMatcher.LookupAttribute(entity, aKw)
		if err != nil {
			it.err = err
			_ = outerIt.Close()
			return nil
		}
		if !found {
			continue
		}
		collected = append(collected, Tuple{entity, value})
	}
	outerErr := outerIt.Error()
	closeErr := outerIt.Close()
	if outerErr != nil {
		it.err = outerErr
		return nil
	}
	if closeErr != nil {
		it.err = closeErr
		return nil
	}

	// An environment-bound V narrows the branch: only tuples carrying the
	// environment's value may match — the same narrowing the scan path
	// receives via cacheableBranchInput, expressed as a SemiJoin against
	// the environment projected onto the V symbol.
	if it.envRel != nil && query.ContainsSymbol(it.envRel.Symbols(), vVar.Name) {
		vEnv, err := it.envRel.Project([]query.Symbol{vVar.Name})
		if err != nil {
			it.err = err
			return nil
		}
		branchRel := newMaterializedRelationFromSet(
			branchSyms,
			collected,
			it.options,
			deduplicatedProperties(branchSyms),
		)
		narrowed := SemiJoin(branchRel, vEnv, []query.Symbol{vVar.Name})
		var filtered []Tuple
		nit := narrowed.Iterator()
		for nit.Next() {
			filtered = append(filtered, nit.Tuple())
		}
		narrowErr := nit.Error()
		if cErr := nit.Close(); narrowErr == nil {
			narrowErr = cErr
		}
		if narrowErr != nil {
			it.err = narrowErr
			return nil
		}
		collected = filtered
	}

	return &cachedBranch{
		groupedTupleIndex: groupTuples(collected, []int{eIdx}, bIdx),
		branchSyms:        branchSyms,
	}
}

// isCacheableBranch returns true if the branch can be evaluated once with
// join variables free, cached, and probed per outer tuple.
//
// Two cases:
//  1. SubqueryPattern with only $ inputs (uncorrelated subquery)
//  2. In an or-join: DataPattern-only branches where the join variables
//     are the only connection to the outer context. Evaluated with join
//     vars free, the DataPattern returns ALL matches; the cache indexes
//     by join vars for O(1) per-tuple probe.
func isCacheableBranch(branch []query.Clause, isOrJoin bool) bool {
	for _, c := range branch {
		switch cl := c.(type) {
		case *query.SubqueryPattern:
			// Uncorrelated subquery: inputs are only $ (database source)
			for _, input := range cl.Inputs {
				switch inp := input.(type) {
				case query.Constant:
					if sym, ok := inp.Value.(query.Symbol); ok && sym.IsSource() {
						continue
					}
					return false
				case query.Variable:
					if inp.Name.IsSource() {
						continue
					}
					return false
				default:
					return false
				}
			}
			return true
		case *query.DataPattern:
			// DataPattern in or-join: can be evaluated with join vars free
			continue
		default:
			// Expressions, predicates, etc. — not cacheable
			return false
		}
	}
	// All clauses are DataPatterns — cacheable in or-join context
	return isOrJoin && len(branch) > 0
}

// isDataPatternOnlyBranch reports whether a branch consists solely of
// DataPatterns (and is non-empty). In an or-join, such a branch is the one
// outerJoinKeys narrows to the outer join keys: it can be evaluated once with
// the join variables bound to the outer relation's values. Subquery branches,
// or branches containing expressions/predicates, are not narrowable this way.
func isDataPatternOnlyBranch(branch []query.Clause) bool {
	if len(branch) == 0 {
		return false
	}
	for _, c := range branch {
		if _, ok := c.(*query.DataPattern); !ok {
			return false
		}
	}
	return true
}

// hasNarrowableBranch reports whether any branch is a DataPattern-only branch —
// the branches outerJoinKeys narrows. The OrFallbackRelation iterator uses this
// to decide whether the outer relation must be materialized for re-iteration
// before iteration begins.
func hasNarrowableBranch(branches [][]query.Clause) bool {
	for _, branch := range branches {
		if isDataPatternOnlyBranch(branch) {
			return true
		}
	}
	return false
}

// buildCachedBranch builds a hash index over a branch result keyed on
// the specified join symbols. Only join variables are used as keys —
// not all shared symbols — because branch results may contain symbols
// with the same name but different values than the outer relation.
func buildCachedBranch(
	branchResult Relation,
	outerSyms []query.Symbol,
	joinSyms []query.Symbol,
) (*cachedBranch, error) {
	branchSyms := branchResult.Symbols()

	// Use only join symbols as cache keys
	keySyms := joinSyms
	if len(keySyms) == 0 {
		// Fallback: use all shared symbols (non-or-join path)
		keySyms = nil
		for _, osym := range outerSyms {
			if query.ContainsSymbol(branchSyms, osym) {
				keySyms = append(keySyms, osym)
			}
		}
	}

	var bIdx, oIdx []int
	for _, ksym := range keySyms {
		oi := query.SymbolIndex(outerSyms, ksym)
		if oi < 0 {
			continue
		}
		if bi := query.SymbolIndex(branchSyms, ksym); bi >= 0 {
			oIdx = append(oIdx, oi)
			bIdx = append(bIdx, bi)
		}
	}
	if len(bIdx) == 0 {
		return nil, nil
	}

	// Copy only when the branch iterator reuses its tuple workspace; the
	// cache retains these tuples for the query's lifetime.
	needsCopy := branchResult.RequiresCopy()
	var collected []Tuple
	if n := branchResult.Size(); n >= 0 {
		collected = make([]Tuple, 0, n)
	}
	iter := branchResult.Iterator()
	for iter.Next() {
		t := iter.Tuple()
		if needsCopy {
			cp := make(Tuple, len(t))
			copy(cp, t)
			t = cp
		}
		collected = append(collected, t)
	}
	iterErr := iter.Error()
	if closeErr := iter.Close(); iterErr == nil {
		iterErr = closeErr
	}
	if iterErr != nil {
		return nil, iterErr
	}

	return &cachedBranch{
		groupedTupleIndex: groupTuples(collected, oIdx, bIdx),
		branchSyms:        branchSyms,
	}, nil
}

func (it *OrFallbackIterator) Next() bool {
	for {
		if it.done {
			return false
		}

		var advanced bool
		if it.shortCircuit {
			advanced = it.nextShortCircuit()
		} else {
			advanced = it.nextCorrelatedUnion()
		}
		if !advanced {
			return false
		}
		if it.seen == nil {
			return true
		}
		if !it.seen.PutIfAbsent(NewTupleKeyFull(it.currentTuple), struct{}{}) {
			return true
		}
	}
}

// nextCorrelatedUnion streams through ALL branches per outer tuple.
// Uses the same projectedIterator as the short-circuit path, but instead of
// stopping after the first successful branch, advances to the next branch
// when the current one is exhausted.
func (it *OrFallbackIterator) nextCorrelatedUnion() bool {
	// Initialize: advance to first outer tuple before trying any branches
	if it.unionInputRel == nil && it.unionBranchIdx == 0 {
		if !it.outerIter.Next() {
			if e := it.outerIter.Error(); it.err == nil {
				it.err = e
			}
			it.done = true
			return false
		}
		it.unionOuterTuple = it.outerIter.Tuple()
		it.unionInputRel, _ = it.branchInput(it.unionOuterTuple)
	}

	for {
		// If we have a current branch iterator, try to get next tuple from it
		if it.currentBranchIter != nil {
			if it.currentBranchIter.Next() {
				it.currentTuple = it.currentBranchIter.Tuple()
				return true
			}
			branchErr := it.currentBranchIter.Error()
			closeErr := it.currentBranchIter.Close()
			it.currentBranchIter = nil
			if branchErr != nil {
				it.err = branchErr
				it.done = true
				return false
			}
			if closeErr != nil {
				it.err = closeErr
				it.done = true
				return false
			}
		}

		// Try remaining branches for the current outer tuple
		for it.unionBranchIdx < len(it.branches) {
			branch := it.branches[it.unionBranchIdx]
			it.unionBranchIdx++

			branchResult, err := it.executor.executeInnerClauses(it.ctx, branch, it.unionInputRel)
			if err != nil {
				it.err = err
				it.done = true
				return false
			}
			if branchResult == nil {
				continue
			}

			branchIter := branchResult.Iterator()
			if branchIter.Next() {
				branchSyms := branchResult.Symbols()
				firstTuple := branchIter.Tuple()

				plan := it.projectionFor(it.unionBranchIdx-1, branchSyms)
				if !plan.identity {
					it.currentTuple = plan.project(firstTuple, it.unionOuterTuple)
				} else if branchResult.RequiresCopy() {
					it.currentTuple = copyTuple(firstTuple)
				} else {
					it.currentTuple = firstTuple
				}
				it.currentBranchIter = &projectedIterator{
					inner:          branchIter,
					branchRelation: branchResult,
					plan:           plan,
					outerTuple:     it.unionOuterTuple,
				}
				return true
			}
			branchErr := branchIter.Error()
			closeErr := branchIter.Close()
			if branchErr != nil {
				it.err = branchErr
				it.done = true
				return false
			}
			if closeErr != nil {
				it.err = closeErr
				it.done = true
				return false
			}
		}

		// All branches exhausted for current outer tuple — advance to next
		if !it.outerIter.Next() {
			if e := it.outerIter.Error(); it.err == nil {
				it.err = e
			}
			it.done = true
			return false
		}

		it.unionOuterTuple = it.outerIter.Tuple()
		it.unionBranchIdx = 0
		it.unionInputRel, _ = it.branchInput(it.unionOuterTuple)
	}
}

// outerJoinKeys returns the outer relation projected onto the or-join's join
// symbols, deduplicated and memoized. Passing these as bindings to a cacheable
// DataPattern-only or-join branch narrows its single evaluation to the entities
// actually present in the outer relation — so a get-else on one bound entity is
// a point lookup instead of a full scan of the attribute extent.
//
// Returns nil (the caller then falls back to the existing unbound scan) when
// narrowing can't be applied safely: no outer relation, no join symbols, a
// non-re-iterable streaming outer (a second Iterator() call panics), a join
// symbol absent from the outer relation, or no extractable keys.
//
// The result has identical reachable content to the unbounded scan's cache:
// the per-tuple probe only ever looks up outer entities' join keys, so the
// unbounded scan's extra tuples are dead entries. Deduplicating the keys keeps
// the matcher from emitting duplicate (entity, value) tuples for a repeated
// outer entity, matching the unbounded scan's cardinality.
func (it *OrFallbackIterator) outerJoinKeys() Relation {
	if it.joinKeysComputed {
		return it.joinKeyRel
	}
	it.joinKeysComputed = true

	if it.outerRel == nil || len(it.joinSyms) == 0 {
		return nil
	}
	// StreamingRelation panics on a second Iterator() call; the per-tuple loop
	// already holds one iterator over it.outerRel, so we can only re-iterate a
	// materialized outer. (buildBranchFromEACache guards the same way.)
	if _, isStreaming := it.outerRel.(*StreamingRelation); isStreaming {
		return nil
	}

	// Map each join symbol to its tuple position in the outer relation.
	pos := query.SymbolIndexTable(it.outerSyms, it.joinSyms)
	for _, p := range pos {
		if p < 0 {
			return nil // join symbol not in the outer relation — can't narrow
		}
	}

	// Collect distinct join-key tuples. Deduplication probes the outer tuple
	// positions directly; the key tuple materializes only on first sight.
	seen := NewTupleKeyMap()
	var keys []Tuple
	oit := it.outerRel.Iterator()
	for oit.Next() {
		t := oit.Tuple()
		ok := true
		for _, p := range pos {
			if p >= len(t) {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		if existed := seen.PutIfAbsentPositions(t, pos, true); existed {
			continue
		}
		key := make(Tuple, len(pos))
		for i, p := range pos {
			key[i] = t[p]
		}
		keys = append(keys, key)
	}
	outerErr := oit.Error()
	closeErr := oit.Close()
	if outerErr != nil {
		it.err = outerErr
		return nil
	}
	if closeErr != nil {
		it.err = closeErr
		return nil
	}

	if len(keys) == 0 {
		return nil
	}
	it.joinKeyRel = newMaterializedRelationFromSet(
		it.joinSyms,
		keys,
		it.options,
		deduplicatedProperties(it.joinSyms),
	)
	return it.joinKeyRel
}

// cacheableBranchInput returns the input relation for a cacheable
// DataPattern-only branch's single evaluation: the outer join keys joined
// with the branches' environment bindings, so the one scan is narrowed by
// both. The sides never share symbols (a join symbol present in the outer is
// branch-visible, and envSyms excludes branch-visible symbols; a join symbol
// absent from the outer nils outerJoinKeys), so the join is an N×1
// decoration that preserves the key relation's set property — environment
// values are constant for the query. With no join keys the environment alone
// narrows the scan; nil means no narrowing input at all (the caller's
// unbound-scan fallback).
func (it *OrFallbackIterator) cacheableBranchInput() Relation {
	if it.cacheableInputComputed {
		return it.cacheableInput
	}
	it.cacheableInputComputed = true

	keys := it.outerJoinKeys()
	switch {
	case it.envRel == nil:
		it.cacheableInput = keys
	case keys == nil:
		it.cacheableInput = it.envRel
	default:
		it.cacheableInput = keys.Join(it.envRel)
	}
	return it.cacheableInput
}

// nextShortCircuit tries branches in order until one returns results (fallback semantics).
func (it *OrFallbackIterator) nextShortCircuit() bool {
	for {
		// Drain an in-progress cached-branch emit: the remaining probe matches
		// come straight off the branch cache's backing.
		if it.cachedMatches != nil {
			if it.cachedMatchPos < len(it.cachedMatches) {
				matched := it.cachedMatches[it.cachedMatchPos]
				it.cachedMatchPos++
				if it.cachedPlan.identity {
					it.currentTuple = matched
				} else {
					it.currentTuple = it.cachedPlan.project(matched, it.cachedOuter)
				}
				return true
			}
			it.cachedMatches = nil
			it.cachedOuter = nil
		}

		// If we have a current branch iterator, try to get next tuple from it
		if it.currentBranchIter != nil {
			if it.currentBranchIter.Next() {
				// projectedIterator.Tuple() handles copying when needed
				it.currentTuple = it.currentBranchIter.Tuple()
				return true
			}
			branchErr := it.currentBranchIter.Error()
			closeErr := it.currentBranchIter.Close()
			it.currentBranchIter = nil
			if branchErr != nil {
				it.err = branchErr
				it.done = true
				return false
			}
			if closeErr != nil {
				it.err = closeErr
				it.done = true
				return false
			}
		}

		// Need to advance to next outer tuple
		if !it.outerIter.Next() {
			if e := it.outerIter.Error(); it.err == nil {
				it.err = e
			}
			// Emit annotation when outer iterator exhausted
			if it.options.Handler != nil {
				it.options.Handler(annotations.Event{
					Name:  annotations.OrFallbackOuterExhausted,
					Start: time.Now(),
					Data:  map[string]interface{}{},
				})
			}
			it.done = true
			return false
		}

		outerTuple := it.outerIter.Tuple()

		// Emit annotation for each outer tuple being processed
		if it.options.Handler != nil {
			it.options.Handler(annotations.Event{
				Name:  annotations.OrFallbackOuterTuple,
				Start: time.Now(),
				Data: map[string]interface{}{
					"tuple": fmt.Sprintf("%v", outerTuple),
				},
			})
		}

		// The single-tuple branch input is only consumed by non-cached
		// branches; once every branch is cached it is never built.
		var inputRel Relation
		var visibleTuple Tuple
		inputReady := false

		// Try each branch until one returns results
		for branchIdx, branch := range it.branches {
			var branchResult Relation

			// Check cache for uncorrelated branches (O(1) probe): matching
			// tuples emit directly from the cache's backing, and an empty probe
			// falls through to the next branch.
			if cb, cached := it.branchCache[branchIdx]; cached {
				if it.emitCachedMatches(branchIdx, cb, cb.probe(outerTuple), outerTuple) {
					return true
				}
				continue
			} else {
				// Execute the branch once, then cache + probe per outer tuple.
				// Cacheable branches are not given the single outer tuple as
				// inputRel (that would re-evaluate per tuple); instead:
				//   - uncorrelated SubqueryPatterns run with no input;
				//   - DataPattern-only or-join branches run narrowed to the
				//     outer relation's join keys (it.outerJoinKeys()), so the
				//     scan is bounded by the outer entities rather than the whole
				//     attribute extent. The branch cache then indexes the
				//     already-narrowed result for O(1) per-tuple probes. Without
				//     this, get-else on a single bound entity full-scans the
				//     attribute. See
				//     docs/bugs/BUG_GETELSE_SCAN_REWRITE_NOT_NARROWED_BY_BOUND_CHILD.md.
				//
				// Fast path: for DataPattern branches in or-join, try building
				// the branch cache from EA cache (LookupAttribute) instead of
				// a storage scan. Falls back to the scan path if the EA cache
				// isn't warm.
				isOrJoin := len(it.joinSyms) > 0
				isCacheable := isCacheableBranch(branch, isOrJoin)
				// A DataPattern-only branch in an or-join benefits from
				// join-key + environment narrowing; uncorrelated subqueries
				// (also cacheable) must still run with no input — the
				// subquery is its own scope with its own environment.
				narrowable := isOrJoin && isDataPatternOnlyBranch(branch)
				var execInput Relation
				if isCacheable {
					if narrowable {
						execInput = it.cacheableBranchInput()
					}
				} else {
					if !inputReady {
						inputRel, visibleTuple = it.branchInput(outerTuple)
						inputReady = true
					}
					execInput = inputRel
				}

				// Report the narrowing decision so the choice (and any silent
				// fall-back to a full scan) is observable rather than inferred
				// from index selection.
				// See docs/bugs/BUG_GETELSE_SCAN_REWRITE_NOT_NARROWED_BY_BOUND_CHILD.md.
				if narrowable {
					if it.options.Handler != nil {
						data := map[string]interface{}{
							"branch":   branchIdx,
							"narrowed": execInput != nil,
						}
						if execInput != nil {
							data["join_keys"] = execInput.Size()
						} else {
							data["reason"] = "outer not re-iterable for join-key extraction"
						}
						it.options.Handler(annotations.Event{
							Name:  annotations.OrFallbackBranchNarrowed,
							Start: time.Now(),
							Data:  data,
						})
					}
				}

				// Try EA-cache-based branch build for DataPattern-only or-join branches.
				// Only use when the EA cache is warm (prefetch has run for these entities).
				// The prefetch runs in DefaultQueryExecutor.Execute for entities
				// in the outer relation. For inner subquery or-joins, the prefetch
				// hasn't run yet, so fall back to the storage scan path.
				if isCacheable && isOrJoin && !isCacheableBranch(branch, false) && it.prefetched {
					cb := it.buildBranchFromEACache(branch)
					if it.err != nil {
						it.done = true
						return false
					}
					if cb != nil {
						if it.branchCache == nil {
							it.branchCache = make(map[int]*cachedBranch)
						}
						it.branchCache[branchIdx] = cb
						if it.emitCachedMatches(branchIdx, cb, cb.probe(outerTuple), outerTuple) {
							return true
						}
						continue
					}
				}

				var err error
				branchResult, err = it.executor.executeInnerClauses(it.ctx, branch, execInput)
				if err != nil {
					it.err = err
					it.done = true
					return false
				}

				if branchResult != nil {
					// First evaluation: cache the once-evaluated branch.
					// Cacheable branches (execInput is the narrowed join-key
					// relation for DataPattern or-join branches, or nil for
					// uncorrelated subqueries) are indexed and probed;
					// correlated branches with a real per-tuple inputRel are
					// filtered instead. execInput==nil also covers a unit
					// outer relation (no join keys) whose pass-through
					// behavior must be preserved.
					if isCacheable || execInput == nil {
						if it.options.Handler != nil {
							it.options.Handler(annotations.Event{
								Name: annotations.OrFallbackCacheBuild,
								Data: map[string]interface{}{
									"branch":      branchIdx,
									"branch_syms": branchResult.Symbols(),
									"outer_syms":  it.outerSyms,
									"branch_size": branchResult.Size(),
								},
							})
						}
						cb, err := buildCachedBranch(branchResult, it.outerSyms, it.joinSyms)
						if err != nil {
							it.err = err
							it.done = true
							return false
						}
						if cb != nil {
							if it.branchCache == nil {
								it.branchCache = make(map[int]*cachedBranch)
							}
							it.branchCache[branchIdx] = cb
							// Emit from the freshly-built cache instead of scanning
							if it.emitCachedMatches(branchIdx, cb, cb.probe(outerTuple), outerTuple) {
								return true
							}
							continue
						}
						// No shared symbols — pass through unfiltered
					} else {
						// Correlated branch — filter per tuple, matching only
						// branch-visible symbols: a branch local sharing an
						// outer name must not be filtered against it.
						branchResult = filterBranchToOuterTuple(branchResult, visibleTuple, it.branchVisibleSyms)
					}
				}
			}

			if branchResult != nil {
				branchIter := branchResult.Iterator()
				if branchIter.Next() {
					if it.options.Handler != nil {
						it.options.Handler(annotations.Event{
							Name:  annotations.OrFallbackBranchSuccess,
							Start: time.Now(),
							Data: map[string]interface{}{
								"branch_index": branchIdx,
								"branch_syms":  branchResult.Symbols(),
								"first_tuple":  fmt.Sprintf("%v", branchIter.Tuple()),
							},
						})
					}

					branchSyms := branchResult.Symbols()
					firstTuple := branchIter.Tuple()

					plan := it.projectionFor(branchIdx, branchSyms)
					if !plan.identity {
						it.currentTuple = plan.project(firstTuple, outerTuple)
					} else if branchResult.RequiresCopy() {
						it.currentTuple = copyTuple(firstTuple)
					} else {
						it.currentTuple = firstTuple
					}
					it.currentBranchIter = &projectedIterator{
						inner:          branchIter,
						branchRelation: branchResult,
						plan:           plan,
						outerTuple:     outerTuple,
					}
					return true
				}
				branchErr := branchIter.Error()
				closeErr := branchIter.Close()
				if branchErr != nil {
					it.err = branchErr
					it.done = true
					return false
				}
				if closeErr != nil {
					it.err = closeErr
					it.done = true
					return false
				}
			}
		}
		// No branch matched for this outer tuple - continue to next outer tuple
	}
}

func (it *OrFallbackIterator) Tuple() Tuple {
	return it.currentTuple
}

func (it *OrFallbackIterator) Close() error {
	var closeErr error
	if it.currentBranchIter != nil {
		closeErr = it.currentBranchIter.Close()
		it.currentBranchIter = nil
	}
	if it.outerIter != nil {
		if err := it.outerIter.Close(); closeErr == nil {
			closeErr = err
		}
	}
	it.done = true
	return closeErr
}

// Error returns any error encountered during iteration
func (it *OrFallbackIterator) Error() error {
	return it.err
}

// symbolsMatch checks if two symbol slices have the same symbols in the same order
func symbolsMatch(a, b []query.Symbol) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// projectionPlan precomputes, per output position, where a branch tuple's
// value comes from: a branch-tuple position, else an outer-tuple position
// (a branch such as a ground default doesn't produce every output symbol —
// the outer tuple's values fill the gaps), else nothing. The mapping depends
// only on the branch, output, and outer symbol lists — per-branch constants —
// so one plan serves every tuple the branch emits. identity means the branch
// symbols already match the output symbols exactly and tuples pass through
// unprojected; the emit sites own the copy-vs-alias decision for that case.
type projectionPlan struct {
	identity  bool
	branchPos []int // per output position; -1 = not in the branch tuple
	outerPos  []int // per output position; -1 = not in the outer tuple
}

func newProjectionPlan(srcSyms, dstSyms, outerSyms []query.Symbol) projectionPlan {
	if symbolsMatch(srcSyms, dstSyms) {
		return projectionPlan{identity: true}
	}
	branchPos := make([]int, len(dstSyms))
	outerPos := make([]int, len(dstSyms))
	for i, sym := range dstSyms {
		branchPos[i] = query.SymbolIndex(srcSyms, sym)
		outerPos[i] = query.SymbolIndex(outerSyms, sym)
	}
	return projectionPlan{branchPos: branchPos, outerPos: outerPos}
}

// project maps a branch tuple to the output symbols, filling positions the
// branch doesn't produce from the outer tuple. Not called on identity plans.
func (p projectionPlan) project(tuple, outerTuple Tuple) Tuple {
	result := make(Tuple, len(p.branchPos))
	for i, bp := range p.branchPos {
		if bp >= 0 {
			result[i] = tuple[bp]
			continue
		}
		if op := p.outerPos[i]; op >= 0 && op < len(outerTuple) {
			result[i] = outerTuple[op]
		}
	}
	return result
}

// branchPlan is a memoized projectionPlan together with the branch symbols
// it was computed from — the symbols are the memo's validity key.
type branchPlan struct {
	srcSyms []query.Symbol
	plan    projectionPlan
}

// projectionFor returns the memoized output projection for a branch,
// computing and storing it on first sight of the branch's result symbols.
func (it *OrFallbackIterator) projectionFor(branchIdx int, branchSyms []query.Symbol) projectionPlan {
	if bp, ok := it.branchPlans[branchIdx]; ok && symbolsMatch(bp.srcSyms, branchSyms) {
		return bp.plan
	}
	plan := newProjectionPlan(branchSyms, it.outputSyms, it.outerSyms)
	if it.branchPlans == nil {
		it.branchPlans = make(map[int]branchPlan)
	}
	it.branchPlans[branchIdx] = branchPlan{srcSyms: branchSyms, plan: plan}
	return plan
}

// emitCachedMatches begins emitting a probe result directly from the branch
// cache's backing, setting the first output tuple as current. Returns false
// when there are no matches (the caller falls through to the next branch).
// The matches already form a set — a contiguous span of a deduplicated branch
// relation — so nothing re-deduplicates here, and identity-shaped tuples alias
// the cache's backing exactly as the wrapper relation's tuples did.
func (it *OrFallbackIterator) emitCachedMatches(branchIdx int, cb *cachedBranch, matches []Tuple, outerTuple Tuple) bool {
	if len(matches) == 0 {
		return false
	}
	if it.options.Handler != nil {
		it.options.Handler(annotations.Event{
			Name:  annotations.OrFallbackBranchSuccess,
			Start: time.Now(),
			Data: map[string]interface{}{
				"branch_index": branchIdx,
				"branch_syms":  cb.branchSyms,
				"first_tuple":  fmt.Sprintf("%v", matches[0]),
			},
		})
	}
	plan := it.projectionFor(branchIdx, cb.branchSyms)
	if plan.identity {
		it.currentTuple = matches[0]
	} else {
		it.currentTuple = plan.project(matches[0], outerTuple)
	}
	it.cachedMatches = matches
	it.cachedMatchPos = 1
	it.cachedPlan = plan
	it.cachedOuter = outerTuple
	return true
}

// projectedIterator wraps an iterator and projects tuples to the output
// symbols through a precomputed per-branch plan.
type projectedIterator struct {
	inner          Iterator
	branchRelation Relation // For RequiresCopy check
	plan           projectionPlan
	outerTuple     Tuple // Fallback values for symbols the branch doesn't produce
}

func (it *projectedIterator) Next() bool {
	return it.inner.Next()
}

func (it *projectedIterator) Tuple() Tuple {
	tuple := it.inner.Tuple()

	if !it.plan.identity {
		return it.plan.project(tuple, it.outerTuple)
	}

	// No projection - copy if source is unsafe
	if it.branchRelation != nil && it.branchRelation.RequiresCopy() {
		return copyTuple(tuple)
	}
	return tuple
}

// filterBranchToOuterTuple filters a branch result to the tuples matching the
// current outer tuple on shared symbols. Needed for uncorrelated subqueries
// that return results for ALL outer tuples — without filtering, every outer
// tuple would get every result tuple.
func filterBranchToOuterTuple(branchResult Relation, outerTuple Tuple, outerSyms []query.Symbol) Relation {
	branchSyms := branchResult.Symbols()

	// Find shared symbol positions
	type symPair struct{ outerIdx, branchIdx int }
	var shared []symPair
	for oi, osym := range outerSyms {
		if bi := query.SymbolIndex(branchSyms, osym); bi >= 0 {
			shared = append(shared, symPair{oi, bi})
		}
	}
	if len(shared) == 0 {
		return branchResult
	}

	// Filter to matching tuples
	var tuples []Tuple
	iter := branchResult.Iterator()
	for iter.Next() {
		t := iter.Tuple()
		match := true
		for _, sp := range shared {
			if sp.outerIdx >= len(outerTuple) || sp.branchIdx >= len(t) {
				match = false
				break
			}
			if !datalog.ValuesEqual(outerTuple[sp.outerIdx], t[sp.branchIdx]) {
				match = false
				break
			}
		}
		if match {
			cp := make(Tuple, len(t))
			copy(cp, t)
			tuples = append(tuples, cp)
		}
	}
	iterErr := iter.Error()
	closeErr := iter.Close()
	result := NewMaterializedRelation(branchSyms, tuples)
	if iterErr != nil {
		result.err = iterErr
	} else if closeErr != nil {
		result.err = closeErr
	}
	return result
}

func (it *projectedIterator) Close() error {
	return it.inner.Close()
}

func (it *projectedIterator) Error() error { return it.inner.Error() }
