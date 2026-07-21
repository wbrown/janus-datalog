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
	ctx            Context
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

	// headerSyms is the declared header of an explicit-join form (or-join
	// JoinVars; or-default-join RequiredVars ∪ OutputVars). Branch
	// evaluation may see only the outer tuple's header bindings: a branch
	// variable outside the header is a local, and a name collision with an
	// outer symbol must not bind (alpha-equivalence). nil means an
	// inference form (plain or / or-default), which unifies on every
	// shared free variable by language rule and sees the full outer tuple.
	headerSyms []query.Symbol
}

// NewOrFallbackRelation creates a streaming OR relation.
// When shortCircuit=true, uses fallback semantics (first match wins).
// When shortCircuit=false, uses correlated union (all branches contribute).
func NewOrFallbackRelation(
	executor *DefaultQueryExecutor,
	ctx Context,
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
	if collector := ctx.Collector(); collector != nil {
		collector.Add(annotations.Event{
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
			if collector := r.ctx.Collector(); collector != nil {
				collector.Add(annotations.Event{
					Name: "or-fallback/outer.materialized",
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

	// Branch-visible outer bindings. Inference forms (headerSyms nil) see
	// the full outer tuple; explicit-header forms see only the header's
	// projection — branch variables outside the header are locals.
	branchVisibleSyms := outer.Symbols()
	var branchVisibleIdx []int
	if r.headerSyms != nil {
		branchVisibleSyms = nil
		branchVisibleIdx = make([]int, 0, len(r.headerSyms))
		for i, sym := range outer.Symbols() {
			if query.ContainsSymbol(r.headerSyms, sym) {
				branchVisibleSyms = append(branchVisibleSyms, sym)
				branchVisibleIdx = append(branchVisibleIdx, i)
			}
		}
	}

	// Emit annotation for iterator creation
	// Note: Don't call outer.Size() - it may block for streaming relations
	if collector := r.ctx.Collector(); collector != nil {
		collector.Add(annotations.Event{
			Name:  "or-fallback/iterator.created",
			Start: time.Now(),
			Data: map[string]interface{}{
				"iterator_count": r.iteratorCount,
				"outer_symbols":  fmt.Sprintf("%v", outer.Symbols()),
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
		branchVisibleIdx:  branchVisibleIdx,
		prefetched:        r.prefetched,
		seen:              seen,
		err:               setupErr,
		done:              setupErr != nil,
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
	ctx          Context
	branches     [][]query.Clause
	shortCircuit bool // true = fallback, false = correlated union
	outerIter    Iterator
	outerRel     Relation // Materialized outer relation (for EA cache branch building)
	outerSyms    []query.Symbol
	options      ExecutorOptions
	joinSyms     []query.Symbol // From or-join: used as cache key (not all shared symbols)

	// Branch-visible outer bindings: the outer symbols a branch may see,
	// with their positions in the outer tuple. Equal to outerSyms (idx nil)
	// for inference forms; restricted to the declared header for
	// explicit-join forms — branch variables outside the header are locals
	// and must not capture outer bindings of the same name.
	branchVisibleSyms []query.Symbol
	branchVisibleIdx  []int

	// Current state
	currentBranchIter Iterator
	currentTuple      Tuple
	outputSyms        []query.Symbol
	seen              *TupleKeyMap
	done              bool
	err               error

	// Cached-branch emit state: a probe result being emitted directly from
	// the branch cache's shared backing — no wrapper relation, no
	// re-deduplication (the rows are a contiguous span of a relation that is
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

	// Correlated union state: track which branch we're iterating within the current outer tuple
	unionBranchIdx  int      // next branch to try for current outer tuple
	unionOuterTuple Tuple    // current outer tuple being processed
	unionInputRel   Relation // inputRel for current outer tuple
}

// branchInput returns the single-tuple relation of outer bindings the
// branches may see for one outer tuple, and the visible tuple itself (for
// shared-symbol filtering). Explicit-header forms project the outer tuple to
// the header; inference forms pass it whole. A nil relation means no visible
// bindings (unit outer, or an empty header projection) — branches evaluate
// with no input.
func (it *OrFallbackIterator) branchInput(outerTuple Tuple) (Relation, Tuple) {
	visible := outerTuple
	if it.branchVisibleIdx != nil {
		visible = make(Tuple, len(it.branchVisibleIdx))
		for i, idx := range it.branchVisibleIdx {
			visible[i] = outerTuple[idx]
		}
	}
	if len(it.branchVisibleSyms) == 0 {
		return nil, visible
	}
	return NewMaterializedRelationWithOptions(
		it.branchVisibleSyms,
		[]Tuple{visible},
		it.options,
	), visible
}

// cachedBranch holds an uncorrelated branch result grouped for per-outer-tuple
// probing. All rows live in one shared backing, contiguous by key hash; a probe
// hashes the outer tuple's key positions, verifies the key against the span's
// first row, and returns the subslice — zero allocations. A span whose rows
// carry more than one distinct key (same-hash collision) is marked mixed and
// diverts to per-key row groups for that hash only.
type cachedBranch struct {
	rows       []Tuple
	spans      map[uint64]rowSpan
	collisions map[uint64][][]Tuple
	branchSyms []query.Symbol
	outerIdx   []int
	branchIdx  []int
}

// rowSpan is a contiguous region of cachedBranch.rows holding every row whose
// key hashes to the span's map key.
type rowSpan struct {
	start, end int32
	mixed      bool
}

// groupBranchRows arranges collected branch rows into hash-contiguous spans
// over one shared backing. Counting-sort placement: one pass counts rows per
// key hash, a second places each row into its span, spans laid out in
// first-seen order. Rows within a span keep their collection order.
func groupBranchRows(collected []Tuple, branchSyms []query.Symbol, oIdx, bIdx []int) *cachedBranch {
	offsets := make(map[uint64]int32, len(collected))
	for _, t := range collected {
		offsets[hashTuplePositions(t, bIdx)]++
	}
	rows := make([]Tuple, len(collected))
	spans := make(map[uint64]rowSpan, len(offsets))
	cursor := int32(0)
	for _, t := range collected {
		h := hashTuplePositions(t, bIdx)
		if _, ok := spans[h]; !ok {
			count := offsets[h]
			spans[h] = rowSpan{start: cursor, end: cursor + count}
			// offsets[h] becomes the span's fill position from here on.
			offsets[h] = cursor
			cursor += count
		}
		rows[offsets[h]] = t
		offsets[h]++
	}
	cb := &cachedBranch{
		rows:       rows,
		spans:      spans,
		branchSyms: branchSyms,
		outerIdx:   oIdx,
		branchIdx:  bIdx,
	}
	cb.regroupCollidingSpans()
	return cb
}

// regroupCollidingSpans finds spans whose rows carry more than one distinct
// key — distinct keys sharing a hash — marks them mixed, and builds per-key
// row groups for those hashes. Unmixed spans (the overwhelmingly common case)
// are untouched.
func (cb *cachedBranch) regroupCollidingSpans() {
	for h, span := range cb.spans {
		segment := cb.rows[span.start:span.end]
		if len(segment) < 2 {
			continue
		}
		mixed := false
		for _, row := range segment[1:] {
			if !branchKeysEqual(segment[0], row, cb.branchIdx) {
				mixed = true
				break
			}
		}
		if !mixed {
			continue
		}
		if cb.collisions == nil {
			cb.collisions = make(map[uint64][][]Tuple)
		}
		var groups [][]Tuple
		for _, row := range segment {
			placed := false
			for gi := range groups {
				if branchKeysEqual(groups[gi][0], row, cb.branchIdx) {
					groups[gi] = append(groups[gi], row)
					placed = true
					break
				}
			}
			if !placed {
				groups = append(groups, []Tuple{row})
			}
		}
		cb.collisions[h] = groups
		span.mixed = true
		cb.spans[h] = span
	}
}

// branchKeysEqual reports whether two branch rows carry equal values at the
// key positions.
func branchKeysEqual(a, b Tuple, bIdx []int) bool {
	for _, idx := range bIdx {
		if !datalog.ValuesEqual(a[idx], b[idx]) {
			return false
		}
	}
	return true
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

	// Build branch rows from EA cache lookups.
	// Iterate the materialized outer relation (safe to create second iterator).
	branchSyms := []query.Symbol{eVar.Name, vVar.Name}
	bIdx := []int{0} // E is at position 0 in branch tuples

	var collected []Tuple
	if n := it.outerRel.Size(); n >= 0 {
		collected = make([]Tuple, 0, n)
	}
	// Outer tuples are distinct but their entity column can repeat; one
	// lookup and one row per distinct entity keeps the collected rows a set.
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

	return groupBranchRows(collected, branchSyms, []int{eIdx}, bIdx)
}

// probe returns the branch rows whose key positions equal the outer tuple's
// key positions, or nil. The hit path returns a subslice of the shared
// backing — zero allocations. A hash hit is not a key match: the key is
// verified against the span's first row (every row of an unmixed span
// carries the same key).
func (cb *cachedBranch) probe(outerTuple Tuple) []Tuple {
	h := hashTuplePositions(outerTuple, cb.outerIdx)
	span, ok := cb.spans[h]
	if !ok {
		return nil
	}
	if span.mixed {
		for _, group := range cb.collisions[h] {
			if cb.outerKeyMatches(outerTuple, group[0]) {
				return group
			}
		}
		return nil
	}
	if !cb.outerKeyMatches(outerTuple, cb.rows[span.start]) {
		return nil
	}
	return cb.rows[span.start:span.end]
}

// outerKeyMatches compares the outer tuple's key positions against a branch
// row's key positions.
func (cb *cachedBranch) outerKeyMatches(outer, row Tuple) bool {
	for k, oi := range cb.outerIdx {
		if !datalog.ValuesEqual(outer[oi], row[cb.branchIdx[k]]) {
			return false
		}
	}
	return true
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

	return groupBranchRows(collected, branchSyms, oIdx, bIdx), nil
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
// unbounded scan's extra rows are dead entries. Deduplicating the keys keeps
// the matcher from emitting duplicate (entity, value) rows for a repeated
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

// nextShortCircuit tries branches in order until one returns results (fallback semantics).
func (it *OrFallbackIterator) nextShortCircuit() bool {
	for {
		// Drain an in-progress cached-branch emit: remaining probe rows come
		// straight off the branch cache's backing.
		if it.cachedMatches != nil {
			if it.cachedMatchPos < len(it.cachedMatches) {
				row := it.cachedMatches[it.cachedMatchPos]
				it.cachedMatchPos++
				if it.cachedPlan.identity {
					it.currentTuple = row
				} else {
					it.currentTuple = it.cachedPlan.project(row, it.cachedOuter)
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
			if collector := it.ctx.Collector(); collector != nil {
				collector.Add(annotations.Event{
					Name:  "or-fallback/outer.exhausted",
					Start: time.Now(),
					Data:  map[string]interface{}{},
				})
			}
			it.done = true
			return false
		}

		outerTuple := it.outerIter.Tuple()

		// Emit annotation for each outer tuple being processed
		if collector := it.ctx.Collector(); collector != nil {
			collector.Add(annotations.Event{
				Name:  "or-fallback/outer.tuple",
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
			// rows emit directly from the cache's backing, and an empty probe
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
				// A DataPattern-only branch in an or-join benefits from join-key
				// narrowing; uncorrelated subqueries (also cacheable) must still
				// run with no input.
				narrowable := isOrJoin && isDataPatternOnlyBranch(branch)
				var execInput Relation
				if isCacheable {
					if narrowable {
						execInput = it.outerJoinKeys()
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
					if collector := it.ctx.Collector(); collector != nil {
						data := map[string]interface{}{
							"branch":   branchIdx,
							"narrowed": execInput != nil,
						}
						if execInput != nil {
							data["join_keys"] = execInput.Size()
						} else {
							data["reason"] = "outer not re-iterable for join-key extraction"
						}
						collector.Add(annotations.Event{
							Name:  "or-fallback/branch.narrowed",
							Start: time.Now(),
							Data:  data,
						})
					}
				}

				// Try EA-cache-based branch build for DataPattern-only or-join branches.
				// Only use when the EA cache is warm (prefetch has run for these entities).
				// The prefetch triggers at executeOrJoinClauseFallback for entities
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
						if collector := it.ctx.Collector(); collector != nil {
							collector.Add(annotations.Event{
								Name: "or-fallback/cache-build",
								Data: map[string]interface{}{
									"branch":      branchIdx,
									"branch_syms": fmt.Sprintf("%v", branchResult.Symbols()),
									"outer_syms":  fmt.Sprintf("%v", it.outerSyms),
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
					if collector := it.ctx.Collector(); collector != nil {
						collector.Add(annotations.Event{
							Name:  "or-fallback/branch.success",
							Start: time.Now(),
							Data: map[string]interface{}{
								"branch_index": branchIdx,
								"branch_syms":  fmt.Sprintf("%v", branchResult.Symbols()),
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

// projectionPlan precomputes, per output position, where a branch row's
// value comes from: a branch-tuple position, else an outer-tuple position
// (a branch such as a ground default doesn't produce every output symbol —
// the outer tuple's values fill the gaps), else nothing. The mapping depends
// only on the branch, output, and outer symbol lists — per-branch constants —
// so one plan serves every tuple the branch emits. identity means the branch
// symbols already match the output symbols exactly and rows pass through
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
// The rows already form a set — a contiguous span of a deduplicated branch
// relation — so nothing re-deduplicates here, and identity-shaped rows alias
// the cache's backing exactly as the wrapper relation's tuples did.
func (it *OrFallbackIterator) emitCachedMatches(branchIdx int, cb *cachedBranch, matches []Tuple, outerTuple Tuple) bool {
	if len(matches) == 0 {
		return false
	}
	if collector := it.ctx.Collector(); collector != nil {
		collector.Add(annotations.Event{
			Name:  "or-fallback/branch.success",
			Start: time.Now(),
			Data: map[string]interface{}{
				"branch_index": branchIdx,
				"branch_syms":  fmt.Sprintf("%v", cb.branchSyms),
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

// filterBranchToOuterTuple filters a branch result to rows matching the
// current outer tuple on shared symbols. Needed for uncorrelated subqueries
// that return results for ALL outer tuples — without filtering, every outer
// tuple would get every result row.
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

	// Filter to matching rows
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
