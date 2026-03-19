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
// lazily per outer tuple. For each outer tuple, it tries branches in order
// until one produces results, then yields those results.
type OrFallbackRelation struct {
	executor      *DefaultQueryExecutor
	ctx           Context
	clause        *query.OrClause
	outerRel      Relation
	symbols       []query.Symbol // Determined lazily from first result
	options       ExecutorOptions
	iteratorCount int            // Track how many iterators have been created (for debugging)
	joinSyms      []query.Symbol // From or-join: explicit join variables for cache keying
	prefetched    bool           // True when PrefetchEntities has warmed the EA cache
}

// NewOrFallbackRelation creates a streaming OR fallback relation.
// The symbols are computed upfront from the outer relation and OR clause
// to avoid needing to peek at results.
func NewOrFallbackRelation(
	executor *DefaultQueryExecutor,
	ctx Context,
	clause *query.OrClause,
	outerRel Relation,
	options ExecutorOptions,
) *OrFallbackRelation {
	// Compute output symbols statically:
	// Output = outer symbols + symbols produced by branches (that aren't in outer)
	outerSyms := outerRel.Symbols()
	outerSymSet := make(map[query.Symbol]bool)
	for _, sym := range outerSyms {
		outerSymSet[sym] = true
	}

	// Collect symbols that branches produce (common across all branches)
	branchOutputs := computeOrBranchOutputSymbols(clause)

	// Build output symbols: outer symbols first, then new symbols from branches
	symbols := make([]query.Symbol, len(outerSyms))
	copy(symbols, outerSyms)
	for _, sym := range branchOutputs {
		if !outerSymSet[sym] {
			symbols = append(symbols, sym)
		}
	}

	return &OrFallbackRelation{
		executor: executor,
		ctx:      ctx,
		clause:   clause,
		outerRel: outerRel,
		symbols:  symbols,
		options:  options,
	}
}

// computeOrBranchOutputSymbols computes symbols that OR branches produce.
// Returns symbols that are common to all branches (intersection).
func computeOrBranchOutputSymbols(clause *query.OrClause) []query.Symbol {
	if len(clause.Branches) == 0 {
		return nil
	}

	// Collect outputs from first branch
	firstBranchOutputs := collectBranchOutputSymbols(clause.Branches[0])
	if len(clause.Branches) == 1 {
		return firstBranchOutputs
	}

	// Convert to set for intersection
	commonSet := make(map[query.Symbol]bool)
	for _, sym := range firstBranchOutputs {
		commonSet[sym] = true
	}

	// Intersect with other branches
	for i := 1; i < len(clause.Branches); i++ {
		branchOutputs := collectBranchOutputSymbols(clause.Branches[i])
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
			switch b := clause.Binding.(type) {
			case query.ScalarBinding:
				if !seen[b.Variable] {
					seen[b.Variable] = true
					outputs = append(outputs, b.Variable)
				}
			case query.TupleBinding:
				for _, v := range b.Variables {
					if !seen[v] {
						seen[v] = true
						outputs = append(outputs, v)
					}
				}
			case query.RelationBinding:
				for _, v := range b.Variables {
					if !seen[v] {
						seen[v] = true
						outputs = append(outputs, v)
					}
				}
			case query.CollectionBinding:
				if !seen[b.Variable] {
					seen[b.Variable] = true
					outputs = append(outputs, b.Variable)
				}
			}
		}
	}
	return outputs
}

func (r *OrFallbackRelation) Iterator() Iterator {
	r.iteratorCount++
	outerIter := r.outerRel.Iterator()

	// Emit annotation for iterator creation
	// Note: Don't call r.outerRel.Size() - it may block for streaming relations
	if collector := r.ctx.Collector(); collector != nil {
		collector.Add(annotations.Event{
			Name:  "or-fallback/iterator.created",
			Start: time.Now(),
			Data: map[string]interface{}{
				"iterator_count": r.iteratorCount,
				"outer_symbols":  fmt.Sprintf("%v", r.outerRel.Symbols()),
				"outer_type":     fmt.Sprintf("%T", r.outerRel),
			},
		})
	}

	return &OrFallbackIterator{
		executor:   r.executor,
		ctx:        r.ctx,
		clause:     r.clause,
		outerIter:  outerIter,
		outerRel:   r.outerRel, // Materialized relation for EA cache branch building
		outerSyms:  r.outerRel.Symbols(),
		outputSyms: r.symbols, // Use pre-computed symbols
		options:    r.options,
		joinSyms:   r.joinSyms,
		prefetched: r.prefetched,
	}
}

func (r *OrFallbackRelation) Symbols() []query.Symbol {
	// Symbols are computed at construction time - no peeking needed
	return r.symbols
}

func (r *OrFallbackRelation) Size() int {
	return -1 // Streaming - unknown size
}

func (r *OrFallbackRelation) IsEmpty() bool {
	it := r.Iterator()
	defer it.Close()
	return !it.Next()
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

func (r *OrFallbackRelation) Sorted() []Tuple {
	return r.Materialize().Sorted()
}

func (r *OrFallbackRelation) Project(symbols []query.Symbol) (Relation, error) {
	return r.Materialize().Project(symbols)
}

func (r *OrFallbackRelation) Materialize() Relation {
	var tuples []Tuple
	collectTuplesInto(&tuples, r)

	syms := r.symbols
	if syms == nil {
		syms = r.Symbols()
	}
	return NewMaterializedRelationWithOptions(syms, tuples, r.options)
}

func (r *OrFallbackRelation) Sort(orderBy []query.OrderByClause) Relation {
	return r.Materialize().Sort(orderBy)
}

func (r *OrFallbackRelation) Filter(filter Filter) Relation {
	return FilterRelation(r, filter)
}

func (r *OrFallbackRelation) FilterWithPredicate(pred query.Predicate) Relation {
	return r.Materialize().FilterWithPredicate(pred)
}

func (r *OrFallbackRelation) EvaluateFunction(fn query.Function, outputColumn query.Symbol) Relation {
	return r.Materialize().EvaluateFunction(fn, outputColumn)
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

func (r *OrFallbackRelation) HashJoin(other Relation, joinCols []query.Symbol) Relation {
	return HashJoin(r, other, joinCols)
}

func (r *OrFallbackRelation) SemiJoin(other Relation, joinCols []query.Symbol) Relation {
	return SemiJoin(r, other, joinCols)
}

func (r *OrFallbackRelation) AntiJoin(other Relation, joinCols []query.Symbol) Relation {
	return AntiJoin(r, other, joinCols)
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
	executor  *DefaultQueryExecutor
	ctx       Context
	clause    *query.OrClause
	outerIter Iterator
	outerRel  Relation       // Materialized outer relation (for EA cache branch building)
	outerSyms []query.Symbol
	options   ExecutorOptions
	joinSyms  []query.Symbol // From or-join: used as cache key (not all shared symbols)

	// Current state
	currentBranchIter     Iterator
	currentBranchRelation Relation // Track relation for RequiresCopy check
	currentTuple          Tuple
	outputSyms            []query.Symbol
	done                  bool
	err                   error

	// Cache for uncorrelated branch results. Key: branch index.
	// See ALGEBRA.md "OR-Fallback Branch Caching" for specification.
	branchCache map[int]*cachedBranch
	prefetched  bool // True when PrefetchEntities has warmed the EA cache for outer entities
}

// cachedBranch holds a hash index over an uncorrelated branch result.
// Uses TupleKeyMap from tuple_key.go (same infrastructure as HashJoin).
type cachedBranch struct {
	index      *TupleKeyMap
	branchSyms []query.Symbol
	outerIdx   []int
	branchIdx  []int
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

	// Need E as variable (join key) and A as constant keyword
	eVar, eIsVar := dp.GetE().(query.Variable)
	if !eIsVar {
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
	eIdx := -1
	for i, sym := range it.outerSyms {
		if sym == eVar.Name {
			eIdx = i
			break
		}
	}
	if eIdx < 0 {
		return nil
	}

	// Build branch result from EA cache lookups.
	// Iterate the materialized outer relation (safe to create second iterator).
	branchSyms := []query.Symbol{eVar.Name, vVar.Name}
	idx := NewTupleKeyMap()
	bIdx := []int{0} // E is at position 0 in branch tuples

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
		value, found := lookupMatcher.LookupAttribute(entity, aKw)
		if !found {
			continue
		}
		tuple := Tuple{entity, value}
		key := NewTupleKey(tuple, bIdx)
		if existing, ok := idx.Get(key); ok {
			idx.Put(key, append(existing.([]Tuple), tuple))
		} else {
			idx.Put(key, []Tuple{tuple})
		}
	}
	outerIt.Close()

	return &cachedBranch{
		index:      idx,
		branchSyms: branchSyms,
		outerIdx:   []int{eIdx},
		branchIdx:  bIdx,
	}
}

func (cb *cachedBranch) probe(outerTuple Tuple) []Tuple {
	key := NewTupleKey(outerTuple, cb.outerIdx)
	if matches, ok := cb.index.Get(key); ok {
		return matches.([]Tuple)
	}
	return nil
}

// isCacheableBranch returns true if the branch can be evaluated once with
// join variables free, cached, and probed per outer tuple.
//
// Two cases:
// 1. SubqueryPattern with only $ inputs (uncorrelated subquery)
// 2. In an or-join: DataPattern-only branches where the join variables
//    are the only connection to the outer context. Evaluated with join
//    vars free, the DataPattern returns ALL matches; the cache indexes
//    by join vars for O(1) per-tuple probe.
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

// buildCachedBranch builds a hash index over a branch result keyed on
// the specified join symbols. Only join variables are used as keys —
// not all shared symbols — because branch results may contain symbols
// with the same name but different values than the outer relation.
func buildCachedBranch(branchResult Relation, outerSyms []query.Symbol, joinSyms []query.Symbol) *cachedBranch {
	branchSyms := branchResult.Symbols()

	// Use only join symbols as cache keys
	keySyms := joinSyms
	if len(keySyms) == 0 {
		// Fallback: use all shared symbols (non-or-join path)
		keySyms = nil
		for _, osym := range outerSyms {
			for _, bsym := range branchSyms {
				if osym == bsym {
					keySyms = append(keySyms, osym)
				}
			}
		}
	}

	var bIdx, oIdx []int
	for _, ksym := range keySyms {
		for oi, osym := range outerSyms {
			if osym == ksym {
				for bi, bsym := range branchSyms {
					if bsym == ksym {
						oIdx = append(oIdx, oi)
						bIdx = append(bIdx, bi)
						break
					}
				}
				break
			}
		}
	}
	if len(bIdx) == 0 {
		return nil
	}

	idx := NewTupleKeyMap()
	iter := branchResult.Iterator()
	for iter.Next() {
		t := iter.Tuple()
		cp := make(Tuple, len(t))
		copy(cp, t)
		key := NewTupleKey(cp, bIdx)
		if existing, ok := idx.Get(key); ok {
			idx.Put(key, append(existing.([]Tuple), cp))
		} else {
			idx.Put(key, []Tuple{cp})
		}
	}
	iter.Close()

	return &cachedBranch{index: idx, branchSyms: branchSyms, outerIdx: oIdx, branchIdx: bIdx}
}

func (it *OrFallbackIterator) Next() bool {
	if it.done {
		return false
	}

	for {
		// If we have a current branch iterator, try to get next tuple from it
		if it.currentBranchIter != nil {
			if it.currentBranchIter.Next() {
				// projectedIterator.Tuple() handles copying when needed
				it.currentTuple = it.currentBranchIter.Tuple()
				return true
			}
			// Branch exhausted, close it
			it.currentBranchIter.Close()
			it.currentBranchIter = nil
			it.currentBranchRelation = nil
		}

		// Need to advance to next outer tuple
		if !it.outerIter.Next() {
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

		// Build single-tuple relation for this input.
		// Special case: if outer has no symbols (unit relation), pass nil to avoid
		// creating a ProductRelation that would try to re-iterate streaming results.
		var inputRel Relation
		if len(it.outerSyms) > 0 {
			inputRel = NewMaterializedRelationWithOptions(
				it.outerSyms,
				[]Tuple{outerTuple},
				it.options,
			)
		}

		// Try each branch until one returns results
		for branchIdx, branch := range it.clause.Branches {
			var branchResult Relation

			// Check cache for uncorrelated branches (O(1) probe)
			if cb, cached := it.branchCache[branchIdx]; cached {
				matches := cb.probe(outerTuple)
				branchResult = NewMaterializedRelation(cb.branchSyms, matches)
			} else {
				// Execute the branch.
				// For cacheable branches (uncorrelated SubqueryPatterns, or
				// DataPattern-only branches in or-join), execute WITHOUT
				// inputRel so the result covers ALL values. The branch cache
				// indexes the full result for O(1) per-tuple probes.
				//
				// Fast path: for DataPattern branches in or-join, try building
				// the branch cache from EA cache (LookupAttribute) instead of
				// a full storage scan. Falls back to storage scan if the EA
				// cache isn't available.
				execInput := inputRel
				isOrJoin := len(it.joinSyms) > 0
				isCacheable := isCacheableBranch(branch, isOrJoin)
				if isCacheable {
					execInput = nil
				}

				// Try EA-cache-based branch build for DataPattern-only or-join branches.
				// Only use when the EA cache is warm (prefetch has run for these entities).
				// The prefetch triggers at executeOrJoinClauseFallback for entities
				// in the outer relation. For inner subquery or-joins, the prefetch
				// hasn't run yet, so fall back to the storage scan path.
				eaCacheUsed := false
				if isCacheable && isOrJoin && !isCacheableBranch(branch, false) && it.prefetched {
					if cb := it.buildBranchFromEACache(branch); cb != nil {
						if it.branchCache == nil {
							it.branchCache = make(map[int]*cachedBranch)
						}
						it.branchCache[branchIdx] = cb
						matches := cb.probe(outerTuple)
						branchResult = NewMaterializedRelation(cb.branchSyms, matches)
						eaCacheUsed = true
					}
				}

				if !eaCacheUsed {
				var err error
				branchResult, err = it.executor.executeInnerClauses(it.ctx, branch, execInput)
				if err != nil {
					it.err = err
					it.done = true
					return false
				}

				if branchResult != nil {
					// First evaluation: cache uncorrelated branches
					if execInput == nil {
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
						cb := buildCachedBranch(branchResult, it.outerSyms, it.joinSyms)
						if cb != nil {
							if it.branchCache == nil {
								it.branchCache = make(map[int]*cachedBranch)
							}
							it.branchCache[branchIdx] = cb
							// Probe the freshly-built cache instead of scanning
							matches := cb.probe(outerTuple)
							branchResult = NewMaterializedRelation(cb.branchSyms, matches)
						} else {
							// No shared symbols — pass through unfiltered
						}
					} else {
						// Correlated branch — filter per tuple
						branchResult = filterBranchToOuterTuple(branchResult, outerTuple, it.outerSyms)
					}
				}
				} // end if !eaCacheUsed
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

					if len(branchSyms) != len(it.outputSyms) || !symbolsMatch(branchSyms, it.outputSyms) {
						it.currentTuple = projectTupleWithFallback(firstTuple, branchSyms, it.outputSyms, outerTuple, it.outerSyms)
					} else if branchResult.RequiresCopy() {
						it.currentTuple = copyTuple(firstTuple)
					} else {
						it.currentTuple = firstTuple
					}
					it.currentBranchRelation = branchResult
					it.currentBranchIter = &projectedIterator{
						inner:          branchIter,
						branchRelation: branchResult,
						branchSyms:     branchSyms,
						outputSyms:     it.outputSyms,
						outerTuple:     outerTuple,
						outerSyms:      it.outerSyms,
					}
					return true
				}
				branchIter.Close()
			}
		}
		// No branch matched for this outer tuple - continue to next outer tuple
	}
}

func (it *OrFallbackIterator) Tuple() Tuple {
	return it.currentTuple
}

func (it *OrFallbackIterator) Close() error {
	if it.currentBranchIter != nil {
		it.currentBranchIter.Close()
		it.currentBranchIter = nil
	}
	if it.outerIter != nil {
		it.outerIter.Close()
	}
	it.done = true
	return nil
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

// projectTupleWithFallback projects a tuple from source symbols to target symbols.
// For symbols not in the source, falls back to the outer tuple if provided.
// This is needed when a branch (e.g., ground default) doesn't produce all
// output symbols — the outer tuple's values fill the gaps.
func projectTupleWithFallback(tuple Tuple, srcSyms, dstSyms []query.Symbol, outerTuple Tuple, outerSyms []query.Symbol) Tuple {
	// Build index for source symbols
	srcIdx := make(map[query.Symbol]int)
	for i, sym := range srcSyms {
		srcIdx[sym] = i
	}

	// Build index for outer symbols (fallback)
	outerIdx := make(map[query.Symbol]int)
	for i, sym := range outerSyms {
		outerIdx[sym] = i
	}

	// Create projected tuple
	result := make(Tuple, len(dstSyms))
	for i, sym := range dstSyms {
		if idx, ok := srcIdx[sym]; ok {
			result[i] = tuple[idx]
		} else if idx, ok := outerIdx[sym]; ok && idx < len(outerTuple) {
			// Symbol not in branch result — use outer tuple's value
			result[i] = outerTuple[idx]
		}
	}
	return result
}

// projectedIterator wraps an iterator and projects tuples to a different schema
type projectedIterator struct {
	inner          Iterator
	branchRelation Relation // For RequiresCopy check
	branchSyms     []query.Symbol
	outputSyms     []query.Symbol
	outerTuple     Tuple          // Fallback values for symbols not in branch
	outerSyms      []query.Symbol // Symbols from the outer tuple
}

func (it *projectedIterator) Next() bool {
	return it.inner.Next()
}

func (it *projectedIterator) Tuple() Tuple {
	tuple := it.inner.Tuple()

	if len(it.branchSyms) != len(it.outputSyms) || !symbolsMatch(it.branchSyms, it.outputSyms) {
		return projectTupleWithFallback(tuple, it.branchSyms, it.outputSyms, it.outerTuple, it.outerSyms)
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
		for bi, bsym := range branchSyms {
			if osym == bsym {
				shared = append(shared, symPair{oi, bi})
			}
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
			if !valuesEqual(outerTuple[sp.outerIdx], t[sp.branchIdx]) {
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
	iter.Close()

	return NewMaterializedRelation(branchSyms, tuples)
}

func (it *projectedIterator) Close() error {
	return it.inner.Close()
}

func (it *projectedIterator) Error() error { return it.inner.Error() }
