package executor

import (
	"fmt"
	"strings"
	"time"

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
	iteratorCount int // Track how many iterators have been created (for debugging)
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
		outerSyms:  r.outerRel.Symbols(),
		outputSyms: r.symbols, // Use pre-computed symbols
		options:    r.options,
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
	outerSyms []query.Symbol
	options   ExecutorOptions

	// Current state
	currentBranchIter     Iterator
	currentBranchRelation Relation // Track relation for RequiresCopy check
	currentTuple          Tuple
	outputSyms            []query.Symbol
	done                  bool
	err                   error

	// Cache for uncorrelated branch results.
	// Key: branch index. Value: materialized result relation.
	// An uncorrelated SubqueryPattern (Inputs = only $) produces the
	// same result for every outer tuple, so we execute it once and cache.
	branchCache map[int]Relation
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
			_ = branchIdx // used in annotation
			branchResult, err := it.executor.executeInnerClauses(it.ctx, branch, inputRel)
			if err != nil {
				it.err = err
				it.done = true
				return false
			}

			if branchResult != nil {
				// Filter branch result to rows matching the outer tuple on shared symbols.
				// This is needed when the branch contains an uncorrelated subquery that
				// returns results for ALL outer tuples (e.g., decorrelated subquery).
				branchResult = filterBranchToOuterTuple(branchResult, outerTuple, it.outerSyms)

				branchIter := branchResult.Iterator()
				if branchIter.Next() {
					// This branch has results - use it
					// (outputSyms is pre-computed at iterator construction)

					// Emit annotation for branch success
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

					// Project tuple to match output symbols if needed
					// Different branches may produce different schemas (e.g., subquery vs ground)
					branchSyms := branchResult.Symbols()
					firstTuple := branchIter.Tuple()

					if len(branchSyms) != len(it.outputSyms) || !symbolsMatch(branchSyms, it.outputSyms) {
						// Projection allocates new slice - no additional copy needed
						it.currentTuple = projectTupleToSymbols(firstTuple, branchSyms, it.outputSyms)
					} else if branchResult.RequiresCopy() {
						// No projection but unsafe source - copy once
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

// projectTupleToSymbols projects a tuple from source symbols to target symbols
func projectTupleToSymbols(tuple Tuple, srcSyms, dstSyms []query.Symbol) Tuple {
	// Build index for source symbols
	srcIdx := make(map[query.Symbol]int)
	for i, sym := range srcSyms {
		srcIdx[sym] = i
	}

	// Create projected tuple
	result := make(Tuple, len(dstSyms))
	for i, sym := range dstSyms {
		if idx, ok := srcIdx[sym]; ok {
			result[i] = tuple[idx]
		}
		// If symbol not found, leave as nil (shouldn't happen with proper schema)
	}
	return result
}

// projectedIterator wraps an iterator and projects tuples to a different schema
type projectedIterator struct {
	inner          Iterator
	branchRelation Relation // For RequiresCopy check
	branchSyms     []query.Symbol
	outputSyms     []query.Symbol
}

func (it *projectedIterator) Next() bool {
	return it.inner.Next()
}

func (it *projectedIterator) Tuple() Tuple {
	tuple := it.inner.Tuple()

	if len(it.branchSyms) != len(it.outputSyms) || !symbolsMatch(it.branchSyms, it.outputSyms) {
		// Projection allocates new slice - no additional copy needed
		return projectTupleToSymbols(tuple, it.branchSyms, it.outputSyms)
	}

	// No projection - copy if source is unsafe
	if it.branchRelation != nil && it.branchRelation.RequiresCopy() {
		return copyTuple(tuple)
	}
	return tuple
}

// filterBranchToOuterTuple filters a branch result to rows matching the
// current outer tuple on shared symbols. This is critical for uncorrelated
// subqueries that return results for ALL outer tuples — without filtering,
// every outer tuple would get every result row.
func filterBranchToOuterTuple(branchResult Relation, outerTuple Tuple, outerSyms []query.Symbol) Relation {
	branchSyms := branchResult.Symbols()

	// Find shared symbols between outer and branch
	type symPair struct {
		outerIdx, branchIdx int
	}
	var shared []symPair
	for oi, osym := range outerSyms {
		for bi, bsym := range branchSyms {
			if osym == bsym {
				shared = append(shared, symPair{oi, bi})
			}
		}
	}

	if len(shared) == 0 {
		return branchResult // No shared symbols — nothing to filter
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
