package executor

import (
	"fmt"
	"sort"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// emptyRelationForQuery returns an empty MaterializedRelation with the correct
// symbols for the given query's :find clause, plus any retained :order-by
// symbols: every path that can reach the finalization sort must produce the
// same shape, or sorting an empty result would error on a missing sort symbol.
// This ensures Query never returns nil — callers always get a valid (possibly
// empty) Relation.
func emptyRelationForQuery(q *query.Query) Relation {
	symbols := extractFindSymbols(q.Find)
	symbols = append(symbols, query.RetainedSortSymbols(q)...)
	return NewMaterializedRelation(symbols, nil)
}

func extractFindSymbols(findElements []query.FindElement) []query.Symbol {
	var symbols []query.Symbol
	for _, elem := range findElements {
		switch e := elem.(type) {
		case query.FindVariable:
			symbols = append(symbols, e.Symbol)
		case query.FindAggregate:
			symbols = append(symbols, datalog.NewSymbol(e.String()))
		case query.FindPull:
			symbols = append(symbols, e.Variable)
		}
	}
	return symbols
}

// consumedFindSymbols is the symbols a find clause reads from the relation
// groups: a variable, an aggregate's argument, a pull's entity variable.
func consumedFindSymbols(findElements []query.FindElement) []query.Symbol {
	var symbols []query.Symbol
	for _, elem := range findElements {
		switch e := elem.(type) {
		case query.FindVariable:
			symbols = append(symbols, e.Symbol)
		case query.FindAggregate:
			symbols = append(symbols, e.Arg)
		case query.FindPull:
			symbols = append(symbols, e.Variable)
		}
	}
	return symbols
}

// SortRelation sorts a relation according to the order-by clauses.
// This is a pure function that performs multi-symbol sorting with configurable direction.
// It materializes the relation if not already materialized.
//
// Every sort variable must be a symbol of the relation; one that is not
// resolves to a deferred error, never a silent skip — a stated ordering the
// engine cannot honor must not report success (see
// docs/bugs/resolved/BUG_ORDER_BY_NON_PROJECTED_VARIABLE_SILENTLY_IGNORED.md). Parsed
// queries cannot reach the error: the parser validates sort keys and the
// planner retains them through the final projection. It guards
// API-constructed queries.
func SortRelation(rel Relation, orderBy []query.OrderByClause) Relation {
	// Materialize if not already materialized
	var tuples []Tuple
	err := collectTuplesInto(&tuples, rel)

	// Get symbol indices for sort variables
	symbols := rel.Symbols()
	sortIndices, indexErr := orderBySymbolIndices(symbols, orderBy)
	if err == nil {
		err = indexErr
	}

	// Sort tuples (skipped when erroring — an arbitrary partial order must
	// not masquerade as the requested one)
	if err == nil {
		sort.Slice(tuples, func(i, j int) bool {
			return compareTuplesByOrder(tuples[i], tuples[j], orderBy, sortIndices) < 0
		})
	}

	opts := rel.Options()
	properties := rel.Properties()
	properties.Ordering = append([]query.OrderByClause(nil), orderBy...)
	mat := newMaterializedRelationFromSet(symbols, tuples, opts, properties)
	if err != nil {
		// Carry a deferred source error so it isn't laundered by materialization.
		mat.err = err
	}
	return mat
}

func orderBySymbolIndices(symbols []query.Symbol, orderBy []query.OrderByClause) ([]int, error) {
	indices := make([]int, len(orderBy))
	for i, clause := range orderBy {
		indices[i] = query.SymbolIndex(symbols, clause.Variable)
		if indices[i] < 0 {
			return indices, fmt.Errorf("order-by variable %s is not a symbol of the relation (symbols: %v)",
				clause.Variable, symbols)
		}
	}
	return indices, nil
}

// compareTuplesByOrder returns -1 when left precedes right, 1 when left follows
// right, and 0 when all requested sort keys compare equal.
func compareTuplesByOrder(left, right Tuple, orderBy []query.OrderByClause, indices []int) int {
	for i, clause := range orderBy {
		cmp := datalog.CompareValues(left[indices[i]], right[indices[i]])
		if clause.Descending {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

// environmentSymbolsOf returns the single-valued input symbols of an :in
// clause — the scalar and tuple parameters that form a query scope's
// environment — in spec order, deduplicated. Multi-valued inputs
// (collection, relation) are data, not environment. Returns nil when the
// clause declares no single-valued input.
func environmentSymbolsOf(in []query.InputSpec) []query.Symbol {
	var env []query.Symbol
	add := func(sym query.Symbol) {
		if !query.ContainsSymbol(env, sym) {
			env = append(env, sym)
		}
	}
	for _, input := range in {
		switch inp := input.(type) {
		case query.ScalarInput:
			add(inp.Symbol)
		case query.TupleInput:
			for _, sym := range inp.Symbols {
				add(sym)
			}
		}
	}
	return env
}

// environmentTuple returns the environment relation's symbols and single tuple
// by reference — the loop-invariant side of binding it into per-tuple
// evaluation inside an operator. Never copies: the environment relation is
// the single holder of its content. A nil environment yields nil slices.
// The one-tuple shape is the environment's structural invariant, enforced at
// every construction site (executor binding, subquery entry).
func environmentTuple(env Relation) ([]query.Symbol, Tuple) {
	if env == nil {
		return nil, nil
	}
	return env.Symbols(), env.Get(0)
}

// unifiedTuple is tuple read under the distinct symbols query.SymbolPositions
// gave: the value at the first of each symbol's positions, when the tuple
// agrees at every symbol's positions, and no tuple when it does not.
func unifiedTuple(tuple Tuple, positions [][]int) (Tuple, bool) {
	if !query.AgreesAt(tuple, positions) {
		return nil, false
	}
	out := make(Tuple, len(positions))
	for i, carrying := range positions {
		out[i] = tuple[carrying[0]]
	}
	return out, true
}

// BindQueryInputs is the bound group of a query's inputs, the relation the
// query joins once (docs/proposals/RELATION_INPUT_PROOF.md §8): one relation
// per :in entry that binds, over the entry's distinct symbols, joined into
// one. A scalar, a collection, a tuple and a relation input are one form, a
// set of tuples over a symbol list, so every tuple of the given relation
// binds, each entry symbol read from the relation's own symbol of that name.
// A symbol the entry or the relation carries twice is one variable, bound
// by the tuples whose values at its positions agree. A relation lacking a
// symbol its entry names, a relation count other than the entries that
// bind, and a read that fails are the query's error. With no entry the
// group is the unit relation.
func BindQueryInputs(q *query.Query, inputRelations []Relation) (Relation, error) {
	entries := query.BindingInputs(q.In)
	if len(inputRelations) != len(entries) {
		return nil, fmt.Errorf("%d input relations were given, and the query's :in binds %d", len(inputRelations), len(entries))
	}
	var bound Relation
	for i, entry := range entries {
		rel := inputRelations[i]
		distinct, positions, err := query.SymbolPositions(rel.Symbols(), query.InputSymbols([]query.InputSpec{entry}))
		if err != nil {
			return nil, fmt.Errorf("the relation given for %s: %w", entry, err)
		}
		var tuples []Tuple
		if n := rel.Size(); n > 0 {
			tuples = make([]Tuple, 0, n)
		}
		it := rel.Iterator()
		for it.Next() {
			if tuple, ok := unifiedTuple(it.Tuple(), positions); ok {
				tuples = append(tuples, tuple)
			}
		}
		err = it.Error()
		if closeErr := it.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, fmt.Errorf("reading the relation given for %s: %w", entry, err)
		}
		entryBound := NewMaterializedRelationWithOptions(distinct, tuples, rel.Options())
		if bound == nil {
			bound = entryBound
		} else {
			bound = bound.Join(entryBound)
		}
	}
	if bound == nil {
		return NewUnitRelation(ExecutorOptions{}), nil
	}
	return bound, nil
}
