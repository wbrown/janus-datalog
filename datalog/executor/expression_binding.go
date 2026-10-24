package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// bindingAlignment aligns an expression's binding symbols with a relation's
// symbols. A binding symbol already present in the relation is a bound
// position: applying values unifies against it — equal keeps the tuple,
// unequal drops it, and the position is never written (Datomic unification
// semantics; an expression binding a bound variable is an equality
// constraint). A binding symbol absent from the relation extends the tuple.
// A symbol written twice in the binding names one variable (the groups
// query.SymbolGroups gives): its later occurrences neither bind a relation
// position nor extend, and applying values keeps the tuple only when the
// values at all of its positions agree.
//
// This is the single home of that invariant. Every path that applies
// expression results to relation tuples goes through apply — the ground
// path and the per-tuple path previously implemented it independently and
// diverged (docs/bugs/BUG_EXPRESSION_BINDING_OVERWRITES_BOUND_VARIABLE.md).
type bindingAlignment struct {
	symbols     []query.Symbol // relation symbols followed by extension symbols
	boundAt     []int          // per binding symbol: tuple position, or -1 (extension or later occurrence)
	extensionAt []int          // per binding symbol: output position, or -1 (bound or later occurrence)
	groups      [][]int        // per distinct binding symbol: every binding position naming it
	extensions  int
}

// alignBinding computes the alignment of bindingSyms against relSymbols.
func alignBinding(relSymbols, bindingSyms []query.Symbol) bindingAlignment {
	_, groups := query.SymbolGroups(bindingSyms)
	a := bindingAlignment{
		boundAt:     make([]int, len(bindingSyms)),
		extensionAt: make([]int, len(bindingSyms)),
		groups:      groups,
	}
	for i := range bindingSyms {
		a.boundAt[i] = -1
		a.extensionAt[i] = -1
	}
	symbols := make([]query.Symbol, len(relSymbols), len(relSymbols)+len(groups))
	copy(symbols, relSymbols)
	for _, group := range groups {
		first := group[0]
		a.boundAt[first] = query.SymbolIndex(relSymbols, bindingSyms[first])
		if a.boundAt[first] >= 0 {
			continue
		}
		a.extensionAt[first] = len(symbols)
		symbols = append(symbols, bindingSyms[first])
		a.extensions++
	}
	a.symbols = symbols
	return a
}

// extensionSymbols returns the binding symbols that extend the relation —
// the genuinely new symbols. Bound symbols are absent: their tuple values
// are never changed by apply, so relation properties (ordering, keys) that
// mention them remain valid.
func (a bindingAlignment) extensionSymbols() []query.Symbol {
	return a.symbols[len(a.symbols)-a.extensions:]
}

// extendsTuple reports whether apply adds symbols to the tuple. When it
// does, apply allocates a fresh output tuple; when it does not, apply
// returns the input tuple itself, and the retainer owns the workspace-copy
// decision for RequiresCopy sources.
func (a bindingAlignment) extendsTuple() bool { return a.extensions > 0 }

// bindingTuples is the tuples a function's result yields under an
// expression's binding, the relation the scope joins: one tuple of the value
// under a scalar binding, the one tuple the value destructures to under the
// tuple form, one tuple per element of the value under the collection form,
// and one tuple per inner vector of the value under the relation form. A
// value of another shape than the binding's, or an inner vector of another
// arity, is an error naming both. A tuple may share the value's slice.
func bindingTuples(binding interface{}, value interface{}) ([]Tuple, error) {
	switch b := binding.(type) {
	case query.Symbol:
		return []Tuple{{value}}, nil
	case query.TupleBinding:
		values, ok := query.VectorElements(value)
		if !ok {
			return nil, fmt.Errorf("tuple binding requires tuple result, got %T", value)
		}
		if len(values) != len(b.Variables) {
			return nil, fmt.Errorf("tuple mismatch: %d values, %d variables", len(values), len(b.Variables))
		}
		return []Tuple{values}, nil
	case query.CollectionBinding:
		elements, ok := query.VectorElements(value)
		if !ok {
			return nil, fmt.Errorf("collection binding %s requires a vector result, got %T", b, value)
		}
		tuples := make([]Tuple, len(elements))
		for i, element := range elements {
			tuples[i] = Tuple{element}
		}
		return tuples, nil
	case query.RelationBinding:
		vectors, ok := query.VectorElements(value)
		if !ok {
			return nil, fmt.Errorf("relation binding %s requires a collection of tuples, got %T", b, value)
		}
		tuples := make([]Tuple, len(vectors))
		for i, vector := range vectors {
			values, ok := query.VectorElements(vector)
			if !ok {
				return nil, fmt.Errorf("relation binding %s: tuple %d is no vector, got %T", b, i+1, vector)
			}
			if len(values) != len(b.Variables) {
				return nil, fmt.Errorf("relation binding %s: tuple %d has %d values, %d variables", b, i+1, len(values), len(b.Variables))
			}
			tuples[i] = values
		}
		return tuples, nil
	}
	return nil, fmt.Errorf("unsupported binding type: %T", binding)
}

// relationOver is the materialized relation over the alignment's symbols
// holding each tuple a binding read, applied to no input tuple. The
// alignment is against no relation symbols, so every first occurrence of a
// binding symbol extends, every output tuple is fresh and shares nothing
// with the value the function returned, and a tuple whose repeated symbol
// disagrees is dropped.
func (a bindingAlignment) relationOver(bound []Tuple, opts ExecutorOptions) Relation {
	tuples := make([]Tuple, 0, len(bound))
	for _, values := range bound {
		if out, ok := a.apply(nil, values); ok {
			tuples = append(tuples, out)
		}
	}
	return NewMaterializedRelationWithOptions(a.symbols, tuples, opts)
}

// apply unifies values against the tuple's bound positions and against each
// other where one symbol holds several positions and, on success, returns
// the output tuple: the input unchanged when nothing extends, or the input
// plus the extension values. ok=false means a position did not unify — the
// tuple is dropped, never rewritten.
func (a bindingAlignment) apply(tuple Tuple, values []interface{}) (Tuple, bool) {
	for i, idx := range a.boundAt {
		if idx >= 0 && !datalog.ValuesEqual(tuple[idx], values[i]) {
			return nil, false
		}
	}
	if !query.AgreesAt(values, a.groups) {
		return nil, false
	}
	if a.extensions == 0 {
		return tuple, true
	}
	out := make(Tuple, len(a.symbols))
	copy(out, tuple)
	for i, pos := range a.extensionAt {
		if pos >= 0 {
			out[pos] = values[i]
		}
	}
	return out, true
}
