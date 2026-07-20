package executor

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// bindingAlignment aligns an expression's binding symbols with a relation's
// symbols. A binding symbol already present in the relation is a bound
// position: applying values unifies against it — equal keeps the tuple,
// unequal drops it, and the position is never written (Datomic unification
// semantics; an expression binding a bound variable is an equality
// constraint). A binding symbol absent from the relation extends the tuple.
//
// This is the single home of that invariant. Every path that applies
// expression results to relation tuples goes through apply — the ground
// path and the per-tuple path previously implemented it independently and
// diverged (docs/bugs/BUG_EXPRESSION_BINDING_OVERWRITES_BOUND_VARIABLE.md).
type bindingAlignment struct {
	symbols     []query.Symbol // relation symbols followed by extension symbols
	boundAt     []int          // per binding symbol: tuple position, or -1 (extension)
	extensionAt []int          // per binding symbol: output position, or -1 (bound)
	extensions  int
}

// alignBinding computes the alignment of bindingSyms against relSymbols.
func alignBinding(relSymbols, bindingSyms []query.Symbol) bindingAlignment {
	a := bindingAlignment{
		boundAt:     make([]int, len(bindingSyms)),
		extensionAt: make([]int, len(bindingSyms)),
	}
	symbols := make([]query.Symbol, len(relSymbols), len(relSymbols)+len(bindingSyms))
	copy(symbols, relSymbols)
	for i, sym := range bindingSyms {
		a.boundAt[i] = query.SymbolIndex(relSymbols, sym)
		if a.boundAt[i] >= 0 {
			a.extensionAt[i] = -1
			continue
		}
		a.extensionAt[i] = len(symbols)
		symbols = append(symbols, sym)
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

// apply unifies values against the tuple's bound positions and, on success,
// returns the output tuple: the input unchanged when nothing extends, or
// the input plus the extension values. ok=false means a bound position did
// not unify — the tuple is dropped, never rewritten.
func (a bindingAlignment) apply(tuple Tuple, values []interface{}) (Tuple, bool) {
	for i, idx := range a.boundAt {
		if idx >= 0 && !datalog.ValuesEqual(tuple[idx], values[i]) {
			return nil, false
		}
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
