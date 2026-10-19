package query

import "fmt"

// Substitution is what a call states about names: for each symbol of one
// scope, the symbol of the enclosing scope that stands for it, read once
// from the text and applied to a relation by symbol.
type Substitution struct {
	from []Symbol
	to   []Symbol
}

// SubstitutionOf pairs each symbol of from with the symbol at its place in
// to. Lists of different lengths pair nothing and are an error.
func SubstitutionOf(from, to []Symbol) (Substitution, error) {
	if len(from) != len(to) {
		return Substitution{}, fmt.Errorf("substitution: %d symbols %v against %d symbols %v", len(from), from, len(to), to)
	}
	return Substitution{from: from, to: to}, nil
}

// Of returns the symbol standing for sym, and whether the substitution
// names sym at all.
func (s Substitution) Of(sym Symbol) (Symbol, bool) {
	if i := SymbolIndex(s.from, sym); i >= 0 {
		return s.to[i], true
	}
	return nil, false
}

// Over is the part of the substitution naming symbols among the given ones,
// the part a member of a product carries when the product is renamed.
func (s Substitution) Over(symbols []Symbol) Substitution {
	var from, to []Symbol
	for i, named := range s.from {
		if ContainsSymbol(symbols, named) {
			from = append(from, named)
			to = append(to, s.to[i])
		}
	}
	return Substitution{from: from, to: to}
}

// Rename returns symbols with each symbol the substitution names replaced
// by the symbol standing for it, at the same position, and every other
// symbol unchanged. A named symbol the list lacks is an error naming it and
// the list, and two attributes renamed to one name are an error naming it:
// one name for two attributes is a selection on their equality, which a
// rename never performs.
func (s Substitution) Rename(symbols []Symbol) ([]Symbol, error) {
	for _, named := range s.from {
		if !ContainsSymbol(symbols, named) {
			return nil, fmt.Errorf("rename: symbol %s not found among the symbols %v", named, symbols)
		}
	}
	renamed := make([]Symbol, len(symbols))
	for i, sym := range symbols {
		if to, ok := s.Of(sym); ok {
			renamed[i] = to
		} else {
			renamed[i] = sym
		}
		if j := SymbolIndex(renamed[:i], renamed[i]); j >= 0 {
			return nil, fmt.Errorf("rename: %s would name both %s and %s", renamed[i], symbols[j], sym)
		}
	}
	return renamed, nil
}
