package query

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// ValidateEntityBinding rejects non-Identity values bound to a data pattern's
// entity position. The entity position is inhabited only by Identity; any
// other value there is a query defect that fails loudly at the user
// boundaries (query-text constants at match entry, :in inputs at query
// entry). Strings become entities by boundary construction (NewIdentity,
// #identity, #id literals), never by comparison-time coercion.
func ValidateEntityBinding(v interface{}) error {
	if v == nil {
		return nil
	}
	if _, ok := v.(datalog.Identity); !ok {
		return fmt.Errorf("data pattern entity position requires an identity, got %T (construct one with NewIdentity or an #identity literal)", v)
	}
	return nil
}

// ValidateAttributeBinding is the attribute-position counterpart of
// ValidateEntityBinding: the attribute position is inhabited only by Keyword.
func ValidateAttributeBinding(v interface{}) error {
	if v == nil {
		return nil
	}
	if _, ok := v.(datalog.Keyword); !ok {
		return fmt.Errorf("data pattern attribute position requires a keyword, got %T (construct one with NewKeyword or a :keyword literal)", v)
	}
	return nil
}

// PositionSymbols returns the sets of symbols that occupy the entity and
// attribute positions of a data pattern anywhere in the query — directly,
// inside not/or containers, or transitively as an argument to a subquery
// whose corresponding :in parameter occupies such a position in the nested
// query. One walk collects both sets.
//
// The entity position is inhabited only by Identity and the attribute
// position only by Keyword, so input values bound to these symbols must have
// those types. Validating that at the input boundary turns a query defect
// into a loud error before execution; interior data flow (values joined from
// other positions) is never validated — a mistyped value there is the
// equality join's ordinary typed non-match.
func PositionSymbols(q *Query) (entity, attribute map[Symbol]bool) {
	entity = make(map[Symbol]bool)
	attribute = make(map[Symbol]bool)
	positionSymbolsInClauses(q.Where, entity, attribute)
	return entity, attribute
}

func positionSymbolsInClauses(clauses []Clause, entity, attribute map[Symbol]bool) {
	for _, clause := range clauses {
		switch c := clause.(type) {
		case *DataPattern:
			if v, ok := c.GetE().(Variable); ok {
				entity[v.Name] = true
			}
			if v, ok := c.GetA().(Variable); ok {
				attribute[v.Name] = true
			}
		case *NotClause:
			positionSymbolsInClauses(c.Clauses, entity, attribute)
		case *NotJoinClause:
			positionSymbolsInClauses(c.Clauses, entity, attribute)
		case *OrClause:
			for _, branch := range c.Branches {
				positionSymbolsInClauses(branch, entity, attribute)
			}
		case *OrJoinClause:
			for _, branch := range c.Branches {
				positionSymbolsInClauses(branch, entity, attribute)
			}
		case *OrDefaultClause:
			for _, branch := range c.Branches {
				positionSymbolsInClauses(branch, entity, attribute)
			}
		case *OrDefaultJoinClause:
			for _, branch := range c.Branches {
				positionSymbolsInClauses(branch, entity, attribute)
			}
		case *SubqueryPattern:
			positionSymbolsFromSubquery(*c, entity, attribute)
		}
	}
}

// positionSymbolsFromSubquery maps the nested query's entity- and
// attribute-position :in parameters back onto the outer argument symbols: an
// outer symbol passed into an inner typed-position parameter is itself bound
// to that position.
func positionSymbolsFromSubquery(p SubqueryPattern, entity, attribute map[Symbol]bool) {
	if p.Query == nil {
		return
	}
	innerEntity, innerAttribute := PositionSymbols(p.Query)
	if len(innerEntity) == 0 && len(innerAttribute) == 0 {
		return
	}

	// The argument list may or may not carry the database markers alongside
	// the value parameters; detect which alignment the arguments use. Any
	// other shape is malformed and maps nothing — the analysis is
	// conservative, and an unmapped symbol degrades to the interior typed
	// non-match rather than the boundary error.
	dbCount := 0
	for _, spec := range p.Query.In {
		if _, ok := spec.(DatabaseInput); ok {
			dbCount++
		}
	}
	includesDBArgs := len(p.Inputs) == len(p.Query.In)
	if !includesDBArgs && len(p.Inputs) != len(p.Query.In)-dbCount {
		return
	}

	argIdx := 0
	for _, spec := range p.Query.In {
		if _, ok := spec.(DatabaseInput); ok {
			if includesDBArgs {
				argIdx++
			}
			continue
		}
		if argIdx >= len(p.Inputs) {
			return
		}
		arg := p.Inputs[argIdx]
		argIdx++

		var innerSym Symbol
		switch s := spec.(type) {
		case ScalarInput:
			innerSym = s.Symbol
		case CollectionInput:
			innerSym = s.Symbol
		default:
			// Tuple/relation parameters receive a single outer value that is
			// itself a tuple or relation, never an entity or attribute.
			continue
		}
		v, ok := arg.(Variable)
		if !ok {
			continue
		}
		if innerEntity[innerSym] {
			entity[v.Name] = true
		}
		if innerAttribute[innerSym] {
			attribute[v.Name] = true
		}
	}
}
