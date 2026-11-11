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

// ValidateTransactionBinding is the transaction-position counterpart of
// ValidateEntityBinding: the transaction position is inhabited only by
// ElementID, given as a value or through a pointer. An integer names no
// transaction, since the Lamport is one component of an ElementID.
func ValidateTransactionBinding(v interface{}) error {
	if v == nil {
		return nil
	}
	if _, ok := datalog.DerefElementID(v); !ok {
		return fmt.Errorf("data pattern transaction position requires an ElementID, got %T (construct one with datalog.ElementID or an #eid [lamport replica] literal)", v)
	}
	return nil
}

// PositionSymbols returns the sets of symbols that occupy the entity,
// attribute and transaction positions of a data pattern anywhere in the query
// — directly, inside not/or containers, transitively as an argument to a
// subquery whose corresponding :in parameter occupies such a position in the
// nested query, or as an argument to a rule invocation whose corresponding
// head position a pattern of the predicate's bodies occupies, directly,
// through a rule a body invokes, or through a subquery a body runs. rules is
// the set the query's invocations resolve against, nil for a query declaring
// no %. One walk collects the three sets.
//
// The entity position is inhabited only by Identity, the attribute position
// only by Keyword and the transaction position only by ElementID, so input
// values bound to these symbols must have those types. Validating that at the
// input boundary turns a query defect into a loud error before execution;
// interior data flow (values joined from other positions) is never validated
// — a mistyped value there is the equality join's ordinary typed non-match.
func PositionSymbols(q *Query, rules *RuleSet) (entity, attribute, transaction map[Symbol]bool) {
	return positionSymbolsOf(q, predicatePositions(rules))
}

// positionSymbolsOf is PositionSymbols over a query under a rule set's
// positionTyping table.
func positionSymbolsOf(q *Query, typing map[Symbol]positionTyping) (entity, attribute, transaction map[Symbol]bool) {
	entity = make(map[Symbol]bool)
	attribute = make(map[Symbol]bool)
	transaction = make(map[Symbol]bool)
	positionSymbolsInClauses(q.Where, typing, entity, attribute, transaction)
	return entity, attribute, transaction
}

// positionTyping is, per head position of a predicate, whether a pattern of
// the predicate's bodies occupies the entity, attribute or transaction
// position with that position's head variable.
type positionTyping struct {
	entity, attribute, transaction []bool
}

// predicatePositions is each predicate's positionTyping under rules, nil for
// no rules. It is the least fixpoint over the set: a body's invocation reads
// the table as it stands, so a position reached through another predicate, or
// through the predicate's own recursion, is marked once that predicate's pass
// marks it, and the passes repeat until none marks a new position.
func predicatePositions(rules *RuleSet) map[Symbol]positionTyping {
	if rules == nil {
		return nil
	}
	typing := make(map[Symbol]positionTyping)
	for _, name := range rules.Predicates() {
		head, _ := rules.Head(name)
		typing[name] = positionTyping{
			entity:      make([]bool, len(head)),
			attribute:   make([]bool, len(head)),
			transaction: make([]bool, len(head)),
		}
	}
	for changed := true; changed; {
		changed = false
		for _, name := range rules.Predicates() {
			for _, rule := range rules.Rules(name) {
				entity := make(map[Symbol]bool)
				attribute := make(map[Symbol]bool)
				transaction := make(map[Symbol]bool)
				positionSymbolsInClauses(rule.Body, typing, entity, attribute, transaction)
				t := typing[name]
				for i, head := range rule.Head {
					if entity[head] && !t.entity[i] {
						t.entity[i] = true
						changed = true
					}
					if attribute[head] && !t.attribute[i] {
						t.attribute[i] = true
						changed = true
					}
					if transaction[head] && !t.transaction[i] {
						t.transaction[i] = true
						changed = true
					}
				}
			}
		}
	}
	return typing
}

func positionSymbolsInClauses(clauses []Clause, typing map[Symbol]positionTyping, entity, attribute, transaction map[Symbol]bool) {
	WalkClauses(clauses, func(clause Clause) bool {
		switch c := clause.(type) {
		case *DataPattern:
			if v, ok := c.GetE().(Variable); ok {
				entity[v.Name] = true
			}
			if v, ok := c.GetA().(Variable); ok {
				attribute[v.Name] = true
			}
			if v, ok := c.GetT().(Variable); ok {
				transaction[v.Name] = true
			}
		case *SubqueryPattern:
			// The subquery's inner positions map back through its argument
			// list, not through a direct walk of its :where.
			positionSymbolsFromSubquery(*c, typing, entity, attribute, transaction)
		case *RuleInvocation:
			positionSymbolsFromInvocation(c, typing, entity, attribute, transaction)
		}
		return true
	})
}

// positionSymbolsFromInvocation maps the predicate's typed head positions
// onto the invocation's variable arguments: an argument at a position a
// pattern of the predicate's bodies occupies is itself bound to that
// position. A predicate the table does not hold, and an argument past the
// head, map nothing; the invocation check refuses both when the query runs.
func positionSymbolsFromInvocation(inv *RuleInvocation, typing map[Symbol]positionTyping, entity, attribute, transaction map[Symbol]bool) {
	t, ok := typing[inv.Name]
	if !ok {
		return
	}
	for i := 0; i < len(inv.Args) && i < len(t.entity); i++ {
		v, ok := inv.Args[i].(Variable)
		if !ok {
			continue
		}
		if t.entity[i] {
			entity[v.Name] = true
		}
		if t.attribute[i] {
			attribute[v.Name] = true
		}
		if t.transaction[i] {
			transaction[v.Name] = true
		}
	}
}

// positionSymbolsFromSubquery maps the nested query's entity-, attribute- and
// transaction-position :in parameters back onto the outer argument symbols:
// an outer symbol passed into an inner typed-position parameter is itself
// bound to that position.
func positionSymbolsFromSubquery(p SubqueryPattern, typing map[Symbol]positionTyping, entity, attribute, transaction map[Symbol]bool) {
	if p.Query == nil {
		return
	}
	innerEntity, innerAttribute, innerTransaction := positionSymbolsOf(p.Query, typing)
	if len(innerEntity) == 0 && len(innerAttribute) == 0 && len(innerTransaction) == 0 {
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
		if innerTransaction[innerSym] {
			transaction[v.Name] = true
		}
	}
}
