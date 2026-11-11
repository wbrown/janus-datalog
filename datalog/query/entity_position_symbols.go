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
// attribute and transaction positions of a data pattern anywhere in the
// query, read under the scoping the runtime evaluates the text in: directly;
// inside a headerless not or or, which shares a variable with its scope by
// name; inside a header form, whose header is its whole interface, so a
// branch variable the header does not name is the branch's own whatever its
// name; as a pattern's later occurrence of a variable, which a prepared body
// writes as the variable's UnifiedVariable; transitively as an argument to a
// subquery whose corresponding :in parameter occupies such a position in the
// nested query; or as an argument to a rule invocation whose corresponding
// head position a pattern of the predicate's bodies occupies, directly,
// through a rule a body invokes, or through a subquery a body runs. rules is
// the set the query's invocations resolve against, nil for a query declaring
// no %. The walk reads the query's own text and the bodies of the predicates
// its invocations reach at any depth, the text the query evaluates, and no
// other rule of the set. One walk collects the three sets.
//
// The entity position is inhabited only by Identity, the attribute position
// only by Keyword and the transaction position only by ElementID, so input
// values bound to these symbols must have those types. Validating that at the
// input boundary turns a query defect into a loud error before execution;
// interior data flow (values joined from other positions) is never validated
// — a mistyped value there is the equality join's ordinary typed non-match. A
// constant written as a subquery's or an invocation's argument binds into
// the position its parameter or head occupies as an input does, so the walk
// refuses one of another type through the position's validator, naming the
// clause and the argument; that refusal is the error. A query naming a
// variable in a namespace the engine mints into is refused first, as
// *EngineNamespaceError, the refusal Prepare makes: the walk reads a unified
// occurrence as its origin, and a variable spelled as one in the query's own
// text is the user's, not the engine's.
func PositionSymbols(q *Query, rules *RuleSet) (entity, attribute, transaction map[Symbol]bool, err error) {
	if v := engineVariable(q, rules); v != nil {
		return nil, nil, nil, &EngineNamespaceError{Variable: v}
	}
	var roots []Symbol
	for _, inv := range Invocations(q.Where) {
		roots = append(roots, inv.Name)
	}
	typing, err := predicatePositions(rules, roots)
	if err != nil {
		return nil, nil, nil, err
	}
	return positionSymbolsOf(q, typing)
}

// positionSymbolsOf is PositionSymbols over a query under a rule set's
// positionTyping table.
func positionSymbolsOf(q *Query, typing map[Symbol]positionTyping) (entity, attribute, transaction map[Symbol]bool, err error) {
	entity = make(map[Symbol]bool)
	attribute = make(map[Symbol]bool)
	transaction = make(map[Symbol]bool)
	if err := positionSymbolsInClauses(q.Where, typing, entity, attribute, transaction); err != nil {
		return nil, nil, nil, err
	}
	return entity, attribute, transaction, nil
}

// positionTyping is, per head position of a predicate, whether a pattern of
// the predicate's bodies occupies the entity, attribute or transaction
// position with that position's head variable.
type positionTyping struct {
	entity, attribute, transaction []bool
}

// predicatePositions is the positionTyping of each predicate the roots reach
// under rules (RuleSet.reach), the predicates whose bodies a query invoking
// the roots evaluates; nil for no rules. It is the least fixpoint over them:
// a body's invocation reads the table as it stands, so a position reached
// through another predicate, or through the predicate's own recursion, is
// marked once that predicate's pass marks it, and the passes repeat until
// none marks a new position. A constant argument of another type at a
// position the table holds typed is refused as a pass meets it, naming the
// rule.
func predicatePositions(rules *RuleSet, roots []Symbol) (map[Symbol]positionTyping, error) {
	if rules == nil {
		return nil, nil
	}
	reached := rules.reach(roots)
	typing := make(map[Symbol]positionTyping, len(reached))
	for _, name := range rules.Predicates() {
		if !reached[name] {
			continue
		}
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
			if !reached[name] {
				continue
			}
			for _, rule := range rules.Rules(name) {
				entity := make(map[Symbol]bool)
				attribute := make(map[Symbol]bool)
				transaction := make(map[Symbol]bool)
				if err := positionSymbolsInClauses(rule.Body, typing, entity, attribute, transaction); err != nil {
					return nil, fmt.Errorf("rule %s: %w", name, err)
				}
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
	return typing, nil
}

// positionSymbolsInClauses marks in the three sets the variables clauses bind
// into the typed positions of the scope they stand in, as the runtime scopes
// them: a pattern's variable at a position, a unified later occurrence
// standing for its origin; a subquery's or an invocation's argument through
// the position its parameter or head occupies; a headerless form's branches
// by name, since such a form shares with its scope by name; and a header
// form's branches through the header alone. A constant argument of another
// type at a typed position is the error.
func positionSymbolsInClauses(clauses []Clause, typing map[Symbol]positionTyping, entity, attribute, transaction map[Symbol]bool) error {
	var err error
	WalkClauses(clauses, func(clause Clause) bool {
		if err != nil {
			return false
		}
		switch c := clause.(type) {
		case *DataPattern:
			if v, ok := c.GetE().(Variable); ok {
				bindPosition(entity, v.Name)
			}
			if v, ok := c.GetA().(Variable); ok {
				bindPosition(attribute, v.Name)
			}
			if v, ok := c.GetT().(Variable); ok {
				bindPosition(transaction, v.Name)
			}
		case *SubqueryPattern:
			// The subquery's inner positions map back through its argument
			// list, not through a direct walk of its :where.
			err = positionSymbolsFromSubquery(*c, typing, entity, attribute, transaction)
		case *RuleInvocation:
			err = positionSymbolsFromInvocation(c, typing, entity, attribute, transaction)
		case *NotJoinClause:
			err = positionSymbolsUnderHeader(c.RequiredVars, nil, [][]Clause{c.Clauses}, typing, entity, attribute, transaction)
			return false
		case *OrJoinClause:
			err = positionSymbolsUnderHeader(c.RequiredVars, c.OutputVars, c.Branches, typing, entity, attribute, transaction)
			return false
		case *OrDefaultJoinClause:
			err = positionSymbolsUnderHeader(c.RequiredVars, c.OutputVars, c.Branches, typing, entity, attribute, transaction)
			return false
		}
		return true
	})
	return err
}

// bindPosition marks sym as bound into the position set, a unified later
// occurrence as the variable it was written for.
func bindPosition(set map[Symbol]bool, sym Symbol) {
	if origin, ok := UnifiedOrigin(sym); ok {
		sym = origin
	}
	set[sym] = true
}

// positionSymbolsUnderHeader walks a header form's branches as the scopes
// they are: each branch binds into sets of its own, and the header's
// variables alone carry their positions out, since a branch variable the
// header does not name is the branch's own whatever its name.
func positionSymbolsUnderHeader(required, outputs []Symbol, branches [][]Clause, typing map[Symbol]positionTyping, entity, attribute, transaction map[Symbol]bool) error {
	innerEntity := make(map[Symbol]bool)
	innerAttribute := make(map[Symbol]bool)
	innerTransaction := make(map[Symbol]bool)
	for _, branch := range branches {
		if err := positionSymbolsInClauses(branch, typing, innerEntity, innerAttribute, innerTransaction); err != nil {
			return err
		}
	}
	for _, header := range [][]Symbol{required, outputs} {
		for _, sym := range header {
			if innerEntity[sym] {
				entity[sym] = true
			}
			if innerAttribute[sym] {
				attribute[sym] = true
			}
			if innerTransaction[sym] {
				transaction[sym] = true
			}
		}
	}
	return nil
}

// validatePositionConstant refuses value where it binds into a position
// whose type it is not, through that position's validator.
func validatePositionConstant(atEntity, atAttribute, atTransaction bool, value interface{}) error {
	if atEntity {
		if err := ValidateEntityBinding(value); err != nil {
			return err
		}
	}
	if atAttribute {
		if err := ValidateAttributeBinding(value); err != nil {
			return err
		}
	}
	if atTransaction {
		if err := ValidateTransactionBinding(value); err != nil {
			return err
		}
	}
	return nil
}

// constantValue is the value a constant argument carries, a vector literal's
// elements for a VectorConstant, and false for a variable or a blank.
func constantValue(elem PatternElement) (interface{}, bool) {
	switch e := elem.(type) {
	case Constant:
		return e.Value, true
	case VectorConstant:
		return e.Values, true
	}
	return nil, false
}

// positionSymbolsFromInvocation maps the predicate's typed head positions
// onto the invocation's arguments: a variable argument at a position a
// pattern of the predicate's bodies occupies is itself bound to that
// position, and a constant argument there is refused when it is not of the
// position's type, naming the invocation and the argument counted from one.
// A predicate the table does not hold, and an argument past the head, map
// nothing; the invocation check refuses both when the query runs.
func positionSymbolsFromInvocation(inv *RuleInvocation, typing map[Symbol]positionTyping, entity, attribute, transaction map[Symbol]bool) error {
	t, ok := typing[inv.Name]
	if !ok {
		return nil
	}
	for i := 0; i < len(inv.Args) && i < len(t.entity); i++ {
		if v, ok := inv.Args[i].(Variable); ok {
			if t.entity[i] {
				entity[v.Name] = true
			}
			if t.attribute[i] {
				attribute[v.Name] = true
			}
			if t.transaction[i] {
				transaction[v.Name] = true
			}
			continue
		}
		if value, ok := constantValue(inv.Args[i]); ok {
			if err := validatePositionConstant(t.entity[i], t.attribute[i], t.transaction[i], value); err != nil {
				return fmt.Errorf("%s: argument %d: %w", inv, i+1, err)
			}
		}
	}
	return nil
}

// positionSymbolsFromSubquery maps the nested query's entity-, attribute- and
// transaction-position :in symbols back onto the call's arguments, aligned by
// the inner :in's call positions (InputPositions): one per source, rules,
// scalar and collection entry and one per symbol of a tuple or relation
// entry. An outer symbol passed at a position whose symbol occupies a typed
// position is itself bound to that position, and a constant passed there is
// refused when it is not of the position's type, naming the subquery and the
// argument counted from one. A collection parameter binds a vector literal's
// elements one each, so each is read against the parameter's positions, and
// a constant of another shape there binds nothing and is read as nothing,
// since SubqueryPattern.Validate refuses the call. A call passing other than
// one argument per position maps the positions it has arguments for;
// SubqueryPattern.Validate refuses the call when the query runs.
func positionSymbolsFromSubquery(p SubqueryPattern, typing map[Symbol]positionTyping, entity, attribute, transaction map[Symbol]bool) error {
	if p.Query == nil {
		return nil
	}
	innerEntity, innerAttribute, innerTransaction, err := positionSymbolsOf(p.Query, typing)
	if err != nil {
		return err
	}
	positions := InputPositions(p.Query.In)
	for i := 0; i < len(positions) && i < len(p.Inputs); i++ {
		inner := positions[i].Symbol
		if inner == nil {
			continue
		}
		arg := p.Inputs[i]
		if v, ok := arg.(Variable); ok {
			if innerEntity[inner] {
				entity[v.Name] = true
			}
			if innerAttribute[inner] {
				attribute[v.Name] = true
			}
			if innerTransaction[inner] {
				transaction[v.Name] = true
			}
			continue
		}
		value, ok := constantValue(arg)
		if !ok {
			continue
		}
		bound := []interface{}{value}
		if _, collection := positions[i].Spec.(CollectionInput); collection {
			vector, isVector := arg.(VectorConstant)
			if !isVector {
				continue
			}
			bound = vector.Values
		}
		for _, element := range bound {
			if err := validatePositionConstant(innerEntity[inner], innerAttribute[inner], innerTransaction[inner], element); err != nil {
				return fmt.Errorf("%s: argument %d: %w", p, i+1, err)
			}
		}
	}
	return nil
}
