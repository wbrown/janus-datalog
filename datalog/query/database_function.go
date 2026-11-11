package query

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// EntityLookup is the interface for looking up entity attributes in the database.
// This is a focused interface for database functions, avoiding circular dependencies
// with the full PatternMatcher interface.
type EntityLookup interface {
	// LookupAttribute retrieves the value of an attribute for an entity.
	// An absent attribute returns (nil, false, nil).
	LookupAttribute(entity datalog.Identity, attr datalog.Keyword) (interface{}, bool, error)
	// AttributeLookup returns the lookup of attr, which answers at each entity
	// what LookupAttribute answers for that entity and attr. The source reads
	// what attr's definition decides once, when it returns the lookup. A
	// source that cannot look up is an error.
	AttributeLookup(attr datalog.Keyword) (AttributeLookup, error)
}

// AttributeLookup answers an entity's value of the one attribute it was made
// for, as EntityLookup.LookupAttribute answers it.
type AttributeLookup func(entity datalog.Identity) (value interface{}, found bool, err error)

// TypedDefaulter is an optional interface that EntityLookup implementations
// can provide to type-convert default values based on attribute schema.
// This is used by get-else to ensure the default value has the same type
// as the attribute's stored values (e.g., []string instead of []interface{}).
type TypedDefaulter interface {
	TypeDefault(attr datalog.Keyword, defaultVal interface{}) interface{}
}

// DatabaseFunction is a function that requires database access to evaluate.
// Unlike pure Functions, these need read-only access to the database snapshot.
// The source the function reads is its first argument in the syntax: $ or a
// named source, as in Datomic's grammar.
//
// DatabaseFunctions are still semantically pure - given the same database snapshot,
// entity, and attributes, they always return the same result.
type DatabaseFunction interface {
	Function

	// ReadsFrom returns the source the function reads: the source its first
	// argument names, and $ for a function built with no source.
	ReadsFrom() Symbol

	// OnSource returns the function reading source, its other parts kept.
	OnSource(source Symbol) DatabaseFunction

	// EvaluationThrough returns the function's evaluation through lookup.
	EvaluationThrough(lookup EntityLookup) (Evaluation, error)
}

// Evaluation answers a database function's value on a tuple's bindings.
type Evaluation func(bindings map[Symbol]interface{}) (interface{}, error)

// readsFrom is the source a database function's Source field names, nil
// standing for $ as it does on a data pattern.
func readsFrom(source Symbol) Symbol {
	if source == nil {
		return datalog.SymDollar
	}
	return source
}

// OnSource returns the get-else reading source, its other parts kept.
func (g *GetElseFunction) OnSource(source Symbol) DatabaseFunction {
	return &GetElseFunction{Source: source, Entity: g.Entity, Attr: g.Attr, Default: g.Default}
}

// OnSource returns the missing? reading source, its other parts kept.
func (m *MissingFunction) OnSource(source Symbol) DatabaseFunction {
	return &MissingFunction{Source: source, Entity: m.Entity, Attr: m.Attr}
}

// OnSource returns the get-some reading source, its other parts kept.
func (gs *GetSomeFunction) OnSource(source Symbol) DatabaseFunction {
	return &GetSomeFunction{Source: source, Entity: gs.Entity, Attrs: gs.Attrs}
}

// GetElseFunction implements the Datomic get-else function.
// Syntax: [(get-else $src ?entity :attribute default) ?result]
// Returns the attribute value the source holds, or the default if missing.
// The default is a function argument, a variable or a constant, read from
// each tuple's bindings as the entity is.
type GetElseFunction struct {
	Source  Symbol          // The source the function reads; nil stands for $
	Entity  Term            // The entity to look up (usually a variable like ?e)
	Attr    datalog.Keyword // The attribute to retrieve
	Default Term            // The value where the entity lacks the attribute
}

// RequiredSymbols returns the entity's and the default's variables.
func (g *GetElseFunction) RequiredSymbols() []Symbol {
	symbols := append([]Symbol{}, g.Entity.RequiredSymbols()...)
	return append(symbols, g.Default.RequiredSymbols()...)
}

// ReadsFrom returns the source the get-else reads.
func (g *GetElseFunction) ReadsFrom() Symbol {
	return readsFrom(g.Source)
}

// Eval is required by the Function interface but should not be called directly.
// The executor evaluates a DatabaseFunction through EvaluationThrough.
func (g *GetElseFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	return nil, fmt.Errorf("get-else requires database access; use EvaluationThrough instead")
}

// EvaluationThrough returns the get-else's evaluation through lookup: the
// attribute's value at the tuple's entity, or the default the tuple's
// bindings give, typed by the lookup when it types defaults.
func (g *GetElseFunction) EvaluationThrough(lookup EntityLookup) (Evaluation, error) {
	attrLookup, err := lookup.AttributeLookup(g.Attr)
	if err != nil {
		return nil, err
	}
	defaulter, typed := lookup.(TypedDefaulter)
	return func(bindings map[Symbol]interface{}) (interface{}, error) {
		entityVal, ok := g.Entity.Resolve(bindings)
		if !ok {
			return nil, fmt.Errorf("get-else: cannot resolve entity %s", g.Entity)
		}
		entity, err := toIdentity(entityVal)
		if err != nil {
			return nil, fmt.Errorf("get-else: %w", err)
		}
		value, found, err := attrLookup(entity)
		if err != nil {
			return nil, err
		}
		if found {
			return value, nil
		}
		defaultVal, ok := g.Default.Resolve(bindings)
		if !ok {
			return nil, fmt.Errorf("get-else: cannot resolve default %s", g.Default)
		}
		if typed {
			return defaulter.TypeDefault(g.Attr, defaultVal), nil
		}
		return defaultVal, nil
	}, nil
}

func (g *GetElseFunction) String() string {
	return fmt.Sprintf("(get-else %s %s %s %s)", g.ReadsFrom(), g.Entity, g.Attr, g.Default)
}

// MissingFunction implements the Datomic missing? function.
// Syntax: [(missing? $src ?entity :attribute)]
// Returns true if the entity does NOT have the specified attribute in the source.
// This is a predicate (filter), not a binding expression.
type MissingFunction struct {
	Source Symbol          // The source the function reads; nil stands for $
	Entity Term            // The entity to check
	Attr   datalog.Keyword // The attribute to check for
}

func (m *MissingFunction) RequiredSymbols() []Symbol {
	return m.Entity.RequiredSymbols()
}

// ReadsFrom returns the source the missing? reads.
func (m *MissingFunction) ReadsFrom() Symbol {
	return readsFrom(m.Source)
}

// Eval is required by the Function interface but should not be called directly.
// The executor evaluates a DatabaseFunction through EvaluationThrough.
func (m *MissingFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	return nil, fmt.Errorf("missing? requires database access; use EvaluationThrough instead")
}

// EvaluationThrough returns the missing?'s evaluation through lookup: true
// where the tuple's entity has no value of the attribute.
func (m *MissingFunction) EvaluationThrough(lookup EntityLookup) (Evaluation, error) {
	attrLookup, err := lookup.AttributeLookup(m.Attr)
	if err != nil {
		return nil, err
	}
	return func(bindings map[Symbol]interface{}) (interface{}, error) {
		entityVal, ok := m.Entity.Resolve(bindings)
		if !ok {
			return nil, fmt.Errorf("missing?: cannot resolve entity %s", m.Entity)
		}
		entity, err := toIdentity(entityVal)
		if err != nil {
			return nil, fmt.Errorf("missing?: %w", err)
		}
		_, found, err := attrLookup(entity)
		if err != nil {
			return nil, err
		}
		return !found, nil
	}, nil
}

func (m *MissingFunction) String() string {
	return fmt.Sprintf("(missing? %s %s %s)", m.ReadsFrom(), m.Entity, m.Attr)
}

// GetSomeFunction implements the Datomic get-some function.
// Syntax: [(get-some $src ?entity :attr1 :attr2 :attr3) [?found-attr ?value]]
// Returns the first attribute that exists along with its value.
// Useful for fallback chains like "use nickname, else name, else email".
type GetSomeFunction struct {
	Source Symbol            // The source the function reads; nil stands for $
	Entity Term              // The entity to look up
	Attrs  []datalog.Keyword // Attributes to try, in order
}

// GetSomeResult holds the result of a get-some evaluation.
// Found=false signals "no attribute matched" and is the canonical way for
// consumers to drop the tuple.
type GetSomeResult struct {
	Attr  datalog.Keyword
	Value interface{}
	Found bool
}

func (gs *GetSomeFunction) RequiredSymbols() []Symbol {
	return gs.Entity.RequiredSymbols()
}

// ReadsFrom returns the source the get-some reads.
func (gs *GetSomeFunction) ReadsFrom() Symbol {
	return readsFrom(gs.Source)
}

// Eval is required by the Function interface but should not be called directly.
// The executor evaluates a DatabaseFunction through EvaluationThrough.
func (gs *GetSomeFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	return nil, fmt.Errorf("get-some requires database access; use EvaluationThrough instead")
}

// EvaluationThrough returns the get-some's evaluation through lookup: the
// first of its attributes, in order, the tuple's entity has a value of, with
// that value.
func (gs *GetSomeFunction) EvaluationThrough(lookup EntityLookup) (Evaluation, error) {
	attrLookups := make([]AttributeLookup, len(gs.Attrs))
	for i, attr := range gs.Attrs {
		attrLookup, err := lookup.AttributeLookup(attr)
		if err != nil {
			return nil, err
		}
		attrLookups[i] = attrLookup
	}
	return func(bindings map[Symbol]interface{}) (interface{}, error) {
		entityVal, ok := gs.Entity.Resolve(bindings)
		if !ok {
			return nil, fmt.Errorf("get-some: cannot resolve entity %s", gs.Entity)
		}
		entity, err := toIdentity(entityVal)
		if err != nil {
			return nil, fmt.Errorf("get-some: %w", err)
		}
		for i, attrLookup := range attrLookups {
			value, found, err := attrLookup(entity)
			if err != nil {
				return nil, err
			}
			if found {
				return &GetSomeResult{Attr: gs.Attrs[i], Value: value, Found: true}, nil
			}
		}
		// No attribute found — return Found=false so the caller can drop the
		// tuple without abusing error as a soft signal.
		return &GetSomeResult{Found: false}, nil
	}, nil
}

func (gs *GetSomeFunction) String() string {
	attrs := ""
	for i, attr := range gs.Attrs {
		if i > 0 {
			attrs += " "
		}
		attrs += attr.String()
	}
	return fmt.Sprintf("(get-some %s %s %s)", gs.ReadsFrom(), gs.Entity, attrs)
}

// toIdentity converts interface{} to Identity
func toIdentity(val interface{}) (datalog.Identity, error) {
	switch v := val.(type) {
	case datalog.Identity:
		if v == nil {
			return nil, fmt.Errorf("nil identity")
		}
		return v, nil
	case string:
		// Allow string conversion for convenience
		return datalog.NewIdentity(v), nil
	default:
		return nil, fmt.Errorf("expected Identity, got %T", val)
	}
}

// DatabaseFunctionPredicate wraps a DatabaseFunction for use as a filter predicate.
// This is used when database functions like missing? are used without a binding,
// e.g., [(missing? $ ?e :attr)] filters to tuples where the condition is true.
type DatabaseFunctionPredicate struct {
	Function DatabaseFunction
}

// DatabasePredicateLookup is an interface that matchers can implement to provide
// entity lookup capability for predicate evaluation.
type DatabasePredicateLookup interface {
	EntityLookup
}

// RequiredSymbols returns all symbols needed by the wrapped function
func (p *DatabaseFunctionPredicate) RequiredSymbols() []Symbol {
	return p.Function.RequiredSymbols()
}

// Eval is required by the Predicate interface but cannot be called without database access.
// The executor evaluates the predicate through EvaluationThrough.
func (p *DatabaseFunctionPredicate) Eval(bindings map[Symbol]interface{}) (bool, error) {
	return false, fmt.Errorf("database function predicate requires database access; use EvaluationThrough instead")
}

// PredicateEvaluation answers whether a tuple's bindings pass a
// database-function predicate.
type PredicateEvaluation func(bindings map[Symbol]interface{}) (bool, error)

// EvaluationThrough returns the predicate's evaluation through lookup: its
// function's value on the tuple's bindings, which must be a boolean.
func (p *DatabaseFunctionPredicate) EvaluationThrough(lookup EntityLookup) (PredicateEvaluation, error) {
	evaluation, err := p.Function.EvaluationThrough(lookup)
	if err != nil {
		return nil, err
	}
	return func(bindings map[Symbol]interface{}) (bool, error) {
		result, err := evaluation(bindings)
		if err != nil {
			return false, err
		}
		if b, ok := result.(bool); ok {
			return b, nil
		}
		return false, fmt.Errorf("database function predicate expected boolean result, got %T", result)
	}, nil
}

// CanPushToStorage returns false - database function predicates cannot be pushed to storage
func (p *DatabaseFunctionPredicate) CanPushToStorage() bool {
	return false
}

// Selectivity returns an estimate of what fraction of tuples pass the predicate
// Database function predicates typically filter most tuples, so return a low value
func (p *DatabaseFunctionPredicate) Selectivity() float64 {
	return 0.1 // Assume 10% of entities are missing the attribute
}

// clause implements the Clause interface marker method
func (p *DatabaseFunctionPredicate) clause() {}

// String renders the predicate as the bracketed clause the parser reads a
// predicate by, [(missing? $ ?e :attr)].
func (p *DatabaseFunctionPredicate) String() string {
	return "[" + p.Function.String() + "]"
}
