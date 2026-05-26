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
	// Returns (value, true) if the attribute exists, (nil, false) otherwise.
	LookupAttribute(entity datalog.Identity, attr datalog.Keyword) (interface{}, bool)
}

// TypedDefaulter is an optional interface that EntityLookup implementations
// can provide to type-convert default values based on attribute schema.
// This is used by get-else to ensure the default value has the same type
// as the attribute's stored values (e.g., []string instead of []interface{}).
type TypedDefaulter interface {
	TypeDefault(attr datalog.Keyword, defaultVal interface{}) interface{}
}

// DatabaseFunction is a function that requires database access to evaluate.
// Unlike pure Functions, these need read-only access to the database snapshot.
// The database is passed explicitly via the `$` parameter in the syntax.
//
// DatabaseFunctions are still semantically pure - given the same database snapshot,
// entity, and attributes, they always return the same result.
type DatabaseFunction interface {
	Pattern

	// RequiredSymbols returns all symbols needed to evaluate this function
	RequiredSymbols() []Symbol

	// EvalWithLookup evaluates the function with bindings and database access
	EvalWithLookup(bindings map[Symbol]interface{}, lookup EntityLookup) (interface{}, error)

	// ReturnType hints at what type this function returns
	ReturnType() string
}

// GetElseFunction implements the Datomic get-else function.
// Syntax: [(get-else $ ?entity :attribute default-value) ?result]
// Returns the attribute value if it exists, or the default value if missing.
type GetElseFunction struct {
	Entity  Term            // The entity to look up (usually a variable like ?e)
	Attr    datalog.Keyword // The attribute to retrieve
	Default interface{}     // Default value if attribute is missing
}

func (g *GetElseFunction) RequiredSymbols() []Symbol {
	return g.Entity.RequiredSymbols()
}

// Eval is required by the Function interface but should not be called directly.
// Use EvalWithLookup instead, which is called by the executor when it detects
// a DatabaseFunction.
func (g *GetElseFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	return nil, fmt.Errorf("get-else requires database access; use EvalWithLookup instead")
}

func (g *GetElseFunction) EvalWithLookup(bindings map[Symbol]interface{}, lookup EntityLookup) (interface{}, error) {
	// Resolve entity
	entityVal, ok := g.Entity.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("get-else: cannot resolve entity %s", g.Entity)
	}

	// Convert to Identity
	entity, err := toIdentity(entityVal)
	if err != nil {
		return nil, fmt.Errorf("get-else: %w", err)
	}

	// Look up the attribute
	if value, found := lookup.LookupAttribute(entity, g.Attr); found {
		return value, nil
	}

	// Return default value, typed if possible
	if td, ok := lookup.(TypedDefaulter); ok {
		return td.TypeDefault(g.Attr, g.Default), nil
	}
	return g.Default, nil
}

func (g *GetElseFunction) ReturnType() string {
	return "any"
}

func (g *GetElseFunction) String() string {
	return fmt.Sprintf("(get-else $ %s %s %s)", g.Entity, g.Attr, FormatValueEDN(g.Default))
}

// MissingFunction implements the Datomic missing? function.
// Syntax: [(missing? $ ?entity :attribute)]
// Returns true if the entity does NOT have the specified attribute.
// This is a predicate (filter), not a binding expression.
type MissingFunction struct {
	Entity Term            // The entity to check
	Attr   datalog.Keyword // The attribute to check for
}

func (m *MissingFunction) RequiredSymbols() []Symbol {
	return m.Entity.RequiredSymbols()
}

// Eval is required by the Function interface but should not be called directly.
// Use EvalWithLookup instead, which is called by the executor when it detects
// a DatabaseFunction.
func (m *MissingFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	return nil, fmt.Errorf("missing? requires database access; use EvalWithLookup instead")
}

func (m *MissingFunction) EvalWithLookup(bindings map[Symbol]interface{}, lookup EntityLookup) (interface{}, error) {
	// Resolve entity
	entityVal, ok := m.Entity.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("missing?: cannot resolve entity %s", m.Entity)
	}

	// Convert to Identity
	entity, err := toIdentity(entityVal)
	if err != nil {
		return nil, fmt.Errorf("missing?: %w", err)
	}

	// Check if attribute is missing
	_, found := lookup.LookupAttribute(entity, m.Attr)
	return !found, nil
}

func (m *MissingFunction) ReturnType() string {
	return "boolean"
}

func (m *MissingFunction) String() string {
	return fmt.Sprintf("(missing? $ %s %s)", m.Entity, m.Attr)
}

// GetSomeFunction implements the Datomic get-some function.
// Syntax: [(get-some $ ?entity :attr1 :attr2 :attr3) [?found-attr ?value]]
// Returns the first attribute that exists along with its value.
// Useful for fallback chains like "use nickname, else name, else email".
type GetSomeFunction struct {
	Entity Term              // The entity to look up
	Attrs  []datalog.Keyword // Attributes to try, in order
}

// GetSomeResult holds the result of a get-some evaluation.
// Found=false signals "no attribute matched" and is the canonical way for
// consumers to drop the tuple — get-some no longer uses error-as-signal
// for this (which conflated soft "no match" with real eval failures and
// forced upstream loops to swallow every eval error to make it work).
type GetSomeResult struct {
	Attr  datalog.Keyword
	Value interface{}
	Found bool
}

func (gs *GetSomeFunction) RequiredSymbols() []Symbol {
	return gs.Entity.RequiredSymbols()
}

// Eval is required by the Function interface but should not be called directly.
// Use EvalWithLookup instead, which is called by the executor when it detects
// a DatabaseFunction.
func (gs *GetSomeFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	return nil, fmt.Errorf("get-some requires database access; use EvalWithLookup instead")
}

func (gs *GetSomeFunction) EvalWithLookup(bindings map[Symbol]interface{}, lookup EntityLookup) (interface{}, error) {
	// Resolve entity
	entityVal, ok := gs.Entity.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("get-some: cannot resolve entity %s", gs.Entity)
	}

	// Convert to Identity
	entity, err := toIdentity(entityVal)
	if err != nil {
		return nil, fmt.Errorf("get-some: %w", err)
	}

	// Try each attribute in order
	for _, attr := range gs.Attrs {
		if value, found := lookup.LookupAttribute(entity, attr); found {
			return &GetSomeResult{Attr: attr, Value: value, Found: true}, nil
		}
	}

	// No attribute found — return Found=false so the caller can drop the
	// tuple without abusing error as a soft signal.
	return &GetSomeResult{Found: false}, nil
}

func (gs *GetSomeFunction) ReturnType() string {
	return "tuple" // Returns [attr value] pair
}

func (gs *GetSomeFunction) String() string {
	attrs := ""
	for i, attr := range gs.Attrs {
		if i > 0 {
			attrs += " "
		}
		attrs += attr.String()
	}
	return fmt.Sprintf("(get-some $ %s %s)", gs.Entity, attrs)
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
// Use EvalWithLookup instead via the executor.
func (p *DatabaseFunctionPredicate) Eval(bindings map[Symbol]interface{}) (bool, error) {
	return false, fmt.Errorf("database function predicate requires database access; use EvalWithLookup instead")
}

// EvalWithLookup evaluates the predicate with database access
func (p *DatabaseFunctionPredicate) EvalWithLookup(bindings map[Symbol]interface{}, lookup EntityLookup) (bool, error) {
	result, err := p.Function.EvalWithLookup(bindings, lookup)
	if err != nil {
		return false, err
	}
	// The function should return a boolean
	if b, ok := result.(bool); ok {
		return b, nil
	}
	return false, fmt.Errorf("database function predicate expected boolean result, got %T", result)
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

func (p *DatabaseFunctionPredicate) String() string {
	return p.Function.String()
}
