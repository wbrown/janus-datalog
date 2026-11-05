package qb

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// GetElseBuilder builds a get-else expression for retrieving an attribute
// with a default value if the attribute is missing.
type GetElseBuilder struct {
	source      query.Symbol
	entity      *Var
	attr        Attr
	defaultTerm query.Term
}

// GetElse creates a get-else expression for default values.
// Returns the attribute value if it exists, or the default if missing. The
// default is a value or a variable, read from each tuple's bindings.
//
// Example:
//
//	nickname := qb.NewVar("nickname")
//	qb.GetElse(entity, PersonNickname, "Anonymous").As(nickname)
//	// [(get-else $ ?entity :person/nickname "Anonymous") ?nickname]
//
//	fallback := qb.NewVar("fallback")
//	qb.GetElse(entity, PersonNickname, fallback).As(nickname)
//	// [(get-else $ ?entity :person/nickname ?fallback) ?nickname]
func GetElse(entity *Var, attr Attr, defaultVal interface{}) *GetElseBuilder {
	return &GetElseBuilder{entity: entity, attr: attr, defaultTerm: toTerm(defaultVal)}
}

// GetElseFrom creates a get-else expression reading the named source.
//
// Example:
//
//	build := qb.Source("$build")
//	qb.GetElseFrom(build, run, RunFailed, "").As(failed)
//	// [(get-else $build ?run :run/failed "") ?failed]
func GetElseFrom(source *sourceInput, entity *Var, attr Attr, defaultVal interface{}) *GetElseBuilder {
	return &GetElseBuilder{source: source.sym, entity: entity, attr: attr, defaultTerm: toTerm(defaultVal)}
}

// As binds the result of get-else to a variable.
func (g *GetElseBuilder) As(result *Var) *Expression {
	return &Expression{
		fn: &query.GetElseFunction{
			Source:  g.source,
			Entity:  query.VariableTerm{Symbol: g.entity.Symbol()},
			Attr:    g.attr.Keyword(),
			Default: g.defaultTerm,
		},
		binding: result,
	}
}

// MissingBuilder builds a missing? expression/predicate.
// Can be used as a predicate (filter) or as an expression (bind bool).
type MissingBuilder struct {
	source query.Symbol
	entity *Var
	attr   Attr
}

// Missing creates a missing? check for attribute absence.
//
// As a predicate (filter):
//
//	qb.Missing(entity, PersonEmail)
//	// [(missing? $ ?entity :person/email)]
//
// As an expression (bind boolean):
//
//	needsEmail := qb.NewVar("needsEmail")
//	qb.Missing(entity, PersonEmail).As(needsEmail)
//	// [(missing? $ ?entity :person/email) ?needs-email]
func Missing(entity *Var, attr Attr) *MissingBuilder {
	return &MissingBuilder{entity: entity, attr: attr}
}

// MissingFrom creates a missing? check reading the named source.
//
// Example:
//
//	build := qb.Source("$build")
//	qb.MissingFrom(build, run, RunFailed)
//	// [(missing? $build ?run :run/failed)]
func MissingFrom(source *sourceInput, entity *Var, attr Attr) *MissingBuilder {
	return &MissingBuilder{source: source.sym, entity: entity, attr: attr}
}

// toClause implements Clause for predicate usage (no binding).
// When used without .As(), Missing acts as a filter predicate.
func (m *MissingBuilder) toClause() query.Clause {
	return &query.DatabaseFunctionPredicate{
		Function: &query.MissingFunction{
			Source: m.source,
			Entity: query.VariableTerm{Symbol: m.entity.Symbol()},
			Attr:   m.attr.Keyword(),
		},
	}
}

// As binds the boolean result (true if missing, false if present) to a variable.
func (m *MissingBuilder) As(result *Var) *Expression {
	return &Expression{
		fn: &query.MissingFunction{
			Source: m.source,
			Entity: query.VariableTerm{Symbol: m.entity.Symbol()},
			Attr:   m.attr.Keyword(),
		},
		binding: result,
	}
}

// GetSomeBuilder builds a get-some expression for fallback attribute chains.
type GetSomeBuilder struct {
	source query.Symbol
	entity *Var
	attrs  []Attr
}

// GetSome creates a get-some expression for fallback attributes.
// Returns the first attribute the entity carries together with its value,
// useful for fallback chains.
//
// Example:
//
//	found, displayName := qb.NewVar("found"), qb.NewVar("displayName")
//	qb.GetSome(entity, PersonNickname, PersonFullName, PersonEmail).As(found, displayName)
//	// [(get-some $ ?entity :person/nickname :person/fullname :person/email) [?found ?display-name]]
//
// Returns the first available: nickname, then fullname, then email.
func GetSome(entity *Var, attrs ...Attr) *GetSomeBuilder {
	return &GetSomeBuilder{entity: entity, attrs: attrs}
}

// GetSomeFrom creates a get-some expression reading the named source.
//
// Example:
//
//	build := qb.Source("$build")
//	qb.GetSomeFrom(build, run, RunLabel, RunName).As(found, label)
//	// [(get-some $build ?run :run/label :run/name) [?found ?label]]
func GetSomeFrom(source *sourceInput, entity *Var, attrs ...Attr) *GetSomeBuilder {
	return &GetSomeBuilder{source: source.sym, entity: entity, attrs: attrs}
}

// As binds the attribute get-some found, as its keyword, and that
// attribute's value: Datomic's [?attr ?val].
func (g *GetSomeBuilder) As(attr, value *Var) *TupleExpression {
	keywords := make([]datalog.Keyword, len(g.attrs))
	for i, a := range g.attrs {
		keywords[i] = a.Keyword()
	}
	return &TupleExpression{
		fn: &query.GetSomeFunction{
			Source: g.source,
			Entity: query.VariableTerm{Symbol: g.entity.Symbol()},
			Attrs:  keywords,
		},
		bindings: []*Var{attr, value},
	}
}
