package qb

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// GetElseBuilder builds a get-else expression for retrieving an attribute
// with a default value if the attribute is missing.
type GetElseBuilder struct {
	entity     *Var
	attr       Attr
	defaultVal interface{}
}

// GetElse creates a get-else expression for default values.
// Returns the attribute value if it exists, or the default value if missing.
//
// Example:
//
//	nickname := qb.NewVar("nickname")
//	qb.GetElse(entity, PersonNickname, "Anonymous").As(nickname)
//	// [(get-else $ ?entity :person/nickname "Anonymous") ?nickname]
func GetElse(entity *Var, attr Attr, defaultVal interface{}) *GetElseBuilder {
	return &GetElseBuilder{entity: entity, attr: attr, defaultVal: normalizeConstant(defaultVal)}
}

// As binds the result of get-else to a variable.
func (g *GetElseBuilder) As(result *Var) *Expression {
	return &Expression{
		fn: &query.GetElseFunction{
			Entity:  query.VariableTerm{Symbol: g.entity.Symbol()},
			Attr:    g.attr.Keyword(),
			Default: g.defaultVal,
		},
		binding: result,
	}
}

// MissingBuilder builds a missing? expression/predicate.
// Can be used as a predicate (filter) or as an expression (bind bool).
type MissingBuilder struct {
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

// toClause implements Clause for predicate usage (no binding).
// When used without .As(), Missing acts as a filter predicate.
func (m *MissingBuilder) toClause() query.Clause {
	return &query.DatabaseFunctionPredicate{
		Function: &query.MissingFunction{
			Entity: query.VariableTerm{Symbol: m.entity.Symbol()},
			Attr:   m.attr.Keyword(),
		},
	}
}

// As binds the boolean result (true if missing, false if present) to a variable.
func (m *MissingBuilder) As(result *Var) *Expression {
	return &Expression{
		fn: &query.MissingFunction{
			Entity: query.VariableTerm{Symbol: m.entity.Symbol()},
			Attr:   m.attr.Keyword(),
		},
		binding: result,
	}
}

// GetSomeBuilder builds a get-some expression for fallback attribute chains.
type GetSomeBuilder struct {
	entity *Var
	attrs  []Attr
}

// GetSome creates a get-some expression for fallback attributes.
// Returns the first attribute that exists, useful for fallback chains.
//
// Example:
//
//	displayName := qb.NewVar("displayName")
//	qb.GetSome(entity, PersonNickname, PersonFullName, PersonEmail).As(displayName)
//	// [(get-some $ ?entity :person/nickname :person/fullname :person/email) ?display-name]
//
// Returns the first available: nickname, then fullname, then email.
func GetSome(entity *Var, attrs ...Attr) *GetSomeBuilder {
	return &GetSomeBuilder{entity: entity, attrs: attrs}
}

// As binds the result of get-some to a variable.
// Note: get-some actually returns a [attr value] pair in Datomic,
// but for simplicity we just bind the value.
func (g *GetSomeBuilder) As(result *Var) *Expression {
	keywords := make([]datalog.Keyword, len(g.attrs))
	for i, attr := range g.attrs {
		keywords[i] = attr.Keyword()
	}
	return &Expression{
		fn: &query.GetSomeFunction{
			Entity: query.VariableTerm{Symbol: g.entity.Symbol()},
			Attrs:  keywords,
		},
		binding: result,
	}
}
