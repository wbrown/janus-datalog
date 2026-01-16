package qb

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Attr represents a keyword attribute. Define once per attribute
// and reuse throughout your queries.
//
// Example:
//
//	var (
//	    PersonName = qb.Kw(":person/name")
//	    PersonAge  = qb.Kw(":person/age")
//	)
type Attr struct {
	kw datalog.Keyword
}

// Kw creates an attribute keyword. This should be called once per attribute
// in your schema definitions, then reused in queries.
//
// Accepts keywords with or without the leading colon:
//
//	PersonName := qb.Kw(":person/name")  // with colon
//	PersonAge := qb.Kw("person/age")     // without colon (added automatically)
func Kw(s string) Attr {
	return Attr{kw: datalog.NewKeyword(s)}
}

// Keyword returns the underlying datalog.Keyword for internal use.
func (a Attr) Keyword() datalog.Keyword {
	return a.kw
}

// String returns the keyword string representation.
func (a Attr) String() string {
	return a.kw.String()
}

// toPatternElement converts Attr to a query.PatternElement (constant)
func (a Attr) toPatternElement() query.PatternElement {
	return query.Constant{Value: a.kw}
}

// Val wraps a constant value for use in patterns.
// This disambiguates between variables and literal values in the API.
//
// Example:
//
//	qb.Pat(e, PersonCity, qb.V("NYC"))  // literal string value
//	qb.Pat(e, PersonActive, qb.V(true)) // literal boolean
type Val struct {
	value interface{}
}

// V creates a constant value wrapper for use in patterns.
// Use this when you want a literal value, not a variable.
//
// Example:
//
//	qb.Pat(e, PersonCity, qb.V("NYC"))
//	qb.Pat(e, PersonAge, qb.V(int64(30)))
func V(value interface{}) Val {
	return Val{value: value}
}

// Value returns the underlying value.
func (v Val) Value() interface{} {
	return v.value
}

// toPatternElement converts Val to a query.PatternElement
func (v Val) toPatternElement() query.PatternElement {
	return query.Constant{Value: v.value}
}

// toTerm converts Val to a query.Term for use in predicates
func (v Val) toTerm() query.Term {
	return query.ConstantTerm{Value: v.value}
}

// Blank returns a blank/wildcard pattern element.
// Use this when you don't care about a position in a pattern.
//
// Example:
//
//	qb.Pat(e, qb.Blank(), name)  // match any attribute
func Blank() blankElement {
	return blankElement{}
}

type blankElement struct{}

func (b blankElement) toPatternElement() query.PatternElement {
	return query.Blank{}
}
