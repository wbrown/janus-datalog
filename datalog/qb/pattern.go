package qb

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Clause is the interface for anything that can appear in a Where clause.
// This includes patterns, predicates, expressions, and logical combinators.
type Clause interface {
	toClause() query.Clause
}

// Pattern represents a data pattern clause being constructed.
type Pattern struct {
	source   query.Symbol // Source identifier (e.g., "$users"); empty for default
	elements []query.PatternElement
}

// Pat creates a data pattern of any arity.
//
// Common database patterns:
//   - Pat(e, a, v)           - standard [e a v] pattern
//   - Pat(e, a, v, tx)       - transaction-aware [e a v tx] pattern
//   - Pat(e, a, v, tx, op)   - history [e a v tx op] pattern
//
// Patterns can also match input relations of any arity:
//   - Pat(name, age)         - matches [[?name ?age] ...] input
//   - Pat(a, b, c, d, e, f)  - matches 6-tuple input relation
//
// Arguments can be:
//   - *Var: a query variable
//   - Attr: a keyword attribute
//   - Val: a constant value wrapper
//   - blankElement: wildcard (from Blank())
//   - Raw values (string, int64, etc.): converted to constants
//
// Example:
//
//	qb.Pat(e, PersonName, name)              // [e a v] database pattern
//	qb.Pat(e, PersonCity, qb.V("NYC"))       // with constant
//	qb.Pat(e, PersonName, name, tx)          // [e a v tx]
//	qb.Pat(e, PersonName, name, tx, op)      // [e a v tx op] for history
//	qb.Pat(name, age)                        // 2-tuple input relation
func Pat(args ...interface{}) *Pattern {
	if len(args) == 0 {
		panic("Pat requires at least one argument")
	}
	elements := make([]query.PatternElement, len(args))
	for i, arg := range args {
		elements[i] = toPatternElement(arg)
	}
	return &Pattern{elements: elements}
}

// toClause converts Pattern to a query.Clause
func (p *Pattern) toClause() query.Clause {
	return &query.DataPattern{Source: p.source, Elements: p.elements}
}

// patternElementer is implemented by types that can convert to PatternElement
type patternElementer interface {
	toPatternElement() query.PatternElement
}

// toPatternElement converts various types to query.PatternElement
func toPatternElement(v interface{}) query.PatternElement {
	// Check if it implements the interface directly
	if pe, ok := v.(patternElementer); ok {
		return pe.toPatternElement()
	}

	// Handle specific types
	switch x := v.(type) {
	case *Var:
		return x.toPatternElement()
	case Attr:
		return x.toPatternElement()
	case Val:
		return x.toPatternElement()
	case blankElement:
		return x.toPatternElement()
	case query.Blank:
		return x
	case query.Variable:
		return x
	case query.Constant:
		return x
	case datalog.Keyword:
		return query.Constant{Value: x}
	case datalog.Identity:
		return query.Constant{Value: x}
	default:
		// Raw values become constants
		return query.Constant{Value: v}
	}
}

// termer is implemented by types that can convert to Term
type termer interface {
	toTerm() query.Term
}

// toTerm converts various types to query.Term for use in predicates
func toTerm(v interface{}) query.Term {
	// Check if it implements the interface directly
	if t, ok := v.(termer); ok {
		return t.toTerm()
	}

	// Handle specific types
	switch x := v.(type) {
	case *Var:
		return x.toTerm()
	case Val:
		return x.toTerm()
	default:
		// Raw values become constants
		return query.ConstantTerm{Value: v}
	}
}
