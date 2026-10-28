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

// Pat creates a data pattern of one to four elements, entity, attribute,
// value and transaction in that order; a position the pattern omits is
// unbound:
//   - Pat(e)                 - [e], every entity carrying a datom
//   - Pat(e, a)              - [e a], every entity carrying the attribute
//   - Pat(e, a, v)           - [e a v]
//   - Pat(e, a, v, tx)       - [e a v tx], with the transaction
//
// Build refuses a pattern of no element or of more than four, as
// ParseQuery refuses its rendering.
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
//	qb.Pat(e, PersonName, name)              // [e a v]
//	qb.Pat(e, PersonCity, qb.V("NYC"))       // with constant
//	qb.Pat(e, PersonName)                    // [e a], the value unbound
//	qb.Pat(e, PersonName, name, tx)          // [e a v tx]
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
		// Raw values become constants, as the domain admits them.
		return query.Constant{Value: datalog.NormalizeValue(v)}
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
		// Raw values become constants, as the domain admits them.
		return query.ConstantTerm{Value: datalog.NormalizeValue(v)}
	}
}
