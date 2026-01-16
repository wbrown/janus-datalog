// Package qb provides a fluent query builder for constructing Datalog queries
// without string-based variable names. Go variable identity IS the join condition.
//
// Example:
//
//	e, name, age := qb.NewVar(), qb.NewVar(), qb.NewVar()
//	q := qb.Query().
//	    Find(name, age).
//	    Where(
//	        qb.Pat(e, PersonName, name),
//	        qb.Pat(e, PersonAge, age),
//	        qb.Gt(age, 21),
//	    ).MustBuild()
package qb

import (
	"fmt"
	"sync/atomic"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// varCounter generates unique variable IDs
var varCounter atomic.Uint64

// Var represents a query variable. Using the same Var instance
// in multiple patterns creates a join on that variable.
//
// Variables are created with NewVar() and reused across patterns:
//
//	e := qb.NewVar()
//	qb.Pat(e, PersonName, name)  // e bound here
//	qb.Pat(e, PersonAge, age)    // same e = implicit join
type Var struct {
	id   uint64
	name query.Symbol
}

// NewVar creates a new query variable with a unique identity.
// The variable name is auto-generated (e.g., ?v1, ?v2).
// Join semantics come from using the same *Var instance in multiple places.
func NewVar() *Var {
	id := varCounter.Add(1)
	return &Var{
		id:   id,
		name: query.Symbol(fmt.Sprintf("?v%d", id)),
	}
}

// Symbol returns the query.Symbol for this variable.
// Used internally when building query structures.
func (v *Var) Symbol() query.Symbol {
	return v.name
}

// String returns the variable name for debugging.
func (v *Var) String() string {
	return string(v.name)
}

// toPatternElement converts Var to a query.PatternElement
func (v *Var) toPatternElement() query.PatternElement {
	return query.Variable{Name: v.name}
}

// toTerm converts Var to a query.Term for use in predicates
func (v *Var) toTerm() query.Term {
	return query.VariableTerm{Symbol: v.name}
}

// toFindElement converts Var to a query.FindElement
func (v *Var) toFindElement() query.FindElement {
	return query.FindVariable{Symbol: v.name}
}
