package qb

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// InputSpec represents an input specification in the :in clause.
type InputSpec interface {
	toInputSpec() query.InputSpec
}

// dbInput represents the database input ($).
type dbInput struct{}

// DB represents the database input parameter ($) in the :in clause.
// This is typically the first input and is implicit if omitted.
//
// Example:
//
//	qb.Query().
//	    Find(name).
//	    In(qb.DB, qb.Scalar(minAge)).
//	    Where(...)
var DB InputSpec = dbInput{}

func (d dbInput) toInputSpec() query.InputSpec {
	return query.DatabaseInput{Name: datalog.SymDollar}
}

// scalarInput represents a scalar input parameter (?x).
type scalarInput struct {
	v *Var
}

// Scalar creates a scalar input parameter specification.
// The corresponding input value should be a single value.
//
// Example:
//
//	minAge := qb.NewVar("minAge")
//	q := qb.Query().
//	    Find(name, age).
//	    In(qb.DB, qb.Scalar(minAge)).
//	    Where(
//	        qb.Pat(e, PersonName, name),
//	        qb.Pat(e, PersonAge, age),
//	        qb.Gte(age, minAge),
//	    ).MustBuild()
//
//	db.Query(q, 21)  // 21 is bound to minAge
func Scalar(v *Var) InputSpec {
	return scalarInput{v: v}
}

func (s scalarInput) toInputSpec() query.InputSpec {
	return query.ScalarInput{Symbol: s.v.Symbol()}
}

// collectionInput represents a collection input [?x ...].
type collectionInput struct {
	v *Var
}

// Collection creates a collection input parameter specification.
// The corresponding input should be a slice of values.
// Each value is bound to the variable in turn.
//
// Example:
//
//	id := qb.NewVar("id")
//	q := qb.Query().
//	    Find(name).
//	    In(qb.DB, qb.Collection(id)).
//	    Where(qb.Pat(e, PersonID, id), qb.Pat(e, PersonName, name)).
//	    MustBuild()
//
//	db.Query(q, []string{"id1", "id2", "id3"})
func Collection(v *Var) InputSpec {
	return collectionInput{v: v}
}

func (c collectionInput) toInputSpec() query.InputSpec {
	return query.CollectionInput{Symbol: c.v.Symbol()}
}

// tupleInput represents a tuple input [[?x ?y]].
type tupleInput struct {
	vars []*Var
}

// Tuple creates a tuple input parameter specification.
// The corresponding input should be a slice with one value per variable.
//
// Example:
//
//	x, y := qb.NewVar("x"), qb.NewVar("y")
//	q := qb.Query().
//	    Find(name).
//	    In(qb.DB, qb.Tuple(x, y)).
//	    Where(...).
//	    MustBuild()
//
//	db.Query(q, []interface{}{10, 20})
func Tuple(vars ...*Var) InputSpec {
	return tupleInput{vars: vars}
}

func (t tupleInput) toInputSpec() query.InputSpec {
	syms := make([]query.Symbol, len(t.vars))
	for i, v := range t.vars {
		syms[i] = v.Symbol()
	}
	return query.TupleInput{Symbols: syms}
}

// relationInput represents a relation input [[?x ?y] ...].
type relationInput struct {
	vars []*Var
}

// Relation creates a relation input parameter specification.
// The corresponding input should be a slice of slices (tuples).
//
// Example:
//
//	x, y := qb.NewVar("x"), qb.NewVar("y")
//	q := qb.Query().
//	    Find(name).
//	    In(qb.DB, qb.Relation(x, y)).
//	    Where(...).
//	    MustBuild()
//
//	db.Query(q, [][]interface{}{{1, 2}, {3, 4}})
func Relation(vars ...*Var) InputSpec {
	return relationInput{vars: vars}
}

func (r relationInput) toInputSpec() query.InputSpec {
	syms := make([]query.Symbol, len(r.vars))
	for i, v := range r.vars {
		syms[i] = v.Symbol()
	}
	return query.RelationInput{Symbols: syms}
}
