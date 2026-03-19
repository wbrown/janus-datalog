package qb

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// NotClause represents a NOT (negation) clause.
type NotClause struct {
	clauses []interface{}
}

// Not creates a negation clause (anti-join).
// Matches entities that do NOT match the inner patterns.
//
// Example:
//
//	qb.Query().
//	    Find(name).
//	    Where(
//	        qb.Pat(e, PersonName, name),
//	        qb.Not(
//	            qb.Pat(e, PersonCity, qb.V("NYC")),
//	        ),
//	    )
func Not(clauses ...interface{}) *NotClause {
	return &NotClause{clauses: clauses}
}

func (n *NotClause) toClause() query.Clause {
	qclauses := make([]query.Clause, len(n.clauses))
	for i, c := range n.clauses {
		clause, ok := c.(Clause)
		if !ok {
			panic(fmt.Sprintf("Not: argument %d is not a Clause (got %T)", i, c))
		}
		qclauses[i] = clause.toClause()
	}
	return &query.NotClause{Clauses: qclauses}
}

// NotJoinClause represents a NOT-JOIN clause with explicit join variables.
type NotJoinClause struct {
	joinVars []*Var
	clauses  []interface{}
}

// NotJoin creates a negation clause with explicit join variables.
// Only the specified variables are used for joining with the outer query.
//
// Example:
//
//	qb.NotJoin([]*qb.Var{e},
//	    qb.Pat(e, PersonStatus, qb.V("banned")),
//	)
func NotJoin(joinVars []*Var, clauses ...interface{}) *NotJoinClause {
	return &NotJoinClause{joinVars: joinVars, clauses: clauses}
}

func (n *NotJoinClause) toClause() query.Clause {
	joinSyms := make([]query.Symbol, len(n.joinVars))
	for i, v := range n.joinVars {
		joinSyms[i] = v.Symbol()
	}

	qclauses := make([]query.Clause, len(n.clauses))
	for i, c := range n.clauses {
		clause, ok := c.(Clause)
		if !ok {
			panic(fmt.Sprintf("NotJoin: argument %d is not a Clause (got %T)", i, c))
		}
		qclauses[i] = clause.toClause()
	}

	return &query.NotJoinClause{
		JoinVars: joinSyms,
		Clauses:  qclauses,
	}
}

// OrBuilder accumulates branches for an OR clause.
type OrBuilder struct {
	branches [][]interface{}
}

// Or starts building an OR clause.
// Use Branch() to add branches.
//
// Example:
//
//	qb.Or().
//	    Branch(qb.Eq(status, qb.V("active"))).
//	    Branch(qb.Eq(status, qb.V("pending")))
func Or() *OrBuilder {
	return &OrBuilder{}
}

// Branch adds a branch with one or more clauses.
func (o *OrBuilder) Branch(clauses ...interface{}) *OrBuilder {
	o.branches = append(o.branches, clauses)
	return o
}

func (o *OrBuilder) toClause() query.Clause {
	qbranches := make([][]query.Clause, len(o.branches))
	for i, branch := range o.branches {
		qbranches[i] = make([]query.Clause, len(branch))
		for j, c := range branch {
			clause, ok := c.(Clause)
			if !ok {
				panic(fmt.Sprintf("Or: branch %d argument %d is not a Clause (got %T)", i, j, c))
			}
			qbranches[i][j] = clause.toClause()
		}
	}
	return &query.OrClause{Branches: qbranches}
}

// OrJoinBuilder accumulates branches for an OR-JOIN clause.
type OrJoinBuilder struct {
	joinVars []*Var
	branches [][]interface{}
}

// OrJoin starts building an OR-JOIN clause with explicit join variables.
// Only the specified variables are exposed from the union.
//
// Example:
//
//	qb.OrJoin(name).
//	    Branch(qb.Pat(e, PersonNickname, name)).
//	    Branch(qb.Pat(e, PersonName, name))
func OrJoin(joinVars ...*Var) *OrJoinBuilder {
	return &OrJoinBuilder{joinVars: joinVars}
}

// Branch adds a branch with one or more clauses.
func (o *OrJoinBuilder) Branch(clauses ...interface{}) *OrJoinBuilder {
	o.branches = append(o.branches, clauses)
	return o
}

func (o *OrJoinBuilder) toClause() query.Clause {
	joinSyms := make([]query.Symbol, len(o.joinVars))
	for i, v := range o.joinVars {
		joinSyms[i] = v.Symbol()
	}

	qbranches := make([][]query.Clause, len(o.branches))
	for i, branch := range o.branches {
		qbranches[i] = make([]query.Clause, len(branch))
		for j, c := range branch {
			clause, ok := c.(Clause)
			if !ok {
				panic(fmt.Sprintf("OrJoin: branch %d argument %d is not a Clause (got %T)", i, j, c))
			}
			qbranches[i][j] = clause.toClause()
		}
	}

	return &query.OrJoinClause{
		JoinVars: joinSyms,
		Branches: qbranches,
	}
}

// OrDefaultBuilder accumulates branches for an OR-DEFAULT clause (fallback semantics).
type OrDefaultBuilder struct {
	branches [][]interface{}
}

// OrDefault starts building an OR-DEFAULT clause.
// For each outer tuple, tries branches in order until one returns results.
func OrDefault() *OrDefaultBuilder {
	return &OrDefaultBuilder{}
}

// Branch adds a branch with one or more clauses.
func (o *OrDefaultBuilder) Branch(clauses ...interface{}) *OrDefaultBuilder {
	o.branches = append(o.branches, clauses)
	return o
}

func (o *OrDefaultBuilder) toClause() query.Clause {
	qbranches := make([][]query.Clause, len(o.branches))
	for i, branch := range o.branches {
		qbranches[i] = make([]query.Clause, len(branch))
		for j, c := range branch {
			clause, ok := c.(Clause)
			if !ok {
				panic(fmt.Sprintf("OrDefault: branch %d argument %d is not a Clause (got %T)", i, j, c))
			}
			qbranches[i][j] = clause.toClause()
		}
	}
	return &query.OrDefaultClause{Branches: qbranches}
}

// OrDefaultJoinBuilder accumulates branches for an OR-DEFAULT-JOIN clause.
type OrDefaultJoinBuilder struct {
	joinVars []*Var
	branches [][]interface{}
}

// OrDefaultJoin starts building an OR-DEFAULT-JOIN clause with explicit join variables.
func OrDefaultJoin(joinVars ...*Var) *OrDefaultJoinBuilder {
	return &OrDefaultJoinBuilder{joinVars: joinVars}
}

// Branch adds a branch with one or more clauses.
func (o *OrDefaultJoinBuilder) Branch(clauses ...interface{}) *OrDefaultJoinBuilder {
	o.branches = append(o.branches, clauses)
	return o
}

func (o *OrDefaultJoinBuilder) toClause() query.Clause {
	joinSyms := make([]query.Symbol, len(o.joinVars))
	for i, v := range o.joinVars {
		joinSyms[i] = v.Symbol()
	}

	qbranches := make([][]query.Clause, len(o.branches))
	for i, branch := range o.branches {
		qbranches[i] = make([]query.Clause, len(branch))
		for j, c := range branch {
			clause, ok := c.(Clause)
			if !ok {
				panic(fmt.Sprintf("OrDefaultJoin: branch %d argument %d is not a Clause (got %T)", i, j, c))
			}
			qbranches[i][j] = clause.toClause()
		}
	}

	return &query.OrDefaultJoinClause{
		JoinVars: joinSyms,
		Branches: qbranches,
	}
}
