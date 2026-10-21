package qb

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// SubqueryBuilder builds a subquery clause.
type SubqueryBuilder struct {
	innerQuery *QueryBuilder
	inputs     []*Var
	binding    bindingForm
}

// bindingForm interface for subquery result bindings
type bindingForm interface {
	toBindingForm() query.BindingForm
}

// relationBind binds results as the relation [[?a ?b]]
type relationBind struct {
	vars []*Var
}

func (r relationBind) toBindingForm() query.BindingForm {
	syms := make([]query.Symbol, len(r.vars))
	for i, v := range r.vars {
		syms[i] = v.Symbol()
	}
	return query.RelationBinding{Variables: syms}
}

// collectionBind binds results to a collection variable
type collectionBind struct {
	v *Var
}

func (c collectionBind) toBindingForm() query.BindingForm {
	return query.CollectionBinding{Variable: c.v.Symbol()}
}

// Subquery creates a nested query pattern.
// The inner query is executed with the given input variables from the outer scope.
//
// Example:
//
//	// Inner query: find max salary for a department
//	dept := qb.NewVar("dept")
//	maxSalary := qb.NewVar("maxSalary")
//	innerSalary := qb.NewVar("innerSalary")
//
//	innerQ := qb.Query().
//	    Find(qb.Max(innerSalary)).
//	    In(qb.DB, qb.Scalar(dept)).
//	    Where(
//	        qb.Pat(e, EmployeeDept, dept),
//	        qb.Pat(e, EmployeeSalary, innerSalary),
//	    )
//
//	// Outer query uses the subquery
//	qb.Query().
//	    Find(name, maxSalary).
//	    Where(
//	        qb.Pat(p, PersonName, name),
//	        qb.Pat(p, PersonDept, dept),
//	        qb.Subquery(innerQ, dept).BindRelation(maxSalary),
//	    )
func Subquery(innerQuery *QueryBuilder, inputs ...*Var) *SubqueryBuilder {
	return &SubqueryBuilder{
		innerQuery: innerQuery,
		inputs:     inputs,
	}
}

// BindRelation binds the subquery's result as the relation [[?a ?b]] over
// the named variables, one per element of the inner :find, whatever the
// inner query's size.
func (s *SubqueryBuilder) BindRelation(vars ...*Var) *SubqueryBuilder {
	s.binding = relationBind{vars: vars}
	return s
}

// BindCollection binds subquery results to a collection variable.
// Use when the subquery returns a single symbol of values.
func (s *SubqueryBuilder) BindCollection(v *Var) *SubqueryBuilder {
	s.binding = collectionBind{v: v}
	return s
}

// toClause converts SubqueryBuilder to a query.Clause
func (s *SubqueryBuilder) toClause() query.Clause {
	// Build the inner query - panic on error since we're in a fluent chain
	innerQ, err := s.innerQuery.Build()
	if err != nil {
		panic("subquery build failed: " + err.Error())
	}

	// The inputs follow the inner query's :in in order: a source reference at
	// a source position, % at the rules position, and the caller's variables
	// at the value positions, one per value the entry takes under
	// query.InputArity, so a tuple or relation input takes one caller per
	// symbol, as the parsed forms (q [...] $ ?x %) and (q [...] $ ?a ?b %)
	// have them. A caller variable beyond the value positions is passed
	// through, so Build refuses the count as the static shape it is.
	var inputs []query.PatternElement
	callers := s.inputs
	for _, in := range innerQ.In {
		switch in := in.(type) {
		case query.DatabaseInput:
			inputs = append(inputs, query.Constant{Value: in.Name})
		case query.RulesInput:
			inputs = append(inputs, query.Constant{Value: datalog.SymRules})
		default:
			for values := query.InputArity([]query.InputSpec{in}); values > 0 && len(callers) > 0; values-- {
				inputs = append(inputs, query.Variable{Name: callers[0].Symbol()})
				callers = callers[1:]
			}
		}
	}
	for _, v := range callers {
		inputs = append(inputs, query.Variable{Name: v.Symbol()})
	}

	// Convert binding form
	var binding query.BindingForm
	if s.binding != nil {
		binding = s.binding.toBindingForm()
	}

	return &query.SubqueryPattern{
		Query:   innerQ,
		Inputs:  inputs,
		Binding: binding,
	}
}
