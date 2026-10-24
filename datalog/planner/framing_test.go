package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The framing's computations, each pinned at the unit level: what a branch
// is given, what it reads and provides, whether it holds an invocation, and
// the frames a form's branches get under its framing.

// Branches frames a form's branches in the header's order: a branch holding
// an invocation over the required run; a branch reading nothing of what it
// is given over the required symbols the subject does not carry, marked as
// reading nothing; every other branch over the given symbols, with a check
// scope over the required run alone when the framing asks for one. A frame's
// find is the given symbols the branch provides beyond its frame, then the
// framing's find, each symbol once.
func TestBranchesFrameEachBranchInTheHeadersOrder(t *testing.T) {
	entity, value, flag, other := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value"), datalog.NewSymbol("?flag"), datalog.NewSymbol("?other")
	attr := datalog.NewKeyword(":t/a")
	invocation := &query.RuleInvocation{Name: datalog.NewSymbol("reach"), Args: []query.PatternElement{
		query.Variable{Name: entity}, query.Variable{Name: other},
	}}
	f := Framing{
		Required:         []query.Symbol{entity, flag},
		Find:             []query.FindElement{query.FindVariable{Symbol: value}},
		Given:            []query.Symbol{entity, flag, value},
		Carried:          []query.Symbol{value, entity},
		PlanOverRequired: true,
	}
	branches := [][]query.Clause{{invocation}, {branchGround(value, int64(0))}, {branchPattern(entity, value, attr)}}
	frames := f.Branches(branches, nil)
	require.Len(t, frames, 3)
	for i, frame := range frames {
		require.Equal(t, branches[i], frame.Scope.Clauses, "branch %d keeps its clauses", i+1)
	}
	require.Equal(t, []query.Symbol{entity, flag}, frames[0].Scope.Required, "a branch holding an invocation is framed over the required run")
	require.False(t, frames[0].ReadsNothing)
	require.Nil(t, frames[0].Check)
	require.Equal(t, f.Find, frames[0].Scope.Find, "the framing's find follows")
	require.Equal(t, []query.Symbol{flag}, frames[1].Scope.Required, "a branch reading nothing is framed over the required symbols the subject does not carry")
	require.True(t, frames[1].ReadsNothing)
	require.Nil(t, frames[1].Check)
	require.Equal(t, f.Find, frames[1].Scope.Find, "the given symbol the ground provides beyond its frame is its find, once")
	require.Equal(t, []query.Symbol{entity, flag, value}, frames[2].Scope.Required, "a branch reading a given symbol is framed over the given symbols, in the header's order")
	require.False(t, frames[2].ReadsNothing)
	require.NotNil(t, frames[2].Check, "under the union with carried outputs the branch is first planned over the required run alone")
	require.Equal(t, Scope{Required: []query.Symbol{entity, flag}, Find: f.Find, Clauses: branches[2]}, *frames[2].Check)
	require.Empty(t, frames[2].Scope.Find, "every output is in the frame's required run")
}

// ReadSubqueryFrame reads a call's frame from its text: the inner symbols in
// call order, a collection input's as its position's holder; the clauses,
// the opening binding the collection's members from the holder and then the
// inner clauses, a pattern on an inner source reading the source the call
// passes; the substitution of each inner symbol and each find symbol to the
// enclosing name standing for it; and per inner symbol the enclosing
// variable the call passes, nil for a constant argument.
func TestReadSubqueryFrameReadsTheCallsText(t *testing.T) {
	e, n, x, s := datalog.NewSymbol("?e"), datalog.NewSymbol("?n"), datalog.NewSymbol("?x"), datalog.NewSymbol("?s")
	a, c, k := datalog.NewSymbol("?a"), datalog.NewSymbol("?c"), datalog.NewSymbol("?k")
	inner, other := datalog.NewSymbol("$t"), datalog.NewSymbol("$other")
	attr := datalog.NewKeyword(":t/a")
	onInner := &query.DataPattern{Source: inner, Elements: []query.PatternElement{
		query.Variable{Name: s}, query.Constant{Value: attr}, query.Variable{Name: n},
	}}
	count := query.FindAggregate{Function: datalog.SymCount, Arg: s}
	subq := &query.SubqueryPattern{
		Query: &query.Query{
			Find:  []query.FindElement{query.FindVariable{Symbol: x}, count},
			In:    []query.InputSpec{query.DatabaseInput{Name: inner}, query.ScalarInput{Symbol: e}, query.CollectionInput{Symbol: n}, query.ScalarInput{Symbol: x}},
			Where: []query.Clause{onInner},
		},
		Inputs:  []query.PatternElement{query.Constant{Value: other}, query.Variable{Name: a}, query.Constant{Value: []interface{}{int64(1)}}, query.Variable{Name: a}},
		Binding: query.RelationBinding{Variables: []query.Symbol{k, c}},
	}
	frame, err := ReadSubqueryFrame(subq)
	require.NoError(t, err)
	holder := query.ScopeInput(2)
	require.Equal(t, []query.Symbol{e, holder, x}, frame.Given, "the inner symbols in call order, the collection's as its holder")
	require.Equal(t, []query.Symbol{nil, e, holder, x}, frame.Inner, "per call position, nil at the source's")
	require.Equal(t, []query.Symbol{a, nil, a}, frame.StandsFor, "the enclosing variable per inner symbol, nil for the constant")
	require.Equal(t, subq.Query.Find, frame.Find, "the inner find as written")
	require.Len(t, frame.Clauses, 2)
	require.Equal(t, &query.Expression{Function: query.IdentityFunction{Arg: query.VariableTerm{Symbol: holder}}, Binding: query.CollectionBinding{Variable: n}}, frame.Clauses[0], "the opening binds the members from the holder")
	renamed, ok := frame.Clauses[1].(*query.DataPattern)
	require.True(t, ok)
	require.Equal(t, other, renamed.Source, "a pattern on the inner source reads the source the call passes")
	for from, to := range map[query.Symbol]query.Symbol{e: a, x: a, datalog.NewSymbol("(count ?s)"): c} {
		got, named := frame.Subst.Of(from)
		require.True(t, named, "%s stands for %s", from, to)
		require.Equal(t, to, got)
	}
	require.Equal(t, []query.Symbol{k}, frame.Subst.Further(x), "a symbol written at an :in place and a :find place has the find's name as a further name")
	_, named := frame.Subst.Of(holder)
	require.False(t, named, "a constant argument's symbol stands for nothing")
}

func branchPattern(entity, value query.Symbol, attr datalog.Keyword) query.Clause {
	return &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity}, query.Constant{Value: attr}, query.Variable{Name: value},
	}}
}

func branchGround(value query.Symbol, v interface{}) query.Clause {
	return &query.Expression{Function: &query.GroundFunction{Value: v}, Binding: value}
}

// branchSubquery is an uncorrelated subquery binding value from every entity
// carrying attr.
func branchSubquery(value query.Symbol, attr datalog.Keyword) *query.SubqueryPattern {
	inner := datalog.NewSymbol("?inner")
	return &query.SubqueryPattern{
		Query: &query.Query{
			Find:  []query.FindElement{query.FindVariable{Symbol: value}},
			In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
			Where: []query.Clause{branchPattern(inner, value, attr)},
		},
		Inputs:  []query.PatternElement{query.Constant{Value: datalog.SymDollar}},
		Binding: query.RelationBinding{Variables: []query.Symbol{value}},
	}
}

// holdsInvocation finds an invocation at the top of a branch, inside a
// nested form and inside a subquery's inner query, and nothing else.
func TestHoldsInvocationDescendsIntoSubqueryBodies(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	invocation := &query.RuleInvocation{Name: datalog.NewSymbol("reach"), Args: []query.PatternElement{
		query.Variable{Name: entity}, query.Variable{Name: value},
	}}
	require.True(t, HoldsInvocation([]query.Clause{invocation}))
	require.True(t, HoldsInvocation([]query.Clause{&query.NotJoinClause{RequiredVars: []query.Symbol{entity}, Clauses: []query.Clause{invocation}}}))
	require.True(t, HoldsInvocation([]query.Clause{&query.SubqueryPattern{
		Query:   &query.Query{Where: []query.Clause{invocation}},
		Inputs:  []query.PatternElement{query.Constant{Value: datalog.SymDollar}},
		Binding: query.RelationBinding{Variables: []query.Symbol{value}},
	}}))
	require.False(t, HoldsInvocation([]query.Clause{branchPattern(entity, value, datalog.NewKeyword(":t/a"))}))
	require.False(t, HoldsInvocation(nil))
}

// branchProvides reads what each clause provides under the rule set: a
// pattern's variables, an expression's binding, a subquery's binding and a
// nested form's outputs, each once.
func TestBranchProvidesReadsEachClausesProvides(t *testing.T) {
	entity, value, other := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value"), datalog.NewSymbol("?other")
	attr := datalog.NewKeyword(":t/a")
	provided := BranchProvides([]query.Clause{
		branchPattern(entity, value, attr),
		branchGround(other, int64(1)),
		branchSubquery(value, attr),
		&query.OrJoinClause{RequiredVars: []query.Symbol{entity}, OutputVars: []query.Symbol{other}, Branches: [][]query.Clause{{branchPattern(entity, other, attr)}}},
	}, nil)
	require.Equal(t, []query.Symbol{entity, value, other}, provided)
}

// What a branch is given is the required run under the fallback, and the
// required run with the carried outputs under the union, each symbol once.
func TestGivenSymbolsAreTheRequiredRunWithTheCarriedOutputsUnderTheUnion(t *testing.T) {
	entity, value, flag := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value"), datalog.NewSymbol("?flag")
	carried := []query.Symbol{value, flag, value}
	require.Equal(t, []query.Symbol{entity}, GivenSymbols([]query.Symbol{entity}, carried, true), "the fallback gives the required run alone")
	require.Equal(t, []query.Symbol{entity, value, flag}, GivenSymbols([]query.Symbol{entity}, carried, false), "the union gives the carried outputs after the required run, each once")
	require.Empty(t, GivenSymbols(nil, nil, false))
}

// A branch reads a given symbol when a data pattern mentions it or any clause
// consumes it, whether or not another clause provides it, since a clause
// binding a run symbol unifies with it; the reads keep the given order. A
// symbol the branch only provides is not read, so a lone ground or an
// uncorrelated subquery is built once over the environment's tuple and
// joined on what it provides; a symbol no clause names is not read.
func TestBranchReadsAConsumedSymbolOrAPatternsMention(t *testing.T) {
	entity, value, sum, other := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value"), datalog.NewSymbol("?sum"), datalog.NewSymbol("?other")
	attr := datalog.NewKeyword(":t/a")
	given := []query.Symbol{entity, value}
	sumOfBoth := &query.Expression{
		Function: query.ArithmeticFunction{Op: datalog.SymAdd, Args: []query.Term{
			query.VariableTerm{Symbol: entity}, query.VariableTerm{Symbol: value},
		}},
		Binding: sum,
	}
	require.Equal(t, []query.Symbol{entity, value}, BranchReads([]query.Clause{branchSubquery(value, attr), sumOfBoth}, given, nil), "a consumed symbol is read, the subquery providing it notwithstanding")
	require.Equal(t, []query.Symbol{entity, value}, BranchReads([]query.Clause{branchPattern(entity, value, attr)}, given, nil), "a data pattern's mention is a read")
	require.Equal(t, []query.Symbol{entity, value}, BranchReads([]query.Clause{branchPattern(value, entity, attr)}, given, nil), "the reads keep the given order whatever the pattern's")
	invocation := &query.RuleInvocation{Name: datalog.NewSymbol("reach"), Args: []query.PatternElement{
		query.Variable{Name: entity}, query.Variable{Name: other},
	}}
	require.Empty(t, BranchReads([]query.Clause{invocation}, given, nil), "an invocation's mention is not a read: it provides its variables and consumes nothing")
	nested := &query.OrJoinClause{RequiredVars: []query.Symbol{value}, OutputVars: []query.Symbol{other}, Branches: [][]query.Clause{{branchPattern(value, other, attr)}}}
	require.Equal(t, []query.Symbol{value}, BranchReads([]query.Clause{nested}, given, nil), "a nested form's required run is consumed")
	require.Empty(t, BranchReads([]query.Clause{branchGround(value, int64(0))}, given, nil), "a symbol the branch only provides is not read")
	doubled := &query.Expression{
		Function: query.ArithmeticFunction{Op: datalog.SymAdd, Args: []query.Term{
			query.VariableTerm{Symbol: entity}, query.VariableTerm{Symbol: entity},
		}},
		Binding: sum,
	}
	require.Equal(t, []query.Symbol{entity}, BranchReads([]query.Clause{branchGround(entity, int64(1)), doubled}, given, nil), "a symbol the branch provides and then consumes is read")
	cycle := []query.Clause{
		&query.Expression{Function: query.IdentityFunction{Arg: query.VariableTerm{Symbol: entity}}, Binding: value},
		&query.Expression{Function: query.IdentityFunction{Arg: query.VariableTerm{Symbol: value}}, Binding: entity},
	}
	require.Equal(t, []query.Symbol{entity, value}, BranchReads(cycle, given, nil), "each symbol of a cycle of identities, provided by one clause and consumed by the other, is read")
	require.Empty(t, BranchReads([]query.Clause{branchSubquery(other, attr)}, given, nil), "a symbol no clause names is not read")
}
