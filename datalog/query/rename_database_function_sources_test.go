package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// A scope's database functions are read against the sources its call names,
// as its data patterns are: a function on a renamed source, as an expression
// or as a predicate and at any depth, reads the source standing for it and
// keeps every other part, and a function on no renamed source comes back as
// it is.
func TestRenameSources_RenamesEveryDatabaseFunctionsSource(t *testing.T) {
	inner, outer, other := datalog.NewSymbol("$t"), datalog.NewSymbol("$other"), datalog.NewSymbol("$u")
	e, v := datalog.NewSymbol("?e"), datalog.NewSymbol("?v")
	attr := datalog.NewKeyword(":t/a")
	functions := func(source Symbol) []DatabaseFunction {
		return []DatabaseFunction{
			&GetElseFunction{Source: source, Entity: VariableTerm{Symbol: e}, Attr: attr, Default: ConstantTerm{Value: "d"}},
			&MissingFunction{Source: source, Entity: VariableTerm{Symbol: e}, Attr: attr},
			&GetSomeFunction{Source: source, Entity: VariableTerm{Symbol: e}, Attrs: []datalog.Keyword{attr}},
		}
	}
	sources, err := SubstitutionOf([]Symbol{inner}, []Symbol{outer})
	require.NoError(t, err)

	for i, f := range functions(inner) {
		want := functions(outer)[i]
		renamed := RenameSources([]Clause{
			&Expression{Function: f, Binding: v},
			&DatabaseFunctionPredicate{Function: f},
			&OrJoinClause{OutputVars: []Symbol{v}, Branches: [][]Clause{{&Expression{Function: f, Binding: v}}}},
		}, sources)

		expr := renamed[0].(*Expression)
		require.True(t, equalDatabaseFunction(want, expr.Function), "%s as an expression reads $other, got %s", f, expr.Function)
		require.Equal(t, v, expr.Binding, "the expression keeps its binding")
		pred := renamed[1].(*DatabaseFunctionPredicate)
		require.True(t, equalDatabaseFunction(want, pred.Function), "%s as a predicate reads $other, got %s", f, pred.Function)
		branch := renamed[2].(*OrJoinClause).Branches[0][0].(*Expression)
		require.True(t, equalDatabaseFunction(want, branch.Function), "%s inside a branch reads $other, got %s", f, branch.Function)
	}

	byDefault, err := SubstitutionOf([]Symbol{datalog.SymDollar}, []Symbol{outer})
	require.NoError(t, err)
	for i, f := range functions(nil) {
		renamed := RenameSources([]Clause{&Expression{Function: f, Binding: v}}, byDefault)
		require.True(t, equalDatabaseFunction(functions(outer)[i], renamed[0].(*Expression).Function),
			"%s, nil standing for $, reads $other when the inner :in names $", f)
	}

	same := []Clause{
		&Expression{Function: functions(other)[0], Binding: v},
		&DatabaseFunctionPredicate{Function: functions(nil)[1]},
	}
	got := RenameSources(same, sources)
	require.Same(t, same[0], got[0], "a function on a source the call does not rename comes back as it is")
	require.Same(t, same[1], got[1], "a function on $ comes back as it is when the call renames $t alone")
}
