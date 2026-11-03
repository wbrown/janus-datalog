package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Running clauses can fail on a resolved value when they evaluate an
// expression's function or a database-function predicate over a variable, a
// registered predicate function, or a transaction range over a value naming no
// transaction, at any depth of the header forms, in a subquery's body, or in a
// rule body an invocation reaches. Data patterns and the other predicates fail
// on no resolved value, and a recursive rule set of patterns ends the walk.
func TestCanFailOnAValue(t *testing.T) {
	x, y, z := datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?z")
	pattern := func(e, v query.Symbol) *query.DataPattern {
		return &query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: e}, query.Constant{Value: datalog.NewKeyword(":t/a")}, query.Variable{Name: v},
		}}
	}
	plus := &query.Expression{
		Function: &query.ArithmeticFunction{
			Op:   datalog.SymAdd,
			Args: []query.Term{query.VariableTerm{Symbol: y}, query.ConstantTerm{Value: int64(1)}},
		},
		Binding: z,
	}
	less := &query.Comparison{Op: datalog.SymLT, Left: query.VariableTerm{Symbol: y}, Right: query.ConstantTerm{Value: int64(5)}}
	startsWith := &query.StrStartsWithPredicate{Value: query.VariableTerm{Symbol: y}, Prefix: query.ConstantTerm{Value: "a"}}
	registered := &query.FunctionPredicate{Fn: "even?", Args: []query.PatternElement{query.Variable{Name: y}}}
	txRange := &query.TxRangePredicate{TxVar: y, Low: 1, High: 2}
	missing := func(entity query.Term) *query.DatabaseFunctionPredicate {
		return &query.DatabaseFunctionPredicate{Function: &query.MissingFunction{Entity: entity, Attr: datalog.NewKeyword(":t/b")}}
	}
	computes, reach := datalog.NewSymbol("computes"), datalog.NewSymbol("reach")
	rules, err := query.NewRuleSet([]*query.Rule{
		{Name: computes, Head: []query.Symbol{x}, Body: []query.Clause{pattern(x, y), plus}},
		{Name: reach, Head: []query.Symbol{x, y}, Body: []query.Clause{pattern(x, y)}},
		{Name: reach, Head: []query.Symbol{x, y}, Body: []query.Clause{
			pattern(x, z),
			&query.RuleInvocation{Name: reach, Args: []query.PatternElement{query.Variable{Name: z}, query.Variable{Name: y}}},
		}},
	})
	require.NoError(t, err)

	cases := []struct {
		name    string
		clauses []query.Clause
		fails   bool
	}{
		{"data patterns", []query.Clause{pattern(x, y)}, false},
		{"a comparison and a string predicate", []query.Clause{pattern(x, y), less, startsWith}, false},
		{"an expression reading a variable", []query.Clause{pattern(x, y), plus}, true},
		{"an expression reading no variable", []query.Clause{pattern(x, y), &query.Expression{
			Function: query.GroundFunction{Value: int64(0)},
			Binding:  z,
		}}, false},
		{"a registered predicate function", []query.Clause{pattern(x, y), registered}, true},
		{"a transaction range", []query.Clause{pattern(x, y), txRange}, true},
		{"a missing? over a variable", []query.Clause{pattern(x, y), missing(query.VariableTerm{Symbol: y})}, true},
		{"a missing? over a constant entity", []query.Clause{pattern(x, y),
			missing(query.ConstantTerm{Value: datalog.NewIdentity("can-fail:one")})}, false},
		{"an expression reading a variable before one reading none", []query.Clause{pattern(x, y), plus,
			&query.Expression{Function: query.GroundFunction{Value: int64(0)}, Binding: datalog.NewSymbol("?w")}}, true},
		{"a missing? over a variable before an expression reading none", []query.Clause{pattern(x, y),
			missing(query.VariableTerm{Symbol: y}),
			&query.Expression{Function: query.GroundFunction{Value: int64(0)}, Binding: datalog.NewSymbol("?w")}}, true},
		{"an expression in an or-join branch", []query.Clause{pattern(x, y), &query.OrJoinClause{
			RequiredVars: []query.Symbol{y},
			Branches:     [][]query.Clause{{less}, {plus}},
		}}, true},
		{"an expression in a subquery's body", []query.Clause{pattern(x, y), &query.SubqueryPattern{
			Query:   &query.Query{Find: []query.FindElement{query.FindVariable{Symbol: z}}, Where: []query.Clause{pattern(x, y), plus}},
			Inputs:  []query.PatternElement{query.Constant{Value: datalog.SymDollar}},
			Binding: query.RelationBinding{Variables: []query.Symbol{z}},
		}}, true},
		{"an expression in a rule body an invocation reaches", []query.Clause{
			&query.RuleInvocation{Name: computes, Args: []query.PatternElement{query.Variable{Name: x}}},
		}, true},
		{"a recursive rule set of patterns", []query.Clause{
			&query.RuleInvocation{Name: reach, Args: []query.PatternElement{query.Variable{Name: x}, query.Variable{Name: y}}},
		}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.fails, canFailOnAValue(c.clauses, rules))
		})
	}
}

// A predicate or an expression run over a relation can fail on a value as
// canFailOnAValue judges it, except a database function whose entity is one
// of the relation's identity symbols, read as a predicate or under the scalar
// form: a database function fails on a value only where its entity value is
// no identity, every value of an identity symbol is one, and the scalar form
// binds any value. The tuple, collection and relation forms fail on a value
// of another shape, whatever the function's entity.
func TestCanFailOnAValueOverARelationsIdentities(t *testing.T) {
	e, x, d, v, w := datalog.NewSymbol("?e"), datalog.NewSymbol("?x"), datalog.NewSymbol("?d"), datalog.NewSymbol("?v"), datalog.NewSymbol("?w")
	entity := query.VariableTerm{Symbol: e}
	missing := &query.DatabaseFunctionPredicate{Function: &query.MissingFunction{Entity: entity, Attr: datalog.NewKeyword(":t/b")}}
	getElse := &query.Expression{
		Function: &query.GetElseFunction{Entity: entity, Attr: datalog.NewKeyword(":t/a"), Default: query.VariableTerm{Symbol: d}},
		Binding:  v,
	}
	getSome := &query.Expression{
		Function: &query.GetSomeFunction{Entity: entity, Attrs: []datalog.Keyword{datalog.NewKeyword(":t/a")}},
		Binding:  v,
	}
	plus := &query.Expression{
		Function: &query.ArithmeticFunction{Op: datalog.SymAdd, Args: []query.Term{entity, query.ConstantTerm{Value: int64(1)}}},
		Binding:  v,
	}
	registered := &query.FunctionPredicate{Fn: "even?", Args: []query.PatternElement{query.Variable{Name: e}}}
	less := &query.Comparison{Op: datalog.SymLT, Left: entity, Right: query.ConstantTerm{Value: int64(5)}}

	identities := []query.Symbol{e}
	cases := []struct {
		name       string
		clause     query.Clause
		identities []query.Symbol
		fails      bool
	}{
		{"a missing? whose entity is an identity symbol", missing, identities, false},
		{"a missing? whose entity is another symbol", missing, []query.Symbol{x}, true},
		{"a missing? over a relation naming no identity", missing, nil, true},
		{"a get-else whose entity is an identity symbol and whose default is a variable", getElse, identities, false},
		{"a get-else whose entity is another symbol", getElse, []query.Symbol{x}, true},
		{"a get-some whose entity is an identity symbol", getSome, identities, false},
		{"a get-else whose entity is an identity symbol under the tuple form",
			&query.Expression{Function: getElse.Function, Binding: query.TupleBinding{Variables: []query.Symbol{v, w}}}, identities, true},
		{"a get-else whose entity is an identity symbol under the collection form",
			&query.Expression{Function: getElse.Function, Binding: query.CollectionBinding{Variable: v}}, identities, true},
		{"a get-some whose entity is an identity symbol under the relation form",
			&query.Expression{Function: getSome.Function, Binding: query.RelationBinding{Variables: []query.Symbol{v, w}}}, identities, true},
		{"arithmetic over an identity symbol", plus, identities, true},
		{"a registered predicate over an identity symbol", registered, identities, true},
		{"a comparison", less, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.fails, canFailOnAValueOver(tc.clause, tc.identities))
		})
	}
}
