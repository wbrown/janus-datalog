package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// A query holding a shape the parser refuses is refused with the parser's
// message at every boundary: by ValidateQuery and by Prepare over the query
// itself, over an inner query holding the shape, and by the rule set over a
// rule body holding it. The shapes: a form with no branch, a branch with no
// clause, a not or a not-join with no clause, an or-default with fewer than
// two branches, a data pattern of no element or of more than four, a
// negative limit and an unknown aggregate. A refusal inside a form names the
// form and the branch, or the clause of a not or a not-join, by its position
// counted from one.
func TestValidateQueryRefusesTheShapesTheParserRefuses(t *testing.T) {
	e, v, w := sym("?e"), sym("?v"), sym("?w")
	first := scopePattern("?e", "t/a", "?v")
	second := scopePattern("?e", "t/b", "?w")
	noElements := &DataPattern{}
	fiveElements := &DataPattern{Elements: []PatternElement{Variable{Name: e}, Constant{Value: datalog.NewKeyword(":t/a")}, Variable{Name: v}, Variable{Name: w}, Variable{Name: w}}}
	emptyNot := &NotClause{}

	clauseShapes := []struct {
		name   string
		clause Clause
		names  string
	}{
		{"an or with no branch", &OrClause{}, "or clause must have at least one branch"},
		{"an or with a branch of no clause", &OrClause{Branches: [][]Clause{{}, {second}}}, "or branch 1 must have at least one clause"},
		{"an or-join with a branch of no clause", &OrJoinClause{OutputVars: []Symbol{w}, Branches: [][]Clause{{}, {second}}}, "or-join branch 1 must have at least one clause"},
		{"a not with no clause", &NotClause{}, "not clause must have at least one inner clause"},
		{"a not-join with no clause", &NotJoinClause{RequiredVars: []Symbol{e}}, "not-join clause must have at least one inner clause"},
		{"an or-default with one branch", &OrDefaultClause{Branches: [][]Clause{{second}}}, "or-default clause must have at least two branches"},
		{"an or-default-join with a branch of no clause", &OrDefaultJoinClause{RequiredVars: []Symbol{e}, OutputVars: []Symbol{w}, Branches: [][]Clause{{}, {second}}}, "or-default-join branch 1 must have at least one clause"},
		{"a data pattern of no element", noElements, "data pattern must have 1 to 4 elements, got 0"},
		{"a data pattern of five elements", fiveElements, "data pattern must have 1 to 4 elements, got 5"},
		{"a not with no clause in an or's second branch", &OrClause{Branches: [][]Clause{{second}, {emptyNot}}}, "or branch 2: not clause must have at least one inner clause"},
		{"a not with no clause in an or-join's second branch", &OrJoinClause{OutputVars: []Symbol{w}, Branches: [][]Clause{{second}, {emptyNot}}}, "or-join branch 2: not clause must have at least one inner clause"},
		{"a not with no clause in an or-default's second branch", &OrDefaultClause{Branches: [][]Clause{{second}, {emptyNot}}}, "or-default branch 2: not clause must have at least one inner clause"},
		{"a not with no clause in an or-default-join's second branch", &OrDefaultJoinClause{RequiredVars: []Symbol{e}, OutputVars: []Symbol{w}, Branches: [][]Clause{{second}, {emptyNot}}}, "or-default-join branch 2: not clause must have at least one inner clause"},
		{"a not with no clause as a not's second clause", &NotClause{Clauses: []Clause{second, emptyNot}}, "not clause element 2: not clause must have at least one inner clause"},
		{"a not with no clause as a not-join's second clause", &NotJoinClause{RequiredVars: []Symbol{e}, Clauses: []Clause{second, emptyNot}}, "not-join clause element 2: not clause must have at least one inner clause"},
	}
	for _, c := range clauseShapes {
		t.Run(c.name, func(t *testing.T) {
			q := &Query{Find: []FindElement{FindVariable{Symbol: e}}, Where: []Clause{first, c.clause}}
			require.ErrorContains(t, ValidateQuery(q), c.names, "ValidateQuery")
			_, err := Prepare(q, nil)
			require.ErrorContains(t, err, c.names, "Prepare")

			inner := &Query{
				Find:  []FindElement{FindVariable{Symbol: e}},
				Where: []Clause{first, c.clause},
			}
			outer := &Query{
				Find: []FindElement{FindVariable{Symbol: e}},
				Where: []Clause{
					first,
					&SubqueryPattern{
						Query:   inner,
						Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}},
						Binding: RelationBinding{Variables: []Symbol{e}},
					},
				},
			}
			require.ErrorContains(t, ValidateQuery(outer), c.names, "an inner query")

			_, err = NewRuleSet([]*Rule{defineRule("shaped", []string{"?e"}, first, c.clause)})
			require.ErrorContains(t, err, c.names, "a rule body")
		})
	}

	t.Run("a negative limit", func(t *testing.T) {
		limit := -1
		q := &Query{Find: []FindElement{FindVariable{Symbol: e}}, Where: []Clause{first}, Limit: &limit}
		require.ErrorContains(t, ValidateQuery(q), ":limit must be non-negative, got -1", "ValidateQuery")
		_, err := Prepare(q, nil)
		require.ErrorContains(t, err, ":limit must be non-negative, got -1", "Prepare")
	})

	t.Run("an unknown aggregate", func(t *testing.T) {
		q := &Query{Find: []FindElement{FindAggregate{Function: datalog.NewSymbol("median"), Arg: v}}, Where: []Clause{first}}
		require.ErrorContains(t, ValidateQuery(q), "unknown aggregate function: median", "ValidateQuery")
		_, err := Prepare(q, nil)
		require.ErrorContains(t, err, "unknown aggregate function: median", "Prepare")

		outer := &Query{
			Find: []FindElement{FindVariable{Symbol: e}},
			Where: []Clause{
				first,
				&SubqueryPattern{
					Query:   q,
					Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}},
					Binding: RelationBinding{Variables: []Symbol{w}},
				},
			},
		}
		require.ErrorContains(t, ValidateQuery(outer), "unknown aggregate function: median", "an inner query")
	})
}

// A data pattern of one or two elements passes ValidateQuery and Prepare:
// the positions its text omits are unbound.
func TestValidateQueryAcceptsAShortDataPattern(t *testing.T) {
	e := sym("?e")
	shapes := []struct {
		name    string
		pattern *DataPattern
	}{
		{"two elements", &DataPattern{Elements: []PatternElement{Variable{Name: e}, Constant{Value: datalog.NewKeyword(":t/a")}}}},
		{"one element", &DataPattern{Elements: []PatternElement{Variable{Name: e}}}},
	}
	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			q := &Query{Find: []FindElement{FindVariable{Symbol: e}}, Where: []Clause{s.pattern}}
			require.NoError(t, ValidateQuery(q), "ValidateQuery")
			_, err := Prepare(q, nil)
			require.NoError(t, err, "Prepare")
		})
	}
}
