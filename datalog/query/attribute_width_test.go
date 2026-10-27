package query

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// An attribute keyword wider than storage holds is refused where a query's
// text names it, with the width rule's one message: a constant attribute in
// a data pattern, the attribute of get-else, get-some and missing?, and an
// attribute a find pull names, by ValidateQuery and Prepare over the query
// itself, over an inner query holding the clause, and by the rule set over a
// rule body holding it.
func TestValidateQueryRefusesAnAttributeWiderThanStorageHolds(t *testing.T) {
	e, v := sym("?e"), sym("?v")
	fits := datalog.NewKeyword(":t/" + strings.Repeat("a", datalog.MaxAttributeBytes-len(":t/")))
	wide := datalog.NewKeyword(fits.String() + "z")
	require.Equal(t, datalog.MaxAttributeBytes+1, len(wide.String()))
	first := scopePattern("?e", "t/a", "?v")
	names := "attribute " + wide.String() + " is 33 bytes, exceeds the 32-byte storage limit"

	clauses := []struct {
		name   string
		clause Clause
	}{
		{"a data pattern's constant attribute", &DataPattern{Elements: []PatternElement{Variable{Name: e}, Constant{Value: wide}, Variable{Name: v}}}},
		{"a get-else's attribute", &Expression{Function: &GetElseFunction{Entity: VariableTerm{Symbol: e}, Attr: wide, Default: "none"}, Binding: ScalarBinding{Variable: v}}},
		{"a get-some's attribute", &Expression{Function: &GetSomeFunction{Entity: VariableTerm{Symbol: e}, Attrs: []datalog.Keyword{fits, wide}}, Binding: ScalarBinding{Variable: v}}},
		{"a missing? predicate's attribute", &DatabaseFunctionPredicate{Function: &MissingFunction{Entity: VariableTerm{Symbol: e}, Attr: wide}}},
	}
	for _, c := range clauses {
		t.Run(c.name, func(t *testing.T) {
			q := &Query{Find: []FindElement{FindVariable{Symbol: e}}, Where: []Clause{first, c.clause}}
			require.ErrorContains(t, ValidateQuery(q), names, "ValidateQuery")
			_, err := Prepare(q, nil)
			require.ErrorContains(t, err, names, "Prepare")

			inner := &Query{Find: []FindElement{FindVariable{Symbol: e}}, Where: []Clause{first, c.clause}}
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
			require.ErrorContains(t, ValidateQuery(outer), names, "an inner query")

			_, err = NewRuleSet([]*Rule{defineRule("wide", []string{"?e"}, first, c.clause)})
			require.ErrorContains(t, err, names, "a rule body")
		})
	}

	pulls := []struct {
		name    string
		pattern *PullPattern
	}{
		{"a pull attribute", &PullPattern{Specs: []PullAttrSpec{&PullAttribute{Attr: wide}}}},
		{"a pull attribute under a map spec", &PullPattern{Specs: []PullAttrSpec{&PullMapSpec{Attr: fits, Pattern: &PullPattern{Specs: []PullAttrSpec{&PullAttribute{Attr: wide}}}}}}},
		{"a pull default's attribute", &PullPattern{Specs: []PullAttrSpec{&PullDefaultExpr{Attr: wide, Default: "none"}}}},
	}
	for _, p := range pulls {
		t.Run(p.name, func(t *testing.T) {
			q := &Query{Find: []FindElement{FindPull{Variable: e, Pattern: p.pattern}}, Where: []Clause{first}}
			require.ErrorContains(t, ValidateQuery(q), names, "ValidateQuery")
			_, err := Prepare(q, nil)
			require.ErrorContains(t, err, names, "Prepare")
		})
	}

	t.Run("an attribute at the limit is accepted", func(t *testing.T) {
		q := &Query{
			Find: []FindElement{FindPull{Variable: e, Pattern: &PullPattern{Specs: []PullAttrSpec{&PullAttribute{Attr: fits}}}}},
			Where: []Clause{
				&DataPattern{Elements: []PatternElement{Variable{Name: e}, Constant{Value: fits}, Variable{Name: v}}},
				&Expression{Function: &GetElseFunction{Entity: VariableTerm{Symbol: e}, Attr: fits, Default: "none"}, Binding: ScalarBinding{Variable: sym("?w")}},
			},
		}
		require.NoError(t, ValidateQuery(q))
	})
}
