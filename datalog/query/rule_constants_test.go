package query

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// NewRuleSet refuses a set holding a constant outside the value domain at
// every position a body holds one, as ValidateQuery refuses the same
// constant in a query's :where: a pattern element, a ground, a predicate
// term, a get-else default, a subquery argument, and a pattern inside a not
// or an or branch. The refusal names the rule and the value's type, or the
// value itself for NaN and an unsigned integer above int64's range. A set
// whose constants are all of the domain is accepted.
func TestNewRuleSetRefusesAConstantOutsideTheDomain(t *testing.T) {
	e, x := sym("?e"), sym("?x")
	attr := datalog.NewKeyword(":t/a")
	pattern := func(v interface{}) Clause {
		return &DataPattern{Elements: []PatternElement{Variable{Name: e}, Constant{Value: attr}, Constant{Value: v}}}
	}
	ground := func(v interface{}) Clause {
		return &Expression{Function: &GroundFunction{Value: v}, Binding: x}
	}
	scan := &DataPattern{Elements: []PatternElement{Variable{Name: e}, Constant{Value: attr}, Blank{}}}
	outside := []struct {
		name  string
		value interface{}
		names string
	}{
		{"a float32", float32(2), "float32"},
		{"a map", map[string]interface{}{"a": int64(1)}, "map[string]interface {}"},
		{"an unsigned integer above int64's range", uint64(math.MaxInt64) + 1, "9223372036854775808"},
		{"NaN", math.NaN(), "NaN"},
		{"a vector holding a float32", []interface{}{int64(1), float32(2)}, "float32"},
	}
	for _, c := range outside {
		positions := []struct {
			name string
			rule *Rule
		}{
			{"a pattern element", defineRule("r", []string{"?e"}, pattern(c.value))},
			{"a ground", defineRule("r", []string{"?x"}, scan, ground(c.value))},
			{"a predicate term", defineRule("r", []string{"?e"},
				&DataPattern{Elements: []PatternElement{Variable{Name: e}, Constant{Value: attr}, Variable{Name: x}}},
				&Comparison{Op: datalog.SymGT, Left: VariableTerm{Symbol: x}, Right: ConstantTerm{Value: c.value}},
			)},
			{"a get-else default", defineRule("r", []string{"?x"},
				scan,
				&Expression{Function: &GetElseFunction{Entity: VariableTerm{Symbol: e}, Attr: attr, Default: c.value}, Binding: x},
			)},
			{"a subquery argument", defineRule("r", []string{"?e"},
				scan,
				&SubqueryPattern{
					Query:   &Query{Find: []FindElement{FindVariable{Symbol: x}}, In: []InputSpec{DatabaseInput{Name: datalog.SymDollar}, ScalarInput{Symbol: x}}, Where: []Clause{pattern(int64(1))}},
					Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}, Constant{Value: c.value}},
					Binding: RelationBinding{Variables: []Symbol{x}},
				},
			)},
			{"a pattern inside a not", defineRule("r", []string{"?e"}, scan, &NotClause{Clauses: []Clause{pattern(c.value)}})},
			{"a pattern inside an or branch", defineRule("r", []string{"?e"}, scan, &OrClause{Branches: [][]Clause{{pattern(c.value)}, {pattern(int64(1))}}})},
		}
		for _, p := range positions {
			t.Run(c.name+" as "+p.name, func(t *testing.T) {
				var err error
				require.NotPanics(t, func() { _, err = NewRuleSet([]*Rule{p.rule}) })
				require.ErrorContains(t, err, "rule r", "the refusal names the rule")
				require.ErrorContains(t, err, c.names, "the refusal names the constant")
			})
		}
	}

	t.Run("a body of domain values is accepted", func(t *testing.T) {
		rule := defineRule("r", []string{"?x"},
			pattern(math.Inf(1)),
			ground([]interface{}{int64(1), "s", datalog.NewKeyword(":k"), &datalog.ElementID{Lamport: 2, ReplicaID: 1}}),
		)
		_, err := NewRuleSet([]*Rule{rule})
		require.NoError(t, err)
	})
}
