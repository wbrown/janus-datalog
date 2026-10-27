package query

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// ValidateQuery refuses a query holding a constant outside the value domain,
// at every position a constant stands: a pattern element, a ground, a
// predicate term, a get-else default, a subquery argument and a pull
// default, at the top level and inside a nested form or an inner query. The
// refusal names the value's type, or the value itself for an unsigned
// integer above int64's range and for NaN.
func TestValidateQueryRefusesAConstantOutsideTheDomain(t *testing.T) {
	e, x := datalog.NewSymbol("?e"), datalog.NewSymbol("?x")
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
			name  string
			query *Query
		}{
			{"a pattern element", &Query{Find: []FindElement{FindVariable{Symbol: e}}, Where: []Clause{pattern(c.value)}}},
			{"a ground", &Query{Find: []FindElement{FindVariable{Symbol: x}}, Where: []Clause{scan, ground(c.value)}}},
			{"a predicate term", &Query{Find: []FindElement{FindVariable{Symbol: e}}, Where: []Clause{
				&DataPattern{Elements: []PatternElement{Variable{Name: e}, Constant{Value: attr}, Variable{Name: x}}},
				&Comparison{Op: datalog.SymGT, Left: VariableTerm{Symbol: x}, Right: ConstantTerm{Value: c.value}},
			}}},
			{"a get-else default", &Query{Find: []FindElement{FindVariable{Symbol: x}}, Where: []Clause{
				scan,
				&Expression{Function: &GetElseFunction{Entity: VariableTerm{Symbol: e}, Attr: attr, Default: c.value}, Binding: x},
			}}},
			{"a subquery argument", &Query{Find: []FindElement{FindVariable{Symbol: e}}, Where: []Clause{
				scan,
				&SubqueryPattern{
					Query:   &Query{Find: []FindElement{FindVariable{Symbol: x}}, In: []InputSpec{DatabaseInput{Name: datalog.SymDollar}, ScalarInput{Symbol: x}}, Where: []Clause{pattern(int64(1))}},
					Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}, Constant{Value: c.value}},
					Binding: RelationBinding{Variables: []Symbol{x}},
				},
			}}},
			{"an inner query's ground", &Query{Find: []FindElement{FindVariable{Symbol: e}}, Where: []Clause{
				scan,
				&SubqueryPattern{
					Query:   &Query{Find: []FindElement{FindVariable{Symbol: x}}, In: []InputSpec{DatabaseInput{Name: datalog.SymDollar}}, Where: []Clause{ground(c.value)}},
					Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}},
					Binding: RelationBinding{Variables: []Symbol{x}},
				},
			}}},
			{"a pattern inside a not", &Query{Find: []FindElement{FindVariable{Symbol: e}}, Where: []Clause{scan, &NotClause{Clauses: []Clause{pattern(c.value)}}}}},
			{"a pull default", &Query{
				Find:  []FindElement{FindPull{Variable: e, Pattern: &PullPattern{Specs: []PullAttrSpec{&PullDefaultExpr{Attr: attr, Default: c.value}}}}},
				Where: []Clause{scan},
			}},
		}
		for _, p := range positions {
			t.Run(c.name+" as "+p.name, func(t *testing.T) {
				err := ValidateQuery(p.query)
				require.ErrorContains(t, err, c.names)
			})
		}
	}

	t.Run("a query of domain values is accepted", func(t *testing.T) {
		q := &Query{
			Find: []FindElement{FindPull{Variable: e, Pattern: &PullPattern{Specs: []PullAttrSpec{&PullDefaultExpr{Attr: attr, Default: datalog.ElementID{Lamport: 1, ReplicaID: 1}}}}}},
			Where: []Clause{
				pattern(math.Inf(1)),
				ground([]interface{}{int64(1), "s", datalog.NewKeyword(":k"), &datalog.ElementID{Lamport: 2, ReplicaID: 1}}),
			},
		}
		require.NoError(t, ValidateQuery(q))
	})
}
