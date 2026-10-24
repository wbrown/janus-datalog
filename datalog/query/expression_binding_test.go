package query

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// An expression renders its binding by its form (NESTED_SCOPES_AS_QUERIES.md
// goal 12): the scalar ?a, the tuple form [?a ?b], the collection form
// [?a ...] and the relation form as [[?a ?b] ...], and a vector value as the
// vector of its rendered elements, so a ground of vectors parses back.
func TestExpressionRendersItsBindingForms(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	cases := []struct {
		name string
		expr *Expression
		want string
	}{
		{"scalar",
			&Expression{Function: GroundFunction{Value: int64(1)}, Binding: a},
			"[(ground 1) ?a]"},
		{"tuple",
			&Expression{Function: GroundFunction{Value: []interface{}{int64(1), "x"}}, Binding: TupleBinding{Variables: []Symbol{a, b}}},
			`[(ground [1 "x"]) [?a ?b]]`},
		{"relation",
			&Expression{Function: GroundFunction{Value: []interface{}{[]interface{}{int64(1), "x"}, []interface{}{int64(2), "y"}}}, Binding: RelationBinding{Variables: []Symbol{a, b}}},
			`[(ground [[1 "x"] [2 "y"]]) [[?a ?b] ...]]`},
		{"collection",
			&Expression{Function: GroundFunction{Value: []interface{}{int64(1), "x"}}, Binding: CollectionBinding{Variable: a}},
			`[(ground [1 "x"]) [?a ...]]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.expr.String(); got != c.want {
				t.Errorf("String() = %s, want %s", got, c.want)
			}
		})
	}
}

// ValidateStaticClauseShapes decides a ground's shape against its binding
// from the constant the text holds, so every boundary refuses the same
// shapes naming the repair: a flat ground under the relation form names the
// tuple form, and a ground vector of another length than the tuple form, or
// a ground inner vector of another arity than the relation form, names both
// counts. A function's result is read at run time, so an expression over a
// function is admitted under every form.
func TestValidateStaticClauseShapesDecidesAnExpressionBindingsShape(t *testing.T) {
	a, b, c := datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")
	idx, item, vec := datalog.NewSymbol("?idx"), datalog.NewSymbol("?item"), datalog.NewSymbol("?vec")
	enumerate := EnumerateFunction{VecTerm: VariableTerm{Symbol: vec}}
	vectors := []interface{}{[]interface{}{int64(1), int64(2)}, []interface{}{int64(3), int64(4)}}

	refused := []struct {
		name   string
		expr   *Expression
		repair string
	}{
		{"a flat ground under the relation form",
			&Expression{Function: &GroundFunction{Value: []interface{}{int64(0), int64(0)}}, Binding: RelationBinding{Variables: []Symbol{a, b}}},
			"[?a ?b]"},
		{"a ground vector of another length than the tuple form",
			&Expression{Function: &GroundFunction{Value: []interface{}{int64(1), int64(2)}}, Binding: TupleBinding{Variables: []Symbol{a, b, c}}},
			"2 values, 3 binding variables"},
		{"a ground inner vector of another arity than the relation form",
			&Expression{Function: GroundFunction{Value: []interface{}{[]interface{}{int64(1), int64(2)}, []interface{}{int64(3)}}}, Binding: RelationBinding{Variables: []Symbol{a, b}}},
			"tuple 2"},
		{"a scalar ground under the collection form",
			&Expression{Function: GroundFunction{Value: int64(1)}, Binding: CollectionBinding{Variable: a}},
			"scalar form ?a"},
	}
	for _, c := range refused {
		t.Run(c.name+" is refused", func(t *testing.T) {
			err := ValidateStaticClauseShapes([]Clause{c.expr})
			if err == nil {
				t.Fatalf("ValidateStaticClauseShapes accepts %s", c.expr)
			}
			if !strings.Contains(err.Error(), c.repair) {
				t.Errorf("the refusal %q does not name %q", err, c.repair)
			}
		})
	}

	admitted := []struct {
		name string
		expr *Expression
	}{
		{"a flat ground under the tuple form",
			&Expression{Function: &GroundFunction{Value: []interface{}{int64(0), int64(0)}}, Binding: TupleBinding{Variables: []Symbol{a, b}}}},
		{"a ground of vectors under the tuple form, each a value",
			&Expression{Function: GroundFunction{Value: vectors}, Binding: TupleBinding{Variables: []Symbol{a, b}}}},
		{"a ground of vectors under the relation form",
			&Expression{Function: GroundFunction{Value: vectors}, Binding: RelationBinding{Variables: []Symbol{a, b}}}},
		{"an empty ground under the relation form",
			&Expression{Function: &GroundFunction{Value: []interface{}{}}, Binding: RelationBinding{Variables: []Symbol{a, b}}}},
		{"a function yielding tuples under the relation form",
			&Expression{Function: enumerate, Binding: RelationBinding{Variables: []Symbol{idx, item}}}},
		{"a function yielding tuples under the tuple form, read as one tuple at run time",
			&Expression{Function: enumerate, Binding: TupleBinding{Variables: []Symbol{idx, item}}}},
		{"a ground vector under the collection form",
			&Expression{Function: GroundFunction{Value: []interface{}{int64(1), int64(2)}}, Binding: CollectionBinding{Variable: a}}},
		{"a function yielding tuples under the collection form",
			&Expression{Function: enumerate, Binding: CollectionBinding{Variable: idx}}},
		{"a scalar ground",
			&Expression{Function: GroundFunction{Value: int64(1)}, Binding: a}},
	}
	for _, c := range admitted {
		t.Run(c.name+" is admitted", func(t *testing.T) {
			if err := ValidateStaticClauseShapes([]Clause{c.expr}); err != nil {
				t.Errorf("ValidateStaticClauseShapes refuses %s: %v", c.expr, err)
			}
		})
	}
}
