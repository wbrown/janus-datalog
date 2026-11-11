package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The EA arm admits the pattern a lookup of its attribute answers per entity,
// [?e attr ?v] on the default source with the entity a key symbol, and no
// other. A pattern reading its fourth or fifth position is answered by the
// matcher, which decides those positions once for every route; one naming a
// source, one whose entity is a constant or no key symbol, one with a
// variable attribute and one with a constant value are not the lookup's
// shape either. Each takes the branch's planned build.
func TestTheEAArmAdmitsThePatternALookupAnswers(t *testing.T) {
	e := datalog.NewSymbol("?e")
	v := datalog.NewSymbol("?v")
	attr := datalog.NewKeyword(":t/a")
	key := []query.Symbol{e}
	entity := query.Variable{Name: e}
	attribute := query.Constant{Value: attr}
	value := query.Variable{Name: v}
	pattern := func(source query.Symbol, elements ...query.PatternElement) []query.Clause {
		return []query.Clause{&query.DataPattern{Source: source, Elements: elements}}
	}
	cases := []struct {
		name     string
		branch   []query.Clause
		admitted bool
	}{
		{"three elements, the entity a key symbol", pattern(nil, entity, attribute, value), true},
		{"a blank fourth position", pattern(nil, entity, attribute, value, query.Blank{}), false},
		{"a variable fourth position", pattern(nil, entity, attribute, value, query.Variable{Name: datalog.NewSymbol("?t")}), false},
		{"a constant false fifth position", pattern(nil, entity, attribute, value, query.Blank{}, query.Constant{Value: false}), false},
		{"a variable fifth position", pattern(nil, entity, attribute, value, query.Blank{}, query.Variable{Name: datalog.NewSymbol("?added")}), false},
		{"a named source", pattern(datalog.NewSymbol("$s"), entity, attribute, value), false},
		{"a constant entity", pattern(nil, query.Constant{Value: datalog.NewIdentity("t:e")}, attribute, value), false},
		{"an entity that is no key symbol", pattern(nil, query.Variable{Name: datalog.NewSymbol("?x")}, attribute, value), false},
		{"a variable attribute", pattern(nil, entity, query.Variable{Name: datalog.NewSymbol("?a")}, value), false},
		{"a constant value", pattern(nil, entity, attribute, query.Constant{Value: int64(1)}), false},
		{"two clauses", append(pattern(nil, entity, attribute, value), pattern(nil, entity, attribute, value)...), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotEntity, gotAttr, gotValue, admitted := eaArmShape(c.branch, key)
			require.Equal(t, c.admitted, admitted, "the arm admits the pattern a lookup answers and no other")
			if c.admitted {
				require.Equal(t, e, gotEntity, "the entity symbol")
				require.Equal(t, attr, gotAttr, "the attribute")
				require.Equal(t, v, gotValue, "the value symbol")
			}
		})
	}
}

// A lookup of an attribute answers, per entity, the pattern [?e attr ?v] on
// the default source: three elements, the entity a variable and the attribute
// a constant keyword, whatever the value position holds. Attribute-fetch
// fusion and the EA arm read that shape from lookupShape, so a pattern of
// another shape takes a match, where the matcher decides every position.
func TestALookupAnswersAPatternOfThreeElementsOnTheDefaultSource(t *testing.T) {
	e := datalog.NewSymbol("?e")
	attr := datalog.NewKeyword(":t/a")
	entity := query.Variable{Name: e}
	attribute := query.Constant{Value: attr}
	value := query.Variable{Name: datalog.NewSymbol("?v")}
	pattern := func(source query.Symbol, elements ...query.PatternElement) *query.DataPattern {
		return &query.DataPattern{Source: source, Elements: elements}
	}
	cases := []struct {
		name    string
		pattern *query.DataPattern
		isShape bool
	}{
		{"three elements, a variable value", pattern(nil, entity, attribute, value), true},
		{"three elements, a constant value", pattern(nil, entity, attribute, query.Constant{Value: int64(1)}), true},
		{"a blank fourth position", pattern(nil, entity, attribute, value, query.Blank{}), false},
		{"a variable fourth position", pattern(nil, entity, attribute, value, query.Variable{Name: datalog.NewSymbol("?t")}), false},
		{"a constant false fifth position", pattern(nil, entity, attribute, value, query.Blank{}, query.Constant{Value: false}), false},
		{"a named source", pattern(datalog.NewSymbol("$s"), entity, attribute, value), false},
		{"a constant entity", pattern(nil, query.Constant{Value: datalog.NewIdentity("t:e")}, attribute, value), false},
		{"a variable attribute", pattern(nil, entity, query.Variable{Name: datalog.NewSymbol("?a")}, value), false},
		{"two elements", pattern(nil, entity, attribute), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotEntity, gotAttr, isShape := lookupShape(c.pattern)
			require.Equal(t, c.isShape, isShape, "the lookup's shape is [?e attr ?v] on the default source")
			if c.isShape {
				require.Equal(t, e, gotEntity, "the entity symbol")
				require.Equal(t, attr, gotAttr, "the attribute")
			}
		})
	}
}
