package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A slice source is a database holding a slice's facts: the item at index i is
// the entity "slice:i", holding each accessor's value, every element of a
// slice-valued one.
func TestSliceSourceHoldsTheSlicesFacts(t *testing.T) {
	type Rule struct {
		Key       string
		DependsOn []string
	}
	rules := []Rule{
		{Key: "region-lore", DependsOn: []string{"world-lore", "region"}},
		{Key: "character-eval", DependsOn: []string{"character"}},
	}
	source, err := NewSliceSource(rules, AttributeSchema[Rule]{
		kw(":rule/key"):        func(r Rule) any { return r.Key },
		kw(":rule/depends-on"): func(r Rule) any { return r.DependsOn },
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Close()) }()

	sym := datalog.NewSymbol
	cases := []struct {
		name     string
		elements []query.PatternElement
		want     int
	}{
		{"all keys", []query.PatternElement{query.Variable{Name: sym("?r")}, query.Constant{Value: kw(":rule/key")}, query.Variable{Name: sym("?key")}}, 2},
		{"a key", []query.PatternElement{query.Variable{Name: sym("?r")}, query.Constant{Value: kw(":rule/key")}, query.Constant{Value: "region-lore"}}, 1},
		{"every element of a slice-valued accessor", []query.PatternElement{query.Variable{Name: sym("?r")}, query.Constant{Value: kw(":rule/depends-on")}, query.Variable{Name: sym("?dep")}}, 3},
		{"an element", []query.PatternElement{query.Variable{Name: sym("?r")}, query.Constant{Value: kw(":rule/depends-on")}, query.Constant{Value: "character"}}, 1},
		{"an entity's attributes", []query.PatternElement{query.Constant{Value: datalog.NewIdentity("slice:0")}, query.Variable{Name: sym("?a")}, query.Variable{Name: sym("?v")}}, 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tuples, err := executor.CollectTuples(source.Match(query.PatternQuery(&query.DataPattern{Elements: c.elements}), nil))
			require.NoError(t, err)
			require.Len(t, tuples, c.want)
		})
	}
}

// An empty slice's source holds no fact.
func TestSliceSourceOfAnEmptySliceHoldsNoFact(t *testing.T) {
	type Item struct{ Name string }
	source, err := NewSliceSource([]Item{}, AttributeSchema[Item]{
		kw(":item/name"): func(i Item) any { return i.Name },
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Close()) }()

	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: datalog.NewSymbol("?e")},
		query.Constant{Value: kw(":item/name")},
		query.Variable{Name: datalog.NewSymbol("?name")},
	}}
	tuples, err := executor.CollectTuples(source.Match(query.PatternQuery(pattern), nil))
	require.NoError(t, err)
	require.Empty(t, tuples)
}
