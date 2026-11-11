package executor

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// MockPatternMatcher reads its datoms as written, as a History handle does,
// so the fifth position of a pattern binds whether each datom asserts: true
// for an add and false for a remove. A constant there answers the datoms
// whose operation it names.
func TestMockPatternMatcherBindsWhetherEachDatomAsserts(t *testing.T) {
	doc := datalog.NewIdentity("raw:doc")
	many := datalog.NewKeyword(":w/many")
	e, v, tx, added := datalog.NewSymbol("?e"), datalog.NewSymbol("?v"), datalog.NewSymbol("?tx"), datalog.NewSymbol("?added")
	matcher := &MockPatternMatcher{data: map[string][]datalog.Datom{
		"[:w/many]": {
			{E: doc, A: many, V: "x", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}, Op: datalog.OpCRDTAdd},
			{E: doc, A: many, V: "y", Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}, Op: datalog.OpCRDTAdd},
			{E: doc, A: many, V: "x", Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}, Op: datalog.OpCRDTRemove},
		},
	}}
	withFifth := func(fifth query.PatternElement) *query.DataPattern {
		return &query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: e}, query.Constant{Value: many}, query.Variable{Name: v}, query.Variable{Name: tx}, fifth,
		}}
	}
	// answers matches pattern and renders each tuple's values at render,
	// sorted.
	answers := func(pattern *query.DataPattern, render ...query.Symbol) []string {
		rel, err := matcher.Match(query.PatternQuery(pattern), nil)
		require.NoError(t, err)
		positions, err := query.SymbolIndexTable(rel.Symbols(), render)
		require.NoError(t, err)
		var out []string
		require.NoError(t, ForEach(rel, func(tuple Tuple) error {
			parts := make([]string, len(positions))
			for i, position := range positions {
				parts[i] = fmt.Sprint(tuple[position])
			}
			out = append(out, strings.Join(parts, " "))
			return nil
		}))
		sort.Strings(out)
		return out
	}

	assert.Equal(t, []string{"x false", "x true", "y true"}, answers(withFifth(query.Variable{Name: added}), v, added),
		"a variable binds each datom's answer")
	assert.Equal(t, []string{"x"}, answers(withFifth(query.Constant{Value: false}), v),
		"the constant false answers the withdrawal")
	assert.Equal(t, []string{"x", "y"}, answers(withFifth(query.Constant{Value: true}), v),
		"the constant true answers the assertions")
}
