package planner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A blank position is a wildcard: the explain analysis reads it as unbound,
// as it reads a position the pattern omits, so each pattern here binds its
// attribute alone.
func TestExplainReadsABlankPositionAsUnbound(t *testing.T) {
	e, v, tx := datalog.NewSymbol("?e"), datalog.NewSymbol("?v"), datalog.NewSymbol("?tx")
	name := query.Constant{Value: datalog.NewKeyword(":person/name")}
	cases := []struct {
		name    string
		pattern *query.DataPattern
	}{
		{"blank value", &query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: e}, name, query.Blank{}}}},
		{"blank entity", &query.DataPattern{Elements: []query.PatternElement{query.Blank{}, name, query.Variable{Name: v}}}},
		{"omitted transaction", &query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: e}, name, query.Variable{Name: v}}}},
		{"blank transaction", &query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: e}, name, query.Variable{Name: v}, query.Blank{}}}},
		{"unresolved transaction", &query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: e}, name, query.Variable{Name: v}, query.Variable{Name: tx}}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plan := analyzePatternForExplain(c.pattern, map[query.Symbol]bool{}, nil)
			assert.Equal(t, BoundMask{A: true}, plan.BoundMask)
		})
	}
}

// The rendered phase names every position a pattern binds, the fifth,
// whether the datom asserts, among them: a pattern whose only bound position
// is the fifth renders a Bound line naming it.
func TestExplainRendersABoundFifthPosition(t *testing.T) {
	e, a, v, tx := datalog.NewSymbol("?e"), datalog.NewSymbol("?a"), datalog.NewSymbol("?v"), datalog.NewSymbol("?tx")
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: e}, query.Variable{Name: a}, query.Variable{Name: v}, query.Variable{Name: tx}, query.Constant{Value: false},
	}}
	phase := RealizedPhase{
		Query:    &query.Query{Find: []query.FindElement{query.FindVariable{Symbol: e}}, Where: []query.Clause{pattern}},
		Patterns: []PatternPlan{analyzePatternForExplain(pattern, map[query.Symbol]bool{}, nil)},
	}
	assert.Contains(t, phase.String(), "Bound: E=false A=false V=false T=false Added=true")
}
