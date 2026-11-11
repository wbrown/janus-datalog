package annotations

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A relation that declines to size itself answers -1 from Size, a count no
// relation holds. The renderer renders such a relation over its symbols with
// no count, under either color setting, as it renders a sized relation with
// its count.
func TestRenderRelation_AnUnsizedRelationRendersNoCount(t *testing.T) {
	x, v := datalog.NewSymbol("?x"), datalog.NewSymbol("?v")
	unsized := RelationInfo{Attrs: []query.Symbol{x, v}, TupleCount: -1}
	for _, useColor := range []bool{false, true} {
		line := NewRelationRenderer(useColor).RenderRelation(unsized)
		require.Contains(t, line, "?x ?v", "color %v: the line names the symbols: %q", useColor, line)
		require.NotContains(t, line, "-1", "color %v: the line reports a count no relation holds: %q", useColor, line)
		require.NotContains(t, line, "Tuples", "color %v: the line carries a count for an unsized relation: %q", useColor, line)
	}
	require.Equal(t, "Relation([?x ?v])", NewRelationRenderer(false).RenderRelation(unsized))
	require.Equal(t, "Relation([?x ?v], 3 Tuples)", NewRelationRenderer(false).RenderRelation(RelationInfo{Attrs: []query.Symbol{x, v}, TupleCount: 3}))
}

// The match line of a pattern whose result streams names the result's
// symbols and no count.
func TestFormat_AMatchLineOfAnUnsizedResultNamesNoCount(t *testing.T) {
	x, v := datalog.NewSymbol("?x"), datalog.NewSymbol("?v")
	line := NewPlainTextFormatter(nil).Format(Event{
		Name: MatchesToRelations,
		Data: map[string]interface{}{
			KeyPattern:     "[?x :attr ?v]",
			"match.count":  -1,
			KeySuccess:     true,
			"symbol.order": []query.Symbol{x, v},
		},
	})
	require.Contains(t, line, "Relation([?x ?v])", "the match line renders the unsized result over its symbols: %q", line)
	require.NotContains(t, line, "-1", "the match line reports a count no relation holds: %q", line)
}

// A join line whose side or result is unsized names that relation's symbols
// and no count for it, beside the sized side's count.
func TestFormat_AJoinLineWithAnUnsizedSideNamesNoCount(t *testing.T) {
	id, left, right := datalog.NewSymbol("?id"), datalog.NewSymbol("?left"), datalog.NewSymbol("?right")
	line := NewPlainTextFormatter(nil).Format(Event{
		Name: JoinHash,
		Data: map[string]interface{}{
			"left.size":      2,
			"right.size":     -1,
			"result.size":    -1,
			"left.symbols":   []query.Symbol{id, left},
			"right.symbols":  []query.Symbol{id, right},
			"result.symbols": []query.Symbol{id, left, right},
		},
	})
	require.Contains(t, line, "Relation([?id ?left], 2 Tuples)", "the join line renders the sized side with its count: %q", line)
	require.Contains(t, line, "Relation([?id ?right])", "the join line renders the unsized side over its symbols: %q", line)
	require.Contains(t, line, "Relation([?id ?left ?right])", "the join line renders the unsized result over its symbols: %q", line)
	require.NotContains(t, line, "-1", "the join line reports a count no relation holds: %q", line)
}
