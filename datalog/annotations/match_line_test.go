package annotations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A match line names the result's symbols in the result's order, rendered
// from the symbols the event carries.
func TestFormat_AMatchLineNamesTheResultsSymbols(t *testing.T) {
	x, v := datalog.NewSymbol("?x"), datalog.NewSymbol("?v")
	line := NewPlainTextFormatter(nil).Format(Event{
		Name: MatchesToRelations,
		Data: map[string]interface{}{
			KeyPattern:     "[?x :attr ?v]",
			"match.count":  2,
			KeySuccess:     true,
			"symbol.order": []query.Symbol{x, v},
		},
	})
	require.True(t, strings.Contains(line, "Relation([?x ?v], 2 Tuples)"), "the match line %q renders the relation over the result's symbols", line)
}
