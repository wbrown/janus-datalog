package annotations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A join line names each side's symbols and the result's, rendered from the
// symbols the event carries.
func TestFormat_AJoinLineNamesEachSidesAndTheResultsSymbols(t *testing.T) {
	id, left, right := datalog.NewSymbol("?id"), datalog.NewSymbol("?left"), datalog.NewSymbol("?right")
	line := NewPlainTextFormatter(nil).Format(Event{
		Name: JoinHash,
		Data: map[string]interface{}{
			"left.size":      2,
			"right.size":     2,
			"result.size":    1,
			"left.symbols":   []query.Symbol{id, left},
			"right.symbols":  []query.Symbol{id, right},
			"result.symbols": []query.Symbol{id, left, right},
		},
	})
	for _, name := range []string{"?id", "?left", "?right"} {
		require.True(t, strings.Contains(line, name), "the join line %q names %s", line, name)
	}
}
