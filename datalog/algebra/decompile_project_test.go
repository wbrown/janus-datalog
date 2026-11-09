package algebra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestDecompileSchemaPlaceholderProjectProducesNoClause(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	placeholder := &Node{
		Op:   RuleProject,
		Data: &Project{Symbols: []query.Symbol{entity}},
	}

	clauses, err := Decompile(placeholder)
	require.NoError(t, err)
	require.Empty(t, clauses)
}
