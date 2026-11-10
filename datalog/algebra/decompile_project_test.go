package algebra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
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

// A Project over clauses has no clause form, since Datalog states a
// projection at a scope's find alone, and the decompiler refuses it naming
// that.
func TestAProjectOverClausesHasNoClauseForm(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e :where [?e :t/a ?x] [?e :t/b ?y]]`)
	require.NoError(t, err)
	child, err := Compile(q, nil)
	require.NoError(t, err)
	over := &Node{Op: RuleProject, Children: []*Node{child}, Data: &Project{Symbols: []query.Symbol{datalog.NewSymbol("?e")}}}
	clauses, err := Decompile(over)
	require.EqualError(t, err, "a Project over clauses has no clause form: Datalog states a projection at a scope's find alone")
	require.Nil(t, clauses)
}
