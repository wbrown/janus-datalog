package algebra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestDecompileProjectProducesRelationBindingSubquery(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	parsed, err := parser.ParseQuery(
		`[:find ?entity
		  :where [?entity :item/score ?score]
		         [(> ?score 90)]]`,
	)
	require.NoError(t, err)
	child, err := Compile(parsed)
	require.NoError(t, err)
	project := &Node{
		Op:       RuleProject,
		Children: []*Node{child},
		Data:     &Project{Symbols: []query.Symbol{entity}},
	}

	clauses, err := Decompile(project)
	require.NoError(t, err)
	require.Len(t, clauses, 1)
	subquery, ok := clauses[0].(*query.SubqueryPattern)
	require.True(t, ok)
	require.Equal(t,
		[]query.FindElement{query.FindVariable{Symbol: entity}},
		subquery.Query.Find,
	)
	require.Equal(t,
		[]query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
		subquery.Query.In,
	)
	require.Len(t, subquery.Query.Where, 2)
	require.Equal(t,
		[]query.PatternElement{query.Constant{Value: datalog.SymDollar}},
		subquery.Inputs,
	)
	require.Equal(t,
		query.RelationBinding{Variables: []query.Symbol{entity}},
		subquery.Binding,
	)
}

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
