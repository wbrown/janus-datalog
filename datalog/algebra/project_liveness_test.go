package algebra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestInsertJoinProjectsDropsDeadSymbolsAfterSelectiveChild(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	payload := datalog.NewSymbol("?payload")
	root := compileProjectLivenessQuery(t)
	before := root.String()

	rewritten, err := InsertJoinProjects(root, []query.Symbol{entity, payload})
	require.NoError(t, err)
	require.Equal(t, before, root.String(), "rewrite must not mutate its input")

	require.Equal(t, RuleJoin, rewritten.Op)
	leftProject, ok := rewritten.Children[0].Data.(*Project)
	require.True(t, ok, "dead ?score must be projected after its selecting predicate")
	require.Equal(t, []query.Symbol{entity}, leftProject.Symbols)
	require.Equal(t, RuleSelect, rewritten.Children[0].Children[0].Op)
	require.Equal(t, []query.Symbol{entity, payload}, rewritten.Symbols())
	_, err = Analyze(rewritten)
	require.NoError(t, err)

	again, err := InsertJoinProjects(rewritten, []query.Symbol{entity, payload})
	require.NoError(t, err)
	require.Equal(t, rewritten.String(), again.String(), "rewrite must be idempotent")
}

func TestInsertJoinProjectsRetainsJoinKeysNotInTerminalOutput(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	payload := datalog.NewSymbol("?payload")
	root := compileProjectLivenessQuery(t)

	rewritten, err := InsertJoinProjects(root, []query.Symbol{payload})
	require.NoError(t, err)

	leftProject := rewritten.Children[0].Data.(*Project)
	require.Equal(t, []query.Symbol{entity}, leftProject.Symbols)
	require.Equal(t,
		[]query.Symbol{entity, payload},
		rewritten.Children[1].Symbols(),
		"right join key must survive even though only payload is returned",
	)
}

func TestInsertJoinProjectsDoesNotAddBoundaryWhenAllSymbolsAreLive(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	score := datalog.NewSymbol("?score")
	payload := datalog.NewSymbol("?payload")
	root := compileProjectLivenessQuery(t)

	rewritten, err := InsertJoinProjects(root, []query.Symbol{entity, score, payload})
	require.NoError(t, err)
	require.Equal(t, RuleSelect, rewritten.Children[0].Op)
	require.Equal(t, root.String(), rewritten.String())
}

func TestInsertJoinProjectsIsConservativeAcrossCompoundOperators(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	local := datalog.NewSymbol("?local")
	union := &Node{
		Op: RuleUnion,
		Data: &Union{
			Output: []query.Symbol{entity, value},
		},
		Children: []*Node{
			algebraTestScan(entity, value, local),
			algebraTestScan(entity, value),
		},
	}

	rewritten, err := InsertJoinProjects(union, []query.Symbol{value})
	require.NoError(t, err)
	require.Equal(t, union.String(), rewritten.String())
}

func TestInsertJoinProjectsAcceptsExpressionOnlyRoot(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	root := &Node{
		Op: RuleMap,
		Data: &Map{
			Expression: &query.Expression{
				Function: &query.GetElseFunction{
					Entity:  query.VariableTerm{Symbol: entity},
					Attr:    datalog.NewKeyword(":item/value"),
					Default: int64(0),
				},
				Binding: value,
			},
			Required: []query.Symbol{entity},
			Output:   []query.Symbol{value},
		},
	}

	rewritten, err := InsertJoinProjects(root, []query.Symbol{value, entity})
	require.NoError(t, err)
	require.Equal(t, root.String(), rewritten.String())
}

func compileProjectLivenessQuery(t *testing.T) *Node {
	t.Helper()
	parsed, err := parser.ParseQuery(
		`[:find ?entity ?payload
		  :where [?entity :item/score ?score]
		         [(> ?score 90)]
		         [?entity :item/payload ?payload]]`,
	)
	require.NoError(t, err)
	root, err := Compile(parsed)
	require.NoError(t, err)
	return root
}
