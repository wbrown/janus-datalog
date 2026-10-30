package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Two bindings sharing an entity, each carrying one of the entity's two
// members at the pattern's value variable, answer each member's tuple once:
// each binding answers its own member on the cache route, and the answers
// are a set (docs/proposals/BOUND_VALUE_CACHE_MATCH_PROOF.md, T1, T3).
func TestCacheMatchOverBindingsCarryingTheValueAnswersEachTupleOnce(t *testing.T) {
	tag := datalog.NewKeyword(":bindery/tag")
	declared := schema.NewBuilder().Attribute(":bindery/tag").Type(schema.TypeString).Many().Add().MustBuild()
	db := createOptimizerModeDB(t, optimizerModes[0], DatabaseOptions{Schema: declared})
	quire := datalog.NewIdentity("bindery:quire1")
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(quire, tag, "folded"))
	require.NoError(t, tx.Add(quire, tag, "sewn"))
	_, err := tx.Commit()
	require.NoError(t, err)

	matcher := db.matcherWithExecOptions(db.effectivePlannerOptions()).(*PatternMatcher)
	e, v := datalog.NewSymbol("?e"), datalog.NewSymbol("?v")
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: e}, query.Constant{Value: tag}, query.Variable{Name: v},
	}}
	bindings := executor.NewMaterializedRelation([]query.Symbol{e, v}, []executor.Tuple{{quire, "folded"}, {quire, "sewn"}})
	rel, err := matcher.MatchWithConstraints(query.PatternQuery(pattern), executor.Relations{bindings}, nil)
	require.NoError(t, err)
	tuples, err := executor.CollectTuples(rel, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{quire, "folded"}, {quire, "sewn"}}, tuples, "each member's tuple once")
}
