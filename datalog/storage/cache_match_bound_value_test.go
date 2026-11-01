package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Under a constant attribute a binding carrying the pattern's value answers
// the datoms agreeing with it at the entity and the value, and nothing else
// (docs/proposals/BOUND_VALUE_CACHE_MATCH_PROOF.md, T1, T2). Over a
// cardinality-many attribute a binding carrying a member answers that
// member's tuple, and a binding carrying a value no member equals answers
// nothing. Over a cardinality-one attribute a binding carrying the entity's
// value answers its tuple, and a binding carrying another value answers
// nothing.
func TestCacheMatchOverBindingsCarryingTheValueAnswersTheAgreeingTuplesAlone(t *testing.T) {
	tag, rank := datalog.NewKeyword(":bindery/tag"), datalog.NewKeyword(":bindery/rank")
	declared := schema.NewBuilder().Attribute(":bindery/tag").Type(schema.TypeString).Many().Add().MustBuild()
	db := createOptimizerModeDB(t, optimizerModes[0], DatabaseOptions{Schema: declared})
	quire, sheet := datalog.NewIdentity("bindery:quire1"), datalog.NewIdentity("bindery:sheet1")
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(quire, tag, "folded"))
	require.NoError(t, tx.Add(quire, tag, "sewn"))
	require.NoError(t, tx.Add(sheet, rank, int64(3)))
	_, err := tx.Commit()
	require.NoError(t, err)

	matcher := db.matcherWithExecOptions(db.effectivePlannerOptions()).(*PatternMatcher)
	e, v := datalog.NewSymbol("?e"), datalog.NewSymbol("?v")
	answers := func(attribute datalog.Keyword, bindings ...executor.Tuple) [][]interface{} {
		t.Helper()
		pattern := &query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: e}, query.Constant{Value: attribute}, query.Variable{Name: v},
		}}
		bound := executor.NewMaterializedRelation([]query.Symbol{e, v}, bindings)
		rel, err := matcher.MatchWithConstraints(query.PatternQuery(pattern), executor.Relations{bound}, nil)
		require.NoError(t, err)
		tuples, err := executor.CollectTuples(rel, nil)
		require.NoError(t, err)
		return tuples
	}

	require.ElementsMatch(t, [][]interface{}{{quire, "folded"}},
		answers(tag, executor.Tuple{quire, "folded"}, executor.Tuple{quire, "pressed"}),
		"the member's tuple, and nothing for the value no member equals")
	require.Empty(t, answers(rank, executor.Tuple{sheet, int64(4)}),
		"a value other than the entity's answers nothing")
	require.ElementsMatch(t, [][]interface{}{{sheet, int64(3)}},
		answers(rank, executor.Tuple{sheet, int64(3)}),
		"the entity's value answers its tuple")
}

// A cache match over bindings carrying each entity and its value, under a
// cardinality-one attribute, allocates its answers, one tuple each, their
// slice and the relation, and nothing of the answers' weight beside them:
// each binding answers at most the tuple agreeing with it, the answers are a
// set, and no key map is built
// (docs/proposals/BOUND_VALUE_CACHE_MATCH_PROOF.md, T3, T4).
func TestCacheMatchOverBindingsCarryingTheValueAllocatesItsAnswersAlone(t *testing.T) {
	const n = 1000
	rank := datalog.NewKeyword(":bindery/rank")
	db := createOptimizerModeDB(t, optimizerModes[0], DatabaseOptions{})
	tx := db.NewTransaction()
	tuples := make([]executor.Tuple, n)
	for i := range tuples {
		quire := datalog.NewIdentity(fmt.Sprintf("bindery:quire%d", i))
		require.NoError(t, tx.Add(quire, rank, int64(i)))
		tuples[i] = executor.Tuple{quire, int64(i)}
	}
	_, err := tx.Commit()
	require.NoError(t, err)

	matcher := db.matcherWithExecOptions(db.effectivePlannerOptions()).(*PatternMatcher)
	e, v := datalog.NewSymbol("?e"), datalog.NewSymbol("?v")
	q := query.PatternQuery(&query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: e}, query.Constant{Value: rank}, query.Variable{Name: v},
	}})
	bindings := executor.NewMaterializedRelation([]query.Symbol{e, v}, tuples)
	match := func() int {
		rel, err := matcher.MatchWithConstraints(q, executor.Relations{bindings}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return rel.Size()
	}
	require.Equal(t, n, match(), "one answer per binding, with the cache warm after it")

	perMatch := bytesPerMatch(func() { match() })
	assert.LessOrEqual(t, perMatch, answerBytes(n)+besideAnswers, "bytes per match of %d bindings carrying their values: %d, against answers of %d", n, perMatch, answerBytes(n))
}
