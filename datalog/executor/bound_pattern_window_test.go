package executor_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A pattern composed over its subject's window stands in the consumed
// group's place, over the group's symbols and the pattern's other variable,
// reports no size, and answers each subject tuple joined with its matches;
// a group sharing no variable with the pattern stands as it is.
func TestExecuteBoundPatternStandsInTheConsumedGroupsPlace(t *testing.T) {
	entity, value, other := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value"), datalog.NewSymbol("?other")
	attr := datalog.NewKeyword(":t/a")
	e1, e2 := datalog.NewIdentity("bound:1"), datalog.NewIdentity("bound:2")
	datoms := []datalog.Datom{
		{E: e1, A: attr, V: int64(5), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: e2, A: attr, V: int64(6), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
	}
	subject := NewStreamingRelation([]query.Symbol{entity}, NewMaterializedRelation([]query.Symbol{entity}, []Tuple{{e1}, {e2}}).Iterator())
	unrelated := NewMaterializedRelation([]query.Symbol{other}, []Tuple{{int64(1)}})
	exec := NewQueryExecutor(factsDatabase(t, datoms), nil, ExecutorOptions{})

	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity}, query.Constant{Value: attr}, query.Variable{Name: value},
	}}
	groups, err := exec.ExecuteBoundPattern(NewContext(), pattern, Relations{unrelated, subject})
	require.NoError(t, err)
	require.Len(t, groups, 2, "the result stands in the subject's place beside the group the pattern does not read")
	require.Same(t, unrelated, groups[0])
	result := groups[1]
	assert.Equal(t, []query.Symbol{entity, value}, result.Symbols())
	assert.Equal(t, -1, result.Size(), "the result over a streaming group reports no size")
	tuples, err := CollectTuples(result, nil)
	require.NoError(t, err)
	assert.ElementsMatch(t, [][]interface{}{{e1, int64(5)}, {e2, int64(6)}}, tuples)
}

// A pattern with no match answers the empty relation without reading its
// subject: the pattern's matches are the join's other side, and a join with
// an empty side is empty whatever the subject holds
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, T5).
func TestExecuteBoundPatternWithNoMatchLeavesItsSubjectUnread(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	e1 := datalog.NewIdentity("bound:1")
	datoms := []datalog.Datom{
		{E: e1, A: datalog.NewKeyword(":t/other"), V: int64(5), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}
	subjectIter := NewSliceIterator([]Tuple{{e1}})
	subject := NewStreamingRelation([]query.Symbol{entity}, subjectIter)
	exec := NewQueryExecutor(factsDatabase(t, datoms), nil, ExecutorOptions{})

	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity}, query.Constant{Value: datalog.NewKeyword(":t/a")}, query.Variable{Name: value},
	}}
	groups, err := exec.ExecuteBoundPattern(NewContext(), pattern, Relations{subject})
	require.NoError(t, err)
	require.Len(t, groups, 1, "the result stands in the subject's place")
	assert.Equal(t, []query.Symbol{entity, value}, groups[0].Symbols())
	tuples, err := CollectTuples(groups[0], nil)
	require.NoError(t, err)
	assert.Empty(t, tuples)
	assert.Equal(t, -1, subjectIter.Pos(), "the subject is never pulled")
}
