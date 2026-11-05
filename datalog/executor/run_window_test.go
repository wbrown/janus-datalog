package executor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A run builds a keyed branch over the window's keys its frame has not run,
// builds nothing for a window whose keys all ran before, and builds a
// branch framed over no subject symbol at the first window alone.
func TestRunWindow_BuildsEachBranchOverItsRun(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	attr := datalog.NewKeyword(":item/value")
	datoms, _ := WindowItems(2, attr)
	e1, e2 := datalog.NewIdentity("window-item1"), datalog.NewIdentity("window-item2")
	var events []annotations.Event
	subject := NewMaterializedRelation([]query.Symbol{entity}, nil)
	it := WindowIterator(t, factsDatabase(t, datoms), subject, [][]query.Clause{{BranchPattern(entity, value, attr)}, GroundBranch()}, &events)

	it.SetReaderWindow([]Tuple{{e1}}, 1)
	require.True(t, it.RunWindow())
	require.Equal(t, []int{0, 1}, CacheBuilds(events), "the first window builds the keyed branch over e1 and the ground branch once")
	events = nil
	it.SetReaderWindow([]Tuple{{e1}, {e1}}, 2)
	require.True(t, it.RunWindow())
	require.Empty(t, CacheBuilds(events), "a window of keys that ran before builds nothing")
	it.SetReaderWindow([]Tuple{{e2}}, 3)
	require.True(t, it.RunWindow())
	require.Equal(t, []int{0}, CacheBuilds(events), "the third window builds the keyed branch over e2 alone")
	require.Equal(t, 2, it.BranchCache(0).Len(), "the keyed branch's index holds a tuple per key run")
	require.Equal(t, 1, it.BranchCache(1).Len(), "the ground branch's index holds its one tuple")
}

// Under the fallback a later branch runs over the window's keys the earlier
// branches left unanswered, and a window whose keys every branch has run
// builds nothing.
func TestRunWindow_AFallbacksLaterBranchRunsOverTheUnansweredKeys(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	first, second := datalog.NewKeyword(":item/first"), datalog.NewKeyword(":item/second")
	e1, e2 := datalog.NewIdentity("unanswered1"), datalog.NewIdentity("unanswered2")
	datoms := []datalog.Datom{
		{E: e1, A: first, V: int64(1), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: e2, A: second, V: int64(2), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
	}
	var events []annotations.Event
	subject := NewMaterializedRelation([]query.Symbol{entity}, nil)
	it := WindowIterator(t, factsDatabase(t, datoms), subject, [][]query.Clause{{BranchPattern(entity, value, first)}, {BranchPattern(entity, value, second)}}, &events)

	it.SetReaderWindow([]Tuple{{e1}, {e2}}, 1)
	require.True(t, it.RunWindow())
	require.Equal(t, []int{0, 1}, CacheBuilds(events))
	require.Equal(t, []Tuple{{e1, int64(1)}}, it.BranchCache(0).Probe(Tuple{e1}), "the first branch answers e1")
	require.Equal(t, 1, it.BranchCache(1).Len(), "the second branch runs over e2 alone, the key the first left unanswered")
	require.Equal(t, []Tuple{{e2, int64(2)}}, it.BranchCache(1).Probe(Tuple{e2}))
	for _, e := range events {
		if e.Name == annotations.OrFallbackBranchNarrowed && e.Data["branch"] == 1 {
			require.Equal(t, 1, e.Data["join_keys"], "the second branch's run holds the one unanswered key")
		}
	}

	events = nil
	it.SetReaderWindow([]Tuple{{e2}, {e1}}, 2)
	require.True(t, it.RunWindow())
	require.Empty(t, CacheBuilds(events), "every key of the second window ran before")
}

// A branch that reads nothing of the run but whose result carries the key
// answers the keys its result holds and no other: a later branch runs over
// the window's keys that result leaves out.
func TestRunWindow_ABranchReadingNothingWhoseResultCarriesTheKeyAnswersTheKeysItHolds(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	first, second := datalog.NewKeyword(":item/first"), datalog.NewKeyword(":item/second")
	e1, e2 := datalog.NewIdentity("carried1"), datalog.NewIdentity("carried2")
	datoms := []datalog.Datom{
		{E: e1, A: first, V: int64(1), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: e2, A: second, V: int64(2), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
	}
	// The first branch's subquery takes no argument, so the branch reads
	// nothing of the run, and its binding carries ?entity.
	whole := MustParse(t, `[:find ?entity ?value :where [(q [:find ?entity ?value :in $ :where [?entity :item/first ?value]] $) [[?entity ?value] ...]]]`).Where
	var events []annotations.Event
	subject := NewMaterializedRelation([]query.Symbol{entity}, nil)
	it := WindowIterator(t, factsDatabase(t, datoms), subject, [][]query.Clause{whole, {BranchPattern(entity, value, second)}}, &events)

	it.SetReaderWindow([]Tuple{{e1}, {e2}}, 1)
	require.True(t, it.RunWindow())
	require.Equal(t, []int{0, 1}, CacheBuilds(events), "the first branch is built once over the unit relation, and the second over the key its result leaves out")
	require.Equal(t, []Tuple{{e1, int64(1)}}, it.BranchCache(0).Probe(Tuple{e1}), "the first branch answers e1")
	require.Nil(t, it.BranchCache(0).Probe(Tuple{e2}), "the first branch's result does not hold e2")
	require.NotNil(t, it.BranchCache(1), "the second branch runs for e2")
	require.Equal(t, []Tuple{{e2, int64(2)}}, it.BranchCache(1).Probe(Tuple{e2}), "the second branch answers e2, the key the first branch's result leaves out")
}
