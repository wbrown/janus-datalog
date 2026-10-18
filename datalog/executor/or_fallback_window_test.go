package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The window the iterator reads the subject into, filled, reported and run
// one window at a time, on the iterator's own methods. Each contract below
// is the subject of one test.

// windowIterator is the iterator of a fallback over ?entity binding ?value
// through branches, over subject on matcher, under a cap of eight, with the
// events its handler sees appended to events.
func windowIterator(t *testing.T, matcher PatternMatcher, subject Relation, branches [][]query.Clause, events *[]annotations.Event) *OrFallbackIterator {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	opts := ExecutorOptions{ScopeWindowCap: 8, Handler: func(e annotations.Event) { *events = append(*events, e) }}
	exec := newQueryExecutor(matcher, nil, opts)
	rel := NewOrFallbackRelation(exec, NewContext(), []query.Symbol{entity}, []query.Symbol{value}, branches, subject, opts, true)
	it, ok := rel.Iterator().(*OrFallbackIterator)
	require.True(t, ok)
	require.NoError(t, it.err)
	return it
}

// groundBranch binds ?value to zero and reads nothing of the subject.
func groundBranch() []query.Clause {
	return []query.Clause{&query.Expression{Function: &query.GroundFunction{Value: int64(0)}, Binding: datalog.NewSymbol("?value")}}
}

// cacheBuilds is the branch of each cache-build event in events, in order.
func cacheBuilds(events []annotations.Event) []int {
	var builds []int
	for _, e := range events {
		if e.Name == annotations.OrFallbackCacheBuild {
			builds = append(builds, e.Data["branch"].(int))
		}
	}
	return builds
}

// A fill reads the subject into windows of one tuple, then two, doubling to
// the cap, into a workspace never larger than the cap, and the fill after
// the subject's last tuple reports the subject exhausted and ends the
// iterator.
func TestFillWindow_ReadsTheSubjectInWindowsDoublingToTheCap(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	datoms, tuples := windowItems(20, datalog.NewKeyword(":item/value"))
	var events []annotations.Event
	subject := NewStreamingRelation([]query.Symbol{entity}, NewMaterializedRelation([]query.Symbol{entity}, tuples).Iterator())
	it := windowIterator(t, NewMemoryPatternMatcher(datoms), subject, [][]query.Clause{groundBranch()}, &events)

	var sizes, nextSizes []int
	for it.fillWindow() {
		sizes = append(sizes, len(it.window))
		nextSizes = append(nextSizes, it.windowSize)
		require.LessOrEqual(t, cap(it.window), 8, "the workspace never grows past the cap")
	}
	require.Equal(t, []int{1, 2, 4, 8, 5}, sizes, "twenty tuples under a cap of eight fill five windows")
	require.Equal(t, []int{2, 4, 8, 8, 8}, nextSizes, "the next window doubles to the cap")
	require.Equal(t, 5, it.windows)
	require.True(t, it.done)
	require.NoError(t, it.err)
	exhausted := 0
	for _, e := range events {
		if e.Name == annotations.OrFallbackOuterExhausted {
			exhausted++
		}
	}
	require.Equal(t, 1, exhausted, "the subject's end is reported once")
}

// A fill copies each tuple of a subject that reuses its tuple workspace, so
// a window of several tuples holds each one's own values.
func TestFillWindow_CopiesATupleFromASubjectThatReusesItsWorkspace(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	datoms, _ := windowItems(3, datalog.NewKeyword(":item/value"))
	e1, e2, e3 := datalog.NewIdentity("window-item1"), datalog.NewIdentity("window-item2"), datalog.NewIdentity("window-item3")
	var events []annotations.Event
	subject := newMockUnsafeRelation([]query.Symbol{entity}, [][]interface{}{{e1}, {e2}, {e3}})
	it := windowIterator(t, NewMemoryPatternMatcher(datoms), subject, [][]query.Clause{groundBranch()}, &events)

	require.True(t, it.fillWindow())
	require.Equal(t, []Tuple{{e1}}, it.window)
	require.True(t, it.fillWindow())
	require.Equal(t, []Tuple{{e2}, {e3}}, it.window, "the second window's two tuples each keep their own entity")
}

// A subject's failure is the iterator's error: the fill that meets it
// returns false with the error set and the iterator done.
func TestFillWindow_ASubjectsFailureIsTheIteratorsError(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	datoms, tuples := windowItems(3, datalog.NewKeyword(":item/value"))
	var events []annotations.Event
	subject := NewStreamingRelation([]query.Symbol{entity}, &failingIterator{inner: NewMaterializedRelation([]query.Symbol{entity}, tuples).Iterator(), failAfter: 1})
	it := windowIterator(t, NewMemoryPatternMatcher(datoms), subject, [][]query.Clause{groundBranch()}, &events)

	require.True(t, it.fillWindow(), "the first window holds the tuple before the failure")
	require.False(t, it.fillWindow())
	require.ErrorIs(t, it.err, errInjectedIterator)
	require.True(t, it.done)
}

// The window event carries the window's ordinal, its tuple count and its
// distinct key count over the required symbols the subject carries, and
// with no handler nothing is emitted.
func TestReportWindow_CarriesTheOrdinalTheTupleCountAndTheDistinctKeys(t *testing.T) {
	e1, e2 := datalog.NewIdentity("rw:e1"), datalog.NewIdentity("rw:e2")
	var events []annotations.Event
	it := &OrFallbackIterator{
		options: ExecutorOptions{Handler: func(e annotations.Event) { events = append(events, e) }},
		keyPos:  []int{0},
		window:  []Tuple{{e1, int64(1)}, {e2, int64(2)}, {e1, int64(3)}},
		windows: 2,
	}
	it.reportWindow()
	require.Len(t, events, 1)
	require.Equal(t, annotations.OrFallbackWindow, events[0].Name)
	require.Equal(t, 2, events[0].Data["ordinal"])
	require.Equal(t, 3, events[0].Data["tuple_count"])
	require.Equal(t, 2, events[0].Data["distinct_keys"], "three tuples over two entities")

	silent := &OrFallbackIterator{keyPos: []int{0}, window: it.window, windows: 2}
	silent.reportWindow()
}

// A run builds a keyed branch over the window's keys its frame has not run,
// builds nothing for a window whose keys all ran before, and builds a
// branch framed over no subject symbol at the first window alone.
func TestRunWindow_BuildsEachBranchOverItsRun(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	attr := datalog.NewKeyword(":item/value")
	datoms, _ := windowItems(2, attr)
	e1, e2 := datalog.NewIdentity("window-item1"), datalog.NewIdentity("window-item2")
	var events []annotations.Event
	subject := NewMaterializedRelation([]query.Symbol{entity}, nil)
	it := windowIterator(t, NewMemoryPatternMatcher(datoms), subject, [][]query.Clause{{branchPattern(entity, value, attr)}, groundBranch()}, &events)

	it.window, it.windows = []Tuple{{e1}}, 1
	require.True(t, it.runWindow())
	require.Equal(t, []int{0, 1}, cacheBuilds(events), "the first window builds the keyed branch over e1 and the ground branch once")
	events = nil
	it.window, it.windows = []Tuple{{e1}, {e1}}, 2
	require.True(t, it.runWindow())
	require.Empty(t, cacheBuilds(events), "a window of keys that ran before builds nothing")
	it.window, it.windows = []Tuple{{e2}}, 3
	require.True(t, it.runWindow())
	require.Equal(t, []int{0}, cacheBuilds(events), "the third window builds the keyed branch over e2 alone")
	require.Equal(t, 2, it.branchCache[0].Len(), "the keyed branch's index holds a tuple per key run")
	require.Equal(t, 1, it.branchCache[1].Len(), "the ground branch's index holds its one tuple")
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
	it := windowIterator(t, NewMemoryPatternMatcher(datoms), subject, [][]query.Clause{{branchPattern(entity, value, first)}, {branchPattern(entity, value, second)}}, &events)

	it.window, it.windows = []Tuple{{e1}, {e2}}, 1
	require.True(t, it.runWindow())
	require.Equal(t, []int{0, 1}, cacheBuilds(events))
	require.Equal(t, []Tuple{{e1, int64(1)}}, it.branchCache[0].Probe(Tuple{e1}), "the first branch answers e1")
	require.Equal(t, 1, it.branchCache[1].Len(), "the second branch runs over e2 alone, the key the first left unanswered")
	require.Equal(t, []Tuple{{e2, int64(2)}}, it.branchCache[1].Probe(Tuple{e2}))
	for _, e := range events {
		if e.Name == annotations.OrFallbackBranchNarrowed && e.Data["branch"] == 1 {
			require.Equal(t, 1, e.Data["join_keys"], "the second branch's run holds the one unanswered key")
		}
	}

	events = nil
	it.window, it.windows = []Tuple{{e2}, {e1}}, 2
	require.True(t, it.runWindow())
	require.Empty(t, cacheBuilds(events), "every key of the second window ran before")
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
	whole := mustParse(t, `[:find ?entity ?value :where [(q [:find ?entity ?value :in $ :where [?entity :item/first ?value]] $) [[?entity ?value] ...]]]`).Where
	var events []annotations.Event
	subject := NewMaterializedRelation([]query.Symbol{entity}, nil)
	it := windowIterator(t, NewMemoryPatternMatcher(datoms), subject, [][]query.Clause{whole, {branchPattern(entity, value, second)}}, &events)

	it.window, it.windows = []Tuple{{e1}, {e2}}, 1
	require.True(t, it.runWindow())
	require.Equal(t, []int{0, 1}, cacheBuilds(events), "the first branch is built once over the unit relation, and the second over the key its result leaves out")
	require.Equal(t, []Tuple{{e1, int64(1)}}, it.branchCache[0].Probe(Tuple{e1}), "the first branch answers e1")
	require.Nil(t, it.branchCache[0].Probe(Tuple{e2}), "the first branch's result does not hold e2")
	require.NotNil(t, it.branchCache[1], "the second branch runs for e2")
	require.Equal(t, []Tuple{{e2, int64(2)}}, it.branchCache[1].Probe(Tuple{e2}), "the second branch answers e2, the key the first branch's result leaves out")
}

// A branch's failure is the iterator's error: the run that meets it returns
// false with the error set.
func TestRunWindow_ABranchsFailureIsTheIteratorsError(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	failingAttr := datalog.NewKeyword(":item/failing")
	matcher := &failingScanMatcher{
		failAttr: failingAttr,
		dataRel:  NewMaterializedRelation([]query.Symbol{entity, value}, nil),
	}
	var events []annotations.Event
	subject := NewMaterializedRelation([]query.Symbol{entity}, nil)
	it := windowIterator(t, matcher, subject, [][]query.Clause{{branchPattern(entity, value, failingAttr)}}, &events)

	it.window, it.windows = []Tuple{{datalog.NewIdentity("failing1")}}, 1
	require.False(t, it.runWindow())
	require.ErrorIs(t, it.err, errInjectedIterator)
}
