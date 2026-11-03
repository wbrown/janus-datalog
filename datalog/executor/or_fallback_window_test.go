package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The window a form reads its subject into, filled and reported by the
// window reader one window at a time, and run by the iterator over the
// reader's window. Each contract below is the subject of one test.

// windowIterator is the iterator of a fallback over ?entity binding ?value
// through branches, over subject on matcher, under a cap of eight, with the
// events its handler sees appended to events.
func windowIterator(t *testing.T, matcher PatternMatcher, subject Relation, branches [][]query.Clause, events *[]annotations.Event) *OrFallbackIterator {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	opts := ExecutorOptions{ScopeWindowCap: 8, Handler: func(e annotations.Event) { *events = append(*events, e) }}
	exec := newQueryExecutor(matcher, nil, opts)
	rel, err := NewOrFallbackRelation(exec, NewContext(), []query.Symbol{entity}, []query.Symbol{value}, branches, subject, opts, true)
	require.NoError(t, err)
	it, ok := rel.Iterator().(*OrFallbackIterator)
	require.True(t, ok)
	require.NoError(t, it.err)
	return it
}

// subjectReader is a window reader over subject under a cap of eight, for
// an or-default-join, keyed on no position, with the events its handler sees
// appended to events.
func subjectReader(subject Relation, events *[]annotations.Event) *windowReader {
	opts := ExecutorOptions{ScopeWindowCap: 8, Handler: func(e annotations.Event) { *events = append(*events, e) }}
	return newWindowReader(subject.Iterator(), "or-default-join", subject.RequiresCopy(), nil, nil, opts)
}

// groundBranch binds ?value to zero and reads nothing of the subject.
func groundBranch() []query.Clause {
	return []query.Clause{&query.Expression{Function: &query.GroundFunction{Value: int64(0)}, Binding: datalog.NewSymbol("?value")}}
}

// cacheBuilds is the branch of each of the fallback's build events in
// events, in order; a subquery inside a branch builds under its own form.
func cacheBuilds(events []annotations.Event) []int {
	var builds []int
	for _, e := range events {
		if e.Name == annotations.ScopeBranchBuild && e.Data[annotations.KeyForm] == "or-default-join" {
			builds = append(builds, e.Data["branch"].(int))
		}
	}
	return builds
}

// The reader reads the subject into windows of one tuple, then two, doubling
// to the cap, into a workspace never larger than the cap, and the read after
// the subject's last tuple reports the subject exhausted, under the form,
// and ends.
func TestWindowReader_ReadsTheSubjectInWindowsDoublingToTheCap(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	_, tuples := windowItems(20, datalog.NewKeyword(":item/value"))
	var events []annotations.Event
	subject := NewStreamingRelation([]query.Symbol{entity}, NewMaterializedRelation([]query.Symbol{entity}, tuples).Iterator())
	w := subjectReader(subject, &events)

	var sizes, nextSizes []int
	for w.next() {
		sizes = append(sizes, len(w.window))
		nextSizes = append(nextSizes, w.size)
		require.LessOrEqual(t, cap(w.window), 8, "the workspace never grows past the cap")
	}
	require.Equal(t, []int{1, 2, 4, 8, 5}, sizes, "twenty tuples under a cap of eight fill five windows")
	require.Equal(t, []int{2, 4, 8, 8, 8}, nextSizes, "the next window doubles to the cap")
	require.Equal(t, 5, w.ordinal)
	require.True(t, w.exhausted)
	require.NoError(t, w.err)
	var exhausted []annotations.Event
	for _, e := range events {
		if e.Name == annotations.ScopeSubjectExhausted {
			exhausted = append(exhausted, e)
		}
	}
	require.Len(t, exhausted, 1, "the subject's end is reported once")
	require.Equal(t, "or-default-join", exhausted[0].Data[annotations.KeyForm], "the exhaustion carries the form")
}

// The reader copies each tuple of a subject that reuses its tuple
// workspace, so a window of several tuples holds each one's own values.
func TestWindowReader_CopiesATupleFromASubjectThatReusesItsWorkspace(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	e1, e2, e3 := datalog.NewIdentity("window-item1"), datalog.NewIdentity("window-item2"), datalog.NewIdentity("window-item3")
	var events []annotations.Event
	subject := NewStreamingRelation([]query.Symbol{entity}, newWorkspaceReusingIterator(1, [][]interface{}{{e1}, {e2}, {e3}}))
	w := subjectReader(subject, &events)

	require.True(t, w.next())
	require.Equal(t, []Tuple{{e1}}, w.window)
	require.True(t, w.next())
	require.Equal(t, []Tuple{{e2}, {e3}}, w.window, "the second window's two tuples each keep their own entity")
}

// A subject's failure is the reader's error: the read that meets it returns
// false with the error set.
func TestWindowReader_ASubjectsFailureIsTheReadersError(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	_, tuples := windowItems(3, datalog.NewKeyword(":item/value"))
	var events []annotations.Event
	subject := NewStreamingRelation([]query.Symbol{entity}, &failingIterator{inner: NewMaterializedRelation([]query.Symbol{entity}, tuples).Iterator(), failAfter: 1})
	w := subjectReader(subject, &events)

	require.True(t, w.next(), "the first window holds the tuple before the failure")
	require.False(t, w.next())
	require.ErrorIs(t, w.err, errInjectedIterator)
}

// The window event carries the form, the window's ordinal, its tuple count
// and its distinct key count over the required symbols the subject carries,
// and with no handler nothing is emitted.
func TestWindowReader_ReportsTheOrdinalTheTupleCountAndTheDistinctKeys(t *testing.T) {
	e1, e2 := datalog.NewIdentity("rw:e1"), datalog.NewIdentity("rw:e2")
	var events []annotations.Event
	w := &windowReader{
		form:    "or-join",
		options: ExecutorOptions{Handler: func(e annotations.Event) { events = append(events, e) }},
		keyPos:  []int{0},
		window:  []Tuple{{e1, int64(1)}, {e2, int64(2)}, {e1, int64(3)}},
		ordinal: 2,
	}
	w.reportWindow()
	require.Len(t, events, 1)
	require.Equal(t, annotations.ScopeWindow, events[0].Name)
	require.Equal(t, "or-join", events[0].Data[annotations.KeyForm])
	require.Equal(t, 2, events[0].Data["ordinal"])
	require.Equal(t, 3, events[0].Data["tuple_count"])
	require.Equal(t, 2, events[0].Data["distinct_keys"], "three tuples over two entities")

	silent := &windowReader{keyPos: []int{0}, window: w.window, ordinal: 2}
	silent.reportWindow()
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

	it.reader.window, it.reader.ordinal = []Tuple{{datalog.NewIdentity("failing1")}}, 1
	require.False(t, it.runWindow())
	require.ErrorIs(t, it.err, errInjectedIterator)
}
