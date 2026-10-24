package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The window of docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.2: a
// form's branches run once per window of the subject and are probed per
// window tuple, each window is reported by one event, a key answered by an
// earlier window is probed and never evaluated again, and a branch holding
// an invocation continues one fixpoint across the windows. Each contract is
// the subject of one test over the executor on an in-memory matcher.

// windowItems writes n entities carrying attr and returns their datoms and
// the subject tuples over entity, in entity order.
func windowItems(n int, attr datalog.Keyword) ([]datalog.Datom, []Tuple) {
	var datoms []datalog.Datom
	var tuples []Tuple
	for i := 1; i <= n; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("window-item%d", i))
		datoms = append(datoms, datalog.Datom{E: e, A: attr, V: int64(i), Tx: datalog.ElementID{Lamport: uint64(i), ReplicaID: 1}})
		tuples = append(tuples, Tuple{e})
	}
	return datoms, tuples
}

// windowEvents reads the window events a handler saw, in order: each
// window's ordinal, tuple count and distinct key count.
func windowEvents(events []annotations.Event) (ordinals, tupleCounts, distinctKeys []int) {
	for _, e := range events {
		if e.Name != annotations.ScopeWindow {
			continue
		}
		ordinals = append(ordinals, e.Data["ordinal"].(int))
		tupleCounts = append(tupleCounts, e.Data["tuple_count"].(int))
		distinctKeys = append(distinctKeys, e.Data["distinct_keys"].(int))
	}
	return ordinals, tupleCounts, distinctKeys
}

// Every subject, whether or not it reports a size, is read into windows of
// one tuple, then two, each window twice the last up to the cap, and each
// window reports its ordinal, its tuple count and its distinct key count.
func TestWindowGrowsFromOneTupleToTheCap(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	attr := datalog.NewKeyword(":item/value")
	const subjects = 20
	datoms, tuples := windowItems(subjects, attr)
	q := &query.Query{Where: []query.Clause{&query.OrJoinClause{
		RequiredVars: []query.Symbol{entity},
		OutputVars:   []query.Symbol{value},
		Branches:     [][]query.Clause{{branchPattern(entity, value, attr)}},
	}}}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var events []annotations.Event
			opts := mode.plannerOptions()
			opts.ScopeWindowCap = 8
			opts.Handler = func(e annotations.Event) { events = append(events, e) }
			exec := NewExecutorWithOptions(NewMemoryPatternMatcher(datoms), nil, opts).queryExecutor()
			answers := func(subject Relation) {
				groups, err := exec.Execute(NewContext(), q, []Relation{subject})
				require.NoError(t, err)
				require.Len(t, groups, 1, "the form's result stands in the subject's place")
				collected, err := CollectTuples(groups[0], nil)
				require.NoError(t, err)
				require.Len(t, collected, subjects, "every subject tuple is answered")
			}

			answers(NewStreamingRelation([]query.Symbol{entity}, NewMaterializedRelation([]query.Symbol{entity}, tuples).Iterator()))
			ordinals, tupleCounts, distinctKeys := windowEvents(events)
			require.Equal(t, []int{1, 2, 3, 4, 5}, ordinals, "twenty streaming tuples under a cap of eight are read in five windows")
			require.Equal(t, []int{1, 2, 4, 8, 5}, tupleCounts, "the windows double from one tuple to the cap")
			require.Equal(t, tupleCounts, distinctKeys, "every subject tuple is a distinct key")

			events = nil
			answers(NewMaterializedRelation([]query.Symbol{entity}, tuples))
			ordinals, tupleCounts, _ = windowEvents(events)
			require.Equal(t, []int{1, 2, 3, 4, 5}, ordinals, "a subject that reports a size is read into the same windows")
			require.Equal(t, []int{1, 2, 4, 8, 5}, tupleCounts)
		})
	}
}

// keyCountingMatcher counts the binding tuples the patterns are matched
// against: the keys a branch's evaluation seeks.
type keyCountingMatcher struct {
	PatternMatcher
	keys int
}

func (m *keyCountingMatcher) Match(q *query.Query, bindings Relations) (Relation, error) {
	for _, binding := range bindings {
		if n := binding.Size(); n > 0 {
			m.keys += n
		}
	}
	return m.PatternMatcher.Match(q, bindings)
}

// A key an earlier window answered is probed and never evaluated again: over
// a subject whose keys recur across windows, each key is sought in the
// window it first appears in, and the later windows probe the kept index.
func TestKeyAnsweredByAnEarlierWindowIsProbedNotEvaluated(t *testing.T) {
	entity, n, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?n"), datalog.NewSymbol("?value")
	attr := datalog.NewKeyword(":item/value")
	e1, e2, e3 := datalog.NewIdentity("recur1"), datalog.NewIdentity("recur2"), datalog.NewIdentity("recur3")
	datoms := []datalog.Datom{
		{E: e1, A: attr, V: int64(10), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: e2, A: attr, V: int64(20), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
		{E: e3, A: attr, V: int64(30), Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
	}
	// Six distinct subject tuples over three keys; under a cap past six the
	// windows hold one, two and three tuples, and every key recurs after the
	// window it first appears in.
	tuples := []Tuple{{e1, int64(1)}, {e2, int64(2)}, {e1, int64(3)}, {e2, int64(4)}, {e3, int64(5)}, {e1, int64(6)}}
	q := &query.Query{Where: []query.Clause{&query.OrJoinClause{
		RequiredVars: []query.Symbol{entity},
		OutputVars:   []query.Symbol{value},
		Branches:     [][]query.Clause{{branchPattern(entity, value, attr)}},
	}}}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			matcher := &keyCountingMatcher{PatternMatcher: NewMemoryPatternMatcher(datoms)}
			opts := mode.plannerOptions()
			opts.ScopeWindowCap = 8
			exec := NewExecutorWithOptions(matcher, nil, opts).queryExecutor()
			subject := NewStreamingRelation([]query.Symbol{entity, n}, NewMaterializedRelation([]query.Symbol{entity, n}, tuples).Iterator())
			groups, err := exec.Execute(NewContext(), q, []Relation{subject})
			require.NoError(t, err)
			require.Len(t, groups, 1)
			collected, err := CollectTuples(groups[0], nil)
			require.NoError(t, err)
			require.Len(t, collected, len(tuples), "every subject tuple is answered")
			require.Equal(t, 3, matcher.keys, "each key is sought once, in the window it first appears in, and probed after")
		})
	}
}

// A branch holding an invocation continues one fixpoint across the windows:
// the stratum events fire once per stratum however many windows the subject
// spans, and the deltas summed over every window's iterations count each
// derived tuple once.
func TestFixpointContinuedAcrossWindowsDerivesEachTupleOnce(t *testing.T) {
	const pages = 6
	matcher := NewMemoryPatternMatcher(chainDatoms(pages))
	rs := mustRules(t, followsRulesText)
	q := mustParse(t, `[:find ?x ?y :in $ % :where [?x :page/kind "leaf"] (or-default-join [[?x] ?y] (follows ?x ?y) [(ground :end) ?y])]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var begins, completes, derived, windows int
			opts := mode.plannerOptions()
			opts.ScopeWindowCap = 2
			opts.Handler = func(e annotations.Event) {
				switch e.Name {
				case annotations.RulesStratumBegin:
					begins++
				case annotations.RulesStratumComplete:
					completes++
				case annotations.RulesIteration:
					for _, size := range e.Data["delta"].(map[query.Symbol]int) {
						derived += size
					}
				case annotations.ScopeWindow:
					windows++
				}
			}
			exec := NewExecutorWithOptions(matcher, nil, opts)
			inputs, err := createInputRelationsFromValuesWithOptions(q, []interface{}{datalog.SymDollar, datalog.NewSymbol("%")}, exec.options)
			require.NoError(t, err)
			rel, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, inputs)
			require.NoError(t, err)
			collected, err := CollectTuples(rel, nil)
			require.NoError(t, err)
			require.Len(t, collected, pages*(pages-1)/2+1, "every reachable pair, and the sink's end")
			require.Greater(t, windows, 1, "the subject spans several windows")
			require.Equal(t, 2, begins, "the magic stratum and the adorned stratum each begin once, whatever the windows")
			require.Equal(t, 2, completes, "each stratum completes once, at the last window")
			require.Equal(t, pages+pages*(pages-1)/2, derived, "six magic facts and fifteen reachable pairs, each entering a delta once across the windows")
		})
	}
}

// The iteration bound is counted per window, from the window's first
// iteration, and the tuple bound counts the tuples carried across windows.
func TestIterationBoundIsCountedPerWindowAndTheTupleBoundIsCarried(t *testing.T) {
	const pages = 5
	matcher := NewMemoryPatternMatcher(chainDatoms(pages))
	rs := mustRules(t, followsRulesText)
	form := mustParse(t, `[:find ?x ?y :in $ % :where (or-default-join [[?x] ?y] (follows ?x ?y) [(ground :end) ?y])]`).Where
	x := datalog.NewSymbol("?x")
	run := func(t *testing.T, opts planner.PlannerOptions, order []Tuple) error {
		exec := NewExecutorWithOptions(matcher, nil, opts).queryExecutor()
		subject := NewStreamingRelation([]query.Symbol{x}, NewMaterializedRelation([]query.Symbol{x}, order).Iterator())
		groups, err := exec.Execute(NewContext().WithRules(rs), &query.Query{Where: form}, []Relation{subject})
		if err != nil {
			return err
		}
		require.Len(t, groups, 1)
		_, err = CollectTuples(groups[0], nil)
		return err
	}
	var fromTheHead, fromTheSink []Tuple
	for i := 1; i <= pages; i++ {
		fromTheHead = append(fromTheHead, Tuple{page(i)})
		fromTheSink = append(fromTheSink, Tuple{page(pages + 1 - i)})
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name+"/iterations per window", func(t *testing.T) {
			opts := mode.plannerOptions()
			opts.ScopeWindowCap = 1
			opts.MaxRuleIterations = 4
			err := run(t, opts, fromTheHead)
			var limit *query.RuleIterationLimitError
			require.ErrorAs(t, err, &limit, "the first window, seeded by page1 alone, iterates the magic stratum once per page of the chain, past four, where the whole seed at once converges in one")
		})
		t.Run(mode.name+"/tuples carried", func(t *testing.T) {
			opts := mode.plannerOptions()
			opts.ScopeWindowCap = 1
			opts.MaxDerivedTuples = 9
			err := run(t, opts, fromTheSink)
			var limit *query.DerivedTupleLimitError
			require.ErrorAs(t, err, &limit, "no window alone derives ten tuples from the sink's end, and the tuples carried across the windows do")
		})
	}
}
