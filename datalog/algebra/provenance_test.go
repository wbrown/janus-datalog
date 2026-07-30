package algebra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestRewriteSinkDestinations pins the sink contract: a nil sink is valid and
// does nothing; Collect gates record accumulation (the normal query path pays
// nothing for provenance nobody reads); a handler receives each decision's
// event form regardless of Collect.
func TestRewriteSinkDestinations(t *testing.T) {
	var nilSink *RewriteSink
	nilSink.Record(RewriteRecord{Pass: "p", Action: RewriteApplied}, "pass/apply", nil)
	nilSink.Emit("pass/detail", nil)
	require.Nil(t, nilSink.Records())

	var events []annotations.Event
	handlerOnly := &RewriteSink{Handler: func(e annotations.Event) { events = append(events, e) }}
	handlerOnly.Record(RewriteRecord{Pass: "p", Action: RewriteApplied}, "pass/apply", map[string]interface{}{"k": 1})
	require.Empty(t, handlerOnly.Records(), "Collect off: events only, no records")
	require.Len(t, events, 1)
	require.Equal(t, "pass/apply", events[0].Name)

	collecting := &RewriteSink{Collect: true}
	collecting.Record(RewriteRecord{Pass: "p", Action: RewriteDeclined, Reason: "r"}, "pass/skip", nil)
	require.Len(t, collecting.Records(), 1)
	require.Equal(t, RewriteDeclined, collecting.Records()[0].Action)
	require.Equal(t, "r", collecting.Records()[0].Reason)
}

// TestSinkPredicatesAnswerTwoDifferentQuestions pins Recording and Emitting as
// the sink's two consumption questions, and that they are not the same question.
// A collect-only sink records and does not emit, which is what lets a site whose
// payload is event-only gate on the narrower one.
//
// Both are nil-safe: a nil sink consumes nothing, so a caller holding one need
// not know whether it is nil before asking.
func TestSinkPredicatesAnswerTwoDifferentQuestions(t *testing.T) {
	handler := func(annotations.Event) {}

	for _, tc := range []struct {
		name      string
		sink      *RewriteSink
		recording bool
		emitting  bool
	}{
		{"nil sink", nil, false, false},
		{"zero sink", &RewriteSink{}, false, false},
		{"collect only", &RewriteSink{Collect: true}, true, false},
		{"handler only", &RewriteSink{Handler: handler}, true, true},
		{"both", &RewriteSink{Collect: true, Handler: handler}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.recording, tc.sink.Recording())
			require.Equal(t, tc.emitting, tc.sink.Emitting())
		})
	}

	// The asymmetry is the point: Emitting implies Recording, never the reverse.
	// A test asserting only that both are false on a silent sink would pass with
	// the two predicates collapsed into one.
	collectOnly := &RewriteSink{Collect: true}
	require.True(t, collectOnly.Recording())
	require.False(t, collectOnly.Emitting())
}

// TestSilentSinkBuildsNoProvenance pins the cost the call-site guards exist for,
// which no assertion on records or events can reach: a pass renders its subject and
// builds a payload map for every node it looks at, and the guards inside Record
// and Emit cannot prevent that, because Go evaluates arguments before the call.
// On the normal query path — no handler, no explanation — nothing consumes any
// of it.
//
// A nil sink cannot build provenance at all, so it is the floor a silent sink
// must match.
func TestSilentSinkBuildsNoProvenance(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?s ?mx
	  :where
	  [?s :scenario/name ?n]
	  [(q [:find (max ?h) :in $ ?s :where [?p :price/scenario ?s] [?p :price/high ?h]] $ ?s) [[?mx]]]]`)
	require.NoError(t, err)
	root, err := Compile(q)
	require.NoError(t, err)

	optimizeWith := func(sink *RewriteSink) float64 {
		return testing.AllocsPerRun(20, func() {
			if _, err := NewOptimizer(DecorrelationPass(sink)).Optimize(root); err != nil {
				t.Fatal(err)
			}
		})
	}

	noSink := optimizeWith(nil)
	silent := optimizeWith(&RewriteSink{})
	observing := optimizeWith(&RewriteSink{Collect: true})
	t.Logf("allocations per optimize: no sink %v, silent %v, observing %v",
		noSink, silent, observing)
	require.Equal(t, noSink, silent,
		"the pass builds records or payloads for a silent sink")
	require.Less(t, silent, observing,
		"a collecting sink allocated no more than a silent one")
}

// TestDecorrelationPassRecords pins the typed provenance of the decorrelation
// pass: the correlated grouped-aggregate shape produces a considered record
// followed by an applied record; the pure-DataPattern shape produces a
// declined record carrying the precondition that failed.
func TestDecorrelationPassRecords(t *testing.T) {
	applied, err := parser.ParseQuery(`[:find ?s ?mx
	  :where
	  [?s :scenario/name ?n]
	  [(q [:find (max ?h) :in $ ?s :where [?p :price/scenario ?s] [?p :price/high ?h]] $ ?s) [[?mx]]]]`)
	require.NoError(t, err)
	root, err := Compile(applied)
	require.NoError(t, err)

	sink := &RewriteSink{Collect: true}
	_, err = NewOptimizer(DecorrelationPass(sink)).Optimize(root)
	require.NoError(t, err)

	var sawConsidered, sawApplied bool
	for _, r := range sink.Records() {
		require.Equal(t, "decorrelation", r.Pass)
		require.NotEmpty(t, r.Subject)
		switch r.Action {
		case RewriteConsidered:
			sawConsidered = true
		case RewriteApplied:
			sawApplied = true
		}
	}
	require.True(t, sawConsidered, "the pass considered the lateral join; records=%v", sink.Records())
	require.True(t, sawApplied, "the correlated aggregate shape decorrelates; records=%v", sink.Records())

	declined, err := parser.ParseQuery(`[:find ?e ?v
	  :where
	  [?e :thing/kind "widget"]
	  [(q [:find ?x :in $ ?e :where [?e :thing/value ?x]] $ ?e) [[?v] ...]]]`)
	require.NoError(t, err)
	root, err = Compile(declined)
	require.NoError(t, err)

	sink = &RewriteSink{Collect: true}
	_, err = NewOptimizer(DecorrelationPass(sink)).Optimize(root)
	require.NoError(t, err)

	var declineReason string
	for _, r := range sink.Records() {
		if r.Action == RewriteDeclined {
			declineReason = r.Reason
		}
	}
	require.Equal(t, "pure DataPattern query — indexed lookup is faster", declineReason,
		"records=%v", sink.Records())
}

// TestGetElsePassRecords pins the typed provenance the get-else pass
// produces, and its event forms: an applied record when the
// rewrite fires, a declined record with the failed precondition when the
// entity is an input parameter the child relation does not provide.
func TestGetElsePassRecords(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?title
	  :where
	  [?e :entity/type :entity.type/project]
	  [(get-else $ ?e :project/title "") ?title]]`)
	require.NoError(t, err)
	root, err := Compile(q)
	require.NoError(t, err)

	var events []annotations.Event
	sink := &RewriteSink{
		Collect: true,
		Handler: func(e annotations.Event) { events = append(events, e) },
	}
	_, err = NewOptimizer(GetElseScanRewritePass(sink)).Optimize(root)
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 1, "one get-else candidate, one decision")
	require.Equal(t, "get-else-scan-rewrite", records[0].Pass)
	require.Equal(t, RewriteApplied, records[0].Action)
	require.NotEmpty(t, records[0].Subject)

	var sawApplyEvent bool
	for _, e := range events {
		if e.Name == annotations.AlgebraGetElseScanApply {
			sawApplyEvent = true
		}
	}
	require.True(t, sawApplyEvent, "the applied record emits its event form; events=%v", events)

	// A consumer-only get-else compiles to a bare Map leaf: no child
	// relation exists to join against, and the decline says so.
	childless, err := parser.ParseQuery(`[:find ?title
	  :in $ ?e
	  :where
	  [(get-else $ ?e :project/title "") ?title]]`)
	require.NoError(t, err)
	root, err = Compile(childless)
	require.NoError(t, err)

	sink = &RewriteSink{Collect: true}
	_, err = NewOptimizer(GetElseScanRewritePass(sink)).Optimize(root)
	require.NoError(t, err)

	records = sink.Records()
	require.Len(t, records, 1, "records=%v", records)
	require.Equal(t, RewriteDeclined, records[0].Action)
	require.Equal(t, "no child relation to join against", records[0].Reason,
		"records=%v", records)

	// With a child relation that does not provide the entity variable (an
	// input parameter), the decline names that precondition instead.
	notProvided, err := parser.ParseQuery(`[:find ?title
	  :in $ ?e
	  :where
	  [?x :entity/type :entity.type/project]
	  [(get-else $ ?e :project/title "") ?title]]`)
	require.NoError(t, err)
	root, err = Compile(notProvided)
	require.NoError(t, err)

	sink = &RewriteSink{Collect: true}
	_, err = NewOptimizer(GetElseScanRewritePass(sink)).Optimize(root)
	require.NoError(t, err)

	records = sink.Records()
	require.Len(t, records, 1, "records=%v", records)
	require.Equal(t, RewriteDeclined, records[0].Action)
	require.Equal(t, "entity variable is not provided by the child relation", records[0].Reason,
		"records=%v", records)
}
