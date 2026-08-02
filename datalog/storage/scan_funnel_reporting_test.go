package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// funnelSchema declares one attribute per cardinality so a query can be aimed
// at each dispatch arm by choosing an attribute and which positions it binds.
func funnelSchema(t *testing.T) schema.SchemaProvider {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/tag").Type(schema.TypeString).Many().Add().
		Attribute(":person/skill").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	return s
}

// funnelFixture writes one entity carrying all three attributes, each written
// more than once so history depth is real: intake exceeds resolution output on
// every arm, which is the gap the funnel exists to show.
func funnelFixture(t *testing.T, db *Database) datalog.Identity {
	t.Helper()
	e := datalog.NewIdentity("funnel:alice")

	// Three writes to the cardinality-one attribute: two are history.
	for _, name := range []string{"Alice", "Alicia", "Alys"} {
		tx := db.NewTransaction()
		require.NoError(t, tx.Set(e, datalog.NewKeyword(":person/name"), name))
		_, err := tx.Commit()
		require.NoError(t, err)
	}

	// Add then remove then re-add a tag, so the set's history is deeper than
	// its membership.
	tagKw := datalog.NewKeyword(":person/tag")
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, tagKw, "dev"))
	require.NoError(t, tx.Add(e, tagKw, "ops"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx = db.NewTransaction()
	require.NoError(t, tx.Remove(e, tagKw, "ops"))
	_, err = tx.Commit()
	require.NoError(t, err)

	tx = db.NewTransaction()
	require.NoError(t, tx.Set(e, datalog.NewKeyword(":person/skill"), []string{"go", "sql"}))
	_, err = tx.Commit()
	require.NoError(t, err)

	return e
}

// TestEveryDispatchArmAnnouncesItsRunAndReportsItsFunnel holds every dispatch
// arm to the same two obligations: announce the run it addressed, and report
// what that run cost — on one event each, both naming the same run.
//
// The completion carrying the run, and not merely its index, is what lets a
// scan line be rendered from that event alone. The engine emits from parallel
// workers through one handler, so a consumer that held the announcement to
// annotate the completion would be pairing one worker's run with another's
// cost. An arm holds its bound; the obligation is to report it, not to leave
// the reader to join two events.
//
// One case per arm, rather than one query asserting "some arm reported": delete
// either emit from any arm and exactly that arm's case reds. A test that only
// showed a query producing events stays green while several arms scan silently.
//
// The cache is disabled for the E-and-A-bound arms because matchFromCache
// intercepts them otherwise; that arm has its own case below.
func TestEveryDispatchArmAnnouncesItsRunAndReportsItsFunnel(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			// How E is supplied decides which family of paths runs, and both
			// families are in scope. A literal E — #id hashes the seed at parse
			// time — is a constant and reaches the cardinality dispatch arms.
			// An :in parameter binds E through a binding relation, which
			// matcher_relations.go routes into analyzeReuseStrategy and the
			// binding-driven strategies instead. Those report the same funnel
			// on a different event, and this table covers both rather than
			// aiming only where the constant arms live.
			// No completions column. Every arm reports through
			// annotations.StorageScanComplete — the one completion event — so a
			// per-arm list of names would restate the same constant eight times.
			// Which strategy an arm picked is not what this table asserts, only
			// that it reported.
			// wantResolved is what CRDT resolution produces for the (E, A) groups
			// the arm reads, per funnelFixture: one surviving value for
			// `:person/name` (three writes, last wins) and for `:person/tag` (dev
			// added, ops added then removed), two for `:person/skill` (both RGA
			// elements live). On the vector arms it differs from matched, which
			// counts tuples — the vector binds as one value however many elements
			// it holds.
			for _, tc := range []struct {
				arm          string
				query        string
				inputEntity  bool      // supply E as an :in parameter
				index        IndexType // the run the arm must announce
				noRun        bool      // set instead when the arm addresses none
				wantResolved int
			}{
				{arm: "matchCardinalityManyAsRelation",
					query: `[:find ?v :where [#id "funnel:alice" :person/tag ?v]]`,
					index: EAVT, wantResolved: 1},
				{arm: "matchCardinalityVectorAsRelation",
					query: `[:find ?v :where [#id "funnel:alice" :person/skill ?v]]`,
					index: EATV, wantResolved: 2},
				{arm: "matchCardinalityManyMembership",
					query: `[:find ?tx :where [#id "funnel:alice" :person/tag "dev" ?tx]]`,
					index: EAVT, wantResolved: 1},
				{arm: "matchCardinalityManyScanAllEntities",
					query: `[:find ?e ?v :where [?e :person/tag ?v]]`,
					index: AEVT, wantResolved: 1},
				{arm: "matchCardinalityManyFindEntitiesWithValue",
					query: `[:find ?e :where [?e :person/tag "dev"]]`,
					index: AVET, wantResolved: 1},
				{arm: "matchVectorScanAllEntities",
					query: `[:find ?e ?v :where [?e :person/skill ?v]]`,
					index: AEVT, wantResolved: 2},

				// The general arm — no cardinality dispatch, E unbound — is the
				// one the other six are exceptions to, so a table of exceptions
				// that omitted it would leave the common path pinned nowhere.
				{arm: "unbound scan, general arm",
					query: `[:find ?e ?n :where [?e :person/name ?n]]`,
					index: AETV, wantResolved: 1},

				// A binding relation over the same cardinality-many attribute.
				// Which strategy chooseJoinStrategy picks is its own business
				// and pinning it here would couple this test to that choice —
				// what this case asserts is that whichever it picks reports.
				{arm: "binding-driven, E from :in",
					query: `[:find ?v :in $ ?e :where [?e :person/tag ?v]]`, inputEntity: true,
					noRun: true, wantResolved: 1},
			} {
				t.Run(tc.arm, func(t *testing.T) {
					// Registered before the open, since everything the database
					// builds is constructed with it. The recording flag is what
					// scopes collection to the query: the handler decides what to
					// report, so the fixture's writes below stay out of events.
					var events []annotations.Event
					recording := false
					db := createOptimizerModeDB(t, mode, DatabaseOptions{
						Schema:       funnelSchema(t),
						DisableCache: true,
						AnnotationHandler: func(ev annotations.Event) {
							if recording {
								events = append(events, ev)
							}
						},
					})

					e := funnelFixture(t, db)

					recording = true
					var result executor.Relation
					var err error
					if tc.inputEntity {
						result, err = db.Query(tc.query, e)
					} else {
						result, err = db.Query(tc.query)
					}
					require.NoError(t, err)
					tuples, err := executor.CollectTuples(result, nil)
					require.NoError(t, err)

					selection := lastEventNamed(events, annotations.PatternIndexSelection)
					if tc.noRun {
						require.Nil(t, selection,
							"%s addresses no run of its own and must not announce one", tc.arm)
					} else {
						require.NotNil(t, selection,
							"%s opened a scan and must announce the run it addressed", tc.arm)
						// The IndexType, not a rendering of it: a producer that
						// flattened would still read "EAVT" in a trace, and only
						// a typed comparison catches it.
						require.Equal(t, tc.index, selection.Data[annotations.KeyIndex])
					}

					// One scan, one report. With one completion event name this
					// is a count rather than a search across names: an arm that
					// reported twice is as wrong as one that reported never.
					var completions []annotations.Event
					for _, ev := range events {
						if ev.Name == annotations.StorageScanComplete {
							completions = append(completions, ev)
						}
					}
					require.Len(t, completions, 1,
						"%s opened one scan and owes exactly one report of what it cost",
						tc.arm)
					completion := &completions[0]

					scanned, ok := completion.Data[annotations.KeyDatomsScanned].(int)
					require.True(t, ok, "the completion event must carry intake")
					require.Positive(t, scanned,
						"the arm read the index to answer; zero intake would mean it did not")
					// Every case is a single pattern, so what the arm kept is what
					// came back.
					require.Equal(t, len(tuples),
						completion.Data[annotations.KeyDatomsMatched],
						"%s kept %d tuples and must report that many matched",
						tc.arm, len(tuples))
					require.Equal(t, tc.wantResolved,
						completion.Data[annotations.KeyDatomsResolved],
						"%s resolves %d value(s) for the groups it read", tc.arm, tc.wantResolved)

					// Intake bounds resolution. A scan cannot produce more than
					// it read, so this is the one ordering the three terms
					// always satisfy — `resolved >= matched` is not an
					// invariant, because merge join emits a tuple per (datom,
					// binding tuple) pair and can match more than it resolved.
					//
					// Transposing any arm's two counters reds exactly that arm.
					resolved, _ := completion.Data[annotations.KeyDatomsResolved].(int)
					require.GreaterOrEqual(t, scanned, resolved,
						"%s reports resolving %d from an intake of %d; resolution cannot "+
							"produce datoms the scan never read", tc.arm, resolved, scanned)

					// An arm that priced a run says which run, whole. The index
					// alone is half a run: it names the component order without
					// the components, so "AETV" cannot be told from "AETV under
					// :person/name" and the amplification the funnel reports
					// cannot be attributed to a bound.
					if _, named := completion.Data[annotations.KeyIndex]; named {
						require.Contains(t, completion.Data, annotations.KeyBound,
							"%s named the index it walked; the positions it bound belong on the "+
								"same event, not on one a parallel worker may have interleaved", tc.arm)
					}

					// Announced and priced are the same run. Two events built
					// from one ScanBound cannot disagree; two built from an
					// index and a separately-derived bound can, and that drift
					// is invisible in a trace because both lines read plausibly.
					if !tc.noRun {
						require.Equal(t, selection.Data[annotations.KeyIndex],
							completion.Data[annotations.KeyIndex],
							"%s priced a different index than it announced", tc.arm)
						require.Equal(t, selection.Data[annotations.KeyBound],
							completion.Data[annotations.KeyBound],
							"%s priced a different run than it announced", tc.arm)
						require.Equal(t, selection.Data[annotations.KeyBoundValues],
							completion.Data[annotations.KeyBoundValues])
					}
				})
			}
		})
	}
}

// TestCacheResolvedPatternReportsItsCostAndAnnouncesNoRun pins the one dispatch
// arm that deliberately does not announce a bound, and the one that does not
// report a funnel.
//
// matchFromCache addresses no run: the cache chooses an index by cardinality
// inside resolution, and a hit reads no index at all, so a bound announced here
// would name a run this call did not choose. What it owes its reader is the
// cost, and the second query pins the part that makes the number meaningful —
// a hit reports zero, which is a real answer rather than a missing one.
//
// It also owes that cost in a shape that is true. The funnel models three
// stages of one scan, narrowing — intake, then what resolution made of it, then
// what the pattern kept. A hit performs none of the first two: it reads no
// index and runs no resolution, it reads an entry some earlier call built. So
// the values it serves are not "resolved" and do not sit below an intake of
// zero; asserting they do inverts the one ordering the funnel guarantees. This
// arm reports values served and values matched, with intake beside them as its
// own fact.
// One case per cardinality, because the arm reaches a different resolver for
// each and intake is what separates them: LWW stops at the winner, add-wins and
// RGA read the whole (E, A) group.
func TestCacheResolvedPatternReportsItsCostAndAnnouncesNoRun(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, tc := range []struct {
				card datalog.Keyword
				// A literal E: an :in parameter routes elsewhere.
				query string
				// wantIntake is what the resolver must read on a miss.
				// funnelFixture writes three `:person/name` datoms, of which LWW
				// reads only the first; three `:person/tag` datoms, all of which
				// add-wins needs to decide membership; and two `:person/skill`
				// RGA inserts, both of which the vector resolver needs to order.
				wantIntake int
				// wantServed is what the entry binds. A vector serves one value
				// however many elements it holds.
				wantServed int
			}{
				{card: schema.CardinalityOne,
					query:      `[:find ?v :where [#id "funnel:alice" :person/name ?v]]`,
					wantIntake: 1, wantServed: 1},
				{card: schema.CardinalityMany,
					query:      `[:find ?v :where [#id "funnel:alice" :person/tag ?v]]`,
					wantIntake: 3, wantServed: 1},
				{card: schema.CardinalityVector,
					query:      `[:find ?v :where [#id "funnel:alice" :person/skill ?v]]`,
					wantIntake: 2, wantServed: 1},
			} {
				t.Run(tc.card.String(), func(t *testing.T) {
					// The recording flag scopes collection to the queries below;
					// the handler is registered before the open because
					// everything the database builds is constructed with it.
					var events []annotations.Event
					recording := false
					db := createOptimizerModeDB(t, mode, DatabaseOptions{
						Schema: funnelSchema(t),
						AnnotationHandler: func(ev annotations.Event) {
							if recording {
								events = append(events, ev)
							}
						},
					})

					funnelFixture(t, db)
					recording = true

					read := func() (*annotations.Event, int) {
						t.Helper()
						result, err := db.Query(tc.query)
						require.NoError(t, err)
						tuples, err := executor.CollectTuples(result, nil)
						require.NoError(t, err)
						ev := lastEventNamed(events, annotations.StorageResolveComplete)
						require.NotNil(t, ev, "the cache arm must report what it cost")
						return ev, len(tuples)
					}

					// First read: a miss, so resolution scans and the entry
					// records it.
					miss, tuples := read()
					require.Nil(t, lastEventNamed(events, annotations.PatternIndexSelection),
						"the cache arm addresses no run and must not announce one")
					// The keyword, not a rendering of it. Flattening at the
					// producer would cost an allocation per emit and hand the
					// consumer a string to parse instead of a value to compare
					// by pointer.
					require.Equal(t, tc.card, miss.Data[annotations.KeyCardinality])
					require.Equal(t, tc.wantIntake, miss.Data[annotations.KeyDatomsScanned],
						"a miss resolves from storage, and this resolver reads %d datom(s) to do it",
						tc.wantIntake)
					require.Equal(t, tuples, miss.Data[annotations.KeyDatomsMatched])
					require.Equal(t, tc.wantServed, miss.Data[annotations.KeyValuesServed])
					require.NotContains(t, miss.Data, annotations.KeyDatomsResolved,
						"this arm reports no funnel; a middle term here would be read as one")

					// Second read of the same (E, A): a hit, which reads no index.
					events = nil
					hit, hitTuples := read()
					require.Equal(t, tuples, hitTuples, "a hit answers what the miss did")
					require.Equal(t, 0, hit.Data[annotations.KeyDatomsScanned],
						"a hit reads no index, and zero is the answer rather than an absent field")
					require.Equal(t, hitTuples, hit.Data[annotations.KeyDatomsMatched])
					require.Equal(t, tc.wantServed, hit.Data[annotations.KeyValuesServed],
						"a hit serves the entry's values without reading anything to do it")
					require.NotContains(t, hit.Data, annotations.KeyDatomsResolved,
						"the hit is where the funnel reading fails hardest: one value served "+
							"against zero intake renders as resolution producing what no scan read")
				})
			}
		})
	}
}

// TestBindingDrivenCacheArmReportsWhatItServed pins the completion for the arm
// that answers a binding-driven pattern with E bound and A known. It is the
// first thing MatchWithConstraints tries once bindings are present, which makes
// it the most-travelled read in the engine, and it emitted nothing at all — so
// the busiest path there is was, in a trace, indistinguishable from one that
// never ran.
//
// It reports on the cache-resolve event rather than the scan one for the same
// reason matchFromCache does: a hit walks no index, so there is no funnel to
// narrow and values.served stands where the middle term would be. Only arity
// differs — one call covers a whole binding set — so the keys keep their
// meanings and each total is a sum over the bindings read.
//
// A bound entity lacking the attribute is included deliberately. It resolves to
// no entry and contributes no tuple, but establishing that costs a read, and an
// arm reporting only its matches prices that read at nothing.
func TestBindingDrivenCacheArmReportsWhatItServed(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			testBindingDrivenCacheArmReportsWhatItServed(t, mode)
		})
	}
}

func testBindingDrivenCacheArmReportsWhatItServed(t *testing.T, mode optimizerMode) {
	// The recording flag scopes collection to the matches below; the handler is
	// registered at open because everything the database builds carries it.
	var events []annotations.Event
	recording := false
	db := createOptimizerModeDB(t, mode, DatabaseOptions{
		Schema: funnelSchema(t),
		AnnotationHandler: func(ev annotations.Event) {
			if recording {
				events = append(events, ev)
			}
		},
	})

	name := datalog.NewKeyword(":person/name")
	alice := funnelFixture(t, db)

	bob := datalog.NewIdentity("funnel:bob")
	tx := db.NewTransaction()
	require.NoError(t, tx.Set(bob, name, "Bob"))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Carol carries a different attribute, so she binds, reads, and resolves to
	// nothing.
	carol := datalog.NewIdentity("funnel:carol")
	tx = db.NewTransaction()
	require.NoError(t, tx.Add(carol, datalog.NewKeyword(":person/tag"), "dev"))
	_, err = tx.Commit()
	require.NoError(t, err)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: name},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	// Bound directly rather than through db.Query: the planner is free to reach
	// this pattern with no bindings at all, and this arm is only entered when a
	// binding relation carrying E is already in hand.
	match := func(want int, bound ...datalog.Identity) *annotations.Event {
		t.Helper()
		tuples := make([]executor.Tuple, len(bound))
		for i, e := range bound {
			tuples[i] = executor.Tuple{e}
		}
		bindings := executor.NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?e")}, tuples)

		events = nil
		m := db.Matcher().(*PatternMatcher)
		rel, err := m.MatchWithConstraints(
			query.PatternQuery(pattern), executor.Relations{bindings}, nil)
		require.NoError(t, err)
		got, err := executor.CollectTuples(rel, nil)
		require.NoError(t, err)
		require.Len(t, got, want)

		ev := lastEventNamed(events, annotations.StorageResolveComplete)
		require.NotNil(t, ev, "the arm owes a completion for the binding set it read")
		return ev
	}

	recording = true

	cold := match(2, alice, bob, carol)
	require.Equal(t, 3, cold.Data[annotations.KeyBindingSize],
		"three bindings were read, including the one that resolved to nothing")
	require.Equal(t, 2, cold.Data[annotations.KeyDatomsMatched])
	require.Equal(t, 2, cold.Data[annotations.KeyValuesServed],
		"two entries, each cardinality-one and holding one current value")
	require.Positive(t, cold.Data[annotations.KeyDatomsScanned],
		"a cold cache resolves from storage")
	require.Equal(t, schema.CardinalityOne, cold.Data[annotations.KeyCardinality],
		"A is fixed for this call, so one cardinality describes it")
	require.NotContains(t, cold.Data, annotations.KeyDatomsResolved,
		"this arm reports no funnel; a middle term here would be read as one")
	require.NotContains(t, cold.Data, annotations.KeyIndex,
		"the cache picks an index per entry by cardinality, and this call resolved three")

	warm := match(2, alice, bob)
	require.Equal(t, 0, warm.Data[annotations.KeyDatomsScanned],
		"both entries are cached now, so this call read no index — reporting their "+
			"build cost instead would make a trace of repeated joins claim scans "+
			"that never happened")
	require.Equal(t, 2, warm.Data[annotations.KeyDatomsMatched])
	require.Equal(t, 2, warm.Data[annotations.KeyValuesServed])

	// An absent (E, A) is resolved and not cached, so it is re-established on
	// every call, and it produces no tuple to show for it. Its intake is zero
	// and stays zero however many times it is asked: the prefix names an empty
	// run, so the seek reads no datoms. The event is therefore the only trace
	// this work leaves, and binding.size is what says it happened — an arm that
	// stayed silent here and an arm that was never reached read identically.
	absent := match(0, carol)
	require.Equal(t, 1, absent.Data[annotations.KeyBindingSize])
	require.Equal(t, 0, absent.Data[annotations.KeyDatomsMatched])
	require.Equal(t, 0, absent.Data[annotations.KeyValuesServed])
	require.Equal(t, 0, absent.Data[annotations.KeyDatomsScanned])
}

// TestPatternLessReadsReportUnderEntityAndAttribute pins the reads the Pull API
// and a fused attribute fetch make. No pattern names their subject, so the
// entity and the attribute do, and whether they name a run is what says which
// mechanism answered: the cache picks an index per entry inside resolution and
// a hit walks none, while a storage arm walks exactly one and names it.
//
// A run is named only where the call addressed one. LookupAllAttributes always
// peeks before it resolves, so it walks two and reports the count instead —
// naming an index there would name whichever arm reached it last.
func TestPatternLessReadsReportUnderEntityAndAttribute(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			testPatternLessReadsReportUnderEntityAndAttribute(t, mode)
		})
	}
}

func testPatternLessReadsReportUnderEntityAndAttribute(t *testing.T, mode optimizerMode) {
	name := datalog.NewKeyword(":person/name")
	tag := datalog.NewKeyword(":person/tag")

	open := func(t *testing.T, disableCache bool) (*Database, *[]annotations.Event, datalog.Identity) {
		t.Helper()
		// Registered at open; the flag turns collection on after the fixture, so
		// the fixture's writes stay out of events.
		var events []annotations.Event
		recording := false
		db := createOptimizerModeDB(t, mode, DatabaseOptions{
			Schema:       funnelSchema(t),
			DisableCache: disableCache,
			AnnotationHandler: func(ev annotations.Event) {
				if recording {
					events = append(events, ev)
				}
			},
		})
		e := funnelFixture(t, db)
		recording = true
		return db, &events, e
	}

	resolve := func(t *testing.T, events *[]annotations.Event) *annotations.Event {
		t.Helper()
		ev := lastEventNamed(*events, annotations.StorageResolveComplete)
		require.NotNil(t, ev, "a read that walked the index owes a completion")
		return ev
	}

	t.Run("LookupAttribute answered from storage names its run", func(t *testing.T) {
		db, events, e := open(t, true)
		m := db.Matcher().(*PatternMatcher)

		v, found, err := m.LookupAttribute(e, name)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "Alys", v, "the last of three writes wins")

		ev := resolve(t, events)
		// Typed, not rendered: a producer that flattened would still read
		// correctly in a trace, and only a typed comparison catches it.
		require.Equal(t, e, ev.Data[annotations.KeyEntity])
		require.Equal(t, name, ev.Data[annotations.KeyAttribute])
		require.NotContains(t, ev.Data, annotations.KeyPattern,
			"no pattern named this read")
		require.Equal(t, EATV, ev.Data[annotations.KeyIndex],
			"the storage arm walked one run and owes its name")
		require.Equal(t, 1, ev.Data[annotations.KeyValuesServed])
		// One datom, though three were written: EATV encodes Tx descending, so
		// the first entry under (E, A) is the LWW winner and the arm returns
		// there without reading the history behind it.
		require.Equal(t, 1, ev.Data[annotations.KeyDatomsScanned],
			"the winner is the first entry, and the arm stops there")
		require.NotContains(t, ev.Data, annotations.KeyDatomsMatched,
			"matching is a pattern's business; a zero here would read as no result")
	})

	t.Run("LookupAttribute answered from cache names no run", func(t *testing.T) {
		db, events, e := open(t, false)
		m := db.Matcher().(*PatternMatcher)

		// First read builds the entry; the second is the hit under test.
		_, _, err := m.LookupAttribute(e, name)
		require.NoError(t, err)
		*events = nil
		v, found, err := m.LookupAttribute(e, name)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "Alys", v)

		ev := resolve(t, events)
		require.NotContains(t, ev.Data, annotations.KeyIndex,
			"a hit reads no index, so there is no run to name")
		require.Equal(t, 0, ev.Data[annotations.KeyDatomsScanned],
			"and none to charge intake for")
		require.Equal(t, 1, ev.Data[annotations.KeyValuesServed])
	})

	t.Run("LookupAllAttributes counts the runs it walked", func(t *testing.T) {
		db, events, e := open(t, true)
		m := db.Matcher().(*PatternMatcher)

		values, err := m.LookupAllAttributes(e, tag)
		require.NoError(t, err)
		require.Len(t, values, 1, "one tag was added, removed, and one remains")

		ev := resolve(t, events)
		require.Equal(t, e, ev.Data[annotations.KeyEntity])
		require.Equal(t, tag, ev.Data[annotations.KeyAttribute])
		require.NotContains(t, ev.Data, annotations.KeyIndex,
			"the peek and the resolution are two runs, and neither speaks for the other")
		require.Equal(t, 2, ev.Data[annotations.KeyScansOpened])
		require.Equal(t, 1, ev.Data[annotations.KeyValuesServed])
		// The peek advances once to read the first datom's Op and closes; add-wins
		// resolution then reads the whole (E, A) group, since membership is not
		// decidable from a prefix of the adds and removes. funnelFixture writes
		// three tag datoms, so 1 + 3.
		require.Equal(t, 4, ev.Data[annotations.KeyDatomsScanned],
			"one peek plus the set's three datoms; the history is deeper than the "+
				"membership and intake is what shows it")
	})
}

// TestBulkReadsReportUnderNoSingleSubject pins the two reads whose subject is a
// set of entities rather than one (E, A). Neither can carry a cause, and what
// separates them in a trace is the run: prefetch opens one per entity and names
// none, batch pull shares a single EATV traversal and names it.
func TestBulkReadsReportUnderNoSingleSubject(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			testBulkReadsReportUnderNoSingleSubject(t, mode)
		})
	}
}

func testBulkReadsReportUnderNoSingleSubject(t *testing.T, mode optimizerMode) {
	t.Run("prefetch reports what it filled", func(t *testing.T) {
		var events []annotations.Event
		recording := false
		db := createOptimizerModeDB(t, mode, DatabaseOptions{
			Schema: funnelSchema(t),
			AnnotationHandler: func(ev annotations.Event) {
				if recording {
					events = append(events, ev)
				}
			},
		})

		alice := funnelFixture(t, db)
		bob := datalog.NewIdentity("funnel:bob")
		tx := db.NewTransaction()
		require.NoError(t, tx.Set(bob, datalog.NewKeyword(":person/name"), "Bob"))
		_, err := tx.Commit()
		require.NoError(t, err)

		recording = true
		m := db.Matcher().(*PatternMatcher)
		require.NoError(t, m.PrefetchEntities([]datalog.Identity{alice, bob}))

		ev := lastEventNamed(events, annotations.StorageResolveComplete)
		require.NotNil(t, ev, "a warm that read the index owes a completion")
		require.NotContains(t, ev.Data, annotations.KeyIndex,
			"one scan per entity: naming an index would name whichever came last")
		require.Equal(t, 2, ev.Data[annotations.KeyScansOpened])
		require.Equal(t, 4, ev.Data[annotations.KeyEntriesPopulated],
			"alice carries three attributes and bob one")
		require.NotContains(t, ev.Data, annotations.KeyValuesServed,
			"a warm answers nobody, and a zero here would read as having found none")
		require.Positive(t, ev.Data[annotations.KeyDatomsScanned])
	})

	t.Run("batch pull names its shared run", func(t *testing.T) {
		var events []annotations.Event
		recording := false
		db := createOptimizerModeDB(t, mode, DatabaseOptions{
			Schema: funnelSchema(t),
			AnnotationHandler: func(ev annotations.Event) {
				if recording {
					events = append(events, ev)
				}
			},
		})

		alice := funnelFixture(t, db)

		recording = true
		results, err := db.ResolveAllAttributesMany([]datalog.Identity{alice})
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Len(t, results[0], 3, "alice carries all three attributes")

		ev := lastEventNamed(events, annotations.StorageResolveComplete)
		require.NotNil(t, ev)
		require.Equal(t, EATV, ev.Data[annotations.KeyIndex],
			"one traversal shared across the entity set, and it is nameable")
		require.Equal(t, 1, ev.Data[annotations.KeyScansOpened])
		require.Equal(t, 3, ev.Data[annotations.KeyValuesServed])
		require.Positive(t, ev.Data[annotations.KeyDatomsScanned])
	})
}

// TestUniqueAttributeWalkReportsItsOwnScans enters the branch the suite never
// did: CRDTResolvingIterator's unique CardinalityOne mode, where resolution
// stops being free. Every other cardinality resolves out of the source's own
// ordering, so intake is the source's alone; this one opens an AVET supersession
// scan per entry, and Scanned() adds them to what the source read.
//
// Reaching it needs an attribute that is CardinalityOne *and* unique,
// scanned with E unbound — a bound E goes to the cache, and a bound V goes
// to the claimant lookup, so neither of the shapes a unique attribute is
// usually queried by comes here.
//
// The assertion is a strict inequality against the fixture's own datom count,
// which is what the source reads. No other cardinality can satisfy it: intake
// above the source is the walk's, and only this branch opens those scans.
func TestUniqueAttributeWalkReportsItsOwnScans(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			testUniqueAttributeWalkReportsItsOwnScans(t, mode)
		})
	}
}

func testUniqueAttributeWalkReportsItsOwnScans(t *testing.T, mode optimizerMode) {
	const (
		entities        = 8
		writesPerEntity = 3
	)
	email := datalog.NewKeyword(":person/email")

	s, err := schema.NewBuilder().
		Attribute(":person/email").Type(schema.TypeString).One().
		Unique(schema.UniqueValue).Add().
		Build()
	require.NoError(t, err)

	var events []annotations.Event
	recording := false
	db := createOptimizerModeDB(t, mode, DatabaseOptions{
		Schema: s,
		AnnotationHandler: func(ev annotations.Event) {
			if recording {
				events = append(events, ev)
			}
		},
	})

	// Rewritten rather than written once, so each entity's group has entries
	// behind the winner for the walk to step over.
	for i := 0; i < entities; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("unique:person-%d", i))
		for w := 0; w < writesPerEntity; w++ {
			tx := db.NewTransaction()
			require.NoError(t, tx.Set(e, email, fmt.Sprintf("p%d-v%d@example.com", i, w)))
			_, err := tx.Commit()
			require.NoError(t, err)
		}
	}

	recording = true

	result, err := db.Query(`[:find ?e ?v :where [?e :person/email ?v]]`)
	require.NoError(t, err)
	tuples, err := executor.CollectTuples(result, nil)
	require.NoError(t, err)
	require.Len(t, tuples, entities,
		"the walk resolves one current value per entity")
	for _, tuple := range tuples {
		require.Contains(t, tuple[1], "-v2@example.com",
			"the last write is the one that survives the walk")
	}

	scan := lastScanComplete(events, annotations.ScanDirect)
	require.NotNil(t, scan)
	// The source reads every datom under the attribute; the walk then checks
	// supersession once per group it emits from. One per entity is the floor,
	// not the observed number, so a walk that reads more per check still passes
	// and a walk whose reads stop being counted does not.
	sourceIntake := entities * writesPerEntity
	scanned, ok := scan.Data[annotations.KeyDatomsScanned].(int)
	require.True(t, ok)
	require.GreaterOrEqual(t, scanned, sourceIntake+entities,
		"the source holds %d datoms and the walk checks %d groups, so intake "+
			"is at least %d; %d means the walk's reads reached no one",
		sourceIntake, entities, sourceIntake+entities, scanned)
}

// TestMemoryBackendReportsIntakeNatively closes the gap that left
// memoryIterator.Scanned() asserted only under GOOS=js: the one test that read
// the intake key opened with a temp directory, which is Badger natively, so the
// memory store's counter could have returned a constant zero and the native
// gate would not have noticed. Injecting the store exercises it here.
func TestMemoryBackendReportsIntakeNatively(t *testing.T) {
	var events []annotations.Event
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Store:             NewMemoryStore(nil),
		Schema:            funnelSchema(t),
		AnnotationHandler: func(ev annotations.Event) { events = append(events, ev) },
	})
	require.NoError(t, err)
	defer db.Close()

	e := datalog.NewIdentity("memory:alice")
	name := datalog.NewKeyword(":person/name")
	for _, v := range []string{"Alice", "Alicia", "Alys"} {
		tx := db.NewTransaction()
		require.NoError(t, tx.Set(e, name, v))
		_, err = tx.Commit()
		require.NoError(t, err)
	}

	events = nil
	result, err := db.Query(`[:find ?e ?n :where [?e :person/name ?n]]`)
	require.NoError(t, err)
	tuples, err := executor.CollectTuples(result, nil)
	require.NoError(t, err)
	require.Len(t, tuples, 1, "last write wins")

	scan := lastScanComplete(events, annotations.ScanDirect)
	require.NotNil(t, scan)
	require.Equal(t, 3, scan.Data[annotations.KeyDatomsScanned],
		"three writes are three datoms under this attribute, and the memory store read them all")
	require.Equal(t, 1, scan.Data[annotations.KeyDatomsResolved])
}

// lastEventNamed returns the final event with the given name, or nil. Last
// rather than first: a query emits one set of scan events per pattern, and the
// assertions above concern the pattern the case aimed at, which is the one that
// ran most recently.
func lastEventNamed(events []annotations.Event, name string) *annotations.Event {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Name == name {
			return &events[i]
		}
	}
	return nil
}
