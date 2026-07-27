package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
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

// TestEveryDispatchArmAnnouncesItsRunAndReportsItsFunnel is the pin for the
// finding that most of the matcher's dispatch arms opened scans and said
// nothing. Each case aims a query at one arm and asserts two things: the arm
// announced the run it addressed, and it reported what that run cost.
//
// Deleting either emit from any arm reds exactly the row for that arm, which is
// what the previous round lacked — three arms gained emitIndexSelection and
// deleting all three left the suite green.
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
			for _, tc := range []struct {
				arm         string
				query       string
				inputEntity bool     // supply E as an :in parameter
				index       string   // "" when the path announces no run
				completions []string // any one of these carries the funnel
			}{
				{arm: "matchCardinalityManyAsRelation",
					query: `[:find ?v :where [#id "funnel:alice" :person/tag ?v]]`,
					index: "EAVT", completions: []string{annotations.PatternStorageScan}},
				{arm: "matchCardinalityVectorAsRelation",
					query: `[:find ?v :where [#id "funnel:alice" :person/skill ?v]]`,
					index: "EATV", completions: []string{annotations.PatternStorageScan}},
				{arm: "matchCardinalityManyMembership",
					query: `[:find ?tx :where [#id "funnel:alice" :person/tag "dev" ?tx]]`,
					index: "EAVT", completions: []string{annotations.PatternStorageScan}},
				{arm: "matchCardinalityManyScanAllEntities",
					query: `[:find ?e ?v :where [?e :person/tag ?v]]`,
					index: "AEVT", completions: []string{annotations.PatternStorageScan}},
				{arm: "matchCardinalityManyFindEntitiesWithValue",
					query: `[:find ?e :where [?e :person/tag "dev"]]`,
					index: "AVET", completions: []string{annotations.PatternStorageScan}},
				{arm: "matchVectorScanAllEntities",
					query: `[:find ?e ?v :where [?e :person/skill ?v]]`,
					index: "AEVT", completions: []string{annotations.PatternStorageScan}},

				// A binding relation over the same cardinality-many attribute.
				// Which strategy chooseJoinStrategy picks is its own business
				// and pinning it here would couple this test to that choice —
				// what this row asserts is that whichever it picks reports.
				{arm: "binding-driven, E from :in",
					query: `[:find ?v :in $ ?e :where [?e :person/tag ?v]]`, inputEntity: true,
					completions: []string{
						annotations.PatternHashJoinComplete,
						annotations.PatternMergeJoinComplete,
						annotations.PatternPerBindingScanComplete,
					}},
			} {
				t.Run(tc.arm, func(t *testing.T) {
					var events []annotations.Event
					opts := mode.plannerOptions()
					db, err := NewDatabaseWithOptions(DatabaseOptions{
						Path:           t.TempDir(),
						Schema:         funnelSchema(t),
						PlannerOptions: &opts,
						DisableCache:   true,
					})
					require.NoError(t, err)
					defer db.Close()

					e := funnelFixture(t, db)

					db.AnnotationHandler = func(ev annotations.Event) {
						events = append(events, ev)
					}
					var result executor.Relation
					if tc.inputEntity {
						result, err = db.Query(tc.query, e)
					} else {
						result, err = db.Query(tc.query)
					}
					require.NoError(t, err)
					_, err = executor.CollectTuples(result, nil)
					require.NoError(t, err)

					selection := lastEventNamed(events, annotations.PatternIndexSelection)
					if tc.index == "" {
						require.Nil(t, selection,
							"%s addresses no run of its own and must not announce one", tc.arm)
					} else {
						require.NotNil(t, selection,
							"%s opened a scan and must announce the run it addressed", tc.arm)
						require.Equal(t, tc.index, selection.Data["index"])
					}

					var completion *annotations.Event
					for _, name := range tc.completions {
						found := lastEventNamed(events, name)
						if found == nil {
							continue
						}
						require.Nil(t, completion,
							"%s reported through more than one completion event; "+
								"one scan, one report", tc.arm)
						completion = found
					}
					require.NotNil(t, completion,
						"%s opened a scan and must report what it cost; expected one of %v",
						tc.arm, tc.completions)

					scanned, ok := completion.Data["datoms.scanned"].(int)
					require.True(t, ok, "the completion event must carry intake")
					require.Positive(t, scanned,
						"the arm read the index to answer; zero intake would mean it did not")
					require.Contains(t, completion.Data, "datoms.resolved")
					require.Contains(t, completion.Data, "datoms.matched")
				})
			}
		})
	}
}

// TestCacheResolvedPatternReportsItsCostAndAnnouncesNoRun pins the one dispatch
// arm that deliberately does not announce a bound.
//
// matchFromCache addresses no run: the cache chooses an index by cardinality
// inside resolution, and a hit reads no index at all, so a bound announced here
// would name a run this call did not choose. What it owes its reader is the
// cost, and the second query pins the part that makes the number meaningful —
// a hit reports zero, which is a real answer rather than a missing one.
func TestCacheResolvedPatternReportsItsCostAndAnnouncesNoRun(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var events []annotations.Event
			opts := mode.plannerOptions()
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:           t.TempDir(),
				Schema:         funnelSchema(t),
				PlannerOptions: &opts,
			})
			require.NoError(t, err)
			defer db.Close()

			funnelFixture(t, db)
			// A literal E, as above: an :in parameter routes elsewhere.
			const q = `[:find ?v :where [#id "funnel:alice" :person/name ?v]]`

			db.AnnotationHandler = func(ev annotations.Event) { events = append(events, ev) }

			// First read: a miss, so resolution scans and the entry records it.
			result, err := db.Query(q)
			require.NoError(t, err)
			_, err = executor.CollectTuples(result, nil)
			require.NoError(t, err)

			miss := lastEventNamed(events, annotations.PatternCacheResolveComplete)
			require.NotNil(t, miss, "the cache arm must report what it cost")
			require.Nil(t, lastEventNamed(events, annotations.PatternIndexSelection),
				"the cache arm addresses no run and must not announce one")
			require.Equal(t, "db.cardinality/one", miss.Data["cardinality"])
			require.Positive(t, miss.Data["datoms.scanned"],
				"a miss resolved from storage and read the index to do it")
			require.Equal(t, 1, miss.Data["datoms.matched"])

			// Second read of the same (E, A): a hit, which reads no index.
			events = nil
			result, err = db.Query(q)
			require.NoError(t, err)
			_, err = executor.CollectTuples(result, nil)
			require.NoError(t, err)

			hit := lastEventNamed(events, annotations.PatternCacheResolveComplete)
			require.NotNil(t, hit)
			require.Equal(t, 0, hit.Data["datoms.scanned"],
				"a hit reads no index, and zero is the answer rather than an absent field")
			require.Equal(t, 1, hit.Data["datoms.matched"])
		})
	}
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
	rows, err := executor.CollectTuples(result, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1, "last write wins")

	scan := lastEventNamed(events, annotations.PatternStorageScan)
	require.NotNil(t, scan)
	require.Equal(t, 3, scan.Data["datoms.scanned"],
		"three writes are three datoms under this attribute, and the memory store read them all")
	require.Equal(t, 1, scan.Data["datoms.resolved"])
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
