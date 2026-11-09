package storage

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// =============================================================================
// Same-entity attribute-fetch fusion — correctness
// =============================================================================
//
// Fusion executes a same-entity [?e :const-attr ?fresh] pattern as a per-tuple
// LookupAttribute symbol attach instead of a separate match + hash join. These
// tests prove it preserves query semantics.
//
// Pillar 1 — DIFFERENTIAL: every scenario runs the identical query on the
// reference leg, the engine backend with every option off, and on every leg
// of the matrix, each database holding identical data, and asserts identical
// results. If fusion, or any other option, ever changed an answer, these
// fail.
//
// Pillar 2 — COVERAGE GUARD: the differential passes vacuously if fusion never
// fires on the legs that carry it. TestFusion_CoverageFires asserts, on the
// fusion legs, that the `pattern/fused-fetch` annotation fires for a
// CardinalityOne fetch and does NOT fire for a CardinalityMany one — so we
// know those legs actually took the fused path.
//
// Pillar 3 — GATE: TestFusionGate_ModeAndCardinality proves CanFuseAttributeFetch
// (the matcher decision the executor consults) is true in the latest and as-of
// views for an attribute the matcher resolves cardinality-one, declared so or
// with no definition, and false for CardinalityMany, CardinalityVector and
// History. The executor fuses iff that returns true, so this bounds where
// fusion can run.
// =============================================================================

func fusionSchema() *schema.Schema {
	s := schema.NewSchema()
	for _, a := range []string{":place/type", ":place/code", ":place/name", ":place/brief"} {
		s.Add(&schema.AttributeDefinition{
			Ident:       datalog.NewKeyword(a),
			ValueType:   schema.TypeString,
			Cardinality: schema.CardinalityOne,
		})
	}
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":place/tags"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityMany,
	})
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":place/steps"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityVector,
	})
	return s
}

// openFusionDB opens the fusion fixture on the mode's backend under the mode's
// planner options. The caller sets the annotation handler and the cache on
// opts; the schema, the ReplicaID and the planner options come from the
// fixture.
func openFusionDB(t *testing.T, mode optimizerMode, opts DatabaseOptions) *Database {
	t.Helper()
	plannerOpts := mode.plannerOptions()
	opts.Schema = fusionSchema()
	opts.ReplicaID = 1
	opts.PlannerOptions = &plannerOpts
	return openBackendDB(t, mode.backend, opts)
}

func fusionTuples(t *testing.T, db *Database, query string, args ...interface{}) []string {
	t.Helper()
	rel, err := db.Query(query, args...)
	require.NoError(t, err)
	var tuples []string
	it := rel.Iterator()
	for it.Next() {
		tuples = append(tuples, fmt.Sprintf("%v", it.Tuple()))
	}
	require.NoError(t, it.Error())
	it.Close()
	sort.Strings(tuples)
	return tuples
}

// assertLegAnswersAsReference applies the same ops to a database on the reference
// leg and to one on the given leg (identical data, pinned ReplicaID) and
// asserts the query returns identical tuples on both.
func assertLegAnswersAsReference(t *testing.T, mode optimizerMode, apply func(*Database), query string, args ...interface{}) {
	t.Helper()
	reference := openFusionDB(t, referenceMode, DatabaseOptions{})
	apply(reference)
	leg := openFusionDB(t, mode, DatabaseOptions{})
	apply(leg)
	want := fusionTuples(t, reference, query, args...)
	got := fusionTuples(t, leg, query, args...)
	assert.Equal(t, want, got, "%s must return the same tuples as the reference leg", mode.name)
}

const fusionCodeQuery = `[:find ?e ?c :where [?e :place/type "room"] [?e :place/code ?c]]`

func TestFusion_DifferentialLatest(t *testing.T) {
	typ := datalog.NewKeyword(":place/type")
	code := datalog.NewKeyword(":place/code")
	name := datalog.NewKeyword(":place/name")
	brief := datalog.NewKeyword(":place/brief")
	tags := datalog.NewKeyword(":place/tags")
	e1 := datalog.NewIdentity("e1")
	e2 := datalog.NewIdentity("e2")

	commit := func(t *testing.T, db *Database, fn func(tx *Transaction)) {
		tx := db.NewTransaction()
		fn(tx)
		_, err := tx.Commit()
		require.NoError(t, err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {

			t.Run("present", func(t *testing.T) {
				assertLegAnswersAsReference(t, mode, func(db *Database) {
					commit(t, db, func(tx *Transaction) {
						require.NoError(t, tx.Set(e1, typ, "room"))
						require.NoError(t, tx.Set(e1, code, "R1"))
						require.NoError(t, tx.Set(e2, typ, "room"))
						require.NoError(t, tx.Set(e2, code, "R2"))
					})
				}, fusionCodeQuery)
			})

			t.Run("missing attribute drops the entity", func(t *testing.T) {
				assertLegAnswersAsReference(t, mode, func(db *Database) {
					commit(t, db, func(tx *Transaction) {
						require.NoError(t, tx.Set(e1, typ, "room")) // no code
						require.NoError(t, tx.Set(e2, typ, "room"))
						require.NoError(t, tx.Set(e2, code, "R2"))
					})
				}, fusionCodeQuery)
			})

			t.Run("tombstoned attribute drops the entity", func(t *testing.T) {
				assertLegAnswersAsReference(t, mode, func(db *Database) {
					commit(t, db, func(tx *Transaction) {
						require.NoError(t, tx.Set(e1, typ, "room"))
						require.NoError(t, tx.Set(e1, code, "R1"))
						require.NoError(t, tx.Set(e2, typ, "room"))
						require.NoError(t, tx.Set(e2, code, "R2"))
					})
					commit(t, db, func(tx *Transaction) {
						require.NoError(t, tx.Remove(e1, code, "R1"))
					})
				}, fusionCodeQuery)
			})

			t.Run("overwritten attribute uses latest value", func(t *testing.T) {
				assertLegAnswersAsReference(t, mode, func(db *Database) {
					commit(t, db, func(tx *Transaction) {
						require.NoError(t, tx.Set(e1, typ, "room"))
						require.NoError(t, tx.Set(e1, code, "R1"))
					})
					commit(t, db, func(tx *Transaction) {
						require.NoError(t, tx.Set(e1, code, "R2"))
					})
				}, fusionCodeQuery)
			})

			t.Run("multiple fused attributes", func(t *testing.T) {
				assertLegAnswersAsReference(t, mode, func(db *Database) {
					commit(t, db, func(tx *Transaction) {
						require.NoError(t, tx.Set(e1, typ, "room"))
						require.NoError(t, tx.Set(e1, code, "R1"))
						require.NoError(t, tx.Set(e1, name, "N1"))
						require.NoError(t, tx.Set(e2, typ, "room"))
						require.NoError(t, tx.Set(e2, code, "R2"))
						require.NoError(t, tx.Set(e2, name, "N2"))
					})
				}, `[:find ?e ?c ?n :where [?e :place/type "room"] [?e :place/code ?c] [?e :place/name ?n]]`)
			})

			t.Run("missing middle attribute drops entity from bundle", func(t *testing.T) {
				assertLegAnswersAsReference(t, mode, func(db *Database) {
					commit(t, db, func(tx *Transaction) {
						require.NoError(t, tx.Set(e1, typ, "room"))
						require.NoError(t, tx.Set(e1, code, "R1"))
						require.NoError(t, tx.Set(e1, brief, "B1")) // no name
						require.NoError(t, tx.Set(e2, typ, "room"))
						require.NoError(t, tx.Set(e2, code, "R2"))
						require.NoError(t, tx.Set(e2, name, "N2"))
						require.NoError(t, tx.Set(e2, brief, "B2"))
					})
				}, `[:find ?e ?c ?n ?b
				     :where [?e :place/type "room"]
				            [?e :place/code ?c]
				            [?e :place/name ?n]
				            [?e :place/brief ?b]]`)
			})

			t.Run("cardinality-many pattern ends bundle", func(t *testing.T) {
				assertLegAnswersAsReference(t, mode, func(db *Database) {
					commit(t, db, func(tx *Transaction) {
						require.NoError(t, tx.Set(e1, typ, "room"))
						require.NoError(t, tx.Set(e1, code, "R1"))
						require.NoError(t, tx.Add(e1, tags, "a"))
						require.NoError(t, tx.Add(e1, tags, "b"))
						require.NoError(t, tx.Set(e1, name, "N1"))
					})
				}, `[:find ?e ?c ?tag ?n
				     :where [?e :place/type "room"]
				            [?e :place/code ?c]
				            [?e :place/tags ?tag]
				            [?e :place/name ?n]]`)
			})

			t.Run("cardinality-many fetch is not fused, still correct", func(t *testing.T) {
				assertLegAnswersAsReference(t, mode, func(db *Database) {
					commit(t, db, func(tx *Transaction) {
						require.NoError(t, tx.Set(e1, typ, "room"))
						require.NoError(t, tx.Add(e1, tags, "a"))
						require.NoError(t, tx.Add(e1, tags, "b"))
					})
				}, `[:find ?e ?tag :where [?e :place/type "room"] [?e :place/tags ?tag]]`)
			})
		})
	}
}

func TestFusionConstantConstraintDifferential(t *testing.T) {
	typ := datalog.NewKeyword(":place/type")
	code := datalog.NewKeyword(":place/code")
	e1 := datalog.NewIdentity("constraint-e1")
	e2 := datalog.NewIdentity("constraint-e2")
	queryText := `[:find ?e
		:where [?e :place/code "R1"]
		       [?e :place/type "room"]]`

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {

			t.Run("matching value keeps tuple", func(t *testing.T) {
				assertLegAnswersAsReference(t, mode, func(db *Database) {
					tx := db.NewTransaction()
					require.NoError(t, tx.Set(e1, code, "R1"))
					require.NoError(t, tx.Set(e1, typ, "room"))
					require.NoError(t, tx.Set(e2, code, "H1"))
					require.NoError(t, tx.Set(e2, typ, "hall"))
					_, err := tx.Commit()
					require.NoError(t, err)
				}, queryText)
			})

			t.Run("missing value drops tuple", func(t *testing.T) {
				assertLegAnswersAsReference(t, mode, func(db *Database) {
					tx := db.NewTransaction()
					require.NoError(t, tx.Set(e1, code, "R1"))
					_, err := tx.Commit()
					require.NoError(t, err)
				}, queryText)
			})

			t.Run("latest value controls constraint", func(t *testing.T) {
				assertLegAnswersAsReference(t, mode, func(db *Database) {
					tx := db.NewTransaction()
					require.NoError(t, tx.Set(e1, code, "R1"))
					require.NoError(t, tx.Set(e1, typ, "hall"))
					_, err := tx.Commit()
					require.NoError(t, err)
					tx = db.NewTransaction()
					require.NoError(t, tx.Set(e1, typ, "room"))
					_, err = tx.Commit()
					require.NoError(t, err)
				}, queryText)
			})
		})
	}
}

// On the fusion legs, a constant-valued same-entity pattern on a
// cardinality-one attribute is fused as a constraint, and one on a
// cardinality-many attribute is not.
func TestFusionConstantConstraintCoverage(t *testing.T) {
	typ := datalog.NewKeyword(":place/type")
	code := datalog.NewKeyword(":place/code")
	tags := datalog.NewKeyword(":place/tags")
	e1 := datalog.NewIdentity("constraint-coverage")

	for _, mode := range legsWhere(optimizerModes, func(m optimizerMode) bool { return m.fusion }) {
		t.Run(mode.name, func(t *testing.T) {

			t.Run("cardinality one constraint fires", func(t *testing.T) {
				cap := &fusionCapture{n: map[string]int{}}
				db := openFusionDB(t, mode, DatabaseOptions{AnnotationHandler: cap.handler()})
				tx := db.NewTransaction()
				require.NoError(t, tx.Set(e1, code, "R1"))
				require.NoError(t, tx.Set(e1, typ, "room"))
				_, err := tx.Commit()
				require.NoError(t, err)

				tuples := fusionTuples(t, db, `[:find ?e
					:where [?e :place/code "R1"]
					       [?e :place/type "room"]]`)
				require.Len(t, tuples, 1)
				require.Equal(t, 1, cap.get(annotations.PatternFusedConstraint))
			})

			t.Run("cardinality many constraint does not fire", func(t *testing.T) {
				cap := &fusionCapture{n: map[string]int{}}
				db := openFusionDB(t, mode, DatabaseOptions{AnnotationHandler: cap.handler()})
				tx := db.NewTransaction()
				require.NoError(t, tx.Set(e1, code, "R1"))
				require.NoError(t, tx.Add(e1, tags, "selected"))
				_, err := tx.Commit()
				require.NoError(t, err)

				tuples := fusionTuples(t, db, `[:find ?e
					:where [?e :place/code ?c]
					       [?e :place/tags "selected"]]`)
				require.Len(t, tuples, 1)
				require.Zero(t, cap.get(annotations.PatternFusedConstraint))
			})
		})
	}
}

type fusionCapture struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *fusionCapture) handler() annotations.Handler {
	return func(ev annotations.Event) {
		c.mu.Lock()
		c.n[ev.Name]++
		c.mu.Unlock()
	}
}

func (c *fusionCapture) get(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[name]
}

// TestFusion_CoverageFires proves the fusion legs take the fused path for a
// CardinalityOne fetch (so the differential isn't vacuous), in the latest view
// and as of a commit, and do NOT fuse a CardinalityMany fetch.
func TestFusion_CoverageFires(t *testing.T) {
	typ := datalog.NewKeyword(":place/type")
	code := datalog.NewKeyword(":place/code")
	tags := datalog.NewKeyword(":place/tags")
	e1 := datalog.NewIdentity("e1")

	for _, mode := range legsWhere(optimizerModes, func(m optimizerMode) bool { return m.fusion }) {
		t.Run(mode.name, func(t *testing.T) {

			t.Run("card-one fetch fires fusion", func(t *testing.T) {
				cap := &fusionCapture{n: map[string]int{}}
				db := openFusionDB(t, mode, DatabaseOptions{AnnotationHandler: cap.handler()})
				tx := db.NewTransaction()
				require.NoError(t, tx.Set(e1, typ, "room"))
				require.NoError(t, tx.Set(e1, code, "R1"))
				_, err := tx.Commit()
				require.NoError(t, err)

				_ = fusionTuples(t, db, fusionCodeQuery)
				assert.Positive(t, cap.get(annotations.PatternFusedFetch),
					"CardinalityOne fetch must take the fused path")
			})

			for _, disableCache := range []bool{false, true} {
				t.Run(fmt.Sprintf("card-one fetch on an as-of handle fires fusion/cache_disabled_%t", disableCache), func(t *testing.T) {
					cap := &fusionCapture{n: map[string]int{}}
					db := openFusionDB(t, mode, DatabaseOptions{AnnotationHandler: cap.handler(), DisableCache: disableCache})
					tx := db.NewTransaction()
					require.NoError(t, tx.Set(e1, typ, "room"))
					require.NoError(t, tx.Set(e1, code, "R1"))
					position, err := tx.Commit()
					require.NoError(t, err)

					tuples := fusionTuples(t, db.AsOf(position), fusionCodeQuery)
					require.Len(t, tuples, 1)
					assert.Positive(t, cap.get(annotations.PatternFusedFetch),
						"a CardinalityOne fetch on an as-of handle must take the fused path")
				})
			}

			t.Run("card-many fetch does not fire fusion", func(t *testing.T) {
				cap := &fusionCapture{n: map[string]int{}}
				db := openFusionDB(t, mode, DatabaseOptions{AnnotationHandler: cap.handler()})
				tx := db.NewTransaction()
				require.NoError(t, tx.Set(e1, typ, "room"))
				require.NoError(t, tx.Add(e1, tags, "a"))
				require.NoError(t, tx.Add(e1, tags, "b"))
				_, err := tx.Commit()
				require.NoError(t, err)

				_ = fusionTuples(t, db, `[:find ?e ?tag :where [?e :place/type "room"] [?e :place/tags ?tag]]`)
				assert.Zero(t, cap.get(annotations.PatternFusedFetch),
					"CardinalityMany fetch must NOT be fused")
			})
		})
	}
}

// A same-entity fetch of an attribute the schema does not define answers on
// every leg as the reference leg does, and takes the fused path on the fusion
// legs: the lookup and the match both resolve the attribute by LWW, so an
// entity holding two of its datoms answers the latest.
func TestFusion_AnAttributeTheSchemaDoesNotDefineFusesAndAnswersAlike(t *testing.T) {
	typ := datalog.NewKeyword(":place/type")
	note := datalog.NewKeyword(":place/note")
	e1, e2 := datalog.NewIdentity("e1"), datalog.NewIdentity("e2")
	query := `[:find ?e ?n :where [?e :place/type "room"] [?e :place/note ?n]]`

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			apply := func(db *Database) {
				tx := db.NewTransaction()
				require.NoError(t, tx.Set(e1, typ, "room"))
				require.NoError(t, tx.Set(e2, typ, "room"))
				require.NoError(t, tx.Add(e1, note, "first"))
				require.NoError(t, tx.Add(e1, note, "second"))
				_, err := tx.Commit()
				require.NoError(t, err)
			}
			assertLegAnswersAsReference(t, mode, apply, query)
			if !mode.fusion {
				return
			}

			cap := &fusionCapture{n: map[string]int{}}
			db := openFusionDB(t, mode, DatabaseOptions{AnnotationHandler: cap.handler()})
			apply(db)
			_ = fusionTuples(t, db, query)
			assert.Positive(t, cap.get(annotations.PatternFusedFetch),
				"a fetch of an attribute the schema does not define must take the fused path")
		})
	}
}

// TestFusionGate_ModeAndCardinality proves the matcher gate the executor
// consults: fusion is permitted in the latest and as-of views, for an
// attribute the matcher resolves cardinality-one, declared so or with no
// definition, and never under History.
func TestFusionGate_ModeAndCardinality(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			testFusionGateModeAndCardinality(t, mode)
		})
	}
}

func testFusionGateModeAndCardinality(t *testing.T, mode optimizerMode) {
	db := openFusionDB(t, mode, DatabaseOptions{})

	latest, ok := db.Matcher().(*PatternMatcher)
	require.True(t, ok)
	assert.True(t, latest.CanFuseAttributeFetch(datalog.SymDollar, datalog.NewKeyword(":place/code")),
		"latest-mode CardinalityOne is fusable")
	assert.False(t, latest.CanFuseAttributeFetch(datalog.SymDollar, datalog.NewKeyword(":place/tags")),
		"CardinalityMany is not fusable")
	assert.False(t, latest.CanFuseAttributeFetch(datalog.SymDollar, datalog.NewKeyword(":place/steps")),
		"CardinalityVector is not fusable")
	assert.True(t, latest.CanFuseAttributeFetch(datalog.SymDollar, datalog.NewKeyword(":place/unknown")),
		"an attribute the schema does not define is fusable: the lookup and a match both resolve it by LWW")

	hist, ok := db.History().Matcher().(*PatternMatcher)
	require.True(t, ok)
	assert.False(t, hist.CanFuseAttributeFetch(datalog.SymDollar, datalog.NewKeyword(":place/code")),
		"history mode is never fusable (raw multi-version reads)")

	asOf, ok := db.AsOf(datalog.ElementID{Lamport: 1, ReplicaID: 1}).Matcher().(*PatternMatcher)
	require.True(t, ok)
	assert.True(t, asOf.CanFuseAttributeFetch(datalog.SymDollar, datalog.NewKeyword(":place/code")),
		"an as-of CardinalityOne fetch is fusable: the lookup answers the value as of the target")
}

// TestFusionDifferentialAsOfUpdatesAndTombstones runs the as-of query at each
// of three commits on every leg, with the cache on and off, and asserts every
// configuration answers what the reference leg answers with the cache off.
func TestFusionDifferentialAsOfUpdatesAndTombstones(t *testing.T) {
	typ := datalog.NewKeyword(":place/type")
	code := datalog.NewKeyword(":place/code")
	entity := datalog.NewIdentity("asof-fusion")

	type fixture struct {
		name     string
		db       *Database
		firstTx  datalog.ElementID
		secondTx datalog.ElementID
		removeTx datalog.ElementID
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			open := func(mode optimizerMode, disableCache bool) fixture {
				db := openFusionDB(t, mode, DatabaseOptions{DisableCache: disableCache})
				tx := db.NewTransaction()
				require.NoError(t, tx.Set(entity, typ, "room"))
				require.NoError(t, tx.Set(entity, code, "R1"))
				first, err := tx.Commit()
				require.NoError(t, err)

				tx = db.NewTransaction()
				require.NoError(t, tx.Set(entity, code, "R2"))
				second, err := tx.Commit()
				require.NoError(t, err)

				tx = db.NewTransaction()
				require.NoError(t, tx.Remove(entity, code, "R2"))
				removed, err := tx.Commit()
				require.NoError(t, err)
				return fixture{
					name:     fmt.Sprintf("%s/cache_disabled_%t", mode.name, disableCache),
					db:       db,
					firstTx:  first,
					secondTx: second,
					removeTx: removed,
				}
			}

			reference := open(referenceMode, true)
			configurations := []fixture{open(mode, false), open(mode, true)}
			for _, snapshot := range []struct {
				name string
				at   func(fixture) datalog.ElementID
			}{
				{name: "before update", at: func(f fixture) datalog.ElementID { return f.firstTx }},
				{name: "at update", at: func(f fixture) datalog.ElementID { return f.secondTx }},
				{name: "after tombstone", at: func(f fixture) datalog.ElementID { return f.removeTx }},
			} {
				t.Run(snapshot.name, func(t *testing.T) {
					want := fusionTuples(t, reference.db.AsOf(snapshot.at(reference)), fusionCodeQuery)
					for _, configuration := range configurations {
						got := fusionTuples(t, configuration.db.AsOf(snapshot.at(configuration)), fusionCodeQuery)
						require.Equal(t, want, got, "%s answers otherwise than %s", configuration.name, reference.name)
					}
				})
			}
		})
	}
}

func TestFusionLookupFailureIsNotAttributeAbsence(t *testing.T) {
	entity := datalog.NewIdentity("fusion-lookup-error")
	code := datalog.NewKeyword(":place/code")
	queryText := `[:find ?code :in $ ?entity :where [?entity :place/code ?code]]`

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openFusionDB(t, mode, DatabaseOptions{})
			tx := db.NewTransaction()
			require.NoError(t, tx.Set(entity, code, "R1"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Force the bound lookup to reach storage rather than the write-
			// warmed cache, then close through Janus. Every leg must return
			// ErrDBClosed rather than panic or report attribute absence.
			db.cache = NewCache(db.clock)
			require.NoError(t, db.Close())
			require.NoError(t, db.Close(), "Database.Close must be idempotent")

			_, err = db.Query(queryText, entity)
			require.Error(t, err)
			// Backends phrase it differently (Badger: "DB Closed"; the
			// emulator: "badger emulator store closed") — the contract is a
			// closed-store error, not absence or a panic.
			require.Contains(t, strings.ToLower(err.Error()), "closed")
		})
	}
}
