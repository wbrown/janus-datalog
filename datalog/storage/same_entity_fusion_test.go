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
	"github.com/wbrown/janus-datalog/datalog/planner"
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
// Pillar 1 — DIFFERENTIAL: every scenario runs the identical query against a
// fusion-OFF database and a fusion-ON database holding identical data, and
// asserts identical results. If fusion ever changed an answer, these fail.
//
// Pillar 2 — COVERAGE GUARD: the differential passes vacuously if fusion never
// fires when ON. TestFusion_CoverageFires asserts the `pattern/fused-fetch`
// annotation fires for a CardinalityOne fetch and does NOT fire for a
// CardinalityMany one — so we know the ON arm actually took the fused path.
//
// Pillar 3 — GATE: TestFusionGate_ModeAndCardinality proves CanFuseAttributeFetch
// (the matcher decision the executor consults) is true only for latest-mode
// CardinalityOne, and false for CardinalityMany, schemaless, and history mode.
// The executor fuses iff that returns true, so this bounds where fusion can run.
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

func openFusionDB(t *testing.T, fusion bool, handler annotations.Handler, popts *planner.PlannerOptions) *Database {
	t.Helper()
	opts := DefaultPlannerOptions()
	if popts != nil {
		opts = *popts
	}
	opts.EnableAttributeFetchFusion = fusion
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:              t.TempDir(),
		Schema:            fusionSchema(),
		ReplicaID:         1,
		PlannerOptions:    &opts,
		AnnotationHandler: handler,
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func fusionRows(t *testing.T, db *Database, query string, args ...interface{}) []string {
	t.Helper()
	rel, err := db.Query(query, args...)
	require.NoError(t, err)
	var rows []string
	it := rel.Iterator()
	for it.Next() {
		rows = append(rows, fmt.Sprintf("%v", it.Tuple()))
	}
	require.NoError(t, it.Error())
	it.Close()
	sort.Strings(rows)
	return rows
}

// assertFusionEquivalent applies the same ops to a fusion-off and a fusion-on
// database (identical data, pinned ReplicaID) and asserts the query returns
// identical rows. popts sets the databases' remaining planner options
// (nil = defaults); the fusion flag is applied on top.
func assertFusionEquivalent(t *testing.T, popts *planner.PlannerOptions, apply func(*Database), query string, args ...interface{}) {
	t.Helper()
	off := openFusionDB(t, false, nil, popts)
	apply(off)
	on := openFusionDB(t, true, nil, popts)
	apply(on)
	want := fusionRows(t, off, query, args...)
	got := fusionRows(t, on, query, args...)
	assert.Equal(t, want, got, "fusion-ON must return the same rows as fusion-OFF")
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
			popts := mode.plannerOptions()

			t.Run("present", func(t *testing.T) {
				assertFusionEquivalent(t, &popts, func(db *Database) {
					commit(t, db, func(tx *Transaction) {
						require.NoError(t, tx.Set(e1, typ, "room"))
						require.NoError(t, tx.Set(e1, code, "R1"))
						require.NoError(t, tx.Set(e2, typ, "room"))
						require.NoError(t, tx.Set(e2, code, "R2"))
					})
				}, fusionCodeQuery)
			})

			t.Run("missing attribute drops the entity", func(t *testing.T) {
				assertFusionEquivalent(t, &popts, func(db *Database) {
					commit(t, db, func(tx *Transaction) {
						require.NoError(t, tx.Set(e1, typ, "room")) // no code
						require.NoError(t, tx.Set(e2, typ, "room"))
						require.NoError(t, tx.Set(e2, code, "R2"))
					})
				}, fusionCodeQuery)
			})

			t.Run("tombstoned attribute drops the entity", func(t *testing.T) {
				assertFusionEquivalent(t, &popts, func(db *Database) {
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
				assertFusionEquivalent(t, &popts, func(db *Database) {
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
				assertFusionEquivalent(t, &popts, func(db *Database) {
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
				assertFusionEquivalent(t, &popts, func(db *Database) {
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
				assertFusionEquivalent(t, &popts, func(db *Database) {
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
				assertFusionEquivalent(t, &popts, func(db *Database) {
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
			popts := mode.plannerOptions()

			t.Run("matching value keeps tuple", func(t *testing.T) {
				assertFusionEquivalent(t, &popts, func(db *Database) {
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
				assertFusionEquivalent(t, &popts, func(db *Database) {
					tx := db.NewTransaction()
					require.NoError(t, tx.Set(e1, code, "R1"))
					_, err := tx.Commit()
					require.NoError(t, err)
				}, queryText)
			})

			t.Run("latest value controls constraint", func(t *testing.T) {
				assertFusionEquivalent(t, &popts, func(db *Database) {
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

func TestFusionConstantConstraintCoverage(t *testing.T) {
	typ := datalog.NewKeyword(":place/type")
	code := datalog.NewKeyword(":place/code")
	tags := datalog.NewKeyword(":place/tags")
	e1 := datalog.NewIdentity("constraint-coverage")

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()

			t.Run("cardinality one constraint fires", func(t *testing.T) {
				cap := &fusionCapture{n: map[string]int{}}
				db := openFusionDB(t, true, cap.handler(), &popts)
				tx := db.NewTransaction()
				require.NoError(t, tx.Set(e1, code, "R1"))
				require.NoError(t, tx.Set(e1, typ, "room"))
				_, err := tx.Commit()
				require.NoError(t, err)

				rows := fusionRows(t, db, `[:find ?e
					:where [?e :place/code "R1"]
					       [?e :place/type "room"]]`)
				require.Len(t, rows, 1)
				require.Equal(t, 1, cap.get("pattern/fused-constraint"))
			})

			t.Run("cardinality many constraint does not fire", func(t *testing.T) {
				cap := &fusionCapture{n: map[string]int{}}
				db := openFusionDB(t, true, cap.handler(), &popts)
				tx := db.NewTransaction()
				require.NoError(t, tx.Set(e1, code, "R1"))
				require.NoError(t, tx.Add(e1, tags, "selected"))
				_, err := tx.Commit()
				require.NoError(t, err)

				rows := fusionRows(t, db, `[:find ?e
					:where [?e :place/code ?c]
					       [?e :place/tags "selected"]]`)
				require.Len(t, rows, 1)
				require.Zero(t, cap.get("pattern/fused-constraint"))
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

// TestFusion_CoverageFires proves the ON arm actually takes the fused path for a
// CardinalityOne fetch (so the differential isn't vacuous), and does NOT fuse a
// CardinalityMany fetch.
func TestFusion_CoverageFires(t *testing.T) {
	typ := datalog.NewKeyword(":place/type")
	code := datalog.NewKeyword(":place/code")
	tags := datalog.NewKeyword(":place/tags")
	e1 := datalog.NewIdentity("e1")

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()

			t.Run("card-one fetch fires fusion", func(t *testing.T) {
				cap := &fusionCapture{n: map[string]int{}}
				db := openFusionDB(t, true, cap.handler(), &popts)
				tx := db.NewTransaction()
				require.NoError(t, tx.Set(e1, typ, "room"))
				require.NoError(t, tx.Set(e1, code, "R1"))
				_, err := tx.Commit()
				require.NoError(t, err)

				_ = fusionRows(t, db, fusionCodeQuery)
				assert.Positive(t, cap.get("pattern/fused-fetch"),
					"CardinalityOne fetch must take the fused path")
			})

			t.Run("card-many fetch does not fire fusion", func(t *testing.T) {
				cap := &fusionCapture{n: map[string]int{}}
				db := openFusionDB(t, true, cap.handler(), &popts)
				tx := db.NewTransaction()
				require.NoError(t, tx.Set(e1, typ, "room"))
				require.NoError(t, tx.Add(e1, tags, "a"))
				require.NoError(t, tx.Add(e1, tags, "b"))
				_, err := tx.Commit()
				require.NoError(t, err)

				_ = fusionRows(t, db, `[:find ?e ?tag :where [?e :place/type "room"] [?e :place/tags ?tag]]`)
				assert.Zero(t, cap.get("pattern/fused-fetch"),
					"CardinalityMany fetch must NOT be fused")
			})
		})
	}
}

// TestFusionGate_ModeAndCardinality proves the matcher gate the executor
// consults: fusion is permitted only for latest-mode CardinalityOne attributes.
func TestFusionGate_ModeAndCardinality(t *testing.T) {
	db := openFusionDB(t, true, nil, nil)

	latest, ok := db.Matcher().(*PatternMatcher)
	require.True(t, ok)
	assert.True(t, latest.CanFuseAttributeFetch(datalog.NewKeyword(":place/code")),
		"latest-mode CardinalityOne is fusable")
	assert.False(t, latest.CanFuseAttributeFetch(datalog.NewKeyword(":place/tags")),
		"CardinalityMany is not fusable")
	assert.False(t, latest.CanFuseAttributeFetch(datalog.NewKeyword(":place/steps")),
		"CardinalityVector is not fusable")
	assert.False(t, latest.CanFuseAttributeFetch(datalog.NewKeyword(":place/unknown")),
		"schemaless attribute is not fusable")

	hist, ok := db.History().Matcher().(*PatternMatcher)
	require.True(t, ok)
	assert.False(t, hist.CanFuseAttributeFetch(datalog.NewKeyword(":place/code")),
		"history mode is never fusable (raw multi-version reads)")

	asOf, ok := db.AsOf(datalog.ElementID{Lamport: 1, ReplicaID: 1}).Matcher().(*PatternMatcher)
	require.True(t, ok)
	assert.False(t, asOf.CanFuseAttributeFetch(datalog.NewKeyword(":place/code")),
		"as-of mode must use snapshot CRDT resolution, not latest-value fusion")
}

func TestFusionDifferentialAsOfUpdatesAndTombstones(t *testing.T) {
	typ := datalog.NewKeyword(":place/type")
	code := datalog.NewKeyword(":place/code")
	entity := datalog.NewIdentity("asof-fusion")

	type fixture struct {
		db       *Database
		firstTx  datalog.ElementID
		secondTx datalog.ElementID
		removeTx datalog.ElementID
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			open := func(fusion bool) fixture {
				db := openFusionDB(t, fusion, nil, &popts)
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
				return fixture{db: db, firstTx: first, secondTx: second, removeTx: removed}
			}

			off := open(false)
			on := open(true)
			for _, snapshot := range []struct {
				name string
				off  datalog.ElementID
				on   datalog.ElementID
			}{
				{name: "before update", off: off.firstTx, on: on.firstTx},
				{name: "at update", off: off.secondTx, on: on.secondTx},
				{name: "after tombstone", off: off.removeTx, on: on.removeTx},
			} {
				t.Run(snapshot.name, func(t *testing.T) {
					want := fusionRows(t, off.db.AsOf(snapshot.off), fusionCodeQuery)
					got := fusionRows(t, on.db.AsOf(snapshot.on), fusionCodeQuery)
					require.Equal(t, want, got)
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
			popts := mode.plannerOptions()
			for _, fusion := range []bool{false, true} {
				t.Run(fmt.Sprintf("fusion_%t", fusion), func(t *testing.T) {
					db := openFusionDB(t, fusion, nil, &popts)
					tx := db.NewTransaction()
					require.NoError(t, tx.Set(entity, code, "R1"))
					_, err := tx.Commit()
					require.NoError(t, err)

					// Force the bound lookup to reach storage rather than the write-
					// warmed cache, then close through Janus. Both paths must
					// return ErrDBClosed rather than panic or report attribute absence.
					db.cache = NewCache()
					require.NoError(t, db.Close())
					require.NoError(t, db.Close(), "Database.Close must be idempotent")

					_, err = db.Query(queryText, entity)
					require.Error(t, err)
					// Backends phrase it differently (Badger: "DB Closed"; memory
					// store: "memory store closed") — the contract is a closed-store
					// error, not absence or a panic.
					require.Contains(t, strings.ToLower(err.Error()), "closed")
				})
			}
		})
	}
}
