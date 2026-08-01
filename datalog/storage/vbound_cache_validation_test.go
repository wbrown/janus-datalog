package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// =============================================================================
// V-bound validation cache fast-path — CRDT safety proof
// =============================================================================
//
// validatingVBoundIterator.validateCandidate gained a latest-mode CardinalityOne
// fast path that resolves the current (E, A) value from the EA cache instead of a
// per-candidate EATV seek. This file PROVES that fast path is semantically
// identical to the original scan across every CRDT scenario.
//
// Proof strategy (two independent pillars):
//
//  1. DIFFERENTIAL EQUIVALENCE (the actual safety claim). Each scenario runs the
//     identical V-bound query against a cache-ENABLED database and a
//     DisableCache database holding identical data, and asserts identical result
//     sets — AND that both equal a hand-computed expected set. Value-level,
//     path-independent. If the cache path ever diverged from the scan path
//     (e.g. the historical tombstone gap), these fail.
//
//  2. COVERAGE GUARD (so #1 cannot pass trivially). A differential test passes
//     vacuously if the cache branch never fires (then cache-on also scans, and
//     the two runs are identical while proving nothing about the new code — the
//     exact false-pass trap from BUG_CACHE_CARDINALIY_ONE_TOMBSTONE). The
//     v-validation/* annotations are the independent signal that the cache-on
//     run actually took the cache branch and did NOT fall back to a scan:
//       - cache-on  → emits v-validation/cache-resolved, zero scan validation.
//       - cache-off → emits v-validation/result (scan), zero cache-resolved.
//     These annotations are real observability (they complete the
//     candidate→outcome trace); the tests merely read them to prove coverage.
// =============================================================================

// vboundCapture tallies annotation events by name. The mutex is load-bearing:
// the engine emits from parallel workers and does not wrap installed handlers,
// so serializing is the handler's own responsibility.
type vboundCapture struct {
	mu     sync.Mutex
	counts map[string]int
}

func newVBoundCapture() *vboundCapture {
	return &vboundCapture{counts: make(map[string]int)}
}

func (c *vboundCapture) handler() annotations.Handler {
	return func(ev annotations.Event) {
		c.mu.Lock()
		c.counts[ev.Name]++
		c.mu.Unlock()
	}
}

func (c *vboundCapture) reset() {
	c.mu.Lock()
	c.counts = make(map[string]int)
	c.mu.Unlock()
}

func (c *vboundCapture) get(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[name]
}

// placeTypeSchema declares :place/type as a non-unique CardinalityOne string —
// the schema shape that routes [?e :place/type V] (A+V constant, E unbound)
// through validatingVBoundIterator with validation enabled.
func placeTypeSchema() *schema.Schema {
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":place/type"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})
	return s
}

// openVBoundDB opens a fresh database on the mode's backend with a pinned
// ReplicaID (so the two sides of a differential run produce identical
// ElementIDs) and an annotation capture.
func openVBoundDB(t *testing.T, mode optimizerMode, sch schema.SchemaProvider, disableCache bool) (*Database, *vboundCapture) {
	t.Helper()
	cap := newVBoundCapture()
	db := createOptimizerModeDB(t, mode, DatabaseOptions{
		Schema:            sch,
		ReplicaID:         1, // pin so cache-on and cache-off get identical Tx
		DisableCache:      disableCache,
		AnnotationHandler: cap.handler(),
	})
	return db, cap
}

// queryEntitySet runs a V-bound find-?e query and returns the set of matched
// entity identity strings.
func queryEntitySet(t *testing.T, db *Database, queryStr string, args ...interface{}) map[string]bool {
	t.Helper()
	results, err := executor.CollectTuples(db.Query(queryStr, args...))
	require.NoError(t, err)
	set := make(map[string]bool, len(results))
	for _, tuple := range results {
		require.NotEmpty(t, tuple)
		id, ok := tuple[0].(datalog.Identity)
		require.Truef(t, ok, "expected Identity in ?e position, got %T", tuple[0])
		set[id.String()] = true
	}
	return set
}

func expectedSet(ids ...datalog.Identity) map[string]bool {
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id.String()] = true
	}
	return set
}

// assertVBoundEquivalent is the core differential assertion. It builds two
// databases (cache-on, cache-off) from the same ops, runs the same query on
// both, and asserts cache-on == cache-off == expected. When expectCacheBranch
// is true it also asserts the coverage guard: cache-on used the cache branch
// and no scan validation; cache-off used scan validation and no cache branch.
func assertVBoundEquivalent(
	t *testing.T,
	mode optimizerMode,
	sch schema.SchemaProvider,
	apply func(db *Database),
	expected map[string]bool,
	expectCacheBranch bool,
	queryStr string,
	args ...interface{},
) {
	t.Helper()

	dbOn, capOn := openVBoundDB(t, mode, sch, false)
	apply(dbOn)
	capOn.reset() // discard any write-side events; measure only the query
	gotOn := queryEntitySet(t, dbOn, queryStr, args...)

	dbOff, capOff := openVBoundDB(t, mode, sch, true)
	apply(dbOff)
	capOff.reset()
	gotOff := queryEntitySet(t, dbOff, queryStr, args...)

	assert.Equal(t, expected, gotOn, "cache-ON result must match expected")
	assert.Equal(t, expected, gotOff, "cache-OFF result must match expected")
	assert.Equal(t, gotOff, gotOn, "cache-ON must be identical to cache-OFF (DisableCache)")

	// DisableCache can never take the cache branch (nil cache). Always true,
	// regardless of scenario — confirms cache-off is a genuine scan baseline.
	assert.Zero(t, capOff.get("v-validation/cache-resolved"),
		"cache-OFF must never use the cache branch")

	if expectCacheBranch {
		// The essential coverage guard: cache-on actually took the new branch
		// AND did not fall back to a scan. Without this, differential
		// equivalence could pass vacuously (both sides scanning). The scan path
		// is silent on tombstone rejection, so we assert coverage only on the
		// cache-on side, where the cache-resolved event fires unconditionally.
		assert.Positive(t, capOn.get("v-validation/cache-resolved"),
			"cache-ON must exercise the cache fast path (else differential is vacuous)")
		assert.Zero(t, capOn.get("v-validation/result"),
			"cache-ON must NOT fall back to scan validation")
		assert.Zero(t, capOn.get("v-validation/no-winner"),
			"cache-ON must NOT fall back to scan validation")
	}
}

// vboundQueryFor builds a V-bound query for a literal string value.
func vboundQueryFor(v string) string {
	return `[:find ?e :where [?e :place/type "` + v + `"]]`
}

func TestVBoundCache_SimpleSet(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			e1 := datalog.NewIdentity("e1")
			apply := func(db *Database) {
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(e1, datalog.NewKeyword(":place/type"), "room"))
				_, err := tx.Commit()
				require.NoError(t, err)
			}
			assertVBoundEquivalent(t, mode, placeTypeSchema(), apply, expectedSet(e1), true, vboundQueryFor("room"))
		})
	}
}

// Supersession: the entire reason validateCandidate exists. The old (room, e1)
// key remains in AVET after the overwrite; the value-prefix candidate scan still
// surfaces e1, and validation must reject it because the current value is "cave".
func TestVBoundCache_Supersession(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			e1 := datalog.NewIdentity("e1")
			apply := func(db *Database) {
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(e1, datalog.NewKeyword(":place/type"), "room"))
				_, err := tx.Commit()
				require.NoError(t, err)

				tx2 := db.NewTransaction()
				require.NoError(t, tx2.Add(e1, datalog.NewKeyword(":place/type"), "cave"))
				_, err = tx2.Commit()
				require.NoError(t, err)
			}
			// Querying the stale value must return nothing...
			assertVBoundEquivalent(t, mode, placeTypeSchema(), apply, expectedSet(), true, vboundQueryFor("room"))
			// ...and the current value returns e1.
			assertVBoundEquivalent(t, mode, placeTypeSchema(), apply, expectedSet(e1), true, vboundQueryFor("cave"))
		})
	}
}

// Re-add to the same value across an intervening different value. The value-V
// scan now finds two "room" entries for e1; resolution must take the latest.
func TestVBoundCache_ReAddSameValue(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			e1 := datalog.NewIdentity("e1")
			a := datalog.NewKeyword(":place/type")
			apply := func(db *Database) {
				for _, v := range []string{"room", "cave", "room"} {
					tx := db.NewTransaction()
					require.NoError(t, tx.Add(e1, a, v))
					_, err := tx.Commit()
					require.NoError(t, err)
				}
			}
			assertVBoundEquivalent(t, mode, placeTypeSchema(), apply, expectedSet(e1), true, vboundQueryFor("room"))
			assertVBoundEquivalent(t, mode, placeTypeSchema(), apply, expectedSet(), true, vboundQueryFor("cave"))
		})
	}
}

// Tombstone of the queried value. The Remove tombstone lands in the SAME ("room")
// value-group, so the candidate iterator (CRDTResolvingIterator) resolves
// add+remove and drops e1 before validateCandidate is reached — validation does
// not run on either path (expectCacheBranch=false). Differential equivalence is
// the proof: cache-on and cache-off both yield empty. The cache's own tombstone
// rejection is proven separately in TestVBoundCache_TombstoneAfterOverwrite.
func TestVBoundCache_Tombstone(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			e1 := datalog.NewIdentity("e1")
			a := datalog.NewKeyword(":place/type")
			apply := func(db *Database) {
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(e1, a, "room"))
				_, err := tx.Commit()
				require.NoError(t, err)

				tx2 := db.NewTransaction()
				require.NoError(t, tx2.Remove(e1, a, "room"))
				_, err = tx2.Commit()
				require.NoError(t, err)
			}
			assertVBoundEquivalent(t, mode, placeTypeSchema(), apply, expectedSet(), false, vboundQueryFor("room"))
		})
	}
}

// Tombstone after overwrite to a DIFFERENT value: this is the case that drives a
// tombstoned (E, A) through validateCandidate, exercising the cache's
// OneValue()==nil rejection — the exact BUG_CACHE_CARDINALIY_ONE_TOMBSTONE class.
// e1: room@tx1, cave@tx2, remove("cave")@tx3. The "room" value-group holds only
// the stale add, so the candidate iterator emits e1; validateCandidate must then
// reject it because the current (E, A) is a tombstone. expectCacheBranch=true.
func TestVBoundCache_TombstoneAfterOverwrite(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			e1 := datalog.NewIdentity("e1")
			a := datalog.NewKeyword(":place/type")
			apply := func(db *Database) {
				for _, v := range []string{"room", "cave"} {
					tx := db.NewTransaction()
					require.NoError(t, tx.Add(e1, a, v))
					_, err := tx.Commit()
					require.NoError(t, err)
				}
				tx := db.NewTransaction()
				require.NoError(t, tx.Remove(e1, a, "cave"))
				_, err := tx.Commit()
				require.NoError(t, err)
			}
			assertVBoundEquivalent(t, mode, placeTypeSchema(), apply, expectedSet(), true, vboundQueryFor("room"))
		})
	}
}

func TestVBoundCache_TombstoneThenReAdd(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			e1 := datalog.NewIdentity("e1")
			a := datalog.NewKeyword(":place/type")
			apply := func(db *Database) {
				ops := []struct {
					add bool
					v   string
				}{{true, "room"}, {false, "room"}, {true, "room"}}
				for _, op := range ops {
					tx := db.NewTransaction()
					if op.add {
						require.NoError(t, tx.Add(e1, a, op.v))
					} else {
						require.NoError(t, tx.Remove(e1, a, op.v))
					}
					_, err := tx.Commit()
					require.NoError(t, err)
				}
			}
			assertVBoundEquivalent(t, mode, placeTypeSchema(), apply, expectedSet(e1), true, vboundQueryFor("room"))
		})
	}
}

func TestVBoundCache_TombstoneThenReAddDifferent(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			e1 := datalog.NewIdentity("e1")
			a := datalog.NewKeyword(":place/type")
			apply := func(db *Database) {
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(e1, a, "room"))
				_, err := tx.Commit()
				require.NoError(t, err)

				tx2 := db.NewTransaction()
				require.NoError(t, tx2.Remove(e1, a, "room"))
				_, err = tx2.Commit()
				require.NoError(t, err)

				tx3 := db.NewTransaction()
				require.NoError(t, tx3.Add(e1, a, "cave"))
				_, err = tx3.Commit()
				require.NoError(t, err)
			}
			// "room" group holds add+remove → candidate iterator drops e1 before
			// validation (expectCacheBranch=false). "cave" group holds only the re-add →
			// candidate reaches validateCandidate and matches (expectCacheBranch=true).
			assertVBoundEquivalent(t, mode, placeTypeSchema(), apply, expectedSet(), false, vboundQueryFor("room"))
			assertVBoundEquivalent(t, mode, placeTypeSchema(), apply, expectedSet(e1), true, vboundQueryFor("cave"))
		})
	}
}

func TestVBoundCache_MultiEntitySameValue(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			e1 := datalog.NewIdentity("e1")
			e2 := datalog.NewIdentity("e2")
			e3 := datalog.NewIdentity("e3")
			a := datalog.NewKeyword(":place/type")
			apply := func(db *Database) {
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(e1, a, "room"))
				require.NoError(t, tx.Add(e2, a, "room"))
				require.NoError(t, tx.Add(e3, a, "cave"))
				_, err := tx.Commit()
				require.NoError(t, err)
			}
			assertVBoundEquivalent(t, mode, placeTypeSchema(), apply, expectedSet(e1, e2), true, vboundQueryFor("room"))
			assertVBoundEquivalent(t, mode, placeTypeSchema(), apply, expectedSet(e3), true, vboundQueryFor("cave"))
		})
	}
}

// Mixed supersession across many entities: e1 stays room, e2 room→cave, e3
// cave→room. "room" must resolve to {e1, e3}; "cave" to {e2}.
func TestVBoundCache_MultiEntityMixedSupersession(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			e1 := datalog.NewIdentity("e1")
			e2 := datalog.NewIdentity("e2")
			e3 := datalog.NewIdentity("e3")
			a := datalog.NewKeyword(":place/type")
			apply := func(db *Database) {
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(e1, a, "room"))
				require.NoError(t, tx.Add(e2, a, "room"))
				require.NoError(t, tx.Add(e3, a, "cave"))
				_, err := tx.Commit()
				require.NoError(t, err)

				tx2 := db.NewTransaction()
				require.NoError(t, tx2.Add(e2, a, "cave"))
				require.NoError(t, tx2.Add(e3, a, "room"))
				_, err = tx2.Commit()
				require.NoError(t, err)
			}
			assertVBoundEquivalent(t, mode, placeTypeSchema(), apply, expectedSet(e1, e3), true, vboundQueryFor("room"))
			assertVBoundEquivalent(t, mode, placeTypeSchema(), apply, expectedSet(e2), true, vboundQueryFor("cave"))
		})
	}
}

// A value no entity ever held: zero candidates, so the cache branch legitimately
// does not fire (nothing to validate). Differential equivalence still holds.
func TestVBoundCache_NeverExisted(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			e1 := datalog.NewIdentity("e1")
			apply := func(db *Database) {
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(e1, datalog.NewKeyword(":place/type"), "room"))
				_, err := tx.Commit()
				require.NoError(t, err)
			}
			assertVBoundEquivalent(t, mode, placeTypeSchema(), apply, expectedSet(), false, vboundQueryFor("forest"))
		})
	}
}

// Latest-only gate: an AsOf (concrete snapshot) query must NOT use the cache
// fast path (txID != nil). We prove the gate holds (zero cache-resolved events
// under AsOf) and that AsOf cache-on == AsOf cache-off. As-of snapshot
// correctness of the scan path itself is covered by
// TestVBoundValidation_AsOfRespectsSnapshot; here we additionally confirm the
// gated-off path returns the correct snapshot value.
func TestVBoundCache_AsOfGateOff(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			e1 := datalog.NewIdentity("e1")
			a := datalog.NewKeyword(":place/type")

			build := func(disableCache bool) (*Database, *vboundCapture, datalog.ElementID) {
				db, capt := openVBoundDB(t, mode, placeTypeSchema(), disableCache)
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(e1, a, "room"))
				txAtRoom, err := tx.Commit()
				require.NoError(t, err)
				tx2 := db.NewTransaction()
				require.NoError(t, tx2.Add(e1, a, "cave"))
				_, err = tx2.Commit()
				require.NoError(t, err)
				return db, capt, txAtRoom
			}

			dbOn, capOn, txAtRoom := build(false)

			// Latest mode: cache fast path fires; current value is "cave", so "room" is empty.
			capOn.reset()
			latestRoom := queryEntitySet(t, dbOn, vboundQueryFor("room"))
			assert.Equal(t, expectedSet(), latestRoom, "latest: stale 'room' must be empty")
			assert.Positive(t, capOn.get("v-validation/cache-resolved"),
				"latest mode must use the cache fast path")

			// AsOf: gate must be OFF — no cache-resolved events — and the snapshot value
			// (e1 was "room" at txAtRoom) must be returned by the scan path.
			capOn.reset()
			asOfRoom := queryEntitySet(t, dbOn.AsOf(txAtRoom), vboundQueryFor("room"))
			assert.Zero(t, capOn.get("v-validation/cache-resolved"),
				"as-of must NOT use the latest-only cache fast path")
			assert.Equal(t, expectedSet(e1), asOfRoom,
				"as-of (gated to the scan path) must return the snapshot value")

			// No-op: the same AsOf query against a DisableCache database is identical.
			dbOff, _, txOffAtRoom := build(true)
			asOfOff := queryEntitySet(t, dbOff.AsOf(txOffAtRoom), vboundQueryFor("room"))
			assert.Equal(t, asOfOff, asOfRoom, "as-of result must be identical cache-on vs cache-off")
		})
	}
}

// Regression: validatingVBoundIterator.validateCandidate must resolve the
// (E, A) winner UNDER THE AS-OF SNAPSHOT, not against absolute-latest.
//
// The bug: validateCandidate's EATV scan took the first (highest-Tx) entry
// without shouldFilterTx. Under a concrete as-of snapshot, a V-bound query
// [?e :attr V] as-of a transaction where the entity held V was wrongly rejected
// whenever the entity's value changed AFTER the snapshot — the post-snapshot
// value leaked into validation. This is the scan path (the cache fast path is
// gated off for as-of), so it reproduces with the cache enabled or disabled.
//
// Two facets: (a) value overwritten after the snapshot, (b) value tombstoned
// after the snapshot. In both, the as-of query must still see the snapshot value.
func TestVBoundValidation_AsOfRespectsSnapshot(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			a := datalog.NewKeyword(":place/type")

			t.Run("overwrite after snapshot", func(t *testing.T) {
				db, _ := openVBoundDB(t, mode, placeTypeSchema(), false)
				e1 := datalog.NewIdentity("e1")

				tx := db.NewTransaction()
				require.NoError(t, tx.Add(e1, a, "room"))
				txAtRoom, err := tx.Commit()
				require.NoError(t, err)

				tx2 := db.NewTransaction()
				require.NoError(t, tx2.Add(e1, a, "cave"))
				_, err = tx2.Commit()
				require.NoError(t, err)

				// As-of the first commit e1 was "room"; the later "cave" must not leak in.
				got := queryEntitySet(t, db.AsOf(txAtRoom), vboundQueryFor("room"))
				assert.Equal(t, expectedSet(e1), got,
					"as-of must validate against the snapshot value, not absolute-latest")

				// "cave" only exists after the snapshot, so as-of it must not match.
				gotCave := queryEntitySet(t, db.AsOf(txAtRoom), vboundQueryFor("cave"))
				assert.Equal(t, expectedSet(), gotCave,
					"a value set after the snapshot must not match as-of")
			})

			t.Run("tombstone after snapshot", func(t *testing.T) {
				db, _ := openVBoundDB(t, mode, placeTypeSchema(), false)
				e1 := datalog.NewIdentity("e1")

				tx := db.NewTransaction()
				require.NoError(t, tx.Add(e1, a, "room"))
				txAtRoom, err := tx.Commit()
				require.NoError(t, err)

				tx2 := db.NewTransaction()
				require.NoError(t, tx2.Remove(e1, a, "room"))
				_, err = tx2.Commit()
				require.NoError(t, err)

				// As-of the first commit the attribute existed with value "room"; the
				// later tombstone must not leak into validation.
				got := queryEntitySet(t, db.AsOf(txAtRoom), vboundQueryFor("room"))
				assert.Equal(t, expectedSet(e1), got,
					"as-of must see the pre-tombstone snapshot value")
			})
		})
	}
}

// Unique attributes must NOT use the cache fast path: ResolveLWW walks for unique
// attrs (CRDT-unique fallback semantics) which differs from the naive first-entry
// the fast path mirrors. Unique V-bound queries short-circuit via the unique
// winner path before validation, so we assert correctness AND zero cache-resolved.
func TestVBoundCache_UniqueSkipsCacheBranch(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			uniqueSchema := func() *schema.Schema {
				s := schema.NewSchema()
				s.Add(&schema.AttributeDefinition{
					Ident:       datalog.NewKeyword(":place/code"),
					ValueType:   schema.TypeString,
					Cardinality: schema.CardinalityOne,
					Unique:      schema.UniqueValue,
				})
				return s
			}

			e1 := datalog.NewIdentity("e1")
			a := datalog.NewKeyword(":place/code")

			dbOn, capOn := openVBoundDB(t, mode, uniqueSchema(), false)
			tx := dbOn.NewTransaction()
			require.NoError(t, tx.Add(e1, a, "ABC"))
			_, err := tx.Commit()
			require.NoError(t, err)

			capOn.reset()
			got := queryEntitySet(t, dbOn, `[:find ?e :where [?e :place/code "ABC"]]`)
			assert.Equal(t, expectedSet(e1), got, "unique V-bound query result")
			assert.Zero(t, capOn.get("v-validation/cache-resolved"),
				"unique attributes must NOT take the cache fast path")

			// Cross-check: a DisableCache database returns the same entity.
			dbOff, _ := openVBoundDB(t, mode, uniqueSchema(), true)
			txOff := dbOff.NewTransaction()
			require.NoError(t, txOff.Add(e1, a, "ABC"))
			_, err = txOff.Commit()
			require.NoError(t, err)
			gotOff := queryEntitySet(t, dbOff, `[:find ?e :where [?e :place/code "ABC"]]`)
			assert.Equal(t, got, gotOff, "unique result must match between cache-on and cache-off")
		})
	}
}
