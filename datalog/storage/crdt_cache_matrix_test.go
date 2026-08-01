package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// =============================================================================
// CRDT Cache Matrix Tests
// =============================================================================
//
// These tests verify that CRDT resolution works correctly with cache enabled
// AND disabled. This is critical because:
// - Cache is an optimization, not a correctness requirement
// - The code MUST work correctly with DisableCache: true
// - If a test passes only with cache enabled, the fix is incomplete
//
// See: BUG_CRDT_QUERY_RESOLUTION_NOT_APPLIED.md
// =============================================================================

// cacheTestMode represents whether tests run with cache enabled or disabled
type cacheTestMode struct {
	name         string
	disableCache bool
}

var cacheTestModes = []cacheTestMode{
	{"cache_enabled", false},
	{"cache_disabled", true},
}

// createCacheTestDB creates a test database on the mode's backend with the
// given cache mode. handler is registered at open, since everything the
// database builds is constructed with it; nil is annotations-off.
func createCacheTestDB(t *testing.T, mode optimizerMode, disableCache bool, handler annotations.Handler) *Database {
	return createOptimizerModeDB(t, mode, DatabaseOptions{
		DisableCache:      disableCache,
		AnnotationHandler: handler,
	})
}

// assertCacheModesAgree is the second of the two shapes a cache test can take,
// and the stronger one. cacheTestModes above loops so each mode must satisfy
// the same written-down expectation; this builds both and requires them to
// agree with each other, so it needs no expectation at all and cannot be
// weakened by writing one that both modes happen to meet.
//
// That difference is not academic. An expectation is authored against whichever
// mode the author ran, and the mode that reaches a given code path is decided
// by query shape rather than by the loop — a constant-E pattern reaches
// matchFromCache, an :in-bound one does not — so a loop can run twice through
// one implementation and read as covering two.
//
// build populates the database; probe reads it and returns whatever the
// comparison is over. Both run against each database in turn, so probe may
// consume relations, hold iterators, or write — it gets a database of its own.
func assertCacheModesAgree(
	t *testing.T,
	omode optimizerMode,
	build func(t *testing.T, db *Database),
	probe func(t *testing.T, db *Database) interface{},
) {
	t.Helper()

	results := map[bool]interface{}{}
	for _, mode := range cacheTestModes {
		db := createCacheTestDB(t, omode, mode.disableCache, nil)
		build(t, db)
		results[mode.disableCache] = probe(t, db)
	}

	require.Equal(t, results[true], results[false],
		"the EA cache is an optimization, not a correctness input: "+
			"cache-disabled gave %v, cache-enabled gave %v",
		results[true], results[false])
}

// =============================================================================
// Test Matrix: All binding patterns
// =============================================================================

// TestCacheMatrix_AConstant tests when A is a constant in the pattern (baseline)
func TestCacheMatrix_AConstant(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/name"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					personID := datalog.NewIdentity("person-1")

					for _, name := range []string{"Alice", "Bob", "Charlie"} {
						tx := db.NewTransaction()
						tx.Set(personID, datalog.NewKeyword(":person/name"), name)
						tx.Commit()
					}

					// Pattern: A as constant (baseline - should work)
					results, err := executor.CollectTuples(db.Query(
						`[:find ?v :in $ ?e :where [?e :person/name ?v]]`,
						personID))
					require.NoError(t, err)
					assert.Len(t, results, 1, "[%s] A as constant should return 1 result", mode.name)
					if len(results) == 1 {
						assert.Equal(t, "Charlie", results[0][0], "[%s] Should return LWW winner", mode.name)
					}
				})
			}
		})
	}
}

// TestCacheMatrix_AFromScalarInput tests when A comes from a scalar input
func TestCacheMatrix_AFromScalarInput(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/name"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					personID := datalog.NewIdentity("person-1")
					nameAttr := datalog.NewKeyword(":person/name")

					for _, name := range []string{"Alice", "Bob", "Charlie"} {
						tx := db.NewTransaction()
						tx.Set(personID, nameAttr, name)
						tx.Commit()
					}

					// Pattern: A from scalar input
					results, err := executor.CollectTuples(db.Query(
						`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
						personID, nameAttr))
					require.NoError(t, err)
					assert.Len(t, results, 1, "[%s] A from scalar input should return 1 result", mode.name)
					if len(results) == 1 {
						assert.Equal(t, "Charlie", results[0][0], "[%s] Should return LWW winner", mode.name)
					}
				})
			}
		})
	}
}

// TestCacheMatrix_AUnbound tests when A is completely unbound
func TestCacheMatrix_AUnbound(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/name"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/age"),
						ValueType:   schema.TypeLong,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					personID := datalog.NewIdentity("person-1")

					// Write multiple values to name
					for _, name := range []string{"Alice", "Bob", "Charlie"} {
						tx := db.NewTransaction()
						tx.Set(personID, datalog.NewKeyword(":person/name"), name)
						tx.Commit()
					}

					// Write multiple values to age
					for _, age := range []int64{20, 25, 30} {
						tx := db.NewTransaction()
						tx.Set(personID, datalog.NewKeyword(":person/age"), age)
						tx.Commit()
					}

					// Pattern: A completely unbound - should get 2 results (one per attribute)
					results, err := executor.CollectTuples(db.Query(
						`[:find ?a ?v :in $ ?e :where [?e ?a ?v]]`,
						personID))
					require.NoError(t, err)

					// Should return 2 results: one for name (Charlie), one for age (30)
					// NOT 6 results (all historical values)
					assert.Len(t, results, 2, "[%s] Unbound A should return 1 result per attribute (CRDT resolved)", mode.name)

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestCacheMatrix_EConstantAUnbound tests when E is a constant in the pattern (not via input)
// This exercises chooseIndex's E-only path, which previously used EAVT (wrong for CRDT)
func TestCacheMatrix_EConstantAUnbound(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/name"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/age"),
						ValueType:   schema.TypeLong,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					personID := datalog.NewIdentity("person-1")

					// Write multiple values to name
					for _, name := range []string{"Alice", "Bob", "Charlie"} {
						tx := db.NewTransaction()
						tx.Set(personID, datalog.NewKeyword(":person/name"), name)
						tx.Commit()
					}

					// Write multiple values to age
					for _, age := range []int64{20, 25, 30} {
						tx := db.NewTransaction()
						tx.Set(personID, datalog.NewKeyword(":person/age"), age)
						tx.Commit()
					}

					// Pattern: E is CONSTANT in pattern (not via :in), A unbound
					// This goes through matchUnboundAsRelation → chooseIndex with e != nil
					// The bug was: chooseIndex used EAVT (V-before-Tx) for E-only case
					// Fixed: now uses EATV (Tx-first) for proper CRDT resolution
					// The entity constant is an #identity literal: entity position
					// requires an Identity, never a seed string.
					results, err := executor.CollectTuples(db.Query(
						`[:find ?a ?v :where [#identity "` + personID.L85() + `" ?a ?v]]`))
					require.NoError(t, err)

					// Should return 2 results: one for name (Charlie), one for age (30)
					// NOT 6 results (all historical values)
					assert.Len(t, results, 2, "[%s] E constant in pattern should return CRDT-resolved results", mode.name)

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestCacheMatrix_VOnlyBound tests when ONLY V is bound (E and A both unbound)
// This exercises chooseIndex's V-only path, which uses VAET index with per-datom
// cardinality resolution.
//
// VAET has order V → A → E → Tx↓. CRDTResolvingIterator wraps the scan and handles
// many/vector/unknown correctly. Card-one emissions are post-validated with EATV.
func TestCacheMatrix_VOnlyBound(t *testing.T) {

	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, func(e annotations.Event) {
						t.Logf("[TRACE] %s: %v", e.Name, e.Data)
					})

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/manager"),
						ValueType:   schema.TypeRef,
						Cardinality: schema.CardinalityOne,
					})
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":project/lead"),
						ValueType:   schema.TypeRef,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					leader := datalog.NewIdentity("leader-1")
					emp1 := datalog.NewIdentity("emp-1")
					proj1 := datalog.NewIdentity("proj-1")

					// emp1 has multiple manager changes, final is leader
					for _, mgr := range []datalog.Identity{
						datalog.NewIdentity("old-manager"),
						leader,
					} {
						tx := db.NewTransaction()
						tx.Set(emp1, datalog.NewKeyword(":person/manager"), mgr)
						tx.Commit()
					}

					// proj1 has leader as lead
					tx := db.NewTransaction()
					tx.Set(proj1, datalog.NewKeyword(":project/lead"), leader)
					tx.Commit()

					// Pattern: ONLY V bound - "what entities/attributes reference leader?"
					// This uses VAET index with per-datom cardinality resolution
					results, err := executor.CollectTuples(db.Query(
						`[:find ?e ?a :in $ ?v :where [?e ?a ?v]]`,
						leader))
					require.NoError(t, err)

					// Should return 2 results: emp1/:person/manager and proj1/:project/lead
					// NOT extra results from historical assignments (old-manager)
					assert.Len(t, results, 2, "[%s] V-only bound should return CRDT-resolved results", mode.name)

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestCacheMatrix_VOnlyBound_Supersede tests that V-bound queries correctly
// handle superseded values. This is the critical test case from the proof review:
//
// If emp-1/:person/manager was "leader" but is now "new-leader", a query for
// V="leader" should NOT return emp-1, because the CRDT-resolved current value
// is "new-leader", not "leader".
//
// With VAET + cardinalityAwareVBoundIterator, card-one emissions are post-validated
// with EATV point lookup, correctly filtering out stale candidates.
func TestCacheMatrix_VOnlyBound_Supersede(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/manager"),
						ValueType:   schema.TypeRef,
						Cardinality: schema.CardinalityOne,
					})
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":project/lead"),
						ValueType:   schema.TypeRef,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					leader := datalog.NewIdentity("leader-1")
					newLeader := datalog.NewIdentity("new-leader-1")
					emp1 := datalog.NewIdentity("emp-1")
					proj1 := datalog.NewIdentity("proj-1")

					// emp1 manager: leader → new-leader (SUPERSEDED)
					tx := db.NewTransaction()
					tx.Set(emp1, datalog.NewKeyword(":person/manager"), leader)
					tx.Commit()

					tx = db.NewTransaction()
					tx.Set(emp1, datalog.NewKeyword(":person/manager"), newLeader) // supersedes leader
					tx.Commit()

					// proj1 still has leader as lead (NOT superseded)
					tx = db.NewTransaction()
					tx.Set(proj1, datalog.NewKeyword(":project/lead"), leader)
					tx.Commit()

					// Query: find all (E, A) where V = leader
					// CRDT-resolved, emp-1/:person/manager is now "new-leader", NOT "leader"
					// So only proj-1/:project/lead should be returned
					results, err := executor.CollectTuples(db.Query(
						`[:find ?e ?a :in $ ?v :where [?e ?a ?v]]`,
						leader))
					require.NoError(t, err)

					// Should return 1 result: ONLY proj1/:project/lead
					// emp1/:person/manager should NOT be returned because its current value is new-leader
					assert.Len(t, results, 1, "[%s] V-bound query should only return entities where V is the CURRENT value", mode.name)

					if len(results) == 1 {
						// Verify it's proj1, not emp1
						e := results[0][0]
						assert.Equal(t, proj1, e, "[%s] Should return proj-1, not emp-1", mode.name)
					}

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestVOnlyBound_CardinalityMany_Retracted tests that V-bound queries with
// CardinalityMany attributes correctly handle retracted values using add-wins.
//
// Scenario: Add tag "go" then retract it. Query for V="go" should return nothing.
// This tests per-datom cardinality resolution for V-only bound (A is variable).
func TestVOnlyBound_CardinalityMany_Retracted(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, func(e annotations.Event) {
						t.Logf("[TRACE] %s: %v", e.Name, e.Data)
					})

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/tags"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityMany,
					})
					db.SetSchema(s)

					emp1 := datalog.NewIdentity("emp-1")

					// Debug: check schema
					if db.schema != nil {
						attr := db.schema.GetAttribute(datalog.NewKeyword(":person/tags"))
						if attr != nil {
							t.Logf("Schema has :person/tags with cardinality=%v", attr.Cardinality)
						} else {
							t.Log("Schema does NOT have :person/tags")
						}
					} else {
						t.Log("No schema set")
					}

					// Add tag "go"
					tx := db.NewTransaction()
					t.Logf("Adding tag 'go' to emp1=%s, attr=:person/tags", emp1.String())
					err := tx.Add(emp1, datalog.NewKeyword(":person/tags"), "go")
					require.NoError(t, err, "Add tag 'go' should succeed")
					txID, err := tx.Commit()
					require.NoError(t, err, "Commit should succeed")
					t.Logf("Committed transaction: %v", txID)

					// Remove tag "go" using CRDT tombstone (not Retract which deletes)
					tx = db.NewTransaction()
					err = tx.Remove(emp1, datalog.NewKeyword(":person/tags"), "go")
					require.NoError(t, err, "Remove tag 'go' should succeed")
					_, err = tx.Commit()
					require.NoError(t, err, "Commit should succeed")

					// Debug: scan EATV index directly to see ALL datoms
					t.Log("=== Scanning EATV index for all datoms ===")
					store := db.Store()
					iterE, err := store.ScanKeysOnly(ScanBound{Index: EATV})
					require.NoError(t, err)
					countE := 0
					for iterE.Next() {
						d, _ := iterE.Datom()
						if d != nil {
							t.Logf("  EATV datom: E=%s A=%s V=%v Tx=%s Op=%d",
								d.E.String(), d.A.String(), d.V, d.Tx.String(), d.Op)
							countE++
						}
					}
					iterE.Close()
					t.Logf("=== Found %d datoms in EATV ===", countE)

					// Debug: scan VAET index directly to see what's there
					t.Log("=== Scanning VAET index ===")
					iter, err := store.ScanKeysOnly(ScanBound{Index: VAET})
					require.NoError(t, err)
					count := 0
					for iter.Next() {
						d, _ := iter.Datom()
						if d != nil {
							t.Logf("  VAET datom: E=%s A=%s V=%v Tx=%s Op=%d",
								d.E.String(), d.A.String(), d.V, d.Tx.String(), d.Op)
							count++
						}
					}
					iter.Close()
					t.Logf("=== Found %d datoms in VAET ===", count)

					// Query: find all (E, A) where V = "go"
					// Since "go" was retracted, should return nothing
					results, err := executor.CollectTuples(db.Query(
						`[:find ?e ?a :in $ ?v :where [?e ?a ?v]]`,
						"go"))
					require.NoError(t, err)

					// Should return 0 results - "go" was retracted
					assert.Len(t, results, 0, "[%s] V-bound query on retracted card-many value should return nothing", mode.name)

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestVOnlyBound_MixedCardinality tests V-only bound queries where different
// attributes have different cardinalities. The per-datom resolution must handle
// each attribute according to its cardinality.
//
// Scenario:
// - :person/name (CardinalityOne): emp-1 had "Alice" then changed to "Bob"
// - :person/tags (CardinalityMany): emp-1 has tag "Alice" (not retracted)
// Query for V="Alice" should return only (:person/tags, emp-1), NOT (:person/name, emp-1)
func TestVOnlyBound_MixedCardinality(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/name"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/tags"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityMany,
					})
					db.SetSchema(s)

					emp1 := datalog.NewIdentity("emp-1")

					// Set name to "Alice"
					tx := db.NewTransaction()
					tx.Set(emp1, datalog.NewKeyword(":person/name"), "Alice")
					tx.Commit()

					// Change name to "Bob" (supersedes "Alice")
					tx = db.NewTransaction()
					tx.Set(emp1, datalog.NewKeyword(":person/name"), "Bob")
					tx.Commit()

					// Add tag "Alice" (separate from name, still current)
					tx = db.NewTransaction()
					tx.Add(emp1, datalog.NewKeyword(":person/tags"), "Alice")
					tx.Commit()

					// Query: find all (E, A) where V = "Alice"
					results, err := executor.CollectTuples(db.Query(
						`[:find ?e ?a :in $ ?v :where [?e ?a ?v]]`,
						"Alice"))
					require.NoError(t, err)

					// Should return 1 result: emp-1/:person/tags
					// NOT emp-1/:person/name (because name is now "Bob")
					assert.Len(t, results, 1, "[%s] V-bound query with mixed cardinality should apply per-datom resolution", mode.name)

					if len(results) == 1 {
						a := results[0][1]
						assert.Equal(t, datalog.NewKeyword(":person/tags"), a,
							"[%s] Should return :person/tags (card-many), not :person/name (card-one superseded)", mode.name)
					}

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestVOnlyBound_Schemaless tests V-only bound queries on schemaless attributes.
// Schemaless attributes default to CardinalityMany (add-wins) semantics.
func TestVOnlyBound_Schemaless(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					// NO SCHEMA - all attributes are schemaless

					emp1 := datalog.NewIdentity("emp-1")

					// Add value "test" to schemaless attribute
					tx := db.NewTransaction()
					tx.Add(emp1, datalog.NewKeyword(":misc/data"), "test")
					tx.Commit()

					// Remove it using CRDT tombstone (not Retract which physically deletes)
					tx = db.NewTransaction()
					tx.Remove(emp1, datalog.NewKeyword(":misc/data"), "test")
					tx.Commit()

					// Query: find all (E, A) where V = "test"
					// Schemaless uses add-wins, so retracted value should not appear
					results, err := executor.CollectTuples(db.Query(
						`[:find ?e ?a :in $ ?v :where [?e ?a ?v]]`,
						"test"))
					require.NoError(t, err)

					// Should return 0 results - value was retracted
					assert.Len(t, results, 0, "[%s] V-bound query on retracted schemaless value should return nothing", mode.name)

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestCacheMatrix_AVBound tests when A and V are bound (E unbound)
// This exercises the AVET index path
func TestCacheMatrix_AVBound(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/manager"),
						ValueType:   schema.TypeRef,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					manager := datalog.NewIdentity("manager-1")
					emp1 := datalog.NewIdentity("emp-1")
					emp2 := datalog.NewIdentity("emp-2")

					// emp1 has multiple manager changes
					for _, mgr := range []datalog.Identity{
						datalog.NewIdentity("old-manager-1"),
						datalog.NewIdentity("old-manager-2"),
						manager, // Final manager
					} {
						tx := db.NewTransaction()
						tx.Set(emp1, datalog.NewKeyword(":person/manager"), mgr)
						tx.Commit()
					}

					// emp2 also reports to manager
					tx := db.NewTransaction()
					tx.Set(emp2, datalog.NewKeyword(":person/manager"), manager)
					tx.Commit()

					// Pattern: A and V bound - "who has manager-1 as :person/manager?"
					// This uses AVET index
					results, err := executor.CollectTuples(db.Query(
						`[:find ?e :in $ ?mgr :where [?e :person/manager ?mgr]]`,
						manager))
					require.NoError(t, err)

					// Should return 2 results: emp1 and emp2
					assert.Len(t, results, 2, "[%s] A+V bound should return CRDT-resolved results", mode.name)

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestCacheMatrix_EFromCollection_AFromScalar tests E from collection, A from scalar
func TestCacheMatrix_EFromCollection_AFromScalar(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/name"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					nameAttr := datalog.NewKeyword(":person/name")

					// Create 3 entities, each with multiple historical values
					entities := []datalog.Identity{
						datalog.NewIdentity("person-1"),
						datalog.NewIdentity("person-2"),
						datalog.NewIdentity("person-3"),
					}

					for i, entity := range entities {
						for _, suffix := range []string{"First", "Second", "Final"} {
							tx := db.NewTransaction()
							tx.Set(entity, nameAttr, fmt.Sprintf("Person%d-%s", i+1, suffix))
							tx.Commit()
						}
					}

					// Pattern: E from collection, A from scalar
					results, err := executor.CollectTuples(db.Query(
						`[:find ?e ?v :in $ [?e ...] ?a :where [?e ?a ?v]]`,
						entities, nameAttr))
					require.NoError(t, err)

					// Should return 3 results (one per entity, CRDT resolved)
					// NOT 9 results (all historical values)
					assert.Len(t, results, 3, "[%s] E from collection should return 1 result per entity", mode.name)

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestCacheMatrix_CardinalityMany tests CardinalityMany add-wins resolution
func TestCacheMatrix_CardinalityMany(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/tags"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityMany,
					})
					db.SetSchema(s)

					personID := datalog.NewIdentity("person-1")
					tagsAttr := datalog.NewKeyword(":person/tags")

					// Set tags multiple times - each Set replaces the entire set
					tagSets := [][]string{
						{"red", "green", "blue"},
						{"alpha", "beta"},
						{"one", "two", "three"},
					}
					for _, tags := range tagSets {
						tx := db.NewTransaction()
						tagValues := make([]interface{}, len(tags))
						for i, tag := range tags {
							tagValues[i] = tag
						}
						tx.Set(personID, tagsAttr, tagValues)
						tx.Commit()
					}

					expectedCount := 3 // Last set has 3 tags

					// Query with A from scalar input
					results, err := executor.CollectTuples(db.Query(
						`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
						personID, tagsAttr))
					require.NoError(t, err)

					// Should return 3 results (current set members)
					// NOT 8 results (all historical tags)
					assert.Len(t, results, expectedCount, "[%s] CardinalityMany should return current set only", mode.name)

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestCacheMatrix_PullIntoComparison verifies PullInto works in both modes
func TestCacheMatrix_PullIntoComparison(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/name"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					personID := datalog.NewIdentity("person-1")

					for _, name := range []string{"Alice", "Bob", "Charlie"} {
						tx := db.NewTransaction()
						tx.Set(personID, datalog.NewKeyword(":person/name"), name)
						tx.Commit()
					}

					// PullInto should always work
					type Person struct {
						ID   datalog.Identity `datalog:"-,id"`
						Name string           `datalog:"person/name"`
					}
					var person Person
					err := db.PullInto(personID, &person)
					require.NoError(t, err)
					assert.Equal(t, "Charlie", person.Name, "[%s] PullInto should return LWW winner", mode.name)
				})
			}
		})
	}
}

// =============================================================================
// Test Matrix: Additional binding patterns
// =============================================================================

// TestCacheMatrix_AFromCollection tests when A comes from a collection input
func TestCacheMatrix_AFromCollection(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/name"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/age"),
						ValueType:   schema.TypeLong,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					personID := datalog.NewIdentity("person-1")

					// Write multiple values to name
					for _, name := range []string{"Alice", "Bob", "Charlie"} {
						tx := db.NewTransaction()
						tx.Set(personID, datalog.NewKeyword(":person/name"), name)
						tx.Commit()
					}

					// Write multiple values to age
					for _, age := range []int64{20, 25, 30} {
						tx := db.NewTransaction()
						tx.Set(personID, datalog.NewKeyword(":person/age"), age)
						tx.Commit()
					}

					// Pattern: A from collection input - query both attributes
					attrs := []datalog.Keyword{
						datalog.NewKeyword(":person/name"),
						datalog.NewKeyword(":person/age"),
					}
					results, err := executor.CollectTuples(db.Query(
						`[:find ?a ?v :in $ ?e [?a ...] :where [?e ?a ?v]]`,
						personID, attrs))
					require.NoError(t, err)

					// Should return 2 results: one for name (Charlie), one for age (30)
					// NOT 6 results (all historical values)
					assert.Len(t, results, 2, "[%s] A from collection should return 1 result per attribute", mode.name)

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestCacheMatrix_AFromTupleInput tests when E and A come from a tuple input
func TestCacheMatrix_AFromTupleInput(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/name"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					personID := datalog.NewIdentity("person-1")
					nameAttr := datalog.NewKeyword(":person/name")

					for _, name := range []string{"Alice", "Bob", "Charlie"} {
						tx := db.NewTransaction()
						tx.Set(personID, nameAttr, name)
						tx.Commit()
					}

					// Pattern: E and A from tuple input [[?e ?a]]
					// Tuple input syntax requires double brackets
					results, err := executor.CollectTuples(db.Query(
						`[:find ?v :in $ [[?e ?a]] :where [?e ?a ?v]]`,
						[]any{personID, nameAttr}))
					require.NoError(t, err)

					assert.Len(t, results, 1, "[%s] Tuple input should return 1 result", mode.name)
					if len(results) == 1 {
						assert.Equal(t, "Charlie", results[0][0], "[%s] Should return LWW winner", mode.name)
					}

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestCacheMatrix_AFromRelationInput tests when E and A come from a relation input
func TestCacheMatrix_AFromRelationInput(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/name"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/age"),
						ValueType:   schema.TypeLong,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					person1 := datalog.NewIdentity("person-1")
					person2 := datalog.NewIdentity("person-2")
					nameAttr := datalog.NewKeyword(":person/name")
					ageAttr := datalog.NewKeyword(":person/age")

					// Write multiple values for person1's name
					for _, name := range []string{"Alice", "Bob", "Charlie"} {
						tx := db.NewTransaction()
						tx.Set(person1, nameAttr, name)
						tx.Commit()
					}

					// Write multiple values for person2's age
					for _, age := range []int64{20, 25, 30} {
						tx := db.NewTransaction()
						tx.Set(person2, ageAttr, age)
						tx.Commit()
					}

					// Pattern: E and A from relation input [[?e ?a] ...]
					// Relation input uses [[vars] ...] syntax with slice of slices
					// Note: Keywords need to be converted to strings for the input
					relationInput := [][]any{
						{person1, nameAttr},
						{person2, ageAttr},
					}
					results, err := executor.CollectTuples(db.Query(
						`[:find ?e ?a ?v :in $ [[?e ?a] ...] :where [?e ?a ?v]]`,
						relationInput))
					require.NoError(t, err)

					// Should return 2 results: person1's name (Charlie), person2's age (30)
					// NOT 6 results (all historical values)
					assert.Len(t, results, 2, "[%s] Relation input should return 1 result per (E,A) pair", mode.name)

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestCacheMatrix_ABoundViaJoin tests when A is bound via join from another pattern
func TestCacheMatrix_ABoundViaJoin(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/name"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":config/tracked-attr"),
						ValueType:   schema.TypeKeyword,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					personID := datalog.NewIdentity("person-1")
					configID := datalog.NewIdentity("config-1")
					nameAttr := datalog.NewKeyword(":person/name")

					// Write multiple values for person's name
					for _, name := range []string{"Alice", "Bob", "Charlie"} {
						tx := db.NewTransaction()
						tx.Set(personID, nameAttr, name)
						tx.Commit()
					}

					// Config entity stores which attribute to track
					tx := db.NewTransaction()
					tx.Set(configID, datalog.NewKeyword(":config/tracked-attr"), nameAttr)
					tx.Commit()

					// Pattern: A bound via join - get attribute from config, then use it
					results, err := executor.CollectTuples(db.Query(
						`[:find ?v :in $ ?person ?config :where
				  [?config :config/tracked-attr ?a]
				  [?person ?a ?v]]`,
						personID, configID))
					require.NoError(t, err)

					// Should return 1 result (Charlie) - CRDT resolved
					assert.Len(t, results, 1, "[%s] A bound via join should return 1 result", mode.name)
					if len(results) == 1 {
						assert.Equal(t, "Charlie", results[0][0], "[%s] Should return LWW winner", mode.name)
					}

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestCacheMatrix_EAndABothFromCollections tests E and A both from collections
func TestCacheMatrix_EAndABothFromCollections(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/name"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/age"),
						ValueType:   schema.TypeLong,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					entities := []datalog.Identity{
						datalog.NewIdentity("person-1"),
						datalog.NewIdentity("person-2"),
					}
					attrs := []datalog.Keyword{
						datalog.NewKeyword(":person/name"),
						datalog.NewKeyword(":person/age"),
					}

					// Write multiple values for each entity/attribute combination
					for i, entity := range entities {
						for _, name := range []string{"First", "Second", "Final"} {
							tx := db.NewTransaction()
							tx.Set(entity, attrs[0], fmt.Sprintf("Person%d-%s", i+1, name))
							tx.Commit()
						}
						for _, age := range []int64{20, 25, 30} {
							tx := db.NewTransaction()
							tx.Set(entity, attrs[1], age+int64(i*10))
							tx.Commit()
						}
					}

					// Pattern: Both E and A from collections
					results, err := executor.CollectTuples(db.Query(
						`[:find ?e ?a ?v :in $ [?e ...] [?a ...] :where [?e ?a ?v]]`,
						entities, attrs))
					require.NoError(t, err)

					// Should return 4 results: 2 entities × 2 attributes
					// NOT 12 results (all historical values)
					assert.Len(t, results, 4, "[%s] Both collections should return 1 result per (E,A) combination", mode.name)

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestCacheMatrix_WithNotClause tests CRDT resolution with NOT clause
func TestCacheMatrix_WithNotClause(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/name"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/active"),
						ValueType:   schema.TypeBoolean,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					person1 := datalog.NewIdentity("person-1")
					person2 := datalog.NewIdentity("person-2")
					nameAttr := datalog.NewKeyword(":person/name")
					activeAttr := datalog.NewKeyword(":person/active")

					// person1: name changes, ends up active
					for _, name := range []string{"Alice", "Bob", "Charlie"} {
						tx := db.NewTransaction()
						tx.Set(person1, nameAttr, name)
						tx.Commit()
					}
					tx := db.NewTransaction()
					tx.Set(person1, activeAttr, true)
					tx.Commit()

					// person2: name changes, ends up inactive
					for _, name := range []string{"Dave", "Eve", "Frank"} {
						tx := db.NewTransaction()
						tx.Set(person2, nameAttr, name)
						tx.Commit()
					}
					tx = db.NewTransaction()
					tx.Set(person2, activeAttr, false)
					tx.Commit()

					// Query: find names of active people (using NOT to exclude inactive)
					results, err := executor.CollectTuples(db.Query(
						`[:find ?name :where
				  [?e :person/name ?name]
				  [?e :person/active true]
				  (not [?e :person/active false])]`,
					))
					require.NoError(t, err)

					// Should return 1 result (Charlie) - only active person's current name
					assert.Len(t, results, 1, "[%s] NOT clause should work with CRDT resolved values", mode.name)
					if len(results) == 1 {
						assert.Equal(t, "Charlie", results[0][0], "[%s] Should return active person's LWW name", mode.name)
					}

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestCacheMatrix_WithOrClause tests CRDT resolution with OR clause
func TestCacheMatrix_WithOrClause(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/name"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/status"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					person1 := datalog.NewIdentity("person-1")
					person2 := datalog.NewIdentity("person-2")
					person3 := datalog.NewIdentity("person-3")
					nameAttr := datalog.NewKeyword(":person/name")
					statusAttr := datalog.NewKeyword(":person/status")

					// Setup: 3 people with changing names, different final statuses
					for i, person := range []datalog.Identity{person1, person2, person3} {
						for _, name := range []string{"First", "Second", "Final"} {
							tx := db.NewTransaction()
							tx.Set(person, nameAttr, fmt.Sprintf("Person%d-%s", i+1, name))
							tx.Commit()
						}
					}

					// Final statuses: person1=active, person2=pending, person3=inactive
					statuses := []string{"active", "pending", "inactive"}
					for i, person := range []datalog.Identity{person1, person2, person3} {
						tx := db.NewTransaction()
						tx.Set(person, statusAttr, statuses[i])
						tx.Commit()
					}

					// Query: find names where status is "active" OR "pending"
					results, err := executor.CollectTuples(db.Query(
						`[:find ?name :where
				  [?e :person/name ?name]
				  (or [?e :person/status "active"]
				      [?e :person/status "pending"])]`,
					))
					require.NoError(t, err)

					// Should return 2 results (Person1-Final, Person2-Final)
					// NOT 6 results (all historical names of those people)
					assert.Len(t, results, 2, "[%s] OR clause should return CRDT resolved names", mode.name)

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestCacheMatrix_WithAggregation tests CRDT resolution with aggregation
func TestCacheMatrix_WithAggregation(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/score"),
						ValueType:   schema.TypeLong,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					// Create 3 people, each with multiple historical scores
					for i := 1; i <= 3; i++ {
						personID := datalog.NewIdentity(fmt.Sprintf("person-%d", i))
						for _, score := range []int64{10, 20, 30} {
							tx := db.NewTransaction()
							tx.Set(personID, datalog.NewKeyword(":person/score"), score*int64(i))
							tx.Commit()
						}
					}

					// Query: sum of all scores
					results, err := executor.CollectTuples(db.Query(
						`[:find (sum ?score) :where [?e :person/score ?score]]`,
					))
					require.NoError(t, err)

					// Should sum only the CURRENT scores: 30 + 60 + 90 = 180
					// NOT all historical: (10+20+30) + (20+40+60) + (30+60+90) = 360
					assert.Len(t, results, 1, "[%s] Aggregation should have 1 result", mode.name)
					if len(results) == 1 {
						sum, ok := results[0][0].(int64)
						if !ok {
							// Try float64 (some aggregations return float)
							sumFloat, _ := results[0][0].(float64)
							sum = int64(sumFloat)
						}
						assert.Equal(t, int64(180), sum, "[%s] Sum should be of CRDT resolved values only", mode.name)
					}

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestCacheMatrix_CardinalityVector tests CardinalityVector RGA resolution
func TestCacheMatrix_CardinalityVector(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":doc/content"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityVector,
					})
					db.SetSchema(s)

					docID := datalog.NewIdentity("doc-1")
					contentAttr := datalog.NewKeyword(":doc/content")

					// Set vector content multiple times
					vectors := [][]string{
						{"line1", "line2"},
						{"a", "b", "c"},
						{"final1", "final2", "final3", "final4"},
					}
					for _, vec := range vectors {
						tx := db.NewTransaction()
						vals := make([]interface{}, len(vec))
						for i, v := range vec {
							vals[i] = v
						}
						tx.Set(docID, contentAttr, vals)
						tx.Commit()
					}

					// Query with A from scalar input
					results, err := executor.CollectTuples(db.Query(
						`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
						docID, contentAttr))
					require.NoError(t, err)

					// Should return 1 result (the resolved vector as a single value)
					// NOT 4 individual elements or 9 historical elements
					assert.Len(t, results, 1, "[%s] CardinalityVector should return resolved vector as single value", mode.name)

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestCacheMatrix_NeverSetVectorAgainstEmptyVectorLiteral pins that a
// cardinality-vector attribute an entity never had answers a V-bound-to-empty
// pattern the same way with the cache on and off.
//
// The two arms disagree on what an absent vector is. The streaming arm
// (matchCardinalityVectorAsRelation) reads Stats.TotalElements and returns no
// tuple when the (E, A) has no datoms at all, before it ever compares V — a
// never-set attribute is absent, and absent is not the empty vector. The cache
// arm has no such reading available: CacheEntry keeps only vectorList, so
// "never written" and "written and wholly tombstoned" are one state to it, and
// its V-bound branch compares an empty resolved vector against the pattern's
// and matches.
//
// So the same query returns one tuple through the cache and none without it,
// which contradicts this file's premise that the cache is an optimization and
// not a correctness input.
//
// Driven through the matcher rather than db.Query: the V position needs an
// empty-vector constant, and going through the parser would make the case
// depend on how it renders `[]` in that position — a separate question from
// whether the two arms agree once they have one.
//
// See BUG_CACHE_EMPTY_VECTOR_NEVER_SET.
func TestCacheMatrix_NeverSetVectorAgainstEmptyVectorLiteral(t *testing.T) {
	// The optimizer axis is outside the cache axis: the two cache arms are
	// compared against each other, so both must come from the same backend.
	for _, omode := range optimizerModes {
		t.Run(omode.name, func(t *testing.T) {
			tuplesPerMode := map[string]int{}

			for _, mode := range cacheTestModes {
				t.Run(mode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					skill := datalog.NewKeyword(":person/skill")
					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       skill,
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityVector,
					})
					db.SetSchema(s)

					// One entity, written under a different attribute, so the entity
					// exists and :person/skill genuinely never was.
					who := datalog.NewIdentity("person:alice")
					tx := db.NewTransaction()
					require.NoError(t, tx.Set(who, datalog.NewKeyword(":person/name"), "Alice"))
					_, err := tx.Commit()
					require.NoError(t, err)

					pattern := &query.DataPattern{
						Elements: []query.PatternElement{
							query.Constant{Value: who},
							query.Constant{Value: skill},
							query.Constant{Value: []interface{}{}},
						},
					}

					m := db.Matcher().(*PatternMatcher)
					rel, err := m.matchUnboundAsRelation(nil, pattern, nil, nil)
					require.NoError(t, err)
					tuplesPerMode[mode.name] = drainRelation(t, rel)
				})
			}

			require.Equal(t, tuplesPerMode["cache_disabled"], tuplesPerMode["cache_enabled"],
				"the cache is an optimization: a never-set vector attribute cannot match an "+
					"empty-vector literal through one path and not the other (disabled=%d, enabled=%d)",
				tuplesPerMode["cache_disabled"], tuplesPerMode["cache_enabled"])
		})
	}
}

// TestCacheMatrix_ABoundViaSubquery tests when A is bound via a subquery result
// NOTE: This test uses a simpler pattern - the subquery returns the attribute keyword,
// which is then used in the main pattern.
func TestCacheMatrix_ABoundViaSubquery(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/name"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":config/attr"),
						ValueType:   schema.TypeKeyword,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					personID := datalog.NewIdentity("person-1")
					configID := datalog.NewIdentity("config-1")
					nameAttr := datalog.NewKeyword(":person/name")

					// Write multiple values for person's name
					for _, name := range []string{"Alice", "Bob", "Charlie"} {
						tx := db.NewTransaction()
						tx.Set(personID, nameAttr, name)
						tx.Commit()
					}

					// Config stores which attribute to query
					tx := db.NewTransaction()
					tx.Set(configID, datalog.NewKeyword(":config/attr"), nameAttr)
					tx.Commit()

					// First verify the subquery works alone
					subResults, err := executor.CollectTuples(db.Query(
						`[:find ?a :in $ ?c :where [?c :config/attr ?a]]`,
						configID))
					require.NoError(t, err)
					t.Logf("[%s] Subquery results: %v", mode.name, subResults)

					// Pattern: A bound via subquery using scalar binding
					// The subquery returns a single attribute value
					results, err := executor.CollectTuples(db.Query(
						`[:find ?v :in $ ?person ?config :where
				  [(q [:find ?a . :in $ ?c :where [?c :config/attr ?a]] $ ?config) ?a]
				  [?person ?a ?v]]`,
						personID, configID))
					require.NoError(t, err)

					// Should return 1 result (Charlie) - CRDT resolved
					assert.Len(t, results, 1, "[%s] A bound via subquery should return 1 result", mode.name)
					if len(results) == 1 {
						assert.Equal(t, "Charlie", results[0][0], "[%s] Should return LWW winner", mode.name)
					}

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}

// TestCacheMatrix_AsOfQuery tests [(as-of ?tx N)] for point-in-time queries
func TestCacheMatrix_AsOfQuery(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/name"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					personID := datalog.NewIdentity("person-1")
					nameAttr := datalog.NewKeyword(":person/name")

					// Write values, capturing Alice's transaction (the first write)
					// directly from Commit(). A latest query cannot be used to discover a
					// historical transaction: CRDT resolution correctly returns only the
					// current winner for a cardinality-one attribute.
					var aliceTx datalog.ElementID
					for i, name := range []string{"Alice", "Bob", "Charlie"} {
						tx := db.NewTransaction()
						require.NoError(t, tx.Set(personID, nameAttr, name))
						txID, err := tx.Commit()
						require.NoError(t, err)
						if i == 0 {
							aliceTx = txID
						}
					}

					// As-of Alice's transaction, the resolved value must be exactly "Alice".
					// Uses the d.AsOf(elementID) view (the [(as-of ...)] query predicate was
					// removed). This must hold with the cache ENABLED and DISABLED.
					results, err := executor.CollectTuples(db.AsOf(aliceTx).Query(
						`[:find ?v :in $ ?e :where [?e :person/name ?v]]`,
						personID))
					require.NoError(t, err)
					require.Len(t, results, 1, "[%s] as-of Alice's tx should return 1 result", mode.name)
					assert.Equal(t, "Alice", results[0][0], "[%s] as-of Alice's tx should return Alice", mode.name)
				})
			}
		})
	}
}

// TestCacheMatrix_SchemaAfterWrite tests CRDT resolution when data was written
// BEFORE schema was set (schemaless mode). This simulates legacy data migrations
// and the entity browser pattern for older databases.
func TestCacheMatrix_SchemaAfterWrite(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					personID := datalog.NewIdentity("person-1")
					nameAttr := datalog.NewKeyword(":person/name")
					ageAttr := datalog.NewKeyword(":person/age")

					// Write data WITHOUT schema - multiple values to same (E, A)
					// This simulates legacy data written before schema existed
					for _, name := range []string{"Alice", "Bob", "Charlie"} {
						tx := db.NewTransaction()
						tx.Add(personID, nameAttr, name)
						tx.Commit()
					}
					for _, age := range []int64{20, 25, 30} {
						tx := db.NewTransaction()
						tx.Add(personID, ageAttr, age)
						tx.Commit()
					}

					// NOW set schema - read path should apply CRDT resolution
					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       nameAttr,
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					s.Add(&schema.AttributeDefinition{
						Ident:       ageAttr,
						ValueType:   schema.TypeLong,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					// Pattern: E bound, A unbound - entity browser pattern
					results, err := executor.CollectTuples(db.Query(
						`[:find ?a ?v :in $ ?e :where [?e ?a ?v]]`,
						personID))
					require.NoError(t, err)

					// Should return 2 results: one for name (Charlie), one for age (30)
					// NOT 6 results (all historical values)
					assert.Len(t, results, 2, "[%s] Schema-after-write should still apply CRDT resolution", mode.name)

					t.Logf("[%s] Results: %v", mode.name, results)

					// Also test with wildcard pull - same scenario (only when cache is enabled)
					// Wildcard pull requires cache to enumerate attributes
					if !mode.disableCache {
						pullResult, err := db.Pull(personID, "[*]")
						require.NoError(t, err)

						// Pull should also return only current values
						assert.Equal(t, "Charlie", pullResult["person/name"], "[%s] Pull should return LWW name", mode.name)
						assert.Equal(t, int64(30), pullResult["person/age"], "[%s] Pull should return LWW age", mode.name)
					}
				})
			}
		})
	}
}

// TestCacheMatrix_AllUnbound tests CRDT resolution when E, A, V are all unbound.
// This goes through matchUnboundAsRelation - a different code path from join strategies.
func TestCacheMatrix_AllUnbound(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					db := createCacheTestDB(t, omode, mode.disableCache, nil)

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/name"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":person/age"),
						ValueType:   schema.TypeLong,
						Cardinality: schema.CardinalityOne,
					})
					db.SetSchema(s)

					// Create two entities with multiple historical values each
					person1 := datalog.NewIdentity("person-1")
					person2 := datalog.NewIdentity("person-2")

					// person1: multiple name and age updates
					for _, name := range []string{"Alice", "Bob", "Charlie"} {
						tx := db.NewTransaction()
						tx.Set(person1, datalog.NewKeyword(":person/name"), name)
						tx.Commit()
					}
					for _, age := range []int64{20, 25, 30} {
						tx := db.NewTransaction()
						tx.Set(person1, datalog.NewKeyword(":person/age"), age)
						tx.Commit()
					}

					// person2: multiple name and age updates
					for _, name := range []string{"Dave", "Eve", "Frank"} {
						tx := db.NewTransaction()
						tx.Set(person2, datalog.NewKeyword(":person/name"), name)
						tx.Commit()
					}
					for _, age := range []int64{40, 45, 50} {
						tx := db.NewTransaction()
						tx.Set(person2, datalog.NewKeyword(":person/age"), age)
						tx.Commit()
					}

					// Pattern: All unbound - scans entire database
					// This goes through matchUnboundAsRelation
					results, err := executor.CollectTuples(db.Query(`[:find ?e ?a ?v :where [?e ?a ?v]]`))
					require.NoError(t, err)

					// Count only the two person entities (the scan also returns tx:*
					// system entities' :db/txInstant datoms). Identities render as L85
					// hashes, so filter by identity equality, not by seed-string prefix.
					personResults := 0
					for _, r := range results {
						if e, ok := r[0].(datalog.Identity); ok {
							if e.Equal(person1) || e.Equal(person2) {
								personResults++
							}
						}
					}

					// Should return 4 person results: 2 entities × 2 attributes
					// NOT 12 results (all historical values for persons)
					assert.Equal(t, 4, personResults, "[%s] Person entities should have CRDT-resolved results", mode.name)

					t.Logf("[%s] Results: %v", mode.name, results)
				})
			}
		})
	}
}
