package storage

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// extractElementID dereferences *ElementID to ElementID for test assertions.
func extractElementID(t *testing.T, v interface{}) datalog.ElementID {
	t.Helper()
	switch e := v.(type) {
	case *datalog.ElementID:
		return *e
	case datalog.ElementID:
		return e
	default:
		t.Fatalf("expected ElementID or *ElementID, got %T", v)
		return datalog.ElementID{}
	}
}

// TestElementIDBinding_ScalarInput passes an ElementID value as a scalar
// binding (:in $ ?tx) and expects the query to filter datoms to only
// those matching that transaction.
func TestElementIDBinding_ScalarInput(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			nameAttr := datalog.NewKeyword(":person/name")

			// Write two entities in separate transactions
			alice := datalog.NewIdentity("alice")
			tx1 := db.NewTransaction()
			tx1.Set(alice, nameAttr, "Alice")
			_, err := tx1.Commit()
			require.NoError(t, err)

			bob := datalog.NewIdentity("bob")
			tx2 := db.NewTransaction()
			tx2.Set(bob, nameAttr, "Bob")
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Step 1: Query all (entity, value, tx) to discover the ElementIDs
			allResults, err := executor.CollectTuples(db.Query(
				`[:find ?e ?v ?tx :where [?e :person/name ?v ?tx]]`))
			require.NoError(t, err)
			t.Logf("All results: %d", len(allResults))
			require.Len(t, allResults, 2, "Should have 2 datoms (one per entity)")

			// Find Alice's transaction ElementID
			var aliceTx interface{}
			for _, r := range allResults {
				t.Logf("  e=%v v=%v tx=%v (txType=%T)", r[0], r[1], r[2], r[2])
				if r[1] == "Alice" {
					aliceTx = r[2]
				}
			}
			require.NotNil(t, aliceTx, "Should find Alice's tx")
			t.Logf("Alice's tx: %v (type %T)", aliceTx, aliceTx)

			// Dereference pointer if needed
			switch v := aliceTx.(type) {
			case *datalog.ElementID:
				aliceTx = *v
			case datalog.ElementID:
				// already a value
			default:
				t.Fatalf("Unexpected tx type: %T", aliceTx)
			}

			// Step 2: Use Alice's ElementID as a scalar binding to filter by tx.
			// This should return only Alice's datom — not Bob's.
			filteredResults, err := executor.CollectTuples(db.Query(
				`[:find ?e ?v :in $ ?tx :where [?e :person/name ?v ?tx]]`,
				aliceTx))
			require.NoError(t, err)

			t.Logf("Filtered results (tx=%v): %v", aliceTx, filteredResults)
			require.Len(t, filteredResults, 1, "Should return exactly 1 result for Alice's tx")
			assert.Equal(t, "Alice", filteredResults[0][1], "Should return Alice for her specific tx")
		})
	}
}

// TestElementIDBinding_CollectionInput passes a collection of ElementID values
// as [:in $ [?tx ...]] and expects matching datoms.
func TestElementIDBinding_CollectionInput(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			nameAttr := datalog.NewKeyword(":person/name")

			// Write three entities in separate transactions
			alice := datalog.NewIdentity("alice")
			tx1 := db.NewTransaction()
			tx1.Set(alice, nameAttr, "Alice")
			_, err := tx1.Commit()
			require.NoError(t, err)

			bob := datalog.NewIdentity("bob")
			tx2 := db.NewTransaction()
			tx2.Set(bob, nameAttr, "Bob")
			_, err = tx2.Commit()
			require.NoError(t, err)

			charlie := datalog.NewIdentity("charlie")
			tx3 := db.NewTransaction()
			tx3.Set(charlie, nameAttr, "Charlie")
			_, err = tx3.Commit()
			require.NoError(t, err)

			// Discover all ElementIDs
			allResults, err := executor.CollectTuples(db.Query(
				`[:find ?v ?tx :where [?e :person/name ?v ?tx]]`))
			require.NoError(t, err)
			require.Len(t, allResults, 3)

			txByName := make(map[string]datalog.ElementID)
			for _, r := range allResults {
				name := r[0].(string)
				txByName[name] = extractElementID(t, r[1])
			}

			// Pass Alice's and Bob's tx as a collection — should return 2 results
			filteredResults, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ [?tx ...] :where [?e :person/name ?v ?tx]]`,
				[]datalog.ElementID{txByName["Alice"], txByName["Bob"]}))
			require.NoError(t, err)

			t.Logf("Collection results: %v", filteredResults)
			require.Len(t, filteredResults, 2, "Should return 2 results for Alice+Bob txs")

			names := []string{filteredResults[0][0].(string), filteredResults[1][0].(string)}
			sort.Strings(names)
			assert.Equal(t, []string{"Alice", "Bob"}, names)
		})
	}
}

// TestElementIDBinding_RelationInput passes ElementID + Identity pairs as a
// relation binding [:in $ [[?e ?tx] ...]].
func TestElementIDBinding_RelationInput(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			nameAttr := datalog.NewKeyword(":person/name")

			alice := datalog.NewIdentity("alice")
			tx1 := db.NewTransaction()
			tx1.Set(alice, nameAttr, "Alice")
			_, err := tx1.Commit()
			require.NoError(t, err)

			bob := datalog.NewIdentity("bob")
			tx2 := db.NewTransaction()
			tx2.Set(bob, nameAttr, "Bob")
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Discover ElementIDs
			allResults, err := executor.CollectTuples(db.Query(
				`[:find ?e ?v ?tx :where [?e :person/name ?v ?tx]]`))
			require.NoError(t, err)
			require.Len(t, allResults, 2)

			var aliceIdentity, bobIdentity interface{}
			var aliceTx, bobTx datalog.ElementID
			for _, r := range allResults {
				if r[1] == "Alice" {
					aliceIdentity = r[0]
					aliceTx = extractElementID(t, r[2])
				} else {
					bobIdentity = r[0]
					bobTx = extractElementID(t, r[2])
				}
			}

			// Pass as relation binding [[?e ?tx] ...]
			results, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ [[?e ?tx] ...] :where [?e :person/name ?v ?tx]]`,
				[][]interface{}{
					{aliceIdentity, aliceTx},
					{bobIdentity, bobTx},
				}))
			require.NoError(t, err)

			t.Logf("Relation results: %v", results)
			require.Len(t, results, 2, "Should return 2 results from relation binding")

			names := []string{results[0][0].(string), results[1][0].(string)}
			sort.Strings(names)
			assert.Equal(t, []string{"Alice", "Bob"}, names)
		})
	}
}

// TestElementIDBinding_TxJoin verifies that two patterns joined on ?e correctly
// return *ElementID Tx values as distinct symbols. In CRDT storage, each Set()
// gets a unique ElementID, so ?tx1 != ?tx2 even for the same entity.
func TestElementIDBinding_TxJoin(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

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

			nameAttr := datalog.NewKeyword(":person/name")
			ageAttr := datalog.NewKeyword(":person/age")

			// Transaction 1: Alice + age 30
			alice := datalog.NewIdentity("alice")
			tx1 := db.NewTransaction()
			tx1.Set(alice, nameAttr, "Alice")
			tx1.Set(alice, ageAttr, int64(30))
			_, err := tx1.Commit()
			require.NoError(t, err)

			// Transaction 2: Bob + age 25
			bob := datalog.NewIdentity("bob")
			tx2 := db.NewTransaction()
			tx2.Set(bob, nameAttr, "Bob")
			tx2.Set(bob, ageAttr, int64(25))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Join on ?e: find name/age pairs for the same entity.
			// Each attribute's datom has a unique Tx (CRDT per-operation Lamport),
			// so we use separate ?tx1, ?tx2 variables.
			results, err := executor.CollectTuples(db.Query(
				`[:find ?name ?age ?tx1 ?tx2
				  :where [?e :person/name ?name ?tx1]
				         [?e :person/age ?age ?tx2]]`))
			require.NoError(t, err)

			t.Logf("TxJoin results: %v", results)
			require.Len(t, results, 2, "Should return 2 name/age pairs joined on entity")

			// Verify results contain correct name/age pairs and that Tx symbols are ElementIDs
			for _, r := range results {
				name := r[0].(string)
				age := r[1].(int64)
				tx1Val := extractElementID(t, r[2])
				tx2Val := extractElementID(t, r[3])

				t.Logf("  name=%s age=%d tx1=%v tx2=%v", name, age, tx1Val, tx2Val)

				// Each attribute has its own unique Tx
				assert.NotEqual(t, tx1Val, tx2Val, "name and age should have different Tx values")

				if name == "Alice" {
					assert.Equal(t, int64(30), age)
				} else {
					assert.Equal(t, "Bob", name)
					assert.Equal(t, int64(25), age)
				}
			}
		})
	}
}

// TestElementIDBinding_ComparisonPredicate uses [(= ?tx ?target-tx)] to filter
// by a bound ElementID via predicate comparison.
func TestElementIDBinding_ComparisonPredicate(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			nameAttr := datalog.NewKeyword(":person/name")

			alice := datalog.NewIdentity("alice")
			tx1 := db.NewTransaction()
			tx1.Set(alice, nameAttr, "Alice")
			_, err := tx1.Commit()
			require.NoError(t, err)

			bob := datalog.NewIdentity("bob")
			tx2 := db.NewTransaction()
			tx2.Set(bob, nameAttr, "Bob")
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Get Alice's tx
			allResults, err := executor.CollectTuples(db.Query(
				`[:find ?v ?tx :where [?e :person/name ?v ?tx]]`))
			require.NoError(t, err)
			require.Len(t, allResults, 2)

			var aliceTx datalog.ElementID
			for _, r := range allResults {
				if r[0] == "Alice" {
					aliceTx = extractElementID(t, r[1])
				}
			}

			// Filter with equality predicate
			results, err := executor.CollectTuples(db.Query(
				`[:find ?v
				  :in $ ?target-tx
				  :where [?e :person/name ?v ?tx]
				         [(= ?tx ?target-tx)]]`,
				aliceTx))
			require.NoError(t, err)

			t.Logf("Comparison predicate results: %v", results)
			require.Len(t, results, 1, "Should return exactly 1 result with equality predicate")
			assert.Equal(t, "Alice", results[0][0])
		})
	}
}

// TestElementIDBinding_MultipleEntitiesSameTx verifies that each Set() gets a
// unique ElementID (CRDT per-operation Lamport), and that filtering by a specific
// Tx returns only the matching datom. Even within the same Commit(), Alice and
// Bob get distinct Tx values.
func TestElementIDBinding_MultipleEntitiesSameTx(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			nameAttr := datalog.NewKeyword(":person/name")

			// Write BOTH entities in the same Commit — each Set() still gets unique Tx
			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")
			tx1 := db.NewTransaction()
			tx1.Set(alice, nameAttr, "Alice")
			tx1.Set(bob, nameAttr, "Bob")
			_, err := tx1.Commit()
			require.NoError(t, err)

			// Write a third entity in a different Commit
			charlie := datalog.NewIdentity("charlie")
			tx2 := db.NewTransaction()
			tx2.Set(charlie, nameAttr, "Charlie")
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Discover all Tx values — each entity has its own unique Tx
			allResults, err := executor.CollectTuples(db.Query(
				`[:find ?v ?tx :where [?e :person/name ?v ?tx]]`))
			require.NoError(t, err)
			require.Len(t, allResults, 3)

			txByName := make(map[string]datalog.ElementID)
			for _, r := range allResults {
				name := r[0].(string)
				txByName[name] = extractElementID(t, r[1])
			}

			// Each Set() gets unique Tx, even within the same Commit
			assert.NotEqual(t, txByName["Alice"], txByName["Bob"],
				"Alice and Bob should have different Tx values (per-operation Lamport)")
			assert.NotEqual(t, txByName["Bob"], txByName["Charlie"],
				"Bob and Charlie should have different Tx values")

			// Filter by Alice's Tx — should return exactly Alice, not Bob or Charlie
			results, err := executor.CollectTuples(db.Query(
				`[:find ?name :in $ ?tx :where [?e :person/name ?name ?tx]]`,
				txByName["Alice"]))
			require.NoError(t, err)

			t.Logf("Alice-tx results: %v", results)
			require.Len(t, results, 1, "Should return exactly 1 result for Alice's Tx")
			assert.Equal(t, "Alice", results[0][0].(string))

			// Filter by Bob's Tx — should return exactly Bob
			results, err = executor.CollectTuples(db.Query(
				`[:find ?name :in $ ?tx :where [?e :person/name ?name ?tx]]`,
				txByName["Bob"]))
			require.NoError(t, err)

			t.Logf("Bob-tx results: %v", results)
			require.Len(t, results, 1, "Should return exactly 1 result for Bob's Tx")
			assert.Equal(t, "Bob", results[0][0].(string))
		})
	}
}
