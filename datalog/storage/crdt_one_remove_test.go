//go:build !(js && wasm)

package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// =============================================================================
// CardinalityOne Remove Tests
// =============================================================================
//
// These tests verify that tx.Remove() works for CardinalityOne attributes.
// CardinalityOne has one value per (E, A). Remove means "this attribute no
// longer exists." The V parameter to Remove is irrelevant for resolution —
// OpCRDTRemove at highest Tx means the attribute doesn't exist.
//
// Resolution: first entry in EATV (highest Tx) determines everything.
//   - OpNone → emit (current value)
//   - OpCRDTRemove → attribute doesn't exist, skip group
//
// See: docs/bugs/BUG_SCHEMALESS_ATTR_BOUND_QUERY.md
// =============================================================================

// createRemoveTestDB creates a database with a CardinalityOne schema
func createCardinalityOneDB(t *testing.T) (*Database, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "crdt-one-remove-*")
	require.NoError(t, err)

	db, err := NewDatabase(dir)
	require.NoError(t, err)

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/name"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})
	db.SetSchema(s)

	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}
	return db, cleanup
}

// queryBoundValue runs a bound query for a single attribute value
func queryBoundValue(t *testing.T, db *Database, e datalog.Identity, a datalog.Keyword) [][]interface{} {
	t.Helper()
	results, err := executor.CollectTuples(db.Query(
		`[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]`,
		e, a,
	))
	require.NoError(t, err)
	return results
}

// queryUnboundAndFilter runs an unbound query and filters for a specific (E, A)
func queryUnboundForEA(t *testing.T, db *Database, e datalog.Identity, a datalog.Keyword) [][]interface{} {
	t.Helper()
	results, err := executor.CollectTuples(db.Query(
		`[:find ?e ?a ?v :where [?e ?a ?v]]`,
	))
	require.NoError(t, err)

	var filtered [][]interface{}
	for _, tuple := range results {
		if len(tuple) >= 3 {
			if rowE, ok := tuple[0].(datalog.Identity); ok {
				if rowA, ok := tuple[1].(datalog.Keyword); ok {
					if rowE.Hash() == e.Hash() && rowA == a {
						filtered = append(filtered, tuple)
					}
				}
			}
		}
	}
	return filtered
}

// Test 1: Add value, Remove value, query → attribute doesn't exist
func TestCardinalityOneRemove_RoundTrip(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	// Add
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Verify value exists
	results := queryBoundValue(t, db, e, a)
	require.Len(t, results, 1, "value should exist after Add")
	assert.Equal(t, "Alice", results[0][0])

	// Remove
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Bound query → doesn't exist
	results = queryBoundValue(t, db, e, a)
	assert.Len(t, results, 0, "bound query: attribute should not exist after Remove")

	// Unbound query → also doesn't exist
	unboundResults := queryUnboundForEA(t, db, e, a)
	assert.Len(t, unboundResults, 0, "unbound query: attribute should not exist after Remove")
}

// Test 2: Add "Alice", Add "Bob", Remove (any V) → attribute doesn't exist
// V is irrelevant for CardinalityOne remove. The OpCRDTRemove at highest Tx
// means the attribute is gone.
func TestCardinalityOneRemove_AfterOverwrite(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	// Add "Alice"
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Overwrite with "Bob"
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(e, a, "Bob"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Remove — passing "Bob" as V, but V doesn't matter
	tx3 := db.NewTransaction()
	require.NoError(t, tx3.Remove(e, a, "Bob"))
	_, err = tx3.Commit()
	require.NoError(t, err)

	// Attribute doesn't exist
	results := queryBoundValue(t, db, e, a)
	assert.Len(t, results, 0, "attribute should not exist after Remove")
}

// Test 3: Add, Remove, Add again → latest Add wins
func TestCardinalityOneRemove_ThenReAdd(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	// Add
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Remove
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Re-add with different value
	tx3 := db.NewTransaction()
	require.NoError(t, tx3.Add(e, a, "Bob"))
	_, err = tx3.Commit()
	require.NoError(t, err)

	// Latest Add wins
	results := queryBoundValue(t, db, e, a)
	require.Len(t, results, 1, "attribute should exist after re-Add")
	assert.Equal(t, "Bob", results[0][0])
}

// Test 4: Remove before any Add → then Add → value exists
func TestCardinalityOneRemove_BeforeAnyAdd(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	// Remove first (no existing value)
	tx := db.NewTransaction()
	require.NoError(t, tx.Remove(e, a, "phantom"))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Then Add
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Add has higher Tx, wins over pre-existing tombstone
	results := queryBoundValue(t, db, e, a)
	require.Len(t, results, 1, "Add should win over earlier Remove")
	assert.Equal(t, "Alice", results[0][0])
}

// Test 5: V is irrelevant for CardinalityOne remove
// Add "Alice", Remove("Bob") → attribute doesn't exist
// Even though "Bob" was never the value, OpCRDTRemove at highest Tx means gone.
func TestCardinalityOneRemove_VIsIrrelevant(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	// Add "Alice"
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Remove with completely different V — doesn't matter for CardinalityOne
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Bob"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Attribute doesn't exist
	results := queryBoundValue(t, db, e, a)
	assert.Len(t, results, 0,
		"attribute should not exist — V is irrelevant for CardinalityOne Remove")
}

// Test 6: Multiple entities — removing one doesn't affect the other
func TestCardinalityOneRemove_MultipleEntities(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e1 := datalog.NewIdentity("alice")
	e2 := datalog.NewIdentity("bob")
	a := datalog.NewKeyword(":person/name")

	// Add values for both entities
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e1, a, "Alice"))
	require.NoError(t, tx.Add(e2, a, "Bob"))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Remove only entity1's value
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e1, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// entity1: doesn't exist
	results1 := queryBoundValue(t, db, e1, a)
	assert.Len(t, results1, 0, "entity1 attribute should not exist after Remove")

	// entity2: unaffected
	results2 := queryBoundValue(t, db, e2, a)
	require.Len(t, results2, 1, "entity2 attribute should still exist")
	assert.Equal(t, "Bob", results2[0][0])
}

// =============================================================================
// Query Path Coverage After Remove
// =============================================================================

// Test 11: Bound query (E and A via :in) after remove → empty
func TestCardinalityOneRemove_BoundQuery(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	// Add then Remove
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Bound query with E and A from :in
	results, err := executor.CollectTuples(db.Query(
		`[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]`,
		e, a,
	))
	require.NoError(t, err)
	assert.Len(t, results, 0, "bound query should return empty after Remove")
}

// Test 12: V-bound query after remove → empty
func TestCardinalityOneRemove_VBoundQuery(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	// Add then Remove
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// V-bound query: find entities where :person/name = "Alice"
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(db.Schema())

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: a},
			query.Constant{Value: "Alice"},
			query.Blank{},
		},
	}

	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	count := 0
	iter := results.Iterator()
	for iter.Next() {
		count++
	}
	iter.Close()

	assert.Equal(t, 0, count, "V-bound query should return empty after Remove")
}

// Test 13: Unbound query after remove → entity/attribute absent from results
func TestCardinalityOneRemove_UnboundQuery(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	// Add then Remove
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Unbound query — removed attribute should be absent
	unboundResults := queryUnboundForEA(t, db, e, a)
	assert.Len(t, unboundResults, 0, "unbound query should not find removed attribute")
}

// =============================================================================
// P1×S7: Streaming (:in-bound E) with Set() + Remove
// =============================================================================

func TestCardinalityOneRemove_SetThenRemove(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	// Set (not Add)
	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Remove
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	results := queryBoundValue(t, db, e, a)
	assert.Len(t, results, 0, "bound query: attribute should not exist after Set then Remove")
}

// =============================================================================
// P2×S2-S7: Streaming (unbound E) — full scenario coverage
// =============================================================================

func TestCardinalityOneRemove_Unbound_AfterOverwrite(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(e, a, "Bob"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	tx3 := db.NewTransaction()
	require.NoError(t, tx3.Remove(e, a, "Bob"))
	_, err = tx3.Commit()
	require.NoError(t, err)

	results := queryUnboundForEA(t, db, e, a)
	assert.Len(t, results, 0, "unbound: attribute should not exist after overwrite then Remove")
}

func TestCardinalityOneRemove_Unbound_ThenReAdd(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	tx3 := db.NewTransaction()
	require.NoError(t, tx3.Add(e, a, "Bob"))
	_, err = tx3.Commit()
	require.NoError(t, err)

	results := queryUnboundForEA(t, db, e, a)
	require.Len(t, results, 1, "unbound: attribute should exist after re-Add")
	assert.Equal(t, "Bob", results[0][2])
}

func TestCardinalityOneRemove_Unbound_BeforeAnyAdd(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Remove(e, a, "phantom"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	results := queryUnboundForEA(t, db, e, a)
	require.Len(t, results, 1, "unbound: Add should win over earlier Remove")
	assert.Equal(t, "Alice", results[0][2])
}

func TestCardinalityOneRemove_Unbound_VIsIrrelevant(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Bob"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	results := queryUnboundForEA(t, db, e, a)
	assert.Len(t, results, 0, "unbound: V is irrelevant for CardinalityOne Remove")
}

func TestCardinalityOneRemove_Unbound_MultipleEntities(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e1 := datalog.NewIdentity("alice")
	e2 := datalog.NewIdentity("bob")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e1, a, "Alice"))
	require.NoError(t, tx.Add(e2, a, "Bob"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e1, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	results1 := queryUnboundForEA(t, db, e1, a)
	assert.Len(t, results1, 0, "unbound: entity1 should not exist after Remove")

	results2 := queryUnboundForEA(t, db, e2, a)
	require.Len(t, results2, 1, "unbound: entity2 should still exist")
	assert.Equal(t, "Bob", results2[0][2])
}

func TestCardinalityOneRemove_Unbound_SetThenRemove(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	results := queryUnboundForEA(t, db, e, a)
	assert.Len(t, results, 0, "unbound: attribute should not exist after Set then Remove")
}

// =============================================================================
// P3×S2-S7: Streaming (V-bound) — full scenario coverage
// =============================================================================

// countVBoundMatches counts V-bound pattern matches for a given attribute and value
func vBoundMatchCount(t *testing.T, db *Database, a datalog.Keyword, v interface{}) int {
	t.Helper()
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(db.Schema())

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: a},
			query.Constant{Value: v},
			query.Blank{},
		},
	}

	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	count := 0
	iter := results.Iterator()
	for iter.Next() {
		count++
	}
	iter.Close()
	return count
}

func TestCardinalityOneRemove_VBound_AfterOverwrite(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(e, a, "Bob"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	tx3 := db.NewTransaction()
	require.NoError(t, tx3.Remove(e, a, "Bob"))
	_, err = tx3.Commit()
	require.NoError(t, err)

	// V-bound for "Bob" (last written value) → 0
	assert.Equal(t, 0, vBoundMatchCount(t, db, a, "Bob"),
		"V-bound: Bob should not match after Remove")
	// V-bound for "Alice" (earlier value) → also 0, attribute is gone
	assert.Equal(t, 0, vBoundMatchCount(t, db, a, "Alice"),
		"V-bound: Alice should not match after Remove (attribute tombstoned)")
}

func TestCardinalityOneRemove_VBound_ThenReAdd(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	tx3 := db.NewTransaction()
	require.NoError(t, tx3.Add(e, a, "Bob"))
	_, err = tx3.Commit()
	require.NoError(t, err)

	assert.Equal(t, 1, vBoundMatchCount(t, db, a, "Bob"),
		"V-bound: Bob should match after re-Add")
	assert.Equal(t, 0, vBoundMatchCount(t, db, a, "Alice"),
		"V-bound: Alice should not match (superseded by Bob)")
}

func TestCardinalityOneRemove_VBound_BeforeAnyAdd(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Remove(e, a, "phantom"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	assert.Equal(t, 1, vBoundMatchCount(t, db, a, "Alice"),
		"V-bound: Alice should match — Add wins over earlier Remove")
}

func TestCardinalityOneRemove_VBound_VIsIrrelevant(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Remove with different V
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Bob"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Attribute is tombstoned regardless of V in Remove
	assert.Equal(t, 0, vBoundMatchCount(t, db, a, "Alice"),
		"V-bound: Alice should not match — attribute tombstoned regardless of Remove V")
}

func TestCardinalityOneRemove_VBound_MultipleEntities(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e1 := datalog.NewIdentity("alice")
	e2 := datalog.NewIdentity("bob")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e1, a, "Alice"))
	require.NoError(t, tx.Add(e2, a, "Bob"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e1, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	assert.Equal(t, 0, vBoundMatchCount(t, db, a, "Alice"),
		"V-bound: Alice should not match after Remove on entity1")
	assert.Equal(t, 1, vBoundMatchCount(t, db, a, "Bob"),
		"V-bound: Bob should still match on entity2")
}

func TestCardinalityOneRemove_VBound_SetThenRemove(t *testing.T) {
	db, cleanup := createCardinalityOneDB(t)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	assert.Equal(t, 0, vBoundMatchCount(t, db, a, "Alice"),
		"V-bound: should not match after Set then Remove")
}
