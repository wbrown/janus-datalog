package storage

// AETV Index Tests
//
// AETV (A → E → Tx↓ → V) is the CRDT-aware A-primary index.
// It completes the symmetry with EATV (E → A → Tx↓ → V).
//
// Key properties:
// - A is primary sort key (efficient for "all entities with attribute A")
// - E is secondary sort key (groups by entity within attribute)
// - Tx is tertiary with bitwise NOT (descending order, highest Tx first)
// - V comes last (after CRDT resolution determines which entry to use)
//
// Use cases:
// - Batch entity lookup: [?e :attr ?v] with many Es from input
// - Attribute enumeration: [?e :attr ?v] with E unbound
// - Pull API: same attributes for multiple entities
// - Time-travel per attribute

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// =============================================================================
// Key Encoding/Decoding Tests
// =============================================================================

// TestAETVKeyEncoding verifies AETV keys encode and decode correctly.
func TestAETVKeyEncoding(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	datom := &datalog.Datom{
		E:  datalog.NewIdentity("test-entity"),
		A:  datalog.NewKeyword(":test/attribute"),
		V:  "test-value",
		Tx: datalog.ElementID{Lamport: 12345, ReplicaID: 1},
		Op: datalog.OpCRDTAdd,
	}

	// Encode
	key := encoder.EncodeKey(AETV, datom)
	require.NotEmpty(t, key, "AETV key should not be empty")

	// Verify prefix byte
	assert.Equal(t, byte(AETV), key[0], "first byte should be AETV index prefix")

	// Decode
	e, a, v, tx, op, afterRef, err := encoder.DecodeKey(AETV, key)
	require.NoError(t, err, "DecodeKey should succeed")

	// Verify entity
	assert.Equal(t, datom.E.Hash(), e, "entity should match")

	// Verify attribute
	assert.Equal(t, []byte(datom.A.String()), a[:len(datom.A.String())], "attribute should match")

	// Verify value
	assert.Equal(t, byte(datalog.TypeString), v[0], "value type should be string")
	assert.Equal(t, "test-value", string(v[1:]), "value should match")

	// Verify Tx
	decodedTx := Tx(tx).ToElementID()
	assert.Equal(t, datom.Tx.Lamport, decodedTx.Lamport, "Tx Lamport should match")
	assert.Equal(t, datom.Tx.ReplicaID, decodedTx.ReplicaID, "Tx ReplicaID should match")

	// Verify Op
	assert.Equal(t, byte(datalog.OpCRDTAdd), op, "Op should match")

	// Verify AfterRef is zero (non-RGA op)
	var zeroAfterRef [16]byte
	assert.Equal(t, zeroAfterRef, afterRef, "AfterRef should be zero for non-RGA op")
}

// TestAETVKeyEncodingWithAfterRef verifies AfterRef is encoded for RGA ops.
func TestAETVKeyEncodingWithAfterRef(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	elemID := datalog.ElementID{Lamport: 1000, ReplicaID: 100}
	afterRef := datalog.ElementID{Lamport: 500, ReplicaID: 100}

	datom := &datalog.Datom{
		E:        datalog.NewIdentity("test-entity"),
		A:        datalog.NewKeyword(":vector/attr"),
		V:        "element-value",
		Tx:       elemID,
		Op:       datalog.OpRGAInsert,
		AfterRef: afterRef,
	}

	// Encode
	key := encoder.EncodeKey(AETV, datom)

	// Decode
	_, _, _, tx, op, decodedAfterRef, err := encoder.DecodeKey(AETV, key)
	require.NoError(t, err)

	// Verify Op
	assert.Equal(t, byte(datalog.OpRGAInsert), op)

	// Verify Tx
	decodedTx := Tx(tx).ToElementID()
	assert.Equal(t, elemID.Lamport, decodedTx.Lamport)

	// Verify AfterRef
	decodedAfterRefElem := Tx(decodedAfterRef).ToElementID()
	assert.Equal(t, afterRef.Lamport, decodedAfterRefElem.Lamport)
	assert.Equal(t, afterRef.ReplicaID, decodedAfterRefElem.ReplicaID)
}

// TestAETVKeyEncodingAllValueTypes verifies AETV works with all value types.
func TestAETVKeyEncodingAllValueTypes(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	testCases := []struct {
		name  string
		value any
	}{
		{"string", "test-string"},
		{"int64", int64(42)},
		{"float64", float64(3.14159)},
		{"bool_true", true},
		{"bool_false", false},
		{"reference", datalog.Reference(datalog.NewIdentity("ref-target"))},
		{"keyword", datalog.NewKeyword(":status/active")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			datom := &datalog.Datom{
				E:  datalog.NewIdentity("entity-" + tc.name),
				A:  datalog.NewKeyword(":test/attr"),
				V:  tc.value,
				Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1},
				Op: datalog.OpCRDTAdd,
			}

			key := encoder.EncodeKey(AETV, datom)
			require.NotEmpty(t, key)

			_, _, v, _, _, _, err := encoder.DecodeKey(AETV, key)
			require.NoError(t, err, "should decode key for value type %s", tc.name)
			require.NotEmpty(t, v, "decoded value should not be empty for %s", tc.name)
		})
	}
}

// =============================================================================
// Sort Order Tests
// =============================================================================

// TestAETVSortOrderAttributeFirst verifies A is the primary sort key.
func TestAETVSortOrderAttributeFirst(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	entity := datalog.NewIdentity("same-entity")
	tx := datalog.ElementID{Lamport: 100, ReplicaID: 1}

	datom1 := &datalog.Datom{
		E:  entity,
		A:  datalog.NewKeyword(":aaa/first"), // alphabetically first
		V:  "value1",
		Tx: tx,
		Op: datalog.OpCRDTAdd,
	}

	datom2 := &datalog.Datom{
		E:  entity,
		A:  datalog.NewKeyword(":zzz/last"), // alphabetically last
		V:  "value2",
		Tx: tx,
		Op: datalog.OpCRDTAdd,
	}

	key1 := encoder.EncodeKey(AETV, datom1)
	key2 := encoder.EncodeKey(AETV, datom2)

	// :aaa should sort before :zzz
	assert.True(t, bytes.Compare(key1, key2) < 0,
		"attribute :aaa should sort before :zzz")
}

// TestAETVSortOrderTxDescending verifies Tx sorts in descending order.
// This is THE critical property for CRDT resolution.
func TestAETVSortOrderTxDescending(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	entity := datalog.NewIdentity("same-entity")
	attr := datalog.NewKeyword(":same/attr")

	// Create datoms with different Tx values
	datom1 := &datalog.Datom{
		E:  entity,
		A:  attr,
		V:  "old-value",
		Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1}, // older
		Op: datalog.OpCRDTAdd,
	}

	datom2 := &datalog.Datom{
		E:  entity,
		A:  attr,
		V:  "new-value",
		Tx: datalog.ElementID{Lamport: 200, ReplicaID: 1}, // newer
		Op: datalog.OpCRDTAdd,
	}

	key1 := encoder.EncodeKey(AETV, datom1)
	key2 := encoder.EncodeKey(AETV, datom2)

	// With bitwise NOT encoding, higher Tx (200) should sort FIRST (lower bytes)
	assert.True(t, bytes.Compare(key2, key1) < 0,
		"higher Tx (200) should sort before lower Tx (100) due to bitwise NOT")
}

// TestAETVSortOrderEntitySecondary verifies E is secondary sort key within A.
func TestAETVSortOrderEntitySecondary(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	attr := datalog.NewKeyword(":same/attr")
	tx := datalog.ElementID{Lamport: 100, ReplicaID: 1}

	// Different entities, same attribute
	datom1 := &datalog.Datom{
		E:  datalog.NewIdentity("entity-aaa"),
		A:  attr,
		V:  "value1",
		Tx: tx,
		Op: datalog.OpCRDTAdd,
	}

	datom2 := &datalog.Datom{
		E:  datalog.NewIdentity("entity-zzz"),
		A:  attr,
		V:  "value2",
		Tx: tx,
		Op: datalog.OpCRDTAdd,
	}

	key1 := encoder.EncodeKey(AETV, datom1)
	key2 := encoder.EncodeKey(AETV, datom2)

	// Keys should be different (different entities)
	assert.NotEqual(t, key1, key2, "different entities should produce different keys")

	// Both should have same attribute prefix
	// AETV format: [prefix:1][A:32][E:20][Tx:16][V:var][Op:1]
	assert.Equal(t, key1[0:33], key2[0:33], "same attribute should have same prefix")
}

// =============================================================================
// Prefix Range Tests
// =============================================================================

// TestAETVPrefixRangeAttribute verifies prefix range for attribute-only scan.
func TestAETVPrefixRangeAttribute(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	attr := datalog.NewKeyword(":test/attribute")
	var attrBytes Attribute
	copy(attrBytes[:], attr.String())

	start, end := encoder.EncodePrefixRange(AETV, attrBytes[:])

	// Start should be [AETV prefix][attribute bytes]
	assert.Equal(t, byte(AETV), start[0], "start should have AETV prefix")
	assert.True(t, len(start) > 1, "start should have attribute bytes")

	// End should be greater than start
	assert.True(t, bytes.Compare(start, end) < 0, "end should be greater than start")
}

// TestAETVPrefixRangeAttributeEntity verifies prefix range for (A, E) scan.
func TestAETVPrefixRangeAttributeEntity(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	attr := datalog.NewKeyword(":test/attribute")
	entity := datalog.NewIdentity("test-entity")

	var attrBytes Attribute
	copy(attrBytes[:], attr.String())
	eBytes := entity.Hash()

	start, end := encoder.EncodePrefixRange(AETV, attrBytes[:], eBytes[:])

	// Start should be [AETV prefix][attribute][entity]
	assert.Equal(t, byte(AETV), start[0], "start should have AETV prefix")
	expectedLen := 1 + 32 + 20 // prefix + attr + entity
	assert.Equal(t, expectedLen, len(start), "start should have correct length")

	// End should be greater than start
	assert.True(t, bytes.Compare(start, end) < 0, "end should be greater than start")
}

// =============================================================================
// Integration Tests - Storage Scan
// =============================================================================

// TestAETVStorageScan verifies AETV can be written to and scanned from storage.
func TestAETVStorageScan(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Create test data: multiple entities with same attribute
			attr := datalog.NewKeyword(":person/name")
			entities := make([]datalog.Identity, 5)

			tx := db.NewTransaction()
			for i := 0; i < 5; i++ {
				entities[i] = datalog.NewIdentity("person-" + string(rune('A'+i)))
				tx.Add(entities[i], attr, "Name "+string(rune('A'+i)))
			}
			_, err := tx.Commit()
			require.NoError(t, err)

			// Scan AETV for :person/name attribute
			iter, err := db.Store().ScanKeysOnly(ScanBound{Index: AETV, Prefix: []datalog.Value{attr}})
			require.NoError(t, err)
			defer iter.Close()

			// Count results
			count := 0
			for iter.Next() {
				count++
			}

			assert.Equal(t, 5, count, "should find all 5 entities with :person/name")
		})
	}
}

// TestAETVStorageScanTxDescendingOrder verifies scan returns Tx in descending order.
func TestAETVStorageScanTxDescendingOrder(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Create entity with multiple writes (different Tx values)
			entity := datalog.NewIdentity("test-entity")
			attr := datalog.NewKeyword(":test/value")

			// Write three values in sequence (Tx increases with each commit)
			values := []string{"first", "second", "third"}
			for _, v := range values {
				tx := db.NewTransaction()
				tx.Add(entity, attr, v)
				_, err := tx.Commit()
				require.NoError(t, err)
			}

			// Scan AETV for this (A, E) pair
			iter, err := db.Store().ScanKeysOnly(ScanBound{
				Index:  AETV,
				Prefix: []datalog.Value{attr, entity},
			})
			require.NoError(t, err)
			defer iter.Close()

			// Collect Tx values in scan order
			var txValues []uint64
			for iter.Next() {
				d, err := iter.Datom()
				require.NoError(t, err)
				txValues = append(txValues, d.Tx.Lamport)
			}

			require.Len(t, txValues, 3, "should find all 3 writes")

			// Verify descending order (highest Tx first)
			for i := 1; i < len(txValues); i++ {
				assert.True(t, txValues[i-1] > txValues[i],
					"Tx should be in descending order: %d should be > %d", txValues[i-1], txValues[i])
			}

			t.Logf("Tx values in scan order: %v (should be descending)", txValues)
		})
	}
}

// =============================================================================
// CRDT Resolution Tests
// =============================================================================

// TestAETVCRDTResolutionSingleEntity verifies LWW resolution for single entity.
func TestAETVCRDTResolutionSingleEntity(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			// Create database with cache disabled to test AETV path
			db := createOptimizerModeDB(t, mode, DatabaseOptions{DisableCache: true})

			// Set up schema with cardinality-one attribute
			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			// Write multiple values (LWW - last write wins)
			entity := datalog.NewIdentity("test-person")
			names := []string{"Alice", "Bob", "Charlie"}

			for _, name := range names {
				tx := db.NewTransaction()
				tx.Add(entity, datalog.NewKeyword(":person/name"), name)
				_, err := tx.Commit()
				require.NoError(t, err)
			}

			// Query should return only "Charlie" (last write)
			result, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e :where [?e :person/name ?v]]`,
				entity,
			))
			require.NoError(t, err)

			require.Len(t, result, 1, "CardinalityOne should return single result")
			assert.Equal(t, "Charlie", result[0][0], "should return LWW winner")
		})
	}
}

// TestAETVCRDTResolutionMultipleEntities verifies LWW resolution for batch lookup.
func TestAETVCRDTResolutionMultipleEntities(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			// Create database with cache disabled
			db := createOptimizerModeDB(t, mode, DatabaseOptions{DisableCache: true})

			// Set up schema with cardinality-one attribute
			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			// Create 3 entities, each with 3 name updates
			entities := []datalog.Identity{
				datalog.NewIdentity("person-1"),
				datalog.NewIdentity("person-2"),
				datalog.NewIdentity("person-3"),
			}

			expectedNames := make(map[string]string) // entity hash -> expected name

			for _, entity := range entities {
				names := []string{"First", "Second", "Final"}
				for _, name := range names {
					tx := db.NewTransaction()
					tx.Add(entity, datalog.NewKeyword(":person/name"), name+" "+entity.String())
					_, err := tx.Commit()
					require.NoError(t, err)
				}
				hash := entity.Hash()
				expectedNames[string(hash[:])] = "Final " + entity.String()
			}

			// Query with all entities as input - should use AETV with CRDT resolution
			result, err := executor.CollectTuples(db.Query(
				`[:find ?e ?v :in $ [?e ...] :where [?e :person/name ?v]]`,
				entities,
			))
			require.NoError(t, err)

			// Should have exactly 3 results (one per entity)
			require.Len(t, result, 3, "should return one result per entity")

			// Verify each entity got its LWW winner
			for _, tuple := range result {
				entity := tuple[0].(datalog.Identity)
				name := tuple[1].(string)

				hash := entity.Hash()
				expected := expectedNames[string(hash[:])]
				assert.Equal(t, expected, name,
					"entity %s should have LWW winner", entity.String())
			}
		})
	}
}

// TestAETVCRDTResolutionUnboundEntity verifies resolution for attribute enumeration.
func TestAETVCRDTResolutionUnboundEntity(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			// Create database with cache disabled
			db := createOptimizerModeDB(t, mode, DatabaseOptions{DisableCache: true})

			// Set up schema
			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/status"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			// Create entities with status updates
			for i := 0; i < 5; i++ {
				entity := datalog.NewIdentity("person-" + string(rune('A'+i)))

				// Multiple status updates
				statuses := []string{"pending", "active", "completed"}
				for _, status := range statuses {
					tx := db.NewTransaction()
					tx.Add(entity, datalog.NewKeyword(":person/status"), status)
					_, err := tx.Commit()
					require.NoError(t, err)
				}
			}

			// Query all statuses (E unbound) - should use AETV
			result, err := executor.CollectTuples(db.Query(`[:find ?e ?v :where [?e :person/status ?v]]`))
			require.NoError(t, err)

			// Should have 5 results (one per entity, LWW resolved)
			require.Len(t, result, 5, "should return one result per entity")

			// All should have "completed" (last write)
			for _, tuple := range result {
				status := tuple[1].(string)
				assert.Equal(t, "completed", status, "all entities should have LWW winner")
			}
		})
	}
}

// =============================================================================
// Index Selection Tests
// =============================================================================

// TestIndexSelectionAETVForInputBoundE verifies AETV is selected for E-from-input pattern.
func TestIndexSelectionAETVForInputBoundE(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Set up schema with cardinality-one attribute
			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			// Add test data
			entity := datalog.NewIdentity("test-person")
			tx := db.NewTransaction()
			tx.Add(entity, datalog.NewKeyword(":person/name"), "Test Name")
			_, err := tx.Commit()
			require.NoError(t, err)

			// Create pattern and binding relation
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":person/name")},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			}

			opts := getDefaultExecutorOptions()
			inputRel := executor.NewMaterializedRelationWithOptions(
				[]query.Symbol{datalog.NewSymbol("?e")},
				[]executor.Tuple{{entity}},
				opts,
			)

			// Analyze reuse strategy
			strategy, _ := analyzeReuseStrategy(pattern, inputRel)

			// With CardinalityOne schema, should select AETV (once implemented)
			// For now, document the expected behavior
			t.Logf("Strategy type: %v, Index: %v", strategy.Type, strategy.Index)

			// TODO: After AETV implementation, verify:
			// assert.Equal(t, AETV, strategy.Index, "should select AETV for E-from-input with CardinalityOne")
		})
	}
}

// =============================================================================
// Cross-Product Input Tests
// =============================================================================

// TestAETVCrossProductInputs tests when both E and A come from separate collection inputs.
// Query: [:find ?e ?a ?v :in $ [?e ...] [?a ...] :where [?e ?a ?v]]
// This requires the executor to handle the cross-product of inputs correctly.
func TestAETVCrossProductInputs(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{DisableCache: true})

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

			// Write one value for each (entity, attribute) combination
			tx := db.NewTransaction()
			for i, entity := range entities {
				tx.Set(entity, attrs[0], "Name"+string(rune('A'+i)))
				tx.Set(entity, attrs[1], int64(20+i*10))
			}
			_, err := tx.Commit()
			require.NoError(t, err)

			// Query with both E and A from separate collections
			results, err := executor.CollectTuples(db.Query(
				`[:find ?e ?a ?v :in $ [?e ...] [?a ...] :where [?e ?a ?v]]`,
				entities, attrs))
			require.NoError(t, err)

			// Expected: 4 results (2 entities × 2 attributes)
			t.Logf("Results (%d):", len(results))
			for _, r := range results {
				t.Logf("  %v", r)
			}

			assert.Len(t, results, 4, "should return cross-product: 2 entities × 2 attributes")
		})
	}
}

// =============================================================================
// Comparison with EATV Tests
// =============================================================================

// TestAETVSymmetryWithEATV verifies AETV is the A-primary equivalent of EATV.
func TestAETVSymmetryWithEATV(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	entity := datalog.NewIdentity("test-entity")
	attr := datalog.NewKeyword(":test/attr")

	datom := &datalog.Datom{
		E:  entity,
		A:  attr,
		V:  "test-value",
		Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1},
		Op: datalog.OpCRDTAdd,
	}

	eatvKey := encoder.EncodeKey(EATV, datom)
	aetvKey := encoder.EncodeKey(AETV, datom)

	// Both should have Tx in same position relative to their primary key
	// EATV: E → A → Tx → V (Tx is 3rd)
	// AETV: A → E → Tx → V (Tx is 3rd)

	// Decode both and verify Tx matches
	_, _, _, eatvTx, _, _, err := encoder.DecodeKey(EATV, eatvKey)
	require.NoError(t, err)

	_, _, _, aetvTx, _, _, err := encoder.DecodeKey(AETV, aetvKey)
	require.NoError(t, err)

	assert.Equal(t, eatvTx, aetvTx, "Tx should be encoded identically in EATV and AETV")
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkAETVKeyEncoding(b *testing.B) {
	encoder := &BinaryKeyEncoder{}

	datom := &datalog.Datom{
		E:  datalog.NewIdentity("benchmark-entity"),
		A:  datalog.NewKeyword(":benchmark/attribute"),
		V:  "benchmark value string",
		Tx: datalog.ElementID{Lamport: 12345678, ReplicaID: 1},
		Op: datalog.OpCRDTAdd,
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = encoder.EncodeKey(AETV, datom)
	}
}

func BenchmarkAETVKeyDecoding(b *testing.B) {
	encoder := &BinaryKeyEncoder{}

	datom := &datalog.Datom{
		E:  datalog.NewIdentity("benchmark-entity"),
		A:  datalog.NewKeyword(":benchmark/attribute"),
		V:  "benchmark value string",
		Tx: datalog.ElementID{Lamport: 12345678, ReplicaID: 1},
		Op: datalog.OpCRDTAdd,
	}

	key := encoder.EncodeKey(AETV, datom)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _, _, _, _, _ = encoder.DecodeKey(AETV, key)
	}
}
