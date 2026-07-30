//go:build !(js && wasm)

package storage

import (
	"crypto/sha1"
	"os"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// ---- Blob Store Unit Tests ----

func openTestBadger(t *testing.T) (*badger.DB, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "blob-test-*")
	require.NoError(t, err)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	require.NoError(t, err)

	return db, func() {
		db.Close()
		os.RemoveAll(dir)
	}
}

func TestBlobStore_PutGet(t *testing.T) {
	db, cleanup := openTestBadger(t)
	defer cleanup()

	data := []byte("compressed blob content " + strings.Repeat("x", 100))
	hash := sha1.Sum(data)

	// Put
	err := db.Update(func(txn *badger.Txn) error {
		return putBlob(txn.Set, hash, data)
	})
	require.NoError(t, err)

	// Get
	result, err := getBlob(db, hash)
	require.NoError(t, err)
	assert.Equal(t, data, result)
}

func TestBlobStore_ContentAddressing(t *testing.T) {
	db, cleanup := openTestBadger(t)
	defer cleanup()

	data := []byte("deduplicated content")
	hash := sha1.Sum(data)

	// Put twice
	for i := 0; i < 2; i++ {
		err := db.Update(func(txn *badger.Txn) error {
			return putBlob(txn.Set, hash, data)
		})
		require.NoError(t, err)
	}

	// Should still read correctly
	result, err := getBlob(db, hash)
	require.NoError(t, err)
	assert.Equal(t, data, result)

	// Count blob keys
	count := 0
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{blobKeyPrefix}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, count, "should have exactly 1 blob key")
}

func TestBlobStore_Missing(t *testing.T) {
	db, cleanup := openTestBadger(t)
	defer cleanup()

	hash := sha1.Sum([]byte("nonexistent"))
	_, err := getBlob(db, hash)
	assert.Error(t, err)
}

func TestBlobStore_KeyPrefix(t *testing.T) {
	// Verify blob prefix doesn't collide with any index prefix
	assert.Greater(t, blobKeyPrefix, byte(TAEV), "blob prefix must be > highest index prefix")
}

// ---- Tier 3 Integration Tests ----

// makeTier3Data lives in compressed_export_test.go, which is wasm-portable;
// this file is not.

// makeTier3String creates a string version of Tier 3 test data.
// Uses printable ASCII subset for valid string content.
func makeTier3String(size int) string {
	data := make([]byte, size)
	state := uint64(99)
	printable := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .,;:!?-_"
	for i := range data {
		state = state*6364136223846793005 + 1442695040888963407
		data[i] = printable[int((state>>33))%len(printable)]
	}
	return string(data)
}

func TestTier3_WriteRead(t *testing.T) {
	dir, err := os.MkdirTemp("", "tier3-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:                 dir,
		CompressionThreshold: 256,
	})
	require.NoError(t, err)
	defer db.Close()

	entity := datalog.NewIdentity("tier3-entity")
	attr := datalog.NewKeyword(":test/big")
	bigValue := makeTier3Data(100000) // 100KB of moderate-entropy bytes

	// Check if this actually triggers Tier 3
	vType, _, blobData := datalog.EncodeValue(bigValue, 256)
	if vType != datalog.TypeHashedBytes {
		t.Skipf("value reaches type 0x%02x not TypeHashedBytes (len=%d), skipping Tier 3 test", vType, len(bigValue))
	}
	require.NotNil(t, blobData, "Tier 3 should produce BlobData")
	t.Logf("Tier 3: %d bytes → %d compressed (blob hash %x)", len(bigValue), len(blobData.CompressedBytes), blobData.Hash[:8])

	// Write
	tx := db.NewTransaction()
	tx.Add(entity, attr, bigValue)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Read back
	matcher := NewPatternMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entity},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	iter := results.Iterator()
	require.True(t, iter.Next(), "should find the Tier 3 value")
	got := iter.Tuple()[0].([]byte)
	assert.Equal(t, bigValue, got, "Tier 3 value should round-trip correctly")
	assert.False(t, iter.Next(), "should be exactly one result")
}

func TestTier3_ContentDedup(t *testing.T) {
	dir, err := os.MkdirTemp("", "tier3-dedup-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:                 dir,
		CompressionThreshold: 256,
	})
	require.NoError(t, err)
	defer db.Close()

	bigValue := makeTier3Data(100000)

	vType, _, _ := datalog.EncodeValue(bigValue, 256)
	if vType != datalog.TypeHashedBytes {
		t.Skipf("value doesn't reach Tier 3, skipping dedup test")
	}

	// Write same value to two entities
	for i := 0; i < 2; i++ {
		entity := datalog.NewIdentity(strings.Repeat("e", i+1))
		tx := db.NewTransaction()
		tx.Add(entity, datalog.NewKeyword(":test/big"), bigValue)
		_, err = tx.Commit()
		require.NoError(t, err)
	}

	// Count blobs — should be exactly 1 (content-addressed dedup)
	count := 0
	err = requireBadgerStore(t, db).db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{blobKeyPrefix}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, count, "same content should produce one blob (content-addressed)")

	// Both entities should read back correctly
	for i := 0; i < 2; i++ {
		entity := datalog.NewIdentity(strings.Repeat("e", i+1))
		matcher := NewPatternMatcher(db.Store())
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Constant{Value: entity},
				query.Constant{Value: datalog.NewKeyword(":test/big")},
				query.Variable{Name: datalog.NewSymbol("?v")},
				query.Blank{},
			},
		}
		results, err := matcher.Match(query.PatternQuery(pattern), nil)
		require.NoError(t, err)
		iter := results.Iterator()
		require.True(t, iter.Next(), "entity %d should have the value", i)
		assert.Equal(t, bigValue, iter.Tuple()[0].([]byte))
	}
}

func TestTier3_DatomFromKey_NilDB(t *testing.T) {
	enc := &BinaryKeyEncoder{CompressionThreshold: 256}

	bigValue := makeTier3Data(100000)

	vType, _, _ := datalog.EncodeValue(bigValue, 256)
	if vType != datalog.TypeHashedBytes {
		t.Skipf("value doesn't reach Tier 3")
	}

	d := &datalog.Datom{
		E:  datalog.NewIdentity("nil-blob-test"),
		A:  datalog.NewKeyword(":test/big"),
		V:  bigValue,
		Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1},
	}
	key := enc.EncodeKey(EAVT, d)

	// Decode with nil blob reader — should error
	_, err := DatomFromKey(EAVT, key, enc, nil)
	assert.Error(t, err, "Tier 3 decode with nil blob reader should fail")
	assert.Contains(t, err.Error(), "blob lookup")
}

func TestTier3_WriteRead_String(t *testing.T) {
	dir, err := os.MkdirTemp("", "tier3-str-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:                 dir,
		CompressionThreshold: 256,
	})
	require.NoError(t, err)
	defer db.Close()

	entity := datalog.NewIdentity("tier3-str-entity")
	attr := datalog.NewKeyword(":test/big-text")
	bigValue := makeTier3String(100000)

	vType, _, blobData := datalog.EncodeValue(bigValue, 256)
	if vType != datalog.TypeHashedString {
		t.Skipf("string value reaches type 0x%02x not TypeHashedString (len=%d), skipping", vType, len(bigValue))
	}
	require.NotNil(t, blobData)
	t.Logf("Tier 3 string: %d bytes → %d compressed", len(bigValue), len(blobData.CompressedBytes))

	tx := db.NewTransaction()
	tx.Add(entity, attr, bigValue)
	_, err = tx.Commit()
	require.NoError(t, err)

	matcher := NewPatternMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entity},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	iter := results.Iterator()
	require.True(t, iter.Next(), "should find the Tier 3 string value")
	got := iter.Tuple()[0].(string)
	assert.Equal(t, bigValue, got, "Tier 3 string should round-trip correctly")
}

// ---- Tier 3 Query Integration Tests ----

func newTier3DB(t *testing.T, s schema.SchemaProvider) (*Database, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "tier3-query-*")
	require.NoError(t, err)
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:                 dir,
		Schema:               s,
		CompressionThreshold: 256,
	})
	require.NoError(t, err)
	return db, func() { db.Close(); os.RemoveAll(dir) }
}

func skipIfNotTier3Bytes(t *testing.T, data []byte) {
	t.Helper()
	vType, _, _ := datalog.EncodeValue(data, 256)
	if vType != datalog.TypeHashedBytes {
		t.Skipf("data doesn't reach Tier 3 (type 0x%02x)", vType)
	}
}

func skipIfNotTier3String(t *testing.T, data string) {
	t.Helper()
	vType, _, _ := datalog.EncodeValue(data, 256)
	if vType != datalog.TypeHashedString {
		t.Skipf("data doesn't reach Tier 3 (type 0x%02x)", vType)
	}
}

func TestTier3_AVET_ExactMatch(t *testing.T) {
	db, cleanup := newTier3DB(t, nil)
	defer cleanup()

	entity := datalog.NewIdentity("avet-tier3")
	attr := datalog.NewKeyword(":test/big")
	bigValue := makeTier3Data(100000)
	skipIfNotTier3Bytes(t, bigValue)

	tx := db.NewTransaction()
	tx.Add(entity, attr, bigValue)
	_, err := tx.Commit()
	require.NoError(t, err)

	// AVET lookup: A+V bound, find E
	matcher := NewPatternMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Constant{Value: bigValue}, // bound V triggers AVET
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	iter := results.Iterator()
	require.True(t, iter.Next(), "AVET lookup should find the entity for Tier 3 value")
	assert.Equal(t, entity, iter.Tuple()[0])
	assert.False(t, iter.Next(), "should be exactly one result")
}

func TestTier3_AVET_NoFalsePositive(t *testing.T) {
	db, cleanup := newTier3DB(t, nil)
	defer cleanup()

	entity := datalog.NewIdentity("avet-tier3-fp")
	attr := datalog.NewKeyword(":test/big")
	bigValue := makeTier3Data(100000)
	differentValue := makeTier3Data(100001) // different size → different content
	skipIfNotTier3Bytes(t, bigValue)

	tx := db.NewTransaction()
	tx.Add(entity, attr, bigValue)
	_, err := tx.Commit()
	require.NoError(t, err)

	// Search for different value — should find nothing
	matcher := NewPatternMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Constant{Value: differentValue},
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	iter := results.Iterator()
	assert.False(t, iter.Next(), "AVET search for non-existent Tier 3 value should return nothing")
}

func TestTier3_CRDT_CardinalityOne_LWW(t *testing.T) {
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":test/big"),
		ValueType:   schema.TypeBytes,
		Cardinality: schema.CardinalityOne,
	})
	db, cleanup := newTier3DB(t, s)
	defer cleanup()

	entity := datalog.NewIdentity("lww-tier3")
	attr := datalog.NewKeyword(":test/big")

	v1 := makeTier3Data(100000)
	v2 := makeTier3Data(100002) // different content
	skipIfNotTier3Bytes(t, v1)
	skipIfNotTier3Bytes(t, v2)

	// Write version 1
	tx1 := db.NewTransaction()
	tx1.Set(entity, attr, v1)
	_, err := tx1.Commit()
	require.NoError(t, err)

	// Write version 2
	tx2 := db.NewTransaction()
	tx2.Set(entity, attr, v2)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Query should return only the latest (v2)
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entity},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	iter := results.Iterator()
	require.True(t, iter.Next())
	got := iter.Tuple()[0].([]byte)
	assert.Equal(t, v2, got, "LWW should return latest Tier 3 value")
	assert.False(t, iter.Next(), "LWW should return exactly one result")
}

func TestTier3_CRDT_CardinalityMany_AddRetract(t *testing.T) {
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":test/blobs"),
		ValueType:   schema.TypeBytes,
		Cardinality: schema.CardinalityMany,
	})
	db, cleanup := newTier3DB(t, s)
	defer cleanup()

	entity := datalog.NewIdentity("many-tier3")
	attr := datalog.NewKeyword(":test/blobs")

	v1 := makeTier3Data(100000)
	v2 := makeTier3Data(100002)
	skipIfNotTier3Bytes(t, v1)
	skipIfNotTier3Bytes(t, v2)

	// Add both
	tx := db.NewTransaction()
	tx.Add(entity, attr, v1)
	tx.Add(entity, attr, v2)
	_, err := tx.Commit()
	require.NoError(t, err)

	// Both should be present
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entity},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?v")},
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
	assert.Equal(t, 2, count, "cardinality-many should have both Tier 3 values")

	// Retract v2
	tx2 := db.NewTransaction()
	tx2.Retract(entity, attr, v2)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Only v1 should remain
	results2, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	var remaining [][]byte
	iter2 := results2.Iterator()
	for iter2.Next() {
		remaining = append(remaining, iter2.Tuple()[0].([]byte))
	}
	assert.Len(t, remaining, 1, "should have one value after retract")
	if len(remaining) == 1 {
		assert.Equal(t, v1, remaining[0], "remaining value should be v1")
	}
}

func TestTier3_EntityScan_MixedTiers(t *testing.T) {
	db, cleanup := newTier3DB(t, nil)
	defer cleanup()

	entity := datalog.NewIdentity("mixed-tier-entity")
	smallValue := "short name"                                      // Tier 1
	medValue := strings.Repeat("medium content for tier two. ", 20) // Tier 2
	bigValue := makeTier3Data(100000)                               // Tier 3
	skipIfNotTier3Bytes(t, bigValue)

	tx := db.NewTransaction()
	tx.Add(entity, datalog.NewKeyword(":test/name"), smallValue)
	tx.Add(entity, datalog.NewKeyword(":test/content"), medValue)
	tx.Add(entity, datalog.NewKeyword(":test/blob"), bigValue)
	tx.Add(entity, datalog.NewKeyword(":test/score"), int64(42))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Entity scan: E bound, all A+V
	matcher := NewPatternMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entity},
			query.Variable{Name: datalog.NewSymbol("?a")},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	seen := make(map[string]interface{})
	iter := results.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		attr := tuple[0].(datalog.Keyword).String()
		seen[attr] = tuple[1]
	}

	assert.Contains(t, seen, ":test/name")
	assert.Contains(t, seen, ":test/content")
	assert.Contains(t, seen, ":test/blob")
	assert.Contains(t, seen, ":test/score")

	assert.Equal(t, smallValue, seen[":test/name"])
	assert.Equal(t, medValue, seen[":test/content"])
	assert.Equal(t, bigValue, seen[":test/blob"])
	assert.Equal(t, int64(42), seen[":test/score"])
}
