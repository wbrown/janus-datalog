//go:build !(js && wasm)

package storage

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// ---- Export Tests ----

func TestExport_DefaultUncompressed(t *testing.T) {
	db, cleanup := newCompressedDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("export-entity")
	attr := datalog.NewKeyword(":test/content")
	value := longString("export test", 500)

	tx := db.NewTransaction()
	tx.Add(entity, attr, value)
	_, err := tx.Commit()
	require.NoError(t, err)

	// Default export — no #lzj tags
	var buf bytes.Buffer
	err = db.Export(&buf)
	require.NoError(t, err)

	output := buf.String()
	assert.NotContains(t, output, "#lzj", "default export should not contain #lzj tags")
	assert.Contains(t, output, value[:50], "default export should contain the raw string value")
}

func TestExport_Compressed(t *testing.T) {
	db, cleanup := newCompressedDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("export-comp")
	attr := datalog.NewKeyword(":test/content")
	value := longString("compressed export", 500)

	tx := db.NewTransaction()
	tx.Add(entity, attr, value)
	_, err := tx.Commit()
	require.NoError(t, err)

	// Compressed export — should have #lzj tags
	var buf bytes.Buffer
	err = db.ExportCompressed(&buf)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "#lzj", "compressed export should contain #lzj tags")
	assert.NotContains(t, output, value[:50], "compressed export should not contain raw string")
}

func TestExport_CompressedSmallValue(t *testing.T) {
	db, cleanup := newCompressedDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("export-small")
	attr := datalog.NewKeyword(":test/name")
	value := "Alice" // below threshold

	tx := db.NewTransaction()
	tx.Add(entity, attr, value)
	_, err := tx.Commit()
	require.NoError(t, err)

	var buf bytes.Buffer
	err = db.ExportCompressed(&buf)
	require.NoError(t, err)

	output := buf.String()
	// Small values should be plain strings even in compressed export
	assert.Contains(t, output, "\"Alice\"")
}

// ---- Import Tests ----

func TestImport_LZJLiteral(t *testing.T) {
	// Create a database, write a value, export compressed, import into new DB
	db1, cleanup1 := newCompressedDB(t)
	defer cleanup1()

	entity := datalog.NewIdentity("import-entity")
	attr := datalog.NewKeyword(":test/content")
	value := longString("import test value", 500)

	tx := db1.NewTransaction()
	tx.Add(entity, attr, value)
	_, err := tx.Commit()
	require.NoError(t, err)

	// Export compressed
	var buf bytes.Buffer
	err = db1.ExportCompressed(&buf)
	require.NoError(t, err)
	ednData := buf.String()
	assert.Contains(t, ednData, "#lzj")

	// Import into fresh DB
	dir2, err := os.MkdirTemp("", "import-lzj-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)

	db2, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:                 dir2,
		CompressionThreshold: 256,
	})
	require.NoError(t, err)
	defer db2.Close()

	err = db2.Import(strings.NewReader(ednData))
	require.NoError(t, err)

	// Query the imported value
	matcher := NewBadgerMatcher(db2.Store())
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
	got := iter.Tuple()[0].(string)
	assert.Equal(t, value, got, "imported value should match original")
}

func TestImport_MixedFile(t *testing.T) {
	// Export a DB with both small and large values using compressed export
	db1, cleanup1 := newCompressedDB(t)
	defer cleanup1()

	entity := datalog.NewIdentity("mixed-entity")
	tx := db1.NewTransaction()
	tx.Add(entity, datalog.NewKeyword(":test/name"), "Alice")
	tx.Add(entity, datalog.NewKeyword(":test/content"), longString("mixed content", 500))
	tx.Add(entity, datalog.NewKeyword(":test/score"), int64(42))
	_, err := tx.Commit()
	require.NoError(t, err)

	var buf bytes.Buffer
	err = db1.ExportCompressed(&buf)
	require.NoError(t, err)

	// Import into fresh DB
	dir2, err := os.MkdirTemp("", "import-mixed-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)

	db2, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:                 dir2,
		CompressionThreshold: 256,
	})
	require.NoError(t, err)
	defer db2.Close()

	err = db2.Import(strings.NewReader(buf.String()))
	require.NoError(t, err)

	// Verify all values
	matcher := NewBadgerMatcher(db2.Store())
	for _, tc := range []struct {
		attr     string
		expected interface{}
	}{
		{":test/name", "Alice"},
		{":test/content", longString("mixed content", 500)},
		{":test/score", int64(42)},
	} {
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Constant{Value: entity},
				query.Constant{Value: datalog.NewKeyword(tc.attr)},
				query.Variable{Name: datalog.NewSymbol("?v")},
				query.Blank{},
			},
		}
		results, err := matcher.Match(query.PatternQuery(pattern), nil)
		require.NoError(t, err)
		iter := results.Iterator()
		require.True(t, iter.Next(), "should find %s", tc.attr)
		assert.Equal(t, tc.expected, iter.Tuple()[0], "value mismatch for %s", tc.attr)
	}
}

func TestImport_RoundTrip(t *testing.T) {
	// Write to DB1, export compressed, import to DB2, export default from DB2
	// Verify DB2's default export has no #lzj tags but same values
	db1, cleanup1 := newCompressedDB(t)
	defer cleanup1()

	entity := datalog.NewIdentity("roundtrip")
	tx := db1.NewTransaction()
	tx.Add(entity, datalog.NewKeyword(":test/content"), longString("roundtrip test", 800))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Export compressed from DB1
	var compBuf bytes.Buffer
	err = db1.ExportCompressed(&compBuf)
	require.NoError(t, err)
	assert.Contains(t, compBuf.String(), "#lzj")

	// Import into DB2
	dir2, err := os.MkdirTemp("", "roundtrip-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)

	db2, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:                 dir2,
		CompressionThreshold: 256,
	})
	require.NoError(t, err)
	defer db2.Close()

	err = db2.Import(strings.NewReader(compBuf.String()))
	require.NoError(t, err)

	// Export default (uncompressed) from DB2
	var defaultBuf bytes.Buffer
	err = db2.Export(&defaultBuf)
	require.NoError(t, err)
	assert.NotContains(t, defaultBuf.String(), "#lzj",
		"default export from DB2 should not have #lzj tags")
}

func TestImport_AVET_AfterImport(t *testing.T) {
	// Write compressed value to DB1, export with #lzj, import to DB2,
	// verify AVET lookup on the imported value works
	db1, cleanup1 := newCompressedDB(t)
	defer cleanup1()

	entity := datalog.NewIdentity("avet-import")
	attr := datalog.NewKeyword(":test/content")
	value := longString("AVET after import test", 500)

	tx := db1.NewTransaction()
	tx.Add(entity, attr, value)
	_, err := tx.Commit()
	require.NoError(t, err)

	// Export compressed
	var buf bytes.Buffer
	err = db1.Export(&buf, ExportOptions{Compressed: true})
	require.NoError(t, err)

	// Import into fresh compressed DB
	dir2, err := os.MkdirTemp("", "avet-import-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)

	db2, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:                 dir2,
		CompressionThreshold: 256,
	})
	require.NoError(t, err)
	defer db2.Close()

	err = db2.Import(strings.NewReader(buf.String()))
	require.NoError(t, err)

	// AVET lookup: search by value
	matcher := NewBadgerMatcher(db2.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Constant{Value: value}, // bound V → AVET
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	iter := results.Iterator()
	require.True(t, iter.Next(), "AVET lookup should find entity after import")
	gotEntity := iter.Tuple()[0].(datalog.Identity)
	assert.Equal(t, entity.L85(), gotEntity.L85())
}

func TestImport_Tier3_InlineExport(t *testing.T) {
	// Write a Tier 3 value (blob in storage), export (inline in EDN),
	// import to new DB, verify it reads back correctly
	db1, cleanup1 := newCompressedDB(t)
	defer cleanup1()

	entity := datalog.NewIdentity("tier3-export")
	attr := datalog.NewKeyword(":test/big")
	bigValue := string(makeTier3Data(100000))

	vType, _, _ := datalog.EncodeValue(bigValue, 256)
	if vType != datalog.TypeHashedString {
		t.Skipf("value doesn't reach Tier 3 (type 0x%02x)", vType)
	}

	tx := db1.NewTransaction()
	tx.Add(entity, attr, bigValue)
	_, err := tx.Commit()
	require.NoError(t, err)

	// Export compressed — Tier 3 value should be inline #lzj
	var buf bytes.Buffer
	err = db1.Export(&buf, ExportOptions{Compressed: true})
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "#lzj")

	// Import into fresh compressed DB
	dir2, err := os.MkdirTemp("", "tier3-export-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)

	db2, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:                 dir2,
		CompressionThreshold: 256,
	})
	require.NoError(t, err)
	defer db2.Close()

	err = db2.Import(strings.NewReader(buf.String()))
	require.NoError(t, err)

	// Read back
	matcher := NewBadgerMatcher(db2.Store())
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
	require.True(t, iter.Next(), "should find Tier 3 value after import")
	got := iter.Tuple()[0].(string)
	assert.Equal(t, bigValue, got, "Tier 3 value should survive export→import round-trip")
}

func TestImport_ValueEquality_AcrossBoundary(t *testing.T) {
	// Write same value to DB1 (compressed) and DB2 (via #lzj import).
	// Query both — values should be equal.
	value := longString("cross-boundary equality", 500)
	entity := datalog.NewIdentity("eq-entity")
	attr := datalog.NewKeyword(":test/content")

	// DB1: write directly
	db1, cleanup1 := newCompressedDB(t)
	defer cleanup1()

	tx := db1.NewTransaction()
	tx.Add(entity, attr, value)
	_, err := tx.Commit()
	require.NoError(t, err)

	// Export compressed from DB1
	var buf bytes.Buffer
	err = db1.Export(&buf, ExportOptions{Compressed: true})
	require.NoError(t, err)

	// DB2: import from #lzj EDN
	dir2, err := os.MkdirTemp("", "eq-import-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)

	db2, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:                 dir2,
		CompressionThreshold: 256,
	})
	require.NoError(t, err)
	defer db2.Close()

	err = db2.Import(strings.NewReader(buf.String()))
	require.NoError(t, err)

	// Read from both
	readValue := func(db *Database) string {
		matcher := NewBadgerMatcher(db.Store())
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
		return iter.Tuple()[0].(string)
	}

	v1 := readValue(db1)
	v2 := readValue(db2)
	assert.Equal(t, v1, v2, "values should be equal across export→import boundary")
	assert.Equal(t, value, v1)
	assert.Equal(t, value, v2)
}

func TestImport_MalformedLZJ(t *testing.T) {
	dir, err := os.MkdirTemp("", "malformed-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	// Malformed #lzj — invalid L85
	badEDN := `[#identity "0$&1Jt:M;j(7P!6s0BvD4k!,!" :test/x #lzj "!!!INVALID!!!" [1 1] :op/none]`
	err = db.Import(strings.NewReader(badEDN))
	assert.Error(t, err, "import of malformed #lzj should fail")
}
