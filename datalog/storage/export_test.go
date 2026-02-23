package storage

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/codec"
	"github.com/wbrown/janus-datalog/datalog/edn"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Helper to create a temp BadgerDB database
func createTempDatabase(t *testing.T) *Database {
	dir := t.TempDir()
	db, err := NewDatabase(dir)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// Helper to create keywords
func kw(s string) datalog.Keyword {
	return datalog.NewKeyword(s)
}

// =============================================================================
// Unit Tests: Value EDN Formatting
// =============================================================================

func TestFormatValueEDN_String(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple", "hello", `"hello"`},
		{"empty", "", `""`},
		{"with quotes", `say "hello"`, `"say \"hello\""`},
		{"with newline", "line1\nline2", `"line1\nline2"`},
		{"with tab", "col1\tcol2", `"col1\tcol2"`},
		{"with backslash", `path\to\file`, `"path\\to\\file"`},
		{"unicode", "日本語", `"日本語"`},
		{"mixed escapes", "a\"b\\c\nd", `"a\"b\\c\nd"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatValueEDN(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatValueEDN_Int(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected string
	}{
		{"zero", 0, "0"},
		{"positive", 42, "42"},
		{"negative", -42, "-42"},
		{"max", math.MaxInt64, "9223372036854775807"},
		{"min", math.MinInt64, "-9223372036854775808"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatValueEDN(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatValueEDN_Float(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected string
	}{
		{"zero", 0.0, "0.0"},
		{"pi", 3.14159, "3.14159"},
		{"negative", -2.718, "-2.718"},
		{"integer float", 42.0, "42.0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatValueEDN(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}

	// Scientific notation - just verify it's parseable
	t.Run("scientific", func(t *testing.T) {
		result := FormatValueEDN(1e10)
		assert.True(t, strings.Contains(result, "e") || strings.Contains(result, "E") || result == "10000000000.0")
	})
}

func TestFormatValueEDN_Bool(t *testing.T) {
	assert.Equal(t, "true", FormatValueEDN(true))
	assert.Equal(t, "false", FormatValueEDN(false))
}

func TestFormatValueEDN_Time(t *testing.T) {
	// UTC time
	utc := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	result := FormatValueEDN(utc)
	assert.Equal(t, `#inst "2025-01-15T10:30:00Z"`, result)

	// With nanoseconds
	withNanos := time.Date(2025, 1, 15, 10, 30, 0, 123456789, time.UTC)
	result = FormatValueEDN(withNanos)
	assert.Equal(t, `#inst "2025-01-15T10:30:00.123456789Z"`, result)

	// Non-UTC should be converted
	loc, _ := time.LoadLocation("America/New_York")
	nonUTC := time.Date(2025, 1, 15, 10, 30, 0, 0, loc)
	result = FormatValueEDN(nonUTC)
	assert.Contains(t, result, "#inst")
	assert.Contains(t, result, "Z") // Should be UTC
}

func TestFormatValueEDN_Bytes(t *testing.T) {
	tests := []struct {
		name          string
		input         []byte
		expectedChars int // expected L85 character count
	}{
		{"empty", []byte{}, 0},
		{"1 byte", []byte{0x42}, 2},
		{"2 bytes", []byte{0x42, 0x43}, 3},
		{"3 bytes", []byte{0x42, 0x43, 0x44}, 4},
		{"4 bytes", []byte{0x42, 0x43, 0x44, 0x45}, 5},
		{"5 bytes", []byte{0x42, 0x43, 0x44, 0x45, 0x46}, 7},
		{"7 bytes", []byte{0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48}, 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatValueEDN(tt.input)
			assert.True(t, strings.HasPrefix(result, `#bytes "`))
			assert.True(t, strings.HasSuffix(result, `"`))

			// Extract the L85 string
			l85 := result[8 : len(result)-1]
			assert.Equal(t, tt.expectedChars, len(l85))

			// Verify round-trip
			if len(l85) > 0 {
				decoded, err := codec.DecodeL85(l85)
				require.NoError(t, err)
				assert.Equal(t, tt.input, decoded)
			}
		})
	}
}

func TestFormatValueEDN_Identity(t *testing.T) {
	id := datalog.NewIdentity("test-entity")
	result := FormatValueEDN(id)
	assert.True(t, strings.HasPrefix(result, `#identity "`))
	assert.True(t, strings.HasSuffix(result, `"`))

	// L85 hash should be 25 chars
	l85 := result[11 : len(result)-1]
	assert.Equal(t, 25, len(l85))
}

func TestFormatValueEDN_Keyword(t *testing.T) {
	tests := []struct {
		name     string
		input    datalog.Keyword
		expected string
	}{
		{"qualified", kw(":person/name"), ":person/name"},
		{"simple", kw(":status"), ":status"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatValueEDN(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatValueEDN_Symbol(t *testing.T) {
	sym := datalog.NewSymbol("my-symbol")
	result := FormatValueEDN(sym)
	assert.Equal(t, "my-symbol", result)
}

func TestFormatValueEDN_Nil(t *testing.T) {
	assert.Equal(t, "nil", FormatValueEDN(nil))
}

func TestFormatIdentityEDN(t *testing.T) {
	t.Run("valid identity", func(t *testing.T) {
		id := datalog.NewIdentity("test")
		result := FormatIdentityEDN(id)
		assert.True(t, strings.HasPrefix(result, `#identity "`))
		// L85 is 25 chars for 20 bytes
		l85 := result[11 : len(result)-1]
		assert.Equal(t, 25, len(l85))
	})

	t.Run("nil identity", func(t *testing.T) {
		result := FormatIdentityEDN(nil)
		assert.Equal(t, "nil", result)
	})
}

// =============================================================================
// Unit Tests: Value EDN Parsing
// =============================================================================

func TestParseValueNode_String(t *testing.T) {
	tests := []string{"hello", "", `with "quotes"`, "with\nnewline", "日本語"}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			formatted := FormatValueEDN(input)
			// Parse just the string value
			node, err := parseEDNValue(formatted)
			require.NoError(t, err)
			result, err := ParseValueNode(node)
			require.NoError(t, err)
			assert.Equal(t, input, result)
		})
	}
}

func TestParseValueNode_Int(t *testing.T) {
	tests := []int64{0, 42, -42, math.MaxInt64, math.MinInt64}

	for _, input := range tests {
		t.Run("", func(t *testing.T) {
			formatted := FormatValueEDN(input)
			node, err := parseEDNValue(formatted)
			require.NoError(t, err)
			result, err := ParseValueNode(node)
			require.NoError(t, err)
			assert.Equal(t, input, result)
		})
	}
}

func TestParseValueNode_Float(t *testing.T) {
	tests := []float64{0.0, 3.14159, -2.718}

	for _, input := range tests {
		t.Run("", func(t *testing.T) {
			formatted := FormatValueEDN(input)
			node, err := parseEDNValue(formatted)
			require.NoError(t, err)
			result, err := ParseValueNode(node)
			require.NoError(t, err)
			assert.InDelta(t, input, result.(float64), 0.0001)
		})
	}
}

func TestParseValueNode_Bool(t *testing.T) {
	for _, input := range []bool{true, false} {
		t.Run("", func(t *testing.T) {
			formatted := FormatValueEDN(input)
			node, err := parseEDNValue(formatted)
			require.NoError(t, err)
			result, err := ParseValueNode(node)
			require.NoError(t, err)
			assert.Equal(t, input, result)
		})
	}
}

func TestParseValueNode_Time(t *testing.T) {
	t.Run("without nanoseconds", func(t *testing.T) {
		input := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
		formatted := FormatValueEDN(input)
		node, err := parseEDNValue(formatted)
		require.NoError(t, err)
		result, err := ParseValueNode(node)
		require.NoError(t, err)
		assert.Equal(t, input, result)
	})

	t.Run("with nanoseconds", func(t *testing.T) {
		input := time.Date(2025, 1, 15, 10, 30, 0, 123456789, time.UTC)
		formatted := FormatValueEDN(input)
		node, err := parseEDNValue(formatted)
		require.NoError(t, err)
		result, err := ParseValueNode(node)
		require.NoError(t, err)
		assert.Equal(t, input, result)
	})
}

func TestParseValueNode_Bytes(t *testing.T) {
	tests := [][]byte{
		{},
		{0x42},
		{0x42, 0x43},
		{0x42, 0x43, 0x44},
		{0x42, 0x43, 0x44, 0x45},
		{0x42, 0x43, 0x44, 0x45, 0x46},
		{0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48},
	}

	for _, input := range tests {
		t.Run("", func(t *testing.T) {
			formatted := FormatValueEDN(input)
			node, err := parseEDNValue(formatted)
			require.NoError(t, err)
			result, err := ParseValueNode(node)
			require.NoError(t, err)
			assert.Equal(t, input, result)
		})
	}
}

func TestParseValueNode_Identity(t *testing.T) {
	input := datalog.NewIdentity("test-entity")
	formatted := FormatValueEDN(input)
	node, err := parseEDNValue(formatted)
	require.NoError(t, err)
	result, err := ParseValueNode(node)
	require.NoError(t, err)

	resultID := result.(datalog.Identity)
	assert.Equal(t, input.Hash(), resultID.Hash())
}

func TestParseValueNode_Keyword(t *testing.T) {
	input := kw(":person/name")
	formatted := FormatValueEDN(input)
	node, err := parseEDNValue(formatted)
	require.NoError(t, err)
	result, err := ParseValueNode(node)
	require.NoError(t, err)
	assert.Equal(t, input, result)
}

func TestParseValueNode_Errors(t *testing.T) {
	t.Run("unknown tag", func(t *testing.T) {
		node, err := parseEDNValue(`#unknown "value"`)
		require.NoError(t, err)
		_, err = ParseValueNode(node)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown tag")
	})

	t.Run("invalid L85 in bytes", func(t *testing.T) {
		// Use a space character which is NOT in the L85 alphabet
		node, err := parseEDNValue(`#bytes "invalid with space"`)
		require.NoError(t, err)
		_, err = ParseValueNode(node)
		assert.Error(t, err)
	})

	t.Run("invalid timestamp", func(t *testing.T) {
		node, err := parseEDNValue(`#inst "not-a-date"`)
		require.NoError(t, err)
		_, err = ParseValueNode(node)
		assert.Error(t, err)
	})
}

// =============================================================================
// Unit Tests: Datom EDN Round-Trip
// =============================================================================

func TestFormatDatomEDN(t *testing.T) {
	id := datalog.NewIdentity("entity1")
	datom := &datalog.Datom{
		E:  id,
		A:  kw(":person/name"),
		V:  "Alice",
		Tx: datalog.ElementID{Lamport: 12345, ReplicaID: 1},
	}

	result := FormatDatomEDN(datom)
	assert.True(t, strings.HasPrefix(result, "[#identity "))
	assert.Contains(t, result, ":person/name")
	assert.Contains(t, result, `"Alice"`)
	assert.Contains(t, result, "[12345 1]")
	assert.Contains(t, result, ":op/none")
	assert.True(t, strings.HasSuffix(result, ":op/none]"))
}

func TestParseDatomEDN(t *testing.T) {
	id := datalog.NewIdentity("entity1")
	original := &datalog.Datom{
		E:  id,
		A:  kw(":person/name"),
		V:  "Alice",
		Tx: datalog.ElementID{Lamport: 12345, ReplicaID: 1},
	}

	formatted := FormatDatomEDN(original)
	parsed, err := ParseDatomEDN(formatted)
	require.NoError(t, err)

	assert.Equal(t, original.E.Hash(), parsed.E.Hash())
	assert.Equal(t, original.A, parsed.A)
	assert.Equal(t, original.V, parsed.V)
	assert.Equal(t, original.Tx, parsed.Tx)
}

func TestDatomEDN_RoundTrip(t *testing.T) {
	id1 := datalog.NewIdentity("entity1")
	id2 := datalog.NewIdentity("entity2")
	now := time.Now().UTC().Truncate(time.Nanosecond)

	testCases := []struct {
		name  string
		datom datalog.Datom
	}{
		{"string", datalog.Datom{E: id1, A: kw(":test/string"), V: "hello", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}}},
		{"int", datalog.Datom{E: id1, A: kw(":test/int"), V: int64(42), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}}},
		{"float", datalog.Datom{E: id1, A: kw(":test/float"), V: 3.14, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}}},
		{"bool-true", datalog.Datom{E: id1, A: kw(":test/bool"), V: true, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}}},
		{"bool-false", datalog.Datom{E: id1, A: kw(":test/bool"), V: false, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}}},
		{"time", datalog.Datom{E: id1, A: kw(":test/time"), V: now, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}}},
		{"bytes", datalog.Datom{E: id1, A: kw(":test/bytes"), V: []byte{1, 2, 3, 4, 5}, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}}},
		{"ref", datalog.Datom{E: id1, A: kw(":test/ref"), V: id2, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}}},
		{"keyword", datalog.Datom{E: id1, A: kw(":test/kw"), V: kw(":status/active"), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}}},
		{"symbol", datalog.Datom{E: id1, A: kw(":test/sym"), V: datalog.NewSymbol("my-symbol"), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			formatted := FormatDatomEDN(&tc.datom)
			parsed, err := ParseDatomEDN(formatted)
			require.NoError(t, err, "failed to parse: %s", formatted)

			assert.Equal(t, tc.datom.E.Hash(), parsed.E.Hash(), "entity mismatch")
			assert.Equal(t, tc.datom.A, parsed.A, "attribute mismatch")
			assert.Equal(t, tc.datom.Tx, parsed.Tx, "tx mismatch")

			// Value comparison depends on type
			switch expected := tc.datom.V.(type) {
			case time.Time:
				actual := parsed.V.(time.Time)
				assert.True(t, expected.Equal(actual), "time mismatch: %v vs %v", expected, actual)
			case []byte:
				actual := parsed.V.([]byte)
				assert.Equal(t, expected, actual, "bytes mismatch")
			case datalog.Identity:
				actual := parsed.V.(datalog.Identity)
				assert.Equal(t, expected.Hash(), actual.Hash(), "identity ref mismatch")
			default:
				assert.Equal(t, tc.datom.V, parsed.V, "value mismatch")
			}
		})
	}
}

func TestParseDatomEDN_Errors(t *testing.T) {
	// Use a valid L85 identity for tests that need to get past identity parsing
	validID := datalog.NewIdentity("test").L85()

	tests := []struct {
		name  string
		input string
		error string
	}{
		{"not vector", `"just a string"`, "expected vector"},
		{"too few elements", `[#identity "` + validID + `" :attr "val"]`, "expected 4-6 elements"},
		{"too many elements", `[#identity "` + validID + `" :attr "val" 1 :op/none [0 0] extra]`, "expected 4-6 elements"},
		{"invalid attribute", `[#identity "` + validID + `" "not-keyword" "val" 1]`, "invalid attribute"},
		{"invalid tx", `[#identity "` + validID + `" :attr "val" "not-int"]`, "invalid tx"},
		{"invalid identity L85", `[#identity "abc" :attr "val" 1]`, "invalid L85"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseDatomEDN(tt.input)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.error)
		})
	}
}

// =============================================================================
// Integration Tests: Database Export/Import
// =============================================================================

func TestExport_EmptyDatabase(t *testing.T) {
	db := createTempDatabase(t)

	var buf bytes.Buffer
	err := db.Export(&buf)
	require.NoError(t, err)

	assert.Empty(t, buf.String())
}

func TestExport_SingleDatom(t *testing.T) {
	db := createTempDatabase(t)

	// Add a datom
	tx := db.NewTransaction()
	id := datalog.NewIdentity("entity1")
	require.NoError(t, tx.Add(id, kw(":person/name"), "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	var buf bytes.Buffer
	err = db.Export(&buf)
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	// Expect 2 lines: user datom + :db/txInstant transaction metadata
	assert.Equal(t, 2, len(lines))

	// Verify our datom is present
	found := false
	for _, line := range lines {
		if strings.Contains(line, ":person/name") && strings.Contains(line, `"Alice"`) {
			found = true
			break
		}
	}
	assert.True(t, found, "expected to find :person/name datom")
}

func TestExport_MultipleDatoms(t *testing.T) {
	db := createTempDatabase(t)

	// Add multiple datoms with various types
	tx := db.NewTransaction()
	id := datalog.NewIdentity("entity1")
	require.NoError(t, tx.Add(id, kw(":test/string"), "hello"))
	require.NoError(t, tx.Add(id, kw(":test/int"), int64(42)))
	require.NoError(t, tx.Add(id, kw(":test/float"), 3.14))
	require.NoError(t, tx.Add(id, kw(":test/bool"), true))
	require.NoError(t, tx.Add(id, kw(":test/time"), time.Now().UTC()))
	_, err := tx.Commit()
	require.NoError(t, err)

	var buf bytes.Buffer
	err = db.Export(&buf)
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	// Expect 6 lines: 5 user datoms + 1 :db/txInstant
	assert.Equal(t, 6, len(lines))

	// Verify each line is parseable
	for _, line := range lines {
		_, err := ParseDatomEDN(line)
		require.NoError(t, err, "failed to parse: %s", line)
	}
}

func TestImport_EmptyFile(t *testing.T) {
	db := createTempDatabase(t)

	err := db.Import(strings.NewReader(""))
	require.NoError(t, err)

	// Verify database is empty
	var buf bytes.Buffer
	err = db.Export(&buf)
	require.NoError(t, err)
	assert.Empty(t, buf.String())
}

func TestImport_WithComments(t *testing.T) {
	db := createTempDatabase(t)

	id := datalog.NewIdentity("test")
	l85 := id.L85()

	input := `; This is a comment
[#identity "` + l85 + `" :test/val 42 1]

; Another comment
[#identity "` + l85 + `" :test/name "Alice" 1]
`

	err := db.Import(strings.NewReader(input))
	require.NoError(t, err)

	var buf bytes.Buffer
	err = db.Export(&buf)
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	// We imported 2 datoms, but the database already had a tx, so we may have
	// additional :db/txInstant datoms. Just verify our datoms are present.
	assert.GreaterOrEqual(t, len(lines), 2)

	// Verify our datoms are present
	foundVal := false
	foundName := false
	for _, line := range lines {
		if strings.Contains(line, ":test/val") {
			foundVal = true
		}
		if strings.Contains(line, ":test/name") {
			foundName = true
		}
	}
	assert.True(t, foundVal, "expected :test/val datom")
	assert.True(t, foundName, "expected :test/name datom")
}

func TestImport_BatchBoundary(t *testing.T) {
	db := createTempDatabase(t)

	// Generate exactly 5001 datoms (triggers second batch)
	var sb strings.Builder
	id := datalog.NewIdentity("entity")
	l85 := id.L85()

	for i := 0; i < 5001; i++ {
		sb.WriteString(`[#identity "` + l85 + `" :test/idx ` + string(rune('0'+i%10)) + ` 1]`)
		sb.WriteString("\n")
	}

	err := db.Import(strings.NewReader(sb.String()))
	require.NoError(t, err)
}

func TestImport_LargeFile(t *testing.T) {
	db := createTempDatabase(t)

	// Generate 10000 datoms
	var sb strings.Builder
	for i := 0; i < 10000; i++ {
		id := datalog.NewIdentity("entity-" + string(rune(i)))
		l85 := id.L85()
		sb.WriteString(`[#identity "` + l85 + `" :test/idx `)
		sb.WriteString(string(rune('0' + i%10)))
		sb.WriteString(` 1]`)
		sb.WriteString("\n")
	}

	err := db.Import(strings.NewReader(sb.String()))
	require.NoError(t, err)
}

func TestImport_Errors(t *testing.T) {
	validID := datalog.NewIdentity("test").L85()

	t.Run("malformed EDN", func(t *testing.T) {
		db := createTempDatabase(t)
		input := `[#identity "` + validID + `" :test/val 1 1]
[#identity "` + validID + `" :test/val 2 1]
not valid EDN
[#identity "` + validID + `" :test/val 3 1]
`
		err := db.Import(strings.NewReader(input))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "line 3")
	})

	t.Run("wrong element count", func(t *testing.T) {
		db := createTempDatabase(t)
		input := `[#identity "` + validID + `" :test/val]`
		err := db.Import(strings.NewReader(input))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected 4-6 elements")
	})

	t.Run("invalid L85 identity", func(t *testing.T) {
		db := createTempDatabase(t)
		input := `[#identity "abc" :test/val 42 1]`
		err := db.Import(strings.NewReader(input))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid L85")
	})
}

func TestDatabaseRoundTrip(t *testing.T) {
	db1 := createTempDatabase(t)

	// Add diverse data
	tx := db1.NewTransaction()
	id1 := datalog.NewIdentity("person1")
	id2 := datalog.NewIdentity("person2")
	now := time.Now().UTC().Truncate(time.Nanosecond)

	require.NoError(t, tx.Add(id1, kw(":person/name"), "Alice"))
	require.NoError(t, tx.Add(id1, kw(":person/age"), int64(30)))
	require.NoError(t, tx.Add(id1, kw(":person/score"), 95.5))
	require.NoError(t, tx.Add(id1, kw(":person/active"), true))
	require.NoError(t, tx.Add(id1, kw(":person/created"), now))
	require.NoError(t, tx.Add(id1, kw(":person/data"), []byte{1, 2, 3}))
	require.NoError(t, tx.Add(id1, kw(":person/friend"), id2))
	require.NoError(t, tx.Add(id2, kw(":person/name"), "Bob"))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Export
	var buf bytes.Buffer
	err = db1.Export(&buf)
	require.NoError(t, err)

	// Import into fresh database
	db2 := createTempDatabase(t)
	err = db2.Import(strings.NewReader(buf.String()))
	require.NoError(t, err)

	// Export db2
	var buf2 bytes.Buffer
	err = db2.Export(&buf2)
	require.NoError(t, err)

	// Compare exports
	lines1 := strings.Split(strings.TrimSpace(buf.String()), "\n")
	lines2 := strings.Split(strings.TrimSpace(buf2.String()), "\n")
	assert.Equal(t, len(lines1), len(lines2))
}

func TestDatabaseRoundTrip_Deterministic(t *testing.T) {
	db1 := createTempDatabase(t)

	// Add data
	tx := db1.NewTransaction()
	id := datalog.NewIdentity("entity1")
	require.NoError(t, tx.Add(id, kw(":test/a"), "value-a"))
	require.NoError(t, tx.Add(id, kw(":test/b"), "value-b"))
	require.NoError(t, tx.Add(id, kw(":test/c"), "value-c"))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Export → file1
	var buf1 bytes.Buffer
	err = db1.Export(&buf1)
	require.NoError(t, err)

	// Import → db2
	db2 := createTempDatabase(t)
	err = db2.Import(strings.NewReader(buf1.String()))
	require.NoError(t, err)

	// Export db2 → file2
	var buf2 bytes.Buffer
	err = db2.Export(&buf2)
	require.NoError(t, err)

	// Compare byte-for-byte
	assert.Equal(t, buf1.String(), buf2.String())
}

func TestExportImport_PreservesTxIDs(t *testing.T) {
	db1 := createTempDatabase(t)

	// Create datoms with specific tx IDs by doing multiple commits
	id := datalog.NewIdentity("entity1")

	tx1 := db1.NewTransaction()
	require.NoError(t, tx1.Add(id, kw(":test/a"), "value-a"))
	txID1, err := tx1.Commit()
	require.NoError(t, err)

	tx2 := db1.NewTransaction()
	require.NoError(t, tx2.Add(id, kw(":test/b"), "value-b"))
	txID2, err := tx2.Commit()
	require.NoError(t, err)

	// Export
	var buf bytes.Buffer
	err = db1.Export(&buf)
	require.NoError(t, err)

	// Import into fresh database
	db2 := createTempDatabase(t)
	err = db2.Import(strings.NewReader(buf.String()))
	require.NoError(t, err)

	// Export and verify tx IDs are preserved
	var buf2 bytes.Buffer
	err = db2.Export(&buf2)
	require.NoError(t, err)

	// Parse and check tx IDs
	lines := strings.Split(strings.TrimSpace(buf2.String()), "\n")
	var foundTx1, foundTx2 bool
	for _, line := range lines {
		datom, err := ParseDatomEDN(line)
		require.NoError(t, err)
		if datom.Tx == txID1 {
			foundTx1 = true
		}
		if datom.Tx == txID2 {
			foundTx2 = true
		}
	}
	assert.True(t, foundTx1, "txID1 not preserved")
	assert.True(t, foundTx2, "txID2 not preserved")
}

// =============================================================================
// Unit Tests: ElementID EDN Formatting and Parsing
// =============================================================================

func TestFormatValueEDN_ElementID(t *testing.T) {
	tests := []struct {
		name     string
		input    datalog.ElementID
		expected string
	}{
		{"zero/HEAD", datalog.ElementID{Lamport: 0, ReplicaID: 0}, "#eid [0 0]"},
		{"simple", datalog.ElementID{Lamport: 1234, ReplicaID: 5678}, "#eid [1234 5678]"},
		{"large", datalog.ElementID{Lamport: 1706745600000000000, ReplicaID: 12345678901234567}, "#eid [1706745600000000000 12345678901234567]"},
		{"max", datalog.ElementID{Lamport: ^uint64(0), ReplicaID: ^uint64(0)}, "#eid [18446744073709551615 18446744073709551615]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatValueEDN(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseValueNode_ElementID(t *testing.T) {
	tests := []datalog.ElementID{
		{Lamport: 0, ReplicaID: 0},
		{Lamport: 1, ReplicaID: 1},
		{Lamport: 1234, ReplicaID: 5678},
		{Lamport: 1706745600000000000, ReplicaID: 12345678901234567},
	}

	for _, input := range tests {
		t.Run("", func(t *testing.T) {
			formatted := FormatValueEDN(input)
			node, err := parseEDNValue(formatted)
			require.NoError(t, err)
			result, err := ParseValueNode(node)
			require.NoError(t, err)

			got, ok := result.(datalog.ElementID)
			require.True(t, ok, "expected ElementID, got %T", result)
			assert.Equal(t, input.Lamport, got.Lamport)
			assert.Equal(t, input.ReplicaID, got.ReplicaID)
		})
	}
}

func TestParseValueNode_ElementID_Errors(t *testing.T) {
	tests := []struct {
		name  string
		input string
		error string
	}{
		{"not vector", `#eid "string"`, "requires [lamport replica-id] vector"},
		{"one element", `#eid [1]`, "requires exactly 2 elements"},
		{"three elements", `#eid [1 2 3]`, "requires exactly 2 elements"},
		{"lamport not int", `#eid ["str" 1]`, "lamport must be integer"},
		{"replica not int", `#eid [1 "str"]`, "replica-id must be integer"},
		{"negative lamport", `#eid [-1 1]`, "invalid lamport value"},    // ParseUint rejects negative numbers
		{"negative replica", `#eid [1 -1]`, "invalid replica-id value"}, // ParseUint rejects negative numbers
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := parseEDNValue(tt.input)
			require.NoError(t, err)
			_, err = ParseValueNode(node)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.error)
		})
	}
}

func TestDatomEDN_RoundTrip_ElementID(t *testing.T) {
	id := datalog.NewIdentity("entity1")
	eid := datalog.ElementID{Lamport: 1234, ReplicaID: 5678}

	datom := datalog.Datom{
		E:  id,
		A:  kw(":test/element-id"),
		V:  eid,
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
	}

	formatted := FormatDatomEDN(&datom)
	assert.Contains(t, formatted, "#eid")
	assert.Contains(t, formatted, "1234")
	assert.Contains(t, formatted, "5678")

	parsed, err := ParseDatomEDN(formatted)
	require.NoError(t, err)

	assert.Equal(t, datom.E.Hash(), parsed.E.Hash())
	assert.Equal(t, datom.A, parsed.A)
	assert.Equal(t, datom.Tx, parsed.Tx)

	gotEID, ok := parsed.V.(datalog.ElementID)
	require.True(t, ok, "expected ElementID, got %T", parsed.V)
	assert.Equal(t, eid.Lamport, gotEID.Lamport)
	assert.Equal(t, eid.ReplicaID, gotEID.ReplicaID)
}

// =============================================================================
// Unit Tests: CRDTOp EDN Formatting and Parsing
// =============================================================================

func TestFormatCRDTOpEDN(t *testing.T) {
	tests := []struct {
		op       datalog.CRDTOp
		expected string
	}{
		{datalog.OpNone, ":op/none"},
		{datalog.OpCRDTAdd, ":op/add"},
		{datalog.OpCRDTRemove, ":op/remove"},
		{datalog.OpRGAInsert, ":op/rga-insert"},
		{datalog.OpRGATombstone, ":op/rga-tombstone"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := FormatCRDTOpEDN(tt.op)
			assert.Equal(t, tt.expected, result)
		})
	}

	// Unknown op produces a fallback
	t.Run("unknown", func(t *testing.T) {
		result := FormatCRDTOpEDN(datalog.CRDTOp(99))
		assert.Equal(t, ":op/unknown-99", result)
	})
}

func TestParseCRDTOpNode(t *testing.T) {
	tests := []struct {
		keyword  string
		expected datalog.CRDTOp
	}{
		{":op/none", datalog.OpNone},
		{":op/add", datalog.OpCRDTAdd},
		{":op/remove", datalog.OpCRDTRemove},
		{":op/rga-insert", datalog.OpRGAInsert},
		{":op/rga-tombstone", datalog.OpRGATombstone},
	}

	for _, tt := range tests {
		t.Run(tt.keyword, func(t *testing.T) {
			node, err := parseEDNValue(tt.keyword)
			require.NoError(t, err)
			result, err := ParseCRDTOpNode(node)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}

	t.Run("unknown keyword", func(t *testing.T) {
		node, err := parseEDNValue(":op/bogus")
		require.NoError(t, err)
		_, err = ParseCRDTOpNode(node)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown op keyword")
	})

	t.Run("not a keyword", func(t *testing.T) {
		node, err := parseEDNValue(`"not-a-keyword"`)
		require.NoError(t, err)
		_, err = ParseCRDTOpNode(node)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected keyword")
	})
}

// =============================================================================
// Unit Tests: Datom EDN Round-Trip with Op and AfterRef
// =============================================================================

func TestDatomEDN_RoundTrip_WithOp(t *testing.T) {
	id := datalog.NewIdentity("entity1")
	tx := datalog.ElementID{Lamport: 100, ReplicaID: 1}

	ops := []struct {
		name string
		op   datalog.CRDTOp
	}{
		{"OpNone", datalog.OpNone},
		{"OpCRDTAdd", datalog.OpCRDTAdd},
		{"OpCRDTRemove", datalog.OpCRDTRemove},
	}

	for _, tt := range ops {
		t.Run(tt.name, func(t *testing.T) {
			datom := &datalog.Datom{
				E:  id,
				A:  kw(":test/attr"),
				V:  "value",
				Tx: tx,
				Op: tt.op,
			}

			formatted := FormatDatomEDN(datom)
			assert.Contains(t, formatted, FormatCRDTOpEDN(tt.op))

			parsed, err := ParseDatomEDN(formatted)
			require.NoError(t, err)

			assert.Equal(t, datom.E.Hash(), parsed.E.Hash())
			assert.Equal(t, datom.A, parsed.A)
			assert.Equal(t, datom.V, parsed.V)
			assert.Equal(t, datom.Tx, parsed.Tx)
			assert.Equal(t, tt.op, parsed.Op)
			assert.Equal(t, datalog.ElementID{}, parsed.AfterRef)
		})
	}
}

func TestDatomEDN_RoundTrip_WithAfterRef(t *testing.T) {
	id := datalog.NewIdentity("entity1")
	tx := datalog.ElementID{Lamport: 100, ReplicaID: 1}
	afterRef := datalog.ElementID{Lamport: 50, ReplicaID: 1}

	ops := []struct {
		name string
		op   datalog.CRDTOp
	}{
		{"OpRGAInsert", datalog.OpRGAInsert},
		{"OpRGATombstone", datalog.OpRGATombstone},
	}

	for _, tt := range ops {
		t.Run(tt.name, func(t *testing.T) {
			datom := &datalog.Datom{
				E:        id,
				A:        kw(":test/vec"),
				V:        "item",
				Tx:       tx,
				Op:       tt.op,
				AfterRef: afterRef,
			}

			formatted := FormatDatomEDN(datom)
			assert.Contains(t, formatted, FormatCRDTOpEDN(tt.op))
			assert.Contains(t, formatted, "[50 1]")

			parsed, err := ParseDatomEDN(formatted)
			require.NoError(t, err)

			assert.Equal(t, datom.E.Hash(), parsed.E.Hash())
			assert.Equal(t, datom.A, parsed.A)
			assert.Equal(t, datom.V, parsed.V)
			assert.Equal(t, datom.Tx, parsed.Tx)
			assert.Equal(t, tt.op, parsed.Op)
			assert.Equal(t, afterRef, parsed.AfterRef)
		})
	}
}

func TestParseDatomEDN_BackwardCompat(t *testing.T) {
	// Old 4-element format should still parse, with Op=OpNone and zero AfterRef
	validID := datalog.NewIdentity("test").L85()

	t.Run("4-element with int tx (oldest format)", func(t *testing.T) {
		input := `[#identity "` + validID + `" :test/attr "hello" 42]`
		datom, err := ParseDatomEDN(input)
		require.NoError(t, err)
		assert.Equal(t, kw(":test/attr"), datom.A)
		assert.Equal(t, "hello", datom.V)
		assert.Equal(t, uint64(42), datom.Tx.Lamport)
		assert.Equal(t, datalog.OpNone, datom.Op)
		assert.Equal(t, datalog.ElementID{}, datom.AfterRef)
	})

	t.Run("4-element with vector tx", func(t *testing.T) {
		input := `[#identity "` + validID + `" :test/attr "hello" [100 1]]`
		datom, err := ParseDatomEDN(input)
		require.NoError(t, err)
		assert.Equal(t, datalog.ElementID{Lamport: 100, ReplicaID: 1}, datom.Tx)
		assert.Equal(t, datalog.OpNone, datom.Op)
		assert.Equal(t, datalog.ElementID{}, datom.AfterRef)
	})

	t.Run("5-element with op", func(t *testing.T) {
		input := `[#identity "` + validID + `" :test/attr "hello" [100 1] :op/add]`
		datom, err := ParseDatomEDN(input)
		require.NoError(t, err)
		assert.Equal(t, datalog.OpCRDTAdd, datom.Op)
		assert.Equal(t, datalog.ElementID{}, datom.AfterRef)
	})

	t.Run("6-element with op and after-ref", func(t *testing.T) {
		input := `[#identity "` + validID + `" :test/attr "hello" [100 1] :op/rga-insert [50 1]]`
		datom, err := ParseDatomEDN(input)
		require.NoError(t, err)
		assert.Equal(t, datalog.OpRGAInsert, datom.Op)
		assert.Equal(t, datalog.ElementID{Lamport: 50, ReplicaID: 1}, datom.AfterRef)
	})
}

// =============================================================================
// Integration Tests: Database Round-Trip with CRDT Ops
// =============================================================================

func TestDatabaseRoundTrip_CRDTOps(t *testing.T) {
	// Create DB with cardinality-many schema
	s, err := schema.NewBuilder().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)

	db1, err := NewDatabaseWithSchema(t.TempDir(), s)
	require.NoError(t, err)
	defer db1.Close()

	id := datalog.NewIdentity("entity1")
	tags := kw(":person/tags")

	// Add values
	tx := db1.NewTransaction()
	require.NoError(t, tx.Add(id, tags, "warrior"))
	require.NoError(t, tx.Add(id, tags, "veteran"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Remove one
	tx2 := db1.NewTransaction()
	require.NoError(t, tx2.Remove(id, tags, "warrior"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Export
	var buf bytes.Buffer
	err = db1.Export(&buf)
	require.NoError(t, err)

	// Verify exported lines contain Op keywords
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	foundAdd := false
	foundRemove := false
	for _, line := range lines {
		if strings.Contains(line, ":op/add") {
			foundAdd = true
		}
		if strings.Contains(line, ":op/remove") {
			foundRemove = true
		}
	}
	assert.True(t, foundAdd, "expected :op/add in export")
	assert.True(t, foundRemove, "expected :op/remove in export")

	// Import into fresh DB with same schema
	db2, err := NewDatabaseWithSchema(t.TempDir(), s)
	require.NoError(t, err)
	defer db2.Close()

	err = db2.Import(strings.NewReader(buf.String()))
	require.NoError(t, err)

	// Export db2 and compare byte-for-byte
	var buf2 bytes.Buffer
	err = db2.Export(&buf2)
	require.NoError(t, err)
	assert.Equal(t, buf.String(), buf2.String())
}

func TestDatabaseRoundTrip_CRDTSemantics(t *testing.T) {
	// Semantic verification: after export/import, CRDT resolution still works correctly.
	// Tombstoned values must stay dead; live values must remain visible.
	s, err := schema.NewBuilder().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)

	db1, err := NewDatabaseWithSchema(t.TempDir(), s)
	require.NoError(t, err)
	defer db1.Close()

	id := datalog.NewIdentity("entity1")
	tags := kw(":person/tags")

	// Add three tags
	tx := db1.NewTransaction()
	require.NoError(t, tx.Add(id, tags, "warrior"))
	require.NoError(t, tx.Add(id, tags, "veteran"))
	require.NoError(t, tx.Add(id, tags, "leader"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Remove "warrior"
	tx2 := db1.NewTransaction()
	require.NoError(t, tx2.Remove(id, tags, "warrior"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Query original: should see "veteran" and "leader" but not "warrior"
	results1, err := executor.CollectTuples(db1.Query(
		`[:find ?tag :in $ ?e :where [?e :person/tags ?tag]]`, id))
	require.NoError(t, err)
	tags1 := extractStringValues(results1)
	assert.Contains(t, tags1, "veteran")
	assert.Contains(t, tags1, "leader")
	assert.NotContains(t, tags1, "warrior")

	// Export → Import
	var buf bytes.Buffer
	err = db1.Export(&buf)
	require.NoError(t, err)

	db2, err := NewDatabaseWithSchema(t.TempDir(), s)
	require.NoError(t, err)
	defer db2.Close()

	err = db2.Import(strings.NewReader(buf.String()))
	require.NoError(t, err)

	// Query imported DB: same semantic result — "warrior" must still be dead
	results2, err := executor.CollectTuples(db2.Query(
		`[:find ?tag :in $ ?e :where [?e :person/tags ?tag]]`, id))
	require.NoError(t, err)
	tags2 := extractStringValues(results2)
	assert.Contains(t, tags2, "veteran")
	assert.Contains(t, tags2, "leader")
	assert.NotContains(t, tags2, "warrior")
	assert.ElementsMatch(t, tags1, tags2)
}

func TestDatabaseRoundTrip_RGA(t *testing.T) {
	// Round-trip RGA (cardinality-vector) with insert and tombstone ops
	s, err := schema.NewBuilder().
		Attribute(":doc/items").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db1, err := NewDatabaseWithSchema(t.TempDir(), s)
	require.NoError(t, err)
	defer db1.Close()

	id := datalog.NewIdentity("doc1")
	items := kw(":doc/items")

	// Add items
	tx := db1.NewTransaction()
	require.NoError(t, tx.Add(id, items, "first"))
	require.NoError(t, tx.Add(id, items, "second"))
	require.NoError(t, tx.Add(id, items, "third"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Export
	var buf bytes.Buffer
	err = db1.Export(&buf)
	require.NoError(t, err)

	// Verify exported lines contain RGA ops and AfterRef
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	foundRGAInsert := false
	for _, line := range lines {
		if strings.Contains(line, ":op/rga-insert") {
			foundRGAInsert = true
		}
	}
	assert.True(t, foundRGAInsert, "expected :op/rga-insert in export")

	// Import into fresh DB with same schema
	db2, err := NewDatabaseWithSchema(t.TempDir(), s)
	require.NoError(t, err)
	defer db2.Close()

	err = db2.Import(strings.NewReader(buf.String()))
	require.NoError(t, err)

	// Export db2 and compare byte-for-byte
	var buf2 bytes.Buffer
	err = db2.Export(&buf2)
	require.NoError(t, err)
	assert.Equal(t, buf.String(), buf2.String())
}

// =============================================================================
// Helper functions
// =============================================================================

// extractStringValues extracts string values from query result tuples (first symbol)
func extractStringValues(results [][]interface{}) []string {
	var vals []string
	for _, tuple := range results {
		if len(tuple) > 0 {
			if s, ok := tuple[0].(string); ok {
				vals = append(vals, s)
			}
		}
	}
	return vals
}

// parseEDNValue parses a single EDN value and returns the node
func parseEDNValue(s string) (*edn.Node, error) {
	return edn.Parse(s)
}
