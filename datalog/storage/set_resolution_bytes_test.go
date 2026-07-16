//go:build !(js && wasm)

package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestCardinalityMany_ByteValues verifies that []byte values work correctly
// with cardinality-many (add-wins set) CRDT semantics. This is a regression
// test for a panic caused by using []byte as a map key in resolveAddWinsSet.
func TestCardinalityMany_ByteValues(t *testing.T) {
	dir, err := os.MkdirTemp("", "bytes-many-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":test/data"),
		ValueType:   schema.TypeBytes,
		Cardinality: schema.CardinalityMany,
	})

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:   dir,
		Schema: s,
	})
	require.NoError(t, err)
	defer db.Close()

	entity := datalog.NewIdentity("bytes-entity")
	attr := datalog.NewKeyword(":test/data")
	v1 := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	v2 := []byte{0xCA, 0xFE, 0xBA, 0xBE}

	// Add both values
	tx := db.NewTransaction()
	tx.Add(entity, attr, v1)
	tx.Add(entity, attr, v2)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query — should return both without panicking
	matcher := NewBadgerMatcher(db.Store())
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
	assert.Equal(t, 2, count, "should have both []byte values")
}

// TestCardinalityMany_ByteValues_Retract verifies retraction works for []byte
// values in cardinality-many sets.
func TestCardinalityMany_ByteValues_Retract(t *testing.T) {
	dir, err := os.MkdirTemp("", "bytes-retract-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":test/data"),
		ValueType:   schema.TypeBytes,
		Cardinality: schema.CardinalityMany,
	})

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:   dir,
		Schema: s,
	})
	require.NoError(t, err)
	defer db.Close()

	entity := datalog.NewIdentity("bytes-retract")
	attr := datalog.NewKeyword(":test/data")
	v1 := []byte{0x01, 0x02, 0x03}
	v2 := []byte{0x04, 0x05, 0x06}

	// Add both
	tx := db.NewTransaction()
	tx.Add(entity, attr, v1)
	tx.Add(entity, attr, v2)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Retract v2
	tx2 := db.NewTransaction()
	tx2.Retract(entity, attr, v2)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Only v1 should remain
	matcher := NewBadgerMatcher(db.Store())
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

	var values [][]byte
	iter := results.Iterator()
	for iter.Next() {
		values = append(values, iter.Tuple()[0].([]byte))
	}
	assert.Len(t, values, 1, "should have one value after retract")
	if len(values) == 1 {
		assert.Equal(t, v1, values[0])
	}
}
