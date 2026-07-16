//go:build !(js && wasm)

package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func TestDatabaseWithSchema(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "db-schema-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).Add().
		Attribute(":person/age").Type(schema.TypeLong).Add().
		Build()
	require.NoError(t, err)

	// Create database with schema
	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	assert.NotNil(t, db.Schema())
	assert.True(t, db.Schema().HasSchema())
}

func TestSchemaTypeValidation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "db-type-validation-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create schema with type constraints
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).Add().
		Attribute(":person/age").Type(schema.TypeLong).Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")
	age := datalog.NewKeyword(":person/age")

	// Valid data - use Set() for cardinality-one attributes
	tx := db.NewTransaction()
	err = tx.Set(alice, name, "Alice")
	assert.NoError(t, err)
	err = tx.Set(alice, age, int64(30))
	assert.NoError(t, err)
	_, err = tx.Commit()
	assert.NoError(t, err)

	// Invalid type: string instead of long
	tx2 := db.NewTransaction()
	err = tx2.Set(alice, age, "thirty") // Should fail - wrong type
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "schema validation failed")
	assert.Contains(t, err.Error(), "db.type/long")

	// Invalid type: int instead of string
	tx3 := db.NewTransaction()
	err = tx3.Set(alice, name, 123) // Should fail - wrong type
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "schema validation failed")
	assert.Contains(t, err.Error(), "db.type/string")
}

func TestSchemaUnknownAttribute(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "db-unknown-attr-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create schema with only name defined
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")

	// Unknown attribute should be allowed (additive schema)
	tx := db.NewTransaction()
	err = tx.Add(alice, datalog.NewKeyword(":person/email"), "alice@example.com")
	assert.NoError(t, err, "unknown attribute should be allowed")
	_, err = tx.Commit()
	assert.NoError(t, err)
}

// The three Datomic-strict uniqueness tests (TestSchemaUniquenessValue,
// TestSchemaUniquenessWithinTransaction, TestSchemaUniquenessIdempotent)
// were deleted in the CRDT-uniqueness redesign. They asserted that the
// second commit of a duplicate unique value fails with "uniqueness
// violation" — the old write-time gate contract. The new model (see
// docs/proposals/CRDT_UNIQUE_SEMANTICS.md) resolves uniqueness at read
// time via (A, V)-LWW; all writes succeed. Tests for the new contract
// are added in subsequent commits (Commits 2–5 of that redesign).

func TestNoSchemaNoValidation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "db-no-schema-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create database without schema
	db, err := NewDatabase(tmpDir)
	require.NoError(t, err)
	defer db.Close()

	assert.Nil(t, db.Schema())

	// Should accept any data without validation
	alice := datalog.NewIdentity("alice")
	tx := db.NewTransaction()
	err = tx.Add(alice, datalog.NewKeyword(":person/age"), "not a number")
	assert.NoError(t, err)
	_, err = tx.Commit()
	assert.NoError(t, err, "without schema, any value should be accepted")
}

func TestSetSchema(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "db-set-schema-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create database without schema
	db, err := NewDatabase(tmpDir)
	require.NoError(t, err)
	defer db.Close()

	assert.Nil(t, db.Schema())

	// Add schema later
	s, err := schema.NewBuilder().
		Attribute(":person/age").Type(schema.TypeLong).Add().
		Build()
	require.NoError(t, err)

	db.SetSchema(s)
	assert.NotNil(t, db.Schema())

	// Now validation should be enforced
	alice := datalog.NewIdentity("alice")
	tx := db.NewTransaction()
	err = tx.Add(alice, datalog.NewKeyword(":person/age"), "not a number")
	assert.Error(t, err, "schema should now be enforced")
}

func TestNilValueRejected(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "db-nil-value-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create database without schema - nil should still be rejected
	db, err := NewDatabase(tmpDir)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	nameAttr := datalog.NewKeyword(":person/name")

	// Add with nil value should fail
	tx := db.NewTransaction()
	err = tx.Add(alice, nameAttr, nil)
	assert.Error(t, err, "nil value should be rejected")
	assert.Contains(t, err.Error(), "nil value not allowed")

	// Retract with nil value should also fail
	tx2 := db.NewTransaction()
	err = tx2.Retract(alice, nameAttr, nil)
	assert.Error(t, err, "nil value should be rejected for retraction")
	assert.Contains(t, err.Error(), "nil value not allowed")
}
