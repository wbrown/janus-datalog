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

	// Valid data
	tx := db.NewTransaction()
	err = tx.Add(alice, name, "Alice")
	assert.NoError(t, err)
	err = tx.Add(alice, age, int64(30))
	assert.NoError(t, err)
	_, err = tx.Commit()
	assert.NoError(t, err)

	// Invalid type: string instead of long
	tx2 := db.NewTransaction()
	err = tx2.Add(alice, age, "thirty") // Should fail - wrong type
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "schema validation failed")
	assert.Contains(t, err.Error(), "db.type/long")

	// Invalid type: int instead of string
	tx3 := db.NewTransaction()
	err = tx3.Add(alice, name, 123) // Should fail - wrong type
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

func TestSchemaUniquenessValue(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "db-unique-value-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create schema with unique value constraint
	s, err := schema.NewBuilder().
		Attribute(":user/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	email := datalog.NewKeyword(":user/email")

	// First user with email
	tx := db.NewTransaction()
	err = tx.Add(alice, email, "alice@example.com")
	require.NoError(t, err)
	txid, err := tx.Commit()
	require.NoError(t, err)
	t.Logf("First commit txid: %d", txid)

	// Verify data was written by querying directly
	matcher := NewBadgerMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: email},
			query.Constant{Value: "alice@example.com"},
			query.Blank{},
		},
	}
	results, err := matcher.Match(pattern, nil)
	require.NoError(t, err, "matcher.Match should succeed")
	t.Logf("Result columns: %v", results.Columns())

	iter := results.Iterator()
	count := 0
	for iter.Next() {
		tuple := iter.Tuple()
		t.Logf("Found tuple: %v", tuple)
		if len(tuple) > 0 {
			t.Logf("  tuple[0] type: %T, value: %v", tuple[0], tuple[0])
			if id, ok := tuple[0].(datalog.Identity); ok {
				t.Logf("  Is Identity: %s", id.String())
			} else {
				t.Logf("  NOT an Identity type")
			}
		}
		count++
	}
	iter.Close()
	t.Logf("Total matches found: %d", count)
	require.Greater(t, count, 0, "should find at least one existing datom")

	// Second user with same email should fail
	tx2 := db.NewTransaction()
	err = tx2.Add(bob, email, "alice@example.com")
	require.NoError(t, err) // Add succeeds (type check passes)
	_, err = tx2.Commit()
	require.Error(t, err, "should fail uniqueness check")
	assert.Contains(t, err.Error(), "uniqueness violation")
}

func TestSchemaUniquenessWithinTransaction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "db-unique-tx-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create schema with unique value constraint
	s, err := schema.NewBuilder().
		Attribute(":user/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	email := datalog.NewKeyword(":user/email")

	// Two different entities with same email in same transaction
	tx := db.NewTransaction()
	err = tx.Add(alice, email, "shared@example.com")
	require.NoError(t, err)
	err = tx.Add(bob, email, "shared@example.com")
	require.NoError(t, err) // Add succeeds (checked at commit time for cross-entity)
	_, err = tx.Commit()
	assert.Error(t, err, "should fail uniqueness check within transaction")
	assert.Contains(t, err.Error(), "uniqueness violation")
	assert.Contains(t, err.Error(), "already used by entity")
}

func TestSchemaUniquenessIdempotent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "db-unique-idempotent-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create schema with unique value constraint
	s, err := schema.NewBuilder().
		Attribute(":user/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	email := datalog.NewKeyword(":user/email")

	// First assertion
	tx := db.NewTransaction()
	err = tx.Add(alice, email, "alice@example.com")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Same entity, same value should succeed (idempotent)
	tx2 := db.NewTransaction()
	err = tx2.Add(alice, email, "alice@example.com")
	require.NoError(t, err)
	_, err = tx2.Commit()
	assert.NoError(t, err, "idempotent update should succeed")
}

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
