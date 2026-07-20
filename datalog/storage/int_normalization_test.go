package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// These tests cover Bug 2 end-to-end through the public API: a Go int entering
// via tx.Add / a query :in parameter must behave identically to the equivalent
// int64, never silently fail to match and never panic at encode.

func intNormSchema() *schema.Schema {
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/age"),
		ValueType:   schema.TypeLong,
		Cardinality: schema.CardinalityOne,
	})
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/name"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})
	return s
}

func openIntNormDB(t *testing.T) *Database {
	t.Helper()
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:      t.TempDir(),
		Schema:    intNormSchema(),
		ReplicaID: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func queryRows(t *testing.T, db *Database, q string, args ...interface{}) []string {
	t.Helper()
	rel, err := db.Query(q, args...)
	require.NoError(t, err)
	var rows []string
	it := rel.Iterator()
	for it.Next() {
		rows = append(rows, fmt.Sprintf("%v", it.Tuple()))
	}
	require.NoError(t, it.Error())
	it.Close()
	sort.Strings(rows)
	return rows
}

// TestWrite_GoIntValueDoesNotPanic writes an untyped Go int (the natural
// tx.Add(e, attr, 31) call) and asserts it commits without panicking and reads
// back as the canonical int64. Before the fix this passed schema validation and
// then panicked at encode.
func TestWrite_GoIntValueDoesNotPanic(t *testing.T) {
	db := openIntNormDB(t)
	e := datalog.NewIdentity("alice")
	age := datalog.NewKeyword(":person/age")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, age, 31)) // untyped Go int
	_, err := tx.Commit()
	require.NoError(t, err, "committing a Go int value must not panic or error")

	// Reads back as int64.
	rel, err := db.Query(`[:find ?age :in $ ?e :where [?e :person/age ?age]]`, e)
	require.NoError(t, err)
	it := rel.Iterator()
	require.True(t, it.Next())
	got := it.Tuple()[0]
	it.Close()
	assert.Equal(t, int64(31), got, "stored Go int must read back as int64")
}

// TestQuery_IntInputMatchesInt64StoredValue is the core facet-1 regression: a
// parameterized query returns the same rows whether the argument is a Go int or
// the equivalent int64. Before the fix the int argument matched nothing because
// the input relation's int value never joined the stored int64 (ValuesEqual was
// type-strict).
func TestQuery_IntInputMatchesInt64StoredValue(t *testing.T) {
	db := openIntNormDB(t)
	age := datalog.NewKeyword(":person/age")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(datalog.NewIdentity("alice"), age, int64(30)))
	require.NoError(t, tx.Set(datalog.NewIdentity("bob"), age, int64(40)))
	_, err := tx.Commit()
	require.NoError(t, err)

	const q = `[:find ?e :in $ ?age :where [?e :person/age ?age]]`
	rowsInt64 := queryRows(t, db, q, int64(30))
	rowsInt := queryRows(t, db, q, int(30))

	require.Len(t, rowsInt64, 1, "int64 argument should match exactly alice")
	assert.Equal(t, rowsInt64, rowsInt,
		"int argument must return the same rows as int64 argument")
}

// TestPredicateAndJoinAgreeOnIntInt64 proves the two comparison paths agree: an
// int parameter used as a join key (pattern binding, ValuesEqual) and as a
// predicate operand (CompareValues) yield the same match. Before the fix these
// disagreed — the join silently failed while the = predicate matched.
func TestPredicateAndJoinAgreeOnIntInt64(t *testing.T) {
	db := openIntNormDB(t)
	age := datalog.NewKeyword(":person/age")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(datalog.NewIdentity("alice"), age, int64(30)))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Join path: the int input binds ?age and joins the stored int64 directly.
	joinRows := queryRows(t, db,
		`[:find ?e :in $ ?age :where [?e :person/age ?age]]`, int(30))

	// Predicate path: bind ?a from storage, compare it to the int input via (=).
	predRows := queryRows(t, db,
		`[:find ?e :in $ ?age :where [?e :person/age ?a] [(= ?a ?age)]]`, int(30))

	assert.Equal(t, joinRows, predRows,
		"join and predicate must agree for an int parameter")
	require.Len(t, joinRows, 1, "both paths should match alice")
}

// TestRetract_GoIntValueMatchesStoredInt64 pins Retract's write-boundary
// contract: like Add/Set/Remove, an untyped Go int normalizes to the canonical
// int64 before the retraction is recorded, so it retracts the equivalent
// stored value rather than silently missing it.
func TestRetract_GoIntValueMatchesStoredInt64(t *testing.T) {
	s := schema.NewSchema()
	numbers := datalog.NewKeyword(":person/lucky-numbers")
	s.Add(&schema.AttributeDefinition{
		Ident:       numbers,
		ValueType:   schema.TypeLong,
		Cardinality: schema.CardinalityMany,
	})
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:      t.TempDir(),
		Schema:    s,
		ReplicaID: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	e := datalog.NewIdentity("alice")
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, numbers, int64(7)))
	require.NoError(t, tx.Add(e, numbers, int64(30)))
	_, err = tx.Commit()
	require.NoError(t, err)

	tx = db.NewTransaction()
	require.NoError(t, tx.Retract(e, numbers, 30)) // untyped Go int
	_, err = tx.Commit()
	require.NoError(t, err, "retracting a Go int value must not panic or error")

	rows := queryRows(t, db,
		`[:find ?n :in $ ?e :where [?e :person/lucky-numbers ?n]]`, e)
	require.Equal(t, []string{"[7]"}, rows,
		"retract with a Go int must remove the stored int64 value")
}
