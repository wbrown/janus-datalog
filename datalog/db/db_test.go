package db_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func tempDB(t *testing.T, opts ...db.Option) *db.DB {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "db-test")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	d, err := db.Open(filepath.Join(tmpDir, "test.db"), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { d.Close() })
	return d
}

func TestOpenClose(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "db-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	d, err := db.Open(filepath.Join(tmpDir, "test.db"))
	require.NoError(t, err)
	require.NotNil(t, d)

	err = d.Close()
	require.NoError(t, err)
}

func TestQueryRoundTrip(t *testing.T) {
	d := tempDB(t)

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	txID, err := tx.Commit()
	require.NoError(t, err)
	assert.NotEqual(t, datalog.ElementID{}, txID)

	rel, err := d.Query(`[:find ?name :where [?e :person/name ?name]]`)
	require.NoError(t, err)
	iter := rel.Iterator()
	defer iter.Close()
	require.True(t, iter.Next())
	assert.Equal(t, "Alice", iter.Tuple()[0])
	assert.False(t, iter.Next())
}

func TestQueryWithInputs(t *testing.T) {
	d := tempDB(t)

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")
	age := datalog.NewKeyword(":person/age")

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	require.NoError(t, tx.Add(alice, age, int64(30)))
	_, err := tx.Commit()
	require.NoError(t, err)

	rel, err := d.Query(
		`[:find ?name :in $ ?min-age :where [?e :person/name ?name] [?e :person/age ?age] [(>= ?age ?min-age)]]`,
		int64(25),
	)
	require.NoError(t, err)
	iter := rel.Iterator()
	defer iter.Close()
	require.True(t, iter.Next())
	assert.Equal(t, "Alice", iter.Tuple()[0])
	assert.False(t, iter.Next())
}

func TestQueryInto(t *testing.T) {
	d := tempDB(t)

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	var names []string
	err = d.QueryInto(&names, `[:find ?name :where [?e :person/name ?name]]`)
	require.NoError(t, err)
	require.Len(t, names, 1)
	assert.Equal(t, "Alice", names[0])
}

func TestQueryOneInto(t *testing.T) {
	d := tempDB(t)

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	var result string
	found, err := d.QueryOneInto(&result, `[:find ?name :where [?e :person/name ?name]]`)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "Alice", result)

	// Not found case
	var missing string
	found, err = d.QueryOneInto(&missing, `[:find ?name :where [?e :person/nonexistent ?name]]`)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestGetString(t *testing.T) {
	d := tempDB(t)

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	val, found, err := d.GetString(alice, name)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "Alice", val)

	// Missing attribute
	val, found, err = d.GetString(alice, datalog.NewKeyword(":person/missing"))
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, "", val)
}

func TestGetInt(t *testing.T) {
	d := tempDB(t)

	alice := datalog.NewIdentity("alice")
	age := datalog.NewKeyword(":person/age")

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, age, int64(30)))
	_, err := tx.Commit()
	require.NoError(t, err)

	val, found, err := d.GetInt(alice, age)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, int64(30), val)

	// Missing
	val, found, err = d.GetInt(alice, datalog.NewKeyword(":person/missing"))
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, int64(0), val)
}

func TestGetFloat(t *testing.T) {
	d := tempDB(t)

	alice := datalog.NewIdentity("alice")
	score := datalog.NewKeyword(":person/score")

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, score, 3.14))
	_, err := tx.Commit()
	require.NoError(t, err)

	val, found, err := d.GetFloat(alice, score)
	require.NoError(t, err)
	assert.True(t, found)
	assert.InDelta(t, 3.14, val, 0.001)

	// Missing
	val, found, err = d.GetFloat(alice, datalog.NewKeyword(":person/missing"))
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, float64(0), val)
}

func TestGetBool(t *testing.T) {
	d := tempDB(t)

	alice := datalog.NewIdentity("alice")
	active := datalog.NewKeyword(":person/active")

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, active, true))
	_, err := tx.Commit()
	require.NoError(t, err)

	val, found, err := d.GetBool(alice, active)
	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, val)

	// Missing
	val, found, err = d.GetBool(alice, datalog.NewKeyword(":person/missing"))
	require.NoError(t, err)
	assert.False(t, found)
	assert.False(t, val)
}

func TestGetRef(t *testing.T) {
	d := tempDB(t)

	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	friend := datalog.NewKeyword(":person/friend")

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, friend, bob))
	_, err := tx.Commit()
	require.NoError(t, err)

	val, found, err := d.GetRef(alice, friend)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, bob, val)

	// Missing
	val, found, err = d.GetRef(alice, datalog.NewKeyword(":person/missing"))
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, val)
}

func TestGetStrings(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":person/tag").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	d := tempDB(t, db.WithSchema(s))

	alice := datalog.NewIdentity("alice")
	tag := datalog.NewKeyword(":person/tag")

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, tag, "developer"))
	require.NoError(t, tx.Add(alice, tag, "gopher"))
	_, err = tx.Commit()
	require.NoError(t, err)

	vals, err := d.GetStrings(alice, tag)
	require.NoError(t, err)
	assert.Len(t, vals, 2)
	assert.Contains(t, vals, "developer")
	assert.Contains(t, vals, "gopher")

	// Missing — empty slice, no error
	vals, err = d.GetStrings(alice, datalog.NewKeyword(":person/missing"))
	require.NoError(t, err)
	assert.Empty(t, vals)
}

func TestAsOf(t *testing.T) {
	d := tempDB(t)

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")

	tx1 := d.NewTransaction()
	require.NoError(t, tx1.Add(alice, name, "Alice"))
	tx1ID, err := tx1.Commit()
	require.NoError(t, err)

	tx2 := d.NewTransaction()
	require.NoError(t, tx2.Add(alice, name, "Alicia"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Latest should be "Alicia"
	val, found, err := d.GetString(alice, name)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "Alicia", val)

	// AsOf tx1 should be "Alice"
	asOf := d.AsOf(tx1ID)
	val, found, err = asOf.GetString(alice, name)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "Alice", val)
}

func TestHistory(t *testing.T) {
	d := tempDB(t)

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")

	tx1 := d.NewTransaction()
	require.NoError(t, tx1.Add(alice, name, "Alice"))
	_, err := tx1.Commit()
	require.NoError(t, err)

	tx2 := d.NewTransaction()
	require.NoError(t, tx2.Add(alice, name, "Alicia"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// History should return both values
	hist := d.History()
	rel, err := hist.Query(`[:find ?name :where [?e :person/name ?name]]`)
	require.NoError(t, err)
	iter := rel.Iterator()
	defer iter.Close()

	var names []string
	for iter.Next() {
		names = append(names, iter.Tuple()[0].(string))
	}
	assert.Len(t, names, 2)
	assert.Contains(t, names, "Alice")
	assert.Contains(t, names, "Alicia")
}

func TestMustParseQuery(t *testing.T) {
	// Valid query — should not panic
	q := db.MustParseQuery(`[:find ?e :where [?e :person/name "Alice"]]`)
	assert.NotNil(t, q)

	// Invalid query — should panic
	assert.Panics(t, func() {
		db.MustParseQuery(`[:find`)
	})
}

func TestMustParseQueryWithQuery(t *testing.T) {
	d := tempDB(t)

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Pre-parsed query works with Query
	q := db.MustParseQuery(`[:find ?name :where [?e :person/name ?name]]`)
	rel, err := d.Query(q)
	require.NoError(t, err)
	iter := rel.Iterator()
	defer iter.Close()
	require.True(t, iter.Next())
	assert.Equal(t, "Alice", iter.Tuple()[0])
}

func TestAssert(t *testing.T) {
	d := tempDB(t)

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")

	err := d.Assert([]datalog.Datom{{E: alice, A: name, V: "Alice"}})
	require.NoError(t, err)

	val, found, err := d.GetString(alice, name)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "Alice", val)
}

func TestPullInto(t *testing.T) {
	d := tempDB(t)

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")
	age := datalog.NewKeyword(":person/age")

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	require.NoError(t, tx.Add(alice, age, int64(30)))
	_, err := tx.Commit()
	require.NoError(t, err)

	type Person struct {
		Name string `datalog:"name"`
		Age  int64  `datalog:"age"`
	}
	var p Person
	err = d.PullInto(alice, &p)
	require.NoError(t, err)
	assert.Equal(t, "Alice", p.Name)
	assert.Equal(t, int64(30), p.Age)
}

func TestTransactionRollback(t *testing.T) {
	d := tempDB(t)

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	require.NoError(t, tx.Rollback())

	// Should not find the data
	rel, err := d.Query(`[:find ?name :where [?e :person/name ?name]]`)
	require.NoError(t, err)
	iter := rel.Iterator()
	defer iter.Close()
	assert.False(t, iter.Next())
}

func TestSaveStruct(t *testing.T) {
	d := tempDB(t)

	type Person struct {
		ID   datalog.Identity `datalog:"-,id"`
		Name string           `datalog:"name"`
	}

	p := &Person{Name: "Alice"}
	tx := d.NewTransaction()
	id, err := tx.SaveStruct(p)
	require.NoError(t, err)
	assert.NotNil(t, id)
	_, err = tx.Commit()
	require.NoError(t, err)

	val, found, err := d.GetString(id, datalog.NewKeyword(":person/name"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "Alice", val)
}

// Verify DB satisfies Querier interface at compile time.
var _ db.Querier = (*db.DB)(nil)
