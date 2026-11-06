package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

var (
	viewName  = datalog.NewKeyword(":view/name")
	viewAge   = datalog.NewKeyword(":view/age")
	viewEmail = datalog.NewKeyword(":view/email")
	viewTags  = datalog.NewKeyword(":view/tags")
)

// The structs SaveStruct writes, each holding only the fields its case writes.
type viewNamed struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name string           `datalog:"view/name"`
}

type viewAged struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name string           `datalog:"view/name"`
	Age  int64            `datalog:"view/age"`
}

type viewEmailed struct {
	ID    datalog.Identity `datalog:"-,id"`
	Email string           `datalog:"view/email"`
}

type viewTagged struct {
	ID   datalog.Identity `datalog:"-,id"`
	Tags []string         `datalog:"view/tags"`
}

func transactionViewSchema(t testing.TB) schema.SchemaProvider {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(viewName.String()).Type(schema.TypeString).Add().
		Attribute(viewAge.String()).Type(schema.TypeLong).Add().
		Attribute(viewEmail.String()).Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		Attribute(viewTags.String()).Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	return s
}

// transactionViewHeld answers e's values of attr in db's latest view.
func transactionViewHeld(t *testing.T, db *Database, e datalog.Identity, attr datalog.Keyword) []interface{} {
	t.Helper()
	tuples, err := executor.CollectTuples(db.Query(fmt.Sprintf(`[:find ?v :in $ ?e :where [?e %s ?v]]`, attr), e))
	require.NoError(t, err)
	values := make([]interface{}, 0, len(tuples))
	for _, tuple := range tuples {
		values = append(values, tuple[0])
	}
	return values
}

// transactionViewCommit writes each of writes in one transaction and commits
// it.
func transactionViewCommit(t *testing.T, db *Database, writes ...func(tx *Transaction) error) {
	t.Helper()
	tx := db.NewTransaction()
	for _, write := range writes {
		require.NoError(t, write(tx))
	}
	_, err := tx.Commit()
	require.NoError(t, err)
}

func viewAdd(e datalog.Identity, a datalog.Keyword, v interface{}) func(tx *Transaction) error {
	return func(tx *Transaction) error { return tx.Add(e, a, v) }
}

func viewRemove(e datalog.Identity, a datalog.Keyword, v interface{}) func(tx *Transaction) error {
	return func(tx *Transaction) error { return tx.Remove(e, a, v) }
}

func viewSet(e datalog.Identity, a datalog.Keyword, v interface{}) func(tx *Transaction) error {
	return func(tx *Transaction) error { return tx.Set(e, a, v) }
}

func viewSave(record interface{}) func(tx *Transaction) error {
	return func(tx *Transaction) error {
		_, err := tx.SaveStruct(record)
		return err
	}
}

// TestTransactionWritersCompareAgainstTheTransactionsView pins that a
// transaction's SaveStruct, and Set on a set, compare and diff against the
// transaction's view of (E, A): the resolution of its datoms as of the
// transaction's start with the transaction's own writes applied. A value the
// view holds unchanged is not asserted, so a concurrent writer's change to it
// stands. A value the view lacks is asserted, whatever the store's latest
// resolution holds. A withdrawal names only what the view holds.
func TestTransactionWritersCompareAgainstTheTransactionsView(t *testing.T) {
	e1 := datalog.NewIdentity("transaction-view:e1")
	e2 := datalog.NewIdentity("transaction-view:e2")
	e3 := datalog.NewIdentity("transaction-view:e3")

	type held struct {
		e    datalog.Identity
		attr datalog.Keyword
		want []interface{}
	}
	cases := []struct {
		name  string
		write func(t *testing.T, db *Database)
		held  []held
	}{
		{
			name: "a save of a value its transaction changed asserts it",
			write: func(t *testing.T, db *Database) {
				transactionViewCommit(t, db, viewAdd(e1, viewName, "Alice"))
				transactionViewCommit(t, db,
					viewAdd(e1, viewName, "Bob"),
					viewSave(&viewNamed{ID: e1, Name: "Alice"}))
			},
			held: []held{{e1, viewName, []interface{}{"Alice"}}},
		},
		{
			name: "a save leaves a field it did not change to a concurrent writer",
			write: func(t *testing.T, db *Database) {
				transactionViewCommit(t, db, viewAdd(e1, viewName, "Alice"), viewAdd(e1, viewAge, int64(30)))
				t0 := db.NewTransaction()
				transactionViewCommit(t, db, viewSet(e1, viewName, "Bob"))
				require.NoError(t, viewSave(&viewAged{ID: e1, Name: "Alice", Age: 31})(t0))
				_, err := t0.Commit()
				require.NoError(t, err)
			},
			held: []held{
				{e1, viewName, []interface{}{"Bob"}},
				{e1, viewAge, []interface{}{int64(31)}},
			},
		},
		{
			name: "a save of a unique value its transaction gave another entity claims it again",
			write: func(t *testing.T, db *Database) {
				transactionViewCommit(t, db, viewAdd(e1, viewEmail, "x"))
				transactionViewCommit(t, db,
					viewAdd(e2, viewEmail, "x"),
					viewSave(&viewEmailed{ID: e1, Email: "x"}))
			},
			held: []held{
				{e1, viewEmail, []interface{}{"x"}},
				{e2, viewEmail, []interface{}{}},
			},
		},
		{
			name: "a save of the unique value its view falls back to leaves a concurrent claim to it",
			write: func(t *testing.T, db *Database) {
				transactionViewCommit(t, db, viewAdd(e1, viewEmail, "a"))
				transactionViewCommit(t, db, viewAdd(e1, viewEmail, "x"))
				t0 := db.NewTransaction()
				transactionViewCommit(t, db, viewAdd(e3, viewEmail, "a"))
				require.NoError(t, viewAdd(e2, viewEmail, "x")(t0))
				require.NoError(t, viewSave(&viewEmailed{ID: e1, Email: "a"})(t0))
				_, err := t0.Commit()
				require.NoError(t, err)
			},
			held: []held{
				{e1, viewEmail, []interface{}{}},
				{e2, viewEmail, []interface{}{"x"}},
				{e3, viewEmail, []interface{}{"a"}},
			},
		},
		{
			name: "a save of an empty set withdraws a member its transaction added",
			write: func(t *testing.T, db *Database) {
				transactionViewCommit(t, db,
					viewAdd(e1, viewTags, "x"),
					viewSave(&viewTagged{ID: e1, Tags: []string{}}))
			},
			held: []held{{e1, viewTags, []interface{}{}}},
		},
		{
			name: "a save of a set adds a member its transaction withdrew",
			write: func(t *testing.T, db *Database) {
				transactionViewCommit(t, db, viewAdd(e1, viewTags, "x"))
				transactionViewCommit(t, db,
					viewRemove(e1, viewTags, "x"),
					viewSave(&viewTagged{ID: e1, Tags: []string{"x"}}))
			},
			held: []held{{e1, viewTags, []interface{}{"x"}}},
		},
		{
			name: "a save of a set keeps a member a concurrent writer added",
			write: func(t *testing.T, db *Database) {
				transactionViewCommit(t, db, viewAdd(e1, viewTags, "a"))
				t0 := db.NewTransaction()
				transactionViewCommit(t, db, viewAdd(e1, viewTags, "y"))
				require.NoError(t, viewSave(&viewTagged{ID: e1, Tags: []string{"a", "b"}})(t0))
				_, err := t0.Commit()
				require.NoError(t, err)
			},
			held: []held{{e1, viewTags, []interface{}{"a", "b", "y"}}},
		},
		{
			name: "a set of a set keeps a member a concurrent writer added",
			write: func(t *testing.T, db *Database) {
				transactionViewCommit(t, db, viewAdd(e1, viewTags, "a"))
				t0 := db.NewTransaction()
				transactionViewCommit(t, db, viewAdd(e1, viewTags, "y"))
				require.NoError(t, viewSet(e1, viewTags, []interface{}{"a", "b"})(t0))
				_, err := t0.Commit()
				require.NoError(t, err)
			},
			held: []held{{e1, viewTags, []interface{}{"a", "b", "y"}}},
		},
		{
			name: "a save of a set leaves a member a concurrent writer withdrew",
			write: func(t *testing.T, db *Database) {
				transactionViewCommit(t, db, viewAdd(e1, viewTags, "a"), viewAdd(e1, viewTags, "v"))
				t0 := db.NewTransaction()
				transactionViewCommit(t, db, viewRemove(e1, viewTags, "v"))
				require.NoError(t, viewSave(&viewTagged{ID: e1, Tags: []string{"a", "v"}})(t0))
				_, err := t0.Commit()
				require.NoError(t, err)
			},
			held: []held{{e1, viewTags, []interface{}{"a"}}},
		},
		{
			name: "a set of a set leaves a member a concurrent writer withdrew",
			write: func(t *testing.T, db *Database) {
				transactionViewCommit(t, db, viewAdd(e1, viewTags, "a"), viewAdd(e1, viewTags, "v"))
				t0 := db.NewTransaction()
				transactionViewCommit(t, db, viewRemove(e1, viewTags, "v"))
				require.NoError(t, viewSet(e1, viewTags, []interface{}{"a", "v"})(t0))
				_, err := t0.Commit()
				require.NoError(t, err)
			},
			held: []held{{e1, viewTags, []interface{}{"a"}}},
		},
	}

	for _, mode := range optimizerModes {
		for _, disableCache := range []bool{false, true} {
			cacheName := "cache_on"
			if disableCache {
				cacheName = "cache_off"
			}
			t.Run(mode.name+"/"+cacheName, func(t *testing.T) {
				for _, c := range cases {
					t.Run(c.name, func(t *testing.T) {
						db := createOptimizerModeDB(t, mode, DatabaseOptions{
							Schema:       transactionViewSchema(t),
							DisableCache: disableCache,
						})
						c.write(t, db)
						for _, h := range c.held {
							require.ElementsMatch(t, h.want, transactionViewHeld(t, db, h.e, h.attr),
								"the writers compare against the transaction's view: %s of %s", h.attr, h.e)
						}
					})
				}
			})
		}
	}
}
