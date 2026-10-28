package db_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// A datom's value is one scalar of the domain. A write given a vector where
// one datom's value is expected answers an error and appends nothing, and the
// transaction's other writes commit; a slice given to Set for a many or vector
// attribute is the membership, each element one datom's value. Every backend
// the build offers is driven, since each encodes or orders a datom's value in
// its own form.
func TestAVectorGivenAsOneDatomsValueIsRefusedAtTheWrite(t *testing.T) {
	backends := storage.AvailableBackends()
	require.NotEmpty(t, backends, "the build offers no backend")
	alice := datalog.NewIdentity("vector-datom-value-alice")
	bob := datalog.NewIdentity("vector-datom-value-bob")
	name := datalog.NewKeyword(":t/name")
	tags := datalog.NewKeyword(":t/tags")
	vals := datalog.NewKeyword(":t/vals")
	tuples := func(t *testing.T, d *db.DB, q string) [][]interface{} {
		answered, err := executor.CollectTuples(d.Query(q))
		require.NoError(t, err)
		return answered
	}
	commit := func(t *testing.T, tx *db.Transaction) {
		var err error
		require.NotPanics(t, func() { _, err = tx.Commit() })
		require.NoError(t, err)
	}

	for _, backend := range backends {
		t.Run(backend.Name, func(t *testing.T) {
			open := func(t *testing.T, opts ...db.Option) *db.DB {
				source := ""
				if backend.Persistent {
					source = filepath.Join(t.TempDir(), "store")
				}
				d, err := db.Open(source, append([]db.Option{db.WithBackend(backend.Name)}, opts...)...)
				require.NoError(t, err)
				t.Cleanup(func() { d.Close() })
				return d
			}

			t.Run("an attribute the schema does not declare", func(t *testing.T) {
				d := open(t)
				tx := d.NewTransaction()
				for _, vector := range []interface{}{[]interface{}{int64(1), int64(2)}, []float64{1.5}} {
					require.ErrorContains(t, tx.Add(alice, tags, vector), "not a datom's value", "Add %T", vector)
				}
				require.ErrorContains(t, tx.Remove(alice, tags, []interface{}{int64(1)}), "not a datom's value", "Remove")
				require.ErrorContains(t, tx.Retract(alice, tags, []interface{}{int64(1)}), "not a datom's value", "Retract")
				require.ErrorContains(t, tx.Set(alice, tags, []interface{}{int64(1)}), "not a datom's value", "Set")
				require.NoError(t, tx.Add(bob, name, "Bobby"))
				commit(t, tx)
				require.Equal(t, [][]interface{}{{"Bobby"}}, tuples(t, d, `[:find ?n :where [?e :t/name ?n]]`))
				require.Empty(t, tuples(t, d, `[:find ?v :where [?e :t/tags ?v]]`), "the refused writes appended nothing")
			})

			t.Run("a declared vector attribute and a declared many attribute", func(t *testing.T) {
				s := schema.NewBuilder().
					Attribute(":t/name").Type(schema.TypeString).Add().
					Attribute(":t/vals").Type(schema.TypeDouble).Vector().Add().
					Attribute(":t/tags").Type(schema.TypeDouble).Many().Add().
					MustBuild()
				d := open(t, db.WithSchema(s))
				tx := d.NewTransaction()
				require.NoError(t, tx.Add(alice, name, "alice"))
				require.NoError(t, tx.Add(bob, name, "bob"))
				require.ErrorContains(t, tx.Add(alice, vals, []float64{1.5, 2.5}), "not a datom's value", "Add of a whole vector")
				require.ErrorContains(t, tx.Set(bob, vals, [][]float64{{1.5}}), "not a datom's value", "Set with a vector element on a vector attribute")
				require.ErrorContains(t, tx.Set(bob, tags, []interface{}{[]float64{1.5}}), "not a datom's value", "Set with a vector element on a many attribute")
				require.NoError(t, tx.Add(alice, vals, 1.5))
				require.NoError(t, tx.Add(alice, vals, 2.5))
				require.NoError(t, tx.Set(bob, vals, []float64{3.5, 4.5}))
				require.NoError(t, tx.Set(bob, tags, []float64{7.5}))
				commit(t, tx)

				aliceVals := tuples(t, d, `[:find ?vec :where [?e :t/name "alice"] [?e :t/vals ?vec]]`)
				require.Len(t, aliceVals, 1)
				require.True(t, datalog.ValuesEqual([]float64{1.5, 2.5}, aliceVals[0][0]), "one Add per element writes the list, got %#v", aliceVals[0][0])
				bobVals := tuples(t, d, `[:find ?vec :where [?e :t/name "bob"] [?e :t/vals ?vec]]`)
				require.Len(t, bobVals, 1)
				require.True(t, datalog.ValuesEqual([]float64{3.5, 4.5}, bobVals[0][0]), "Set with the slice writes the list, got %#v", bobVals[0][0])
				require.Equal(t, [][]interface{}{{7.5}}, tuples(t, d, `[:find ?v :where [?e :t/name "bob"] [?e :t/tags ?v]]`))
			})
		})
	}
}
