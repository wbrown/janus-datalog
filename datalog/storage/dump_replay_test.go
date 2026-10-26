package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

var (
	replayedEntity = datalog.NewIdentity("replayed:subject")
	replayedTags   = datalog.NewKeyword(":replayed/tags")
)

// declaredTagsDump exports, in EDN or in JDZL, a database that declared
// :replayed/tags as a set of strings and wrote three members of it. Both
// formats go through a file: JDZL patches its trailer offset and needs a
// seekable writer.
func declaredTagsDump(t *testing.T, binary bool) []byte {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(":replayed/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	source, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory-trees", Schema: s})
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })
	tx := source.NewTransaction()
	for _, member := range []string{"a", "b", "c"} {
		require.NoError(t, tx.Add(replayedEntity, replayedTags, member))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "dump")
	file, err := os.Create(path)
	require.NoError(t, err)
	if binary {
		require.NoError(t, source.ExportBinary(file))
	} else {
		require.NoError(t, source.Export(file))
	}
	require.NoError(t, file.Close())
	dump, err := os.ReadFile(path)
	require.NoError(t, err)
	return dump
}

// historyTuples runs q on the database's History and returns every tuple.
func historyTuples(t *testing.T, d *Database, q string) [][]interface{} {
	t.Helper()
	rel, err := d.History().Query(q)
	require.NoError(t, err)
	iter := rel.Iterator()
	defer iter.Close()
	var tuples [][]interface{}
	for iter.Next() {
		tuples = append(tuples, append([]interface{}(nil), iter.Tuple()...))
	}
	require.NoError(t, iter.Error())
	return tuples
}

// A replay puts a dump's datoms into the store with the ElementIDs the dump
// carries and composes no schema: the declaration the dump carries is in the
// store once, its members are in the store, and the database runs with the
// schema it had until an install composes the declared definition.
func TestReplayLoadsTheDumpAndComposesNoSchema(t *testing.T) {
	for _, format := range []struct {
		name   string
		binary bool
		replay func(d *Database, dump []byte) error
	}{
		{"edn", false, func(d *Database, dump []byte) error { return d.replayEDN(bytes.NewReader(dump)) }},
		{"jdzl", true, func(d *Database, dump []byte) error { return d.replayBinary(bytes.NewReader(dump)) }},
		{"edn by its bytes", false, func(d *Database, dump []byte) error { return d.replayDump(bytes.NewReader(dump)) }},
		{"jdzl by its bytes", true, func(d *Database, dump []byte) error { return d.replayDump(bytes.NewReader(dump)) }},
	} {
		t.Run(format.name, func(t *testing.T) {
			dump := declaredTagsDump(t, format.binary)
			d, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory-trees"})
			require.NoError(t, err)
			t.Cleanup(func() { _ = d.Close() })

			require.NoError(t, format.replay(d, dump))

			require.Len(t, historyTuples(t, d, `[:find ?tx :where [?d :db/ident :replayed/tags ?tx]]`), 1,
				"the store holds the dump's one declaration")
			require.Len(t, historyTuples(t, d, `[:find ?m ?tx :where [?e :replayed/tags ?m ?tx]]`), 3,
				"the store holds the dump's members")
			require.Nil(t, d.Schema().GetAttribute(replayedTags), "the replay composes no schema")

			require.NoError(t, d.installSchema(true))
			def := d.Schema().GetAttribute(replayedTags)
			require.NotNil(t, def, "the install composes the declaration the dump carried")
			require.Same(t, schema.CardinalityMany, def.Cardinality)
		})
	}
}
