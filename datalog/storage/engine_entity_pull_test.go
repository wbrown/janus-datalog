package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// The engine's words are declared in every running schema, so an engine
// entity answers a wildcard pull with its words whatever the user's schema
// declares, whether a user attribute is in force, and whether the database
// was reopened; and the engine's own queries answer alike on a database
// declaring nothing and on one given a schema
// (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §8, goal 11).

// markerWords are the words a snapshot marker carries.
var markerWords = []string{"db.snapshot/name", "db.snapshot/at-lamport", "db.snapshot/at-replica", "db.snapshot/created"}

// declarationWords are the words a declaration of a string attribute with no
// doc, uniqueness or zero rule carries.
var declarationWordKeys = []string{"db/ident", "db/cardinality", "db/valueType", "db/uniqueElements", "db/neverZeroValue"}

// requirePulledKeys asserts a wildcard pull's keys: the words the entity
// carries, and the entity's id beside them, which a wildcard names as it
// names every attribute.
func requirePulledKeys(t *testing.T, pulled map[string]interface{}, keys []string, msg string) {
	t.Helper()
	var got []string
	for key := range pulled {
		got = append(got, key)
	}
	require.ElementsMatch(t, append(append([]string(nil), keys...), query.DBIDKey), got, msg)
}

// The set a wildcard pull resolves under the stored declarations is nil until
// they declare an attribute beyond the engine's words, and then holds the
// declared attributes and every engine word.
func TestWildcardAttributes_IsNilUntilTheStoreDeclaresBeyondTheEnginesWords(t *testing.T) {
	require.Nil(t, wildcardAttributes(schema.NewSchema()), "nothing declared resolves every attribute the entity carries")
	engineWordAlone := schema.NewSchema().Add(engineSchema.GetAttribute(attributeDeclarationAttr))
	require.Nil(t, wildcardAttributes(engineWordAlone), "a stored declaration of an engine word declares nothing beyond the engine's words")
	s, err := schema.NewBuilder().Attribute(":t/name").Type(schema.TypeString).Add().Build()
	require.NoError(t, err)
	set := wildcardAttributes(s)
	require.Contains(t, set, kw(":t/name"))
	require.NotContains(t, set, kw(":t/later"), "an attribute the store does not declare is not in the set")
	for _, definition := range engineSchema.Attributes() {
		require.Contains(t, set, definition.Ident, "every engine word is in the set")
	}
}

// readback is what the engine's words answer on one database: a pull naming
// :db/id, a wildcard pull of a user entity, the name joined with its
// commit's instant, the name's history, the commit marks, and the snapshots.
type readback struct {
	withID, wildcard []string
	joined, history  int
	marks, snapshots int
}

func engineReadback(t *testing.T, d *Database) readback {
	t.Helper()
	e := datalog.NewIdentity("e")
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(e, kw(":t/name"), "n1"))
	require.NoError(t, tx.Add(e, kw(":t/tags"), "a"))
	require.NoError(t, tx.Add(e, kw(":t/tags"), "b"))
	_, err := tx.Commit()
	require.NoError(t, err)
	tx = d.NewTransaction()
	require.NoError(t, tx.Set(e, kw(":t/name"), "n2"))
	require.NoError(t, tx.Add(e, kw(":t/later"), "z"))
	_, err = tx.Commit()
	require.NoError(t, err)
	_, err = d.Snapshot("s1")
	require.NoError(t, err)
	var r readback
	withID, err := d.Pull(e, `[:db/id :t/name :t/tags]`)
	require.NoError(t, err)
	require.Equal(t, e, withID["db/id"], ":db/id is the entity")
	require.Equal(t, "n2", withID["t/name"])
	for key := range withID {
		r.withID = append(r.withID, key)
	}
	wildcard, err := d.Pull(e, `[*]`)
	require.NoError(t, err)
	for key := range wildcard {
		r.wildcard = append(r.wildcard, key)
	}
	joined, err := executor.CollectTuples(d.Query(`[:find ?v ?t :where [?e :t/name ?v ?tx] [?tx :db/txInstant ?t]]`))
	require.NoError(t, err)
	r.joined = len(joined)
	history, err := executor.CollectTuples(d.History().Query(`[:find ?v ?tx :where [?e :t/name ?v ?tx]]`))
	require.NoError(t, err)
	r.history = len(history)
	marks, err := executor.CollectTuples(d.Query(`[:find ?tx ?t :where [?tx :db/txInstant ?t]]`))
	require.NoError(t, err)
	r.marks = len(marks)
	snapshots, err := d.Snapshots()
	require.NoError(t, err)
	r.snapshots = len(snapshots)
	return r
}

func TestEngineWords_ReadAlikeOnASchemalessAndASchemaGivenDatabase(t *testing.T) {
	s, err := schema.NewBuilder().Attribute(":t/name").Type(schema.TypeString).Add().
		Attribute(":t/tags").Type(schema.TypeString).Many().Add().Build()
	require.NoError(t, err)
	schemaless, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory"})
	require.NoError(t, err)
	given, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory", Schema: s})
	require.NoError(t, err)
	none, some := engineReadback(t, schemaless), engineReadback(t, given)
	require.ElementsMatch(t, []string{"db/id", "t/name", "t/tags"}, none.withID)
	require.ElementsMatch(t, none.withID, some.withID, "a pull naming :db/id answers the same keys")
	require.ElementsMatch(t, []string{"db/id", "t/name", "t/tags", "t/later"}, none.wildcard, "a wildcard on a database declaring nothing answers every attribute the entity carries, and its id beside them")
	require.ElementsMatch(t, []string{"db/id", "t/name", "t/tags"}, some.wildcard, "a wildcard under a given schema answers the declared attributes, and its id beside them")
	require.Equal(t, 2, none.history, "the name's history holds both assertions")
	require.Equal(t, some.history, none.history)
	require.Equal(t, 3, none.marks, "one commit mark per commit, the snapshot's among them")
	require.Equal(t, 4, some.marks, "one commit mark per commit, the open's declaration of the given schema and the snapshot among them")
	require.Equal(t, some.joined, none.joined, "the name joins its commit's instant alike")
	require.Equal(t, 1, none.snapshots)
	require.Equal(t, some.snapshots, none.snapshots)
}
