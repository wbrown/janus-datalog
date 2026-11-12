package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// An open under the schema its store already states reads the store and writes
// nothing, and it draws no ElementID from the database's clock either: the
// clock stands where the store's newest datom left it. The cache of a database
// being opened holds no view resolved under a user's definition, so the
// install that ends the open has no view to drop and no floor to raise for
// one. The schema declares one attribute of each cardinality, each a
// definition the install composes over the engine's words.
func TestAnOpenUnderAnUnchangedSchemaDrawsNoElementID(t *testing.T) {
	given, err := schema.NewBuilder().
		Attribute(":open/name").Type(schema.TypeString).Add().
		Attribute(":open/tags").Type(schema.TypeString).Many().Add().
		Attribute(":open/steps").Type(schema.TypeLong).Vector().Add().
		Build()
	require.NoError(t, err)
	for _, c := range reopenBackendCases() {
		t.Run(c.name, func(t *testing.T) {
			first := c.open(t, DatabaseOptions{Schema: given})
			tx := first.NewTransaction()
			require.NoError(t, tx.Add(datalog.NewIdentity("open:e"), datalog.NewKeyword(":open/name"), "a"))
			_, err := tx.Commit()
			require.NoError(t, err)
			newest, err := first.store.MaxElementID()
			require.NoError(t, err)

			reopened := c.open(t, DatabaseOptions{Schema: given})
			again, err := reopened.store.MaxElementID()
			require.NoError(t, err)
			require.Equal(t, newest, again, "an open under the schema the store states writes nothing")
			require.Equal(t, newest.Lamport, reopened.clock.Current(),
				"an open under the schema the store states draws no ElementID: the clock stands at the store's newest datom")
		})
	}
}
