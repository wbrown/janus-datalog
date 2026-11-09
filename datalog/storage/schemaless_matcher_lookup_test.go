package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// A matcher holding no schema provider resolves an attribute with no
// definition, and answers it alike through LookupAttribute and through the
// lookup AttributeLookup makes: the value an entity holds, and nothing for an
// attribute it holds none of, with its database's EA cache attached and with
// the cache disabled.
func TestAMatcherWithNoSchemaAnswersThroughEitherLookup(t *testing.T) {
	name := datalog.NewKeyword(":person/name")
	nick := datalog.NewKeyword(":person/nick")
	alice := datalog.NewIdentity("person:alice")
	for _, mode := range optimizerModes {
		for _, disableCache := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cache-off=%v", mode.name, disableCache), func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{DisableCache: disableCache})
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(alice, name, "Alice"))
				_, err := tx.Commit()
				require.NoError(t, err)

				matcher := NewPatternMatcher(db.Store())
				matcher.SetCache(db.Cache())

				value, found, err := matcher.LookupAttribute(alice, name)
				require.NoError(t, err)
				require.True(t, found, "LookupAttribute finds the name alice holds")
				require.Equal(t, "Alice", value)
				value, found, err = matcher.LookupAttribute(alice, nick)
				require.NoError(t, err)
				require.False(t, found, "LookupAttribute finds no nick for alice")
				require.Nil(t, value)

				nameLookup, err := matcher.AttributeLookup(name)
				require.NoError(t, err)
				value, found, err = nameLookup(alice)
				require.NoError(t, err)
				require.True(t, found, "the lookup of :person/name finds the name alice holds")
				require.Equal(t, "Alice", value)
				nickLookup, err := matcher.AttributeLookup(nick)
				require.NoError(t, err)
				value, found, err = nickLookup(alice)
				require.NoError(t, err)
				require.False(t, found, "the lookup of :person/nick finds no nick for alice")
				require.Nil(t, value)
			})
		}
	}
}
