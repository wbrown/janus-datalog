package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestAUniqueSetScannedOverEveryEntityIsServedFromTheCache pins that a
// cardinality-many attribute declared unique, matched with its entity
// unbound, answers every entity's members alike with the cache enabled and
// disabled. With the cache enabled the scan stores each entity's set under the
// unique rule its definition declares, and a second scan is served those
// entries.
func TestAUniqueSetScannedOverEveryEntityIsServedFromTheCache(t *testing.T) {
	tag := datalog.NewKeyword(":scan-many/tag")
	alice := datalog.NewIdentity("scan-many:alice")
	bob := datalog.NewIdentity("scan-many:bob")
	const q = `[:find ?e ?v :where [?e :scan-many/tag ?v]]`
	want := [][]interface{}{{alice, "a1"}, {alice, "a2"}, {bob, "b1"}}

	for _, mode := range legsFor(backendModes) {
		for _, cache := range []struct {
			name    string
			disable bool
		}{{"cache_enabled", false}, {"cache_disabled", true}} {
			t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
				s, err := schema.NewBuilder().
					Attribute(":scan-many/tag").Type(schema.TypeString).Many().Unique(schema.UniqueValue).Add().
					Build()
				require.NoError(t, err)
				db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: cache.disable})

				tx := db.NewTransaction()
				require.NoError(t, tx.Add(alice, tag, "a1"))
				require.NoError(t, tx.Add(alice, tag, "a2"))
				require.NoError(t, tx.Add(bob, tag, "b1"))
				_, err = tx.Commit()
				require.NoError(t, err)

				first, err := executor.CollectTuples(db.Query(q))
				require.NoError(t, err)
				require.ElementsMatch(t, want, first, "the scan answers every entity's members")
				if cache.disable {
					return
				}

				stored := map[datalog.Identity]*CacheEntry{}
				for _, e := range []datalog.Identity{alice, bob} {
					slot, held := db.Cache().slots.Load(CacheKey{E: e, A: tag})
					require.True(t, held && slot.entry != nil, "the scan stores %s's set", e)
					assert.True(t, slot.entry.unique, "%s's entry carries the unique rule the definition declares", e)
					stored[e] = slot.entry
				}

				second, err := executor.CollectTuples(db.Query(q))
				require.NoError(t, err)
				require.ElementsMatch(t, want, second, "the second scan answers every entity's members")
				for e, entry := range stored {
					slot, _ := db.Cache().slots.Load(CacheKey{E: e, A: tag})
					assert.Same(t, entry, slot.entry, "the second scan reads %s's set from the entry the first stored", e)
				}
			})
		}
	}
}
