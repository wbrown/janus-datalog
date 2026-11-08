package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestAHistoryHandleStoresNothingInTheCacheItShares pins that a History
// handle, which reads every datom as written and resolves none, stores no
// entry in the cache it shares with its database. Warming a unique attribute
// and a vector through it, and reading the vector through it, leave the cache
// empty, while the same warming through the latest handle fills it. The
// handle's vector reads answer alike with the cache enabled and disabled.
func TestAHistoryHandleStoresNothingInTheCacheItShares(t *testing.T) {
	email := datalog.NewKeyword(":history-cache/email")
	lines := datalog.NewKeyword(":history-cache/lines")
	doc := datalog.NewIdentity("history-cache:doc")

	for _, mode := range backendModes {
		for _, cache := range []struct {
			name    string
			disable bool
		}{{"cache_enabled", false}, {"cache_disabled", true}} {
			t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
				s, err := schema.NewBuilder().
					Attribute(":history-cache/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
					Attribute(":history-cache/lines").Type(schema.TypeString).Vector().Add().
					Build()
				require.NoError(t, err)
				db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: cache.disable})

				tx := db.NewTransaction()
				require.NoError(t, tx.Set(doc, email, "doc@example.com"))
				require.NoError(t, tx.Add(doc, lines, "first"))
				require.NoError(t, tx.Add(doc, lines, "second"))
				_, err = tx.Commit()
				require.NoError(t, err)
				if !cache.disable {
					db.Cache().Clear()
				}

				history := db.History()
				require.NoError(t, history.WarmCache([]datalog.Keyword{email, lines}))
				nth, err := history.GetVectorNth(doc, lines, 1)
				require.NoError(t, err)
				assert.Equal(t, "second", nth, "the History handle reads the vector's second element")
				length, err := history.GetVectorLength(doc, lines)
				require.NoError(t, err)
				assert.Equal(t, int64(2), length, "the History handle reads the vector's length")

				if cache.disable {
					return
				}
				db.Cache().slots.Range(func(key CacheKey, slot cacheSlot) bool {
					assert.Nil(t, slot.entry, "a History handle stored an entry for %s", key.A)
					return true
				})

				require.NoError(t, db.WarmCache([]datalog.Keyword{email, lines}))
				for _, attr := range []datalog.Keyword{email, lines} {
					slot, held := db.Cache().slots.Load(CacheKey{E: doc, A: attr})
					assert.True(t, held && slot.entry != nil, "the latest handle's warming stores an entry for %s", attr)
				}
			})
		}
	}
}
