package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func itemKindSchema(t testing.TB) *schema.Schema {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(":item/kind").Type(schema.TypeKeyword).Add().
		Attribute(":item/next").Type(schema.TypeRef).Add().
		Build()
	require.NoError(t, err)
	return s
}

// widgetQuery binds each item from the chain and checks its kind: every item
// but the first is the next of another.
const widgetQuery = `[:find ?e :where [?a :item/next ?e] [?e :item/kind :kind/widget]]`

// widgetValidations runs widgetQuery on d and returns the items it answers,
// with how many candidates it validated against storage and how many through
// the cache.
func widgetValidations(t *testing.T, d *Database) (items map[datalog.Identity]bool, fromStorage, fromCache int) {
	t.Helper()
	analysis, err := d.Analyze(widgetQuery)
	require.NoError(t, err)
	items = map[datalog.Identity]bool{}
	it := analysis.Result.Iterator()
	for it.Next() {
		items[it.Tuple()[0].(datalog.Identity)] = true
	}
	require.NoError(t, it.Error())
	require.NoError(t, it.Close())
	for _, ev := range analysis.Events {
		switch ev.Name {
		case annotations.VValidationResult:
			fromStorage++
		case annotations.VValidationCacheResolved:
			fromCache++
		}
	}
	return items, fromStorage, fromCache
}

// TestAsOfHandleValidatesCandidatesThroughItsOwnCache pins that an as-of
// handle, which owns a private EA cache, answers a bound-entity attribute
// check through that cache as the latest view answers it through the
// database's: a second run of one query over the handle, whose cache then
// holds every (E, A) the first run resolved, validates no candidate against
// storage. With the cache disabled each view answers the same items, every
// candidate validated against storage.
func TestAsOfHandleValidatesCandidatesThroughItsOwnCache(t *testing.T) {
	kind := datalog.NewKeyword(":item/kind")
	next := datalog.NewKeyword(":item/next")
	widget := datalog.NewKeyword(":kind/widget")
	const items = 200
	item := func(i int) datalog.Identity { return datalog.NewIdentity(fmt.Sprintf("as-of-cache:item-%d", i)) }

	for _, mode := range legsFor(optimizerModes) {
		for _, disableCache := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cache_disabled_%t", mode.name, disableCache), func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: itemKindSchema(t), DisableCache: disableCache})
				tx := db.NewTransaction()
				for i := 0; i < items; i++ {
					require.NoError(t, tx.Add(item(i), kind, widget))
					if i > 0 {
						require.NoError(t, tx.Add(item(i-1), next, item(i)))
					}
				}
				position, err := tx.Commit()
				require.NoError(t, err)

				// The as-of handle at the store's own position reads the same facts
				// and owns a cache of its own.
				for _, view := range []struct {
					name string
					db   *Database
				}{{"latest view", db}, {"as-of handle", db.AsOf(position)}} {
					widgetValidations(t, view.db)
					answered, fromStorage, fromCache := widgetValidations(t, view.db)
					require.Len(t, answered, items-1, "%s: every item but the first answers", view.name)
					t.Logf("%s, second run: %d candidates validated against storage, %d through the cache", view.name, fromStorage, fromCache)
					if disableCache {
						assert.Zero(t, fromCache, "%s: a database with no cache resolves no candidate through one", view.name)
					} else {
						assert.Zero(t, fromStorage, "%s: the second run validates its candidates through the view's cache", view.name)
					}
				}
			})
		}
	}
}

// TestAsOfHandleValidatesCandidatesAgainstTheValueAsOfItsTarget pins that an
// as-of handle's cache holds the value as of the handle's target: after a
// second commit makes some items gadgets and withdraws the kind of others, a
// handle at each commit answers the items that were widgets there, on its
// first run and on its second, which validates no candidate against storage.
// With the cache disabled each view answers the same items.
func TestAsOfHandleValidatesCandidatesAgainstTheValueAsOfItsTarget(t *testing.T) {
	kind := datalog.NewKeyword(":item/kind")
	next := datalog.NewKeyword(":item/next")
	widget := datalog.NewKeyword(":kind/widget")
	gadget := datalog.NewKeyword(":kind/gadget")
	const items = 60
	item := func(i int) datalog.Identity {
		return datalog.NewIdentity(fmt.Sprintf("as-of-cache-target:item-%d", i))
	}

	for _, mode := range legsFor(optimizerModes) {
		for _, disableCache := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cache_disabled_%t", mode.name, disableCache), func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: itemKindSchema(t), DisableCache: disableCache})
				tx := db.NewTransaction()
				for i := 0; i < items; i++ {
					require.NoError(t, tx.Add(item(i), kind, widget))
					if i > 0 {
						require.NoError(t, tx.Add(item(i-1), next, item(i)))
					}
				}
				first, err := tx.Commit()
				require.NoError(t, err)

				// Items 1, 4, 7, ... become gadgets, and items 2, 5, 8, ... lose their kind.
				tx = db.NewTransaction()
				for i := 0; i < items; i++ {
					switch i % 3 {
					case 1:
						require.NoError(t, tx.Set(item(i), kind, gadget))
					case 2:
						require.NoError(t, tx.Remove(item(i), kind, widget))
					}
				}
				second, err := tx.Commit()
				require.NoError(t, err)

				widgetsAtFirst := map[datalog.Identity]bool{}
				widgetsAtSecond := map[datalog.Identity]bool{}
				for i := 1; i < items; i++ {
					widgetsAtFirst[item(i)] = true
					if i%3 == 0 {
						widgetsAtSecond[item(i)] = true
					}
				}

				for _, view := range []struct {
					name string
					db   *Database
					want map[datalog.Identity]bool
				}{
					{"as of the first commit", db.AsOf(first), widgetsAtFirst},
					{"as of the second commit", db.AsOf(second), widgetsAtSecond},
					{"latest", db, widgetsAtSecond},
				} {
					t.Run(view.name, func(t *testing.T) {
						answered, _, _ := widgetValidations(t, view.db)
						require.Equal(t, view.want, answered, "the first run answers the widgets of its view")
						answered, fromStorage, fromCache := widgetValidations(t, view.db)
						require.Equal(t, view.want, answered, "the second run answers the widgets of its view")
						if disableCache {
							assert.Zero(t, fromCache, "a database with no cache resolves no candidate through one")
						} else {
							assert.Zero(t, fromStorage, "the second run validates its candidates through the view's cache")
						}
					})
				}
			})
		}
	}
}

// BenchmarkBoundAttributeCheckOnAnAsOfHandle measures the query of the test
// above over five thousand chained items on the persistent backend, at the
// head and on the as-of handle at the store's own position, each view warmed
// by one run before the timer starts, and logs the engine's account of each
// first timed run: its time, its v-validation scans, and the candidates
// validated against storage and through the cache.
func BenchmarkBoundAttributeCheckOnAnAsOfHandle(b *testing.B) {
	db, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "badger", Path: b.TempDir(), Schema: itemKindSchema(b)})
	require.NoError(b, err)
	defer db.Close()

	kind := datalog.NewKeyword(":item/kind")
	next := datalog.NewKeyword(":item/next")
	widget := datalog.NewKeyword(":kind/widget")
	const items = 5000
	item := func(i int) datalog.Identity { return datalog.NewIdentity(fmt.Sprintf("as-of-cache:item-%d", i)) }
	tx := db.NewTransaction()
	for i := 0; i < items; i++ {
		require.NoError(b, tx.Add(item(i), kind, widget))
		if i > 0 {
			require.NoError(b, tx.Add(item(i-1), next, item(i)))
		}
	}
	position, err := tx.Commit()
	require.NoError(b, err)

	for _, view := range []struct {
		name string
		db   *Database
	}{{"head", db}, {"as-of", db.AsOf(position)}} {
		b.Run(view.name, func(b *testing.B) {
			warm, err := view.db.Analyze(widgetQuery)
			require.NoError(b, err)
			require.Equal(b, items-1, warm.Result.Size())
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				analysis, err := view.db.Analyze(widgetQuery)
				require.NoError(b, err)
				require.Equal(b, items-1, analysis.Result.Size())
				if i == 0 {
					scans, fromStorage, fromCache := 0, 0, 0
					for _, ev := range analysis.Events {
						switch ev.Name {
						case annotations.StorageScanComplete:
							if ev.Data[annotations.KeyStrategy] == annotations.ScanVValidation {
								scans++
							}
						case annotations.VValidationResult:
							fromStorage++
						case annotations.VValidationCacheResolved:
							fromCache++
						}
					}
					b.Logf("%s: %v, %d v-validation scans, %d candidates validated against storage, %d through the cache",
						view.name, analysis.TotalTime, scans, fromStorage, fromCache)
				}
			}
		})
	}
}
