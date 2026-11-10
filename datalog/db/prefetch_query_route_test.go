package db_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Entity prefetch fills the EA cache on db.Query
// (docs/reference/PLANNER_OPTIONS.md, EnableEntityPrefetch): the fallback's
// prefetch trigger for the one window above the threshold is followed by the
// store's prefetch completion, which populates one entry per attribute of
// each of the window's entities, and with the option off nothing is triggered
// and nothing populated. Under a cap of 128 the subject's 120 entities are
// read in windows of 1, 2, 4, 8, 16, 32 and 57 tuples, the last the one
// window above the threshold.
func TestPrefetchFillsTheCacheOnTheQueryRoute(t *testing.T) {
	const subjectSize, lastWindow, attributes = 120, 57, 3
	n, a, x := datalog.NewKeyword(":t/n"), datalog.NewKeyword(":t/a"), datalog.NewKeyword(":t/x")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: n, ValueType: schema.TypeLong, Cardinality: schema.CardinalityOne})
	s.Add(&schema.AttributeDefinition{Ident: a, ValueType: schema.TypeLong, Cardinality: schema.CardinalityOne})
	s.Add(&schema.AttributeDefinition{Ident: x, ValueType: schema.TypeLong, Cardinality: schema.CardinalityOne})
	const text = `[:find ?e ?v :where [?e :t/n _] (or-default-join [[?e] ?v] [?e :t/a ?v] [(ground 0) ?v])]`
	for _, mode := range legsFor(t, optimizerModes, prefetchOption) {
		t.Run(mode.name, func(t *testing.T) {
			var mu sync.Mutex
			triggers := 0
			var populated []int
			handler := func(e annotations.Event) {
				mu.Lock()
				defer mu.Unlock()
				switch e.Name {
				case annotations.PrefetchTrigger:
					triggers++
				case annotations.StorageResolveComplete:
					if count, ok := e.Data[annotations.KeyEntriesPopulated].(int); ok {
						populated = append(populated, count)
					}
				}
			}
			opts := mode.plannerOptions()
			opts.ScopeWindowCap = 128
			d, err := db.OpenMemory(db.WithSchema(s), db.WithPlannerOptions(opts), db.WithAnnotationHandler(handler))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, d.Close()) })
			tx := d.NewTransaction()
			for i := 1; i <= subjectSize; i++ {
				e := datalog.NewIdentity(fmt.Sprintf("e%d", i))
				require.NoError(t, tx.Add(e, n, int64(i)))
				require.NoError(t, tx.Add(e, a, int64(i)))
				require.NoError(t, tx.Add(e, x, int64(i)))
			}
			_, err = tx.Commit()
			require.NoError(t, err)
			_, err = executor.CollectTuples(d.Query(text))
			require.NoError(t, err)
			mu.Lock()
			defer mu.Unlock()
			if !mode.prefetch {
				require.Zero(t, triggers, "nothing is triggered with the option off")
				require.Empty(t, populated, "nothing is populated with the option off")
				return
			}
			require.Equal(t, 1, triggers, "the one window above the threshold is prefetched")
			require.Equal(t, []int{lastWindow * attributes}, populated, "the store's prefetch populates one entry per attribute of each of the window's entities")
		})
	}
}
