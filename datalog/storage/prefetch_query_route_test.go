package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Entity prefetch fills the EA cache with a prefetched window's entities on
// the Query route as on the executor route (docs/reference/PLANNER_OPTIONS.md,
// EnableEntityPrefetch): after a query whose fallback prefetches the one
// window above the threshold, the cache holds every attribute of that
// window's entities, an attribute no clause names among them, and with the
// option off it holds that attribute for no entity. Under a cap of 128 the
// subject's 120 entities are read in windows of 1, 2, 4, 8, 16, 32 and 57
// tuples, the last the one window above the threshold.
func TestPrefetchFillsTheCacheOnTheQueryRouteAsOnTheExecutorRoute(t *testing.T) {
	const subjectSize, lastWindow = 120, 57
	s, err := schema.NewBuilder().
		Attribute(":t/n").Type(schema.TypeLong).One().Add().
		Attribute(":t/a").Type(schema.TypeLong).One().Add().
		Attribute(":t/x").Type(schema.TypeLong).One().Add().
		Build()
	require.NoError(t, err)
	var facts []fact
	for i := 1; i <= subjectSize; i++ {
		e := named(fmt.Sprintf("e%d", i))
		facts = append(facts, fact{e, ":t/n", int64(i)}, fact{e, ":t/a", int64(i)}, fact{e, ":t/x", int64(i)})
	}
	const text = `[:find ?e ?v :where [?e :t/n _] (or-default-join [[?e] ?v] [?e :t/a ?v] [(ground 0) ?v])]`
	unread := kw(":t/x")
	routes := []struct {
		name string
		run  func(t *testing.T, d *Database)
	}{
		{"Query", func(t *testing.T, d *Database) {
			_, err := executor.CollectTuples(d.Query(text))
			require.NoError(t, err)
		}},
		{"NewExecutor().Execute", func(t *testing.T, d *Database) {
			q, err := parser.ParseQuery(text)
			require.NoError(t, err)
			rel, err := d.NewExecutor().Execute(q)
			require.NoError(t, err)
			_, err = executor.CollectTuples(rel, nil)
			require.NoError(t, err)
		}},
	}
	legs := legsFor(optimizerModes, prefetchOption, fusionOption)
	for _, mode := range append(legsWhere(legs, func(m optimizerMode) bool { return m.prefetch }), legsWhere(legs, func(m optimizerMode) bool { return !m.prefetch })...) {
		for _, route := range routes {
			t.Run(mode.name+"/"+route.name, func(t *testing.T) {
				opts := mode.plannerOptions()
				opts.ScopeWindowCap = 128
				d := openBackendDB(t, mode.backend, DatabaseOptions{Schema: s, PlannerOptions: &opts})
				names := commitFacts(t, d, facts)
				route.run(t, d)
				cached := 0
				for e := range names {
					if d.cache.GetCachedAttrs(e)[unread] {
						cached++
					}
				}
				if !mode.prefetch {
					require.Zero(t, cached, "with the option off no entity holds the unread attribute in the cache")
					return
				}
				require.Equal(t, lastWindow, cached, "the prefetched window's entities hold the unread attribute in the cache")
			})
		}
	}
}
