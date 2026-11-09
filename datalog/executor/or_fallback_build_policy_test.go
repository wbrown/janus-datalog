package executor_test

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
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// A fallback's build of a branch of data patterns alone reports its narrowing
// to each window's keys, on a cold EA cache and on a warm one, and a branch
// holding another clause reports none, though it is built.
func TestAFallbackReportsTheNarrowingOfItsPatternBranchToEachWindowsKeys(t *testing.T) {
	// Every third entity has no :t/a, so its tuple takes the ground branch's
	// default.
	const subjectSize = 120
	tn, ta := datalog.NewKeyword(":t/n"), datalog.NewKeyword(":t/a")
	s, err := schema.NewBuilder().
		Attribute(":t/n").Type(schema.TypeLong).One().Add().
		Attribute(":t/a").Type(schema.TypeLong).One().Add().
		Build()
	require.NoError(t, err)
	const text = `[:find ?e ?v :where [?e :t/n _] (or-default-join [[?e] ?v] [?e :t/a ?v] [(ground 0) ?v])]`
	for _, mode := range executor.OptimizerModes {
		t.Run(mode.Name(), func(t *testing.T) {
			var mu sync.Mutex
			var events []annotations.Event
			opts := mode.PlannerOptions()
			opts.ScopeWindowCap = 128
			d, err := db.OpenMemory(db.WithSchema(s), db.WithPlannerOptions(opts), db.WithAnnotationHandler(func(e annotations.Event) {
				switch e.Name {
				case annotations.ScopeWindow, annotations.ScopeBranchBuild, annotations.PrefetchTrigger:
					if e.Data[annotations.KeyForm] != "or-default-join" {
						return
					}
				case annotations.OrFallbackBranchNarrowed:
				default:
					return
				}
				mu.Lock()
				defer mu.Unlock()
				events = append(events, e)
			}))
			require.NoError(t, err)
			defer func() { require.NoError(t, d.Close()) }()
			tx := d.NewTransaction()
			var want [][]interface{}
			for i := 1; i <= subjectSize; i++ {
				e := datalog.NewIdentity(fmt.Sprintf("fallback-narrowing:e%d", i))
				require.NoError(t, tx.Add(e, tn, int64(i)))
				if i%3 == 0 {
					want = append(want, []interface{}{e, int64(0)})
					continue
				}
				require.NoError(t, tx.Add(e, ta, int64(i)))
				want = append(want, []interface{}{e, int64(i)})
			}
			_, err = tx.Commit()
			require.NoError(t, err)

			got, err := executor.CollectTuples(d.Query(text))
			require.NoError(t, err)
			require.ElementsMatch(t, want, got)

			mu.Lock()
			defer mu.Unlock()
			windowKeys := -1
			var windows, narrowings, groundBuilds, triggers int
			for _, e := range events {
				switch e.Name {
				case annotations.ScopeWindow:
					windows++
					windowKeys = e.Data["distinct_keys"].(int)
				case annotations.PrefetchTrigger:
					triggers++
				case annotations.ScopeBranchBuild:
					if e.Data["branch"] == 1 {
						groundBuilds++
					}
				case annotations.OrFallbackBranchNarrowed:
					narrowings++
					require.Equal(t, 0, e.Data["branch"], "the ground branch reports no narrowing")
					require.Equal(t, true, e.Data["narrowed"])
					require.Equal(t, windowKeys, e.Data["join_keys"], "the pattern branch is narrowed to its window's keys")
				}
			}
			require.Positive(t, windows, "the subject is read in windows")
			require.Equal(t, windows, narrowings, "each window's build of the pattern branch reports its narrowing")
			require.Positive(t, groundBuilds, "the ground branch is built, so its silence is a report withheld")
			if mode.Prefetch() {
				require.Positive(t, triggers, "a window above the threshold is prefetched, so a build is served warm")
			} else {
				require.Zero(t, triggers, "nothing is prefetched with the option off")
			}
		})
	}
}

// A fallback's branch whose value position reads an input the subject lacks
// is narrowed to the input's value, and its build from the EA cache answers
// what its plan answers over the same window.
func TestAFallbackBranchReadingAnInputIsBuiltFromTheCacheAsByItsPlan(t *testing.T) {
	// Every fourth entity's :t/a is the input's value, 1, and every other's is
	// 2.
	const subjectSize = 120
	tn, ta := datalog.NewKeyword(":t/n"), datalog.NewKeyword(":t/a")
	s, err := schema.NewBuilder().
		Attribute(":t/n").Type(schema.TypeLong).One().Add().
		Attribute(":t/a").Type(schema.TypeLong).One().Add().
		Build()
	require.NoError(t, err)
	const text = `[:find ?e :in $ ?v :where [?e :t/n _] (or-default-join [[?e ?v]] [?e :t/a ?v] [(ground 0) ?z])]`
	// window is one window of the subject: its distinct keys and the size of
	// the branch's build over it.
	type window struct{ keys, built int }
	for _, algebra := range []bool{true, false} {
		t.Run(fmt.Sprintf("algebra=%v", algebra), func(t *testing.T) {
			// runQuery runs the query with entity prefetch as given and answers
			// its windows, the fallback's prefetches and the lookups of :t/a.
			runQuery := func(prefetch bool) ([]window, int, int) {
				var mu sync.Mutex
				var windows []window
				var triggers, lookups int
				opts := storage.DefaultPlannerOptions()
				opts.EnableAlgebraOptimizer = algebra
				opts.EnableEntityPrefetch = prefetch
				opts.EnableAttributeFetchFusion = false
				opts.ScopeWindowCap = 128
				d, err := db.OpenMemory(db.WithSchema(s), db.WithPlannerOptions(opts), db.WithAnnotationHandler(func(e annotations.Event) {
					mu.Lock()
					defer mu.Unlock()
					switch e.Name {
					case annotations.ScopeWindow:
						if e.Data[annotations.KeyForm] == "or-default-join" {
							windows = append(windows, window{keys: e.Data["distinct_keys"].(int)})
						}
					case annotations.ScopeBranchBuild:
						if e.Data[annotations.KeyForm] == "or-default-join" && e.Data["branch"] == 0 {
							windows[len(windows)-1].built = e.Data["branch_size"].(int)
						}
					case annotations.PrefetchTrigger:
						if e.Data[annotations.KeyForm] == "or-default-join" {
							triggers++
						}
					case annotations.StorageResolveComplete:
						_, served := e.Data[annotations.KeyValuesServed]
						_, ofPattern := e.Data[annotations.KeyPattern]
						if served && !ofPattern && e.Data[annotations.KeyAttribute] == ta {
							lookups++
						}
					}
				}))
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close()) }()
				tx := d.NewTransaction()
				var want [][]interface{}
				for i := 1; i <= subjectSize; i++ {
					e := datalog.NewIdentity(fmt.Sprintf("fallback-input:e%d", i))
					value := int64(2)
					if i%4 == 0 {
						value = 1
					}
					require.NoError(t, tx.Add(e, tn, int64(i)))
					require.NoError(t, tx.Add(e, ta, value))
					want = append(want, []interface{}{e})
				}
				_, err = tx.Commit()
				require.NoError(t, err)
				got, err := executor.CollectTuples(d.Query(text, int64(1)))
				require.NoError(t, err)
				require.ElementsMatch(t, want, got, "the fallback answers every subject tuple")
				mu.Lock()
				defer mu.Unlock()
				return windows, triggers, lookups
			}

			fromPlan, triggers, lookups := runQuery(false)
			require.Zero(t, triggers, "nothing is prefetched with the option off")
			require.Zero(t, lookups, "every window's branch is built from its plan")
			narrowed := false
			for _, w := range fromPlan {
				narrowed = narrowed || w.built < w.keys
			}
			require.True(t, narrowed, "a window's build holds fewer tuples than its keys, so the input narrows the branch")

			fromCache, triggers, lookups := runQuery(true)
			require.Positive(t, triggers, "a window above the threshold is prefetched")
			require.Positive(t, lookups, "a window's branch is built from the cache")
			require.Equal(t, fromPlan, fromCache, "the cache's build of each window answers what the plan's does")
		})
	}
}
