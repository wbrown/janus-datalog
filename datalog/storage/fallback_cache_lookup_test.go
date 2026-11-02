package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// runFallbackRecordingLookups runs text on a database holding facts under s,
// with a window cap of 128, entity prefetch as given and attribute-fetch
// fusion off, so a lookup of attr is a fallback's build of a branch from the
// EA cache and nothing else. It returns the rendered result and, in the order
// reported, the windows and prefetch triggers the or-default-join reports
// and the lookups of attr.
func runFallbackRecordingLookups(t *testing.T, mode optimizerMode, s *schema.Schema, facts []fact, text string, attr datalog.Keyword, prefetch bool) ([]string, []annotations.Event) {
	t.Helper()
	var mu sync.Mutex
	var events []annotations.Event
	opts := mode.plannerOptions()
	opts.ScopeWindowCap = 128
	opts.EnableEntityPrefetch = prefetch
	opts.EnableAttributeFetchFusion = false
	d := openBackendDB(t, mode.backend, DatabaseOptions{Schema: s, PlannerOptions: &opts, AnnotationHandler: func(e annotations.Event) {
		mu.Lock()
		defer mu.Unlock()
		switch e.Name {
		case annotations.ScopeWindow, annotations.PrefetchTrigger:
			if e.Data[annotations.KeyForm] == "or-default-join" {
				events = append(events, e)
			}
		case annotations.StorageResolveComplete:
			_, served := e.Data[annotations.KeyValuesServed]
			_, ofPattern := e.Data[annotations.KeyPattern]
			if served && !ofPattern && e.Data[annotations.KeyAttribute] == attr {
				events = append(events, e)
			}
		}
	}})
	names := commitFacts(t, d, facts)
	rel, err := d.Query(text)
	require.NoError(t, err)
	result := renderedTuples(t, rel, names)
	mu.Lock()
	defer mu.Unlock()
	return result, events
}

// eventsNamed is the events of events named name, in order.
func eventsNamed(events []annotations.Event, name string) []annotations.Event {
	var named []annotations.Event
	for _, e := range events {
		if e.Name == name {
			named = append(named, e)
		}
	}
	return named
}

// Under EnableEntityPrefetch a fallback serves its branch from the EA cache
// for the window it prefetches, one lookup per entity of that window, and
// builds the windows before it from its plan; with the option off it
// prefetches nothing and looks nothing up
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.10).
func TestFallbackServesThePrefetchedWindowFromTheCache(t *testing.T) {
	// Under a cap of 128 the subject's 120 entities are read in windows of 1,
	// 2, 4, 8, 16, 32 and 57 tuples, the last the one window above the
	// threshold.
	const subjectSize, lastWindow = 120, 57
	s, err := schema.NewBuilder().
		Attribute(":t/n").Type(schema.TypeLong).One().Add().
		Attribute(":t/a").Type(schema.TypeLong).One().Add().
		Build()
	require.NoError(t, err)
	var facts []fact
	var want []string
	for i := 1; i <= subjectSize; i++ {
		e := named(fmt.Sprintf("e%d", i))
		facts = append(facts, fact{e, ":t/n", int64(i)}, fact{e, ":t/a", int64(i)})
		want = append(want, fmt.Sprintf("%s %d", e.name, i))
	}
	const text = `[:find ?e ?v :where [?e :t/n _] (or-default-join [[?e] ?v] [?e :t/a ?v] [(ground 0) ?v])]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			result, events := runFallbackRecordingLookups(t, mode, s, facts, text, kw(":t/a"), true)
			require.ElementsMatch(t, want, result, "every subject tuple takes its :t/a, from the plan before the prefetch and from the cache after it")
			triggers := eventsNamed(events, annotations.PrefetchTrigger)
			require.Len(t, triggers, 1, "the one window above the threshold is prefetched")
			require.Equal(t, lastWindow, triggers[0].Data["entity_count"], "the window's distinct entities are prefetched")
			require.Len(t, eventsNamed(events, annotations.StorageResolveComplete), lastWindow,
				"the branch is served from the cache for the prefetched window alone")

			result, events = runFallbackRecordingLookups(t, mode, s, facts, text, kw(":t/a"), false)
			require.ElementsMatch(t, want, result)
			require.Empty(t, eventsNamed(events, annotations.PrefetchTrigger), "nothing is prefetched with the option off")
			require.Empty(t, eventsNamed(events, annotations.StorageResolveComplete), "the branch is built from its plan with the option off")
		})
	}
}

// A fallback's build of a branch from the EA cache looks each entity of a
// window up once, though the window's keys hold the entity more than once.
func TestFallbackLooksUpEachEntityOfACachedWindowOnce(t *testing.T) {
	// 128 entities carry two :t/k values each, so the subject's 256 tuples
	// hold each entity twice and a window above the threshold holds more
	// tuples than entities.
	const entities = 128
	s, err := schema.NewBuilder().
		Attribute(":t/k").Type(schema.TypeLong).Many().Add().
		Attribute(":t/a").Type(schema.TypeLong).One().Add().
		Build()
	require.NoError(t, err)
	var facts []fact
	var want []string
	for i := 1; i <= entities; i++ {
		e := named(fmt.Sprintf("e%d", i))
		facts = append(facts, fact{e, ":t/k", int64(1)}, fact{e, ":t/k", int64(2)}, fact{e, ":t/a", int64(i)})
		want = append(want, fmt.Sprintf("%s 1 %d", e.name, i), fmt.Sprintf("%s 2 %d", e.name, i))
	}
	const text = `[:find ?e ?k ?v :where [?e :t/k ?k] (or-default-join [[?e ?k] ?v] [?e :t/a ?v] [(ground 0) ?v])]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			result, events := runFallbackRecordingLookups(t, mode, s, facts, text, kw(":t/a"), true)
			require.ElementsMatch(t, want, result)

			var window map[datalog.Identity]bool
			var windowTuples, prefetchedTuples, prefetchedEntities, prefetchedLookups int
			prefetched, inPrefetched := false, false
			for _, e := range events {
				switch e.Name {
				case annotations.ScopeWindow:
					window = map[datalog.Identity]bool{}
					windowTuples = e.Data["tuple_count"].(int)
					inPrefetched = false
				case annotations.PrefetchTrigger:
					if !prefetched {
						prefetched, inPrefetched = true, true
						prefetchedTuples = windowTuples
						prefetchedEntities = e.Data["entity_count"].(int)
					}
				case annotations.StorageResolveComplete:
					require.NotNil(t, window, "a lookup builds a window's branch")
					entity := e.Data[annotations.KeyEntity].(datalog.Identity)
					require.False(t, window[entity], "an entity of a window is looked up once")
					window[entity] = true
					if inPrefetched {
						prefetchedLookups++
					}
				}
			}
			require.True(t, prefetched, "a window above the threshold is prefetched")
			require.Greater(t, prefetchedTuples, prefetchedEntities, "the prefetched window holds an entity more than once")
			require.Equal(t, prefetchedEntities, prefetchedLookups, "each entity of the prefetched window is looked up once")
		})
	}
}
