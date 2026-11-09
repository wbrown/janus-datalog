package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// plannedRouteItems writes six items, each with a kind and a label: items of
// one kind are labeled with the other kind's name.
func plannedRouteItems(t *testing.T, db *Database) {
	t.Helper()
	kind, label := datalog.NewKeyword(":item/kind"), datalog.NewKeyword(":item/label")
	tx := db.NewTransaction()
	for i := 0; i < 6; i++ {
		item := datalog.NewIdentity(fmt.Sprintf("routed-item:%d", i))
		require.NoError(t, tx.Add(item, kind, []string{"gear", "tool"}[i%2]))
		require.NoError(t, tx.Add(item, label, []string{"tool", "gear"}[i%2]))
	}
	_, err := tx.Commit()
	require.NoError(t, err)
}

// windowedForms runs queryText with inputs on a backend under a leg's planner
// options and answers its tuples and the forms its windows were read under.
func windowedForms(t *testing.T, backend Backend, plannerOpts planner.PlannerOptions, disableCache bool, queryText string, inputs ...interface{}) ([][]interface{}, []string) {
	t.Helper()
	var forms []string
	db := openBackendDB(t, backend, DatabaseOptions{
		DisableCache:   disableCache,
		PlannerOptions: &plannerOpts,
		AnnotationHandler: func(event annotations.Event) {
			if event.Name == annotations.ScopeWindow {
				forms = append(forms, event.Data[annotations.KeyForm].(string))
			}
		},
	})
	plannedRouteItems(t, db)
	forms = nil
	result, err := db.Query(queryText, inputs...)
	require.NoError(t, err)
	tuples, err := executor.CollectTuples(result, nil)
	require.NoError(t, err)
	return tuples, forms
}

// plannedRouteReference answers queryText with inputs on the reference leg
// with the cache off.
func plannedRouteReference(t *testing.T, queryText string, inputs ...interface{}) [][]interface{} {
	t.Helper()
	want, _ := windowedForms(t, referenceMode.backend, referenceMode.plannerOptions(), true, queryText, inputs...)
	return want
}

// A pattern sharing a variable with a group the plan does not prove held in
// memory reads that group in windows: the first pattern's matches stream, and
// the second pattern, sharing ?k at its value position, is read over them
// window by window. Every leg answers what the reference leg answers.
func TestAPatternOverAStreamingGroupIsReadInWindows(t *testing.T) {
	const queryText = `[:find ?e ?x :where [?e :item/kind ?k] [?x :item/label ?k]]`
	want := plannedRouteReference(t, queryText)
	require.Len(t, want, 18, "each item of a kind pairs with the three items labeled with it")

	for _, mode := range legsFor(optimizerModes) {
		for _, cache := range cacheTestModes {
			t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
				tuples, forms := windowedForms(t, mode.backend, mode.plannerOptions(), cache.disableCache, queryText)
				require.ElementsMatch(t, want, tuples, "the leg answers what the reference leg answers")
				require.Contains(t, forms, "pattern", "the second pattern reads the first one's matches in windows")
			})
		}
	}
}

// A pattern sharing its variables only with an input relation, which the
// query binds in memory, is bound through it: no window is read. Every leg
// answers what the reference leg answers.
func TestAPatternBoundThroughAnInputRelationTakesTheBoundRoute(t *testing.T) {
	const queryText = `[:find ?e ?k :in $ [?e ...] :where [?e :item/kind ?k]]`
	given := []datalog.Identity{datalog.NewIdentity("routed-item:0"), datalog.NewIdentity("routed-item:1")}
	want := plannedRouteReference(t, queryText, given)
	require.Len(t, want, 2, "each given item answers its kind")

	for _, mode := range legsFor(optimizerModes, fusionOption) {
		for _, cache := range cacheTestModes {
			t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
				tuples, forms := windowedForms(t, mode.backend, mode.plannerOptions(), cache.disableCache, queryText, given)
				require.ElementsMatch(t, want, tuples, "the leg answers what the reference leg answers")
				require.Empty(t, forms, "a pattern bound through an input relation reads no window")
			})
		}
	}
}

// A pattern the plan reads in windows is read in windows whatever execution
// has read of its group before it. On the prefetch legs the first pattern's
// matches are read into memory to find their entities before the second
// pattern runs, and on every leg the second pattern, sharing ?k with them,
// reads them in windows. Every leg answers what the reference leg answers.
func TestAPatternsRouteIsThePlansWhateverExecutionHasReadOfItsGroup(t *testing.T) {
	const queryText = `[:find ?e ?x :where [?e :item/kind ?k] [?x :item/label ?k]]`
	want := plannedRouteReference(t, queryText)
	require.Len(t, want, 18, "each item of a kind pairs with the three items labeled with it")

	for _, mode := range legsFor(optimizerModes) {
		for _, cache := range cacheTestModes {
			t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
				tuples, forms := windowedForms(t, mode.backend, mode.plannerOptions(), cache.disableCache, queryText)
				require.ElementsMatch(t, want, tuples, "the leg answers what the reference leg answers")
				require.Contains(t, forms, "pattern", "the second pattern reads the first one's matches in windows, as the plan states")
			})
		}
	}
}
