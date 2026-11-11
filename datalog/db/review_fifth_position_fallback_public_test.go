package db_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// Through the public API, an or-default-join whose first branch is
// [?e :t/a ?v _ false] answers the default for every subject tuple in the
// latest view and in an as-of view, on every leg as the reference leg
// answers, with the EA cache on and off.
func TestReviewPublicFallbackOfFiveElementsAnswersTheDefaultWithPrefetch(t *testing.T) {
	const text = `[:find ?e ?v :where [?e :t/n _] (or-default-join [[?e] ?v] [?e :t/a ?v _ false] [(ground 0) ?v])]`
	answers := func(t *testing.T, opts []db.Option, asOf bool) []string {
		d, err := db.OpenMemory(opts...)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, d.Close()) })
		names := map[datalog.Identity]string{}
		tx := d.NewTransaction()
		for i := 1; i <= 120; i++ {
			e := datalog.NewIdentity(fmt.Sprintf("e%d", i))
			names[e] = fmt.Sprintf("e%d", i)
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":t/n"), int64(i)))
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":t/a"), int64(i)))
		}
		point, err := tx.Commit()
		require.NoError(t, err)
		view := d
		if asOf {
			view = d.AsOf(point)
		}
		rel, err := view.Query(text)
		require.NoError(t, err)
		tuples, err := executor.CollectTuples(rel, nil)
		require.NoError(t, err)
		var out []string
		for _, tuple := range tuples {
			out = append(out, fmt.Sprintf("%s %v", names[tuple[0].(datalog.Identity)], tuple[1]))
		}
		sort.Strings(out)
		return out
	}
	caches := []struct {
		name string
		opts []db.Option
	}{{"cache", nil}, {"without cache", []db.Option{db.WithoutCache()}}}
	for _, asOf := range []bool{false, true} {
		want := answers(t, []db.Option{db.WithPlannerOptions(referenceMode.plannerOptions())}, asOf)
		for _, mode := range legsFor(t, optimizerModes, prefetchOption) {
			for _, cache := range caches {
				t.Run(fmt.Sprintf("asOf=%v/%s/%s", asOf, mode.name, cache.name), func(t *testing.T) {
					got := answers(t, append([]db.Option{db.WithPlannerOptions(mode.plannerOptions())}, cache.opts...), asOf)
					assert.Equal(t, want, got)
				})
			}
		}
	}
}
