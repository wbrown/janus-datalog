package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// The arm that builds a fallback branch from the EA cache answers, through
// the database's executor, d.NewExecutor().Execute, the CLI's route for a
// query with no inputs, what the reference leg answers: a branch of one data
// pattern reading its fourth or fifth position takes the planned build on
// every leg, with the EA cache on and off, in the latest view and an as-of
// view. The executor's matcher on this route is the database's own, which
// prefetches a window on every prefetch leg.
func TestReviewFallbackArmAnswersWithPrefetchWhatTheReferenceAnswers(t *testing.T) {
	s, facts := fallbackBranchFixture(t)
	answer := func(t *testing.T, mode optimizerMode, disableCache bool, asOf bool, text string) []string {
		d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: disableCache})
		names := commitFacts(t, d, facts)
		view := d
		if asOf {
			point, err := d.store.MaxElementID()
			require.NoError(t, err)
			view = d.AsOf(point)
		}
		q, err := parser.ParseQuery(text)
		require.NoError(t, err, text)
		rel, err := view.NewExecutor().Execute(q)
		require.NoError(t, err, text)
		return renderedTuples(t, rel, names)
	}
	for _, c := range fallbackBranchCases {
		for _, asOf := range []bool{false, true} {
			viewName := "latest"
			if asOf {
				viewName = "as-of"
			}
			reference := answer(t, referenceMode, true, asOf, c.text)
			for _, mode := range legsFor(optimizerModes, prefetchOption, sharingOption) {
				for _, disable := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/%s/disableCache=%v", c.name, viewName, mode.name, disable), func(t *testing.T) {
						assert.Equal(t, reference, answer(t, mode, disable, asOf, c.text), c.text)
					})
				}
			}
		}
	}
}
