package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A group a subquery marked for replay and never read is read whole at the
// find boundary: a result the group has no part in is empty, with no error,
// and one it has a part in is answered.
func TestFindBoundaryReadsAGroupMarkedForReplay(t *testing.T) {
	e0, s0, y0 := named("e0"), named("s0"), named("y0")
	facts := []fact{
		{e0, ":p", int64(1)},
		{s0, ":r", e0},
		{y0, ":q", int64(1)},
	}
	count := `[(q [:find (count ?s) :in $ ?e :where [?s :r ?e]] $ ?e) [[?c]]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, facts)

			empty := "[:find ?e ?c :where [?e :absent _] [?y :q _] " + count + "]"
			rel, err := d.Query(empty)
			require.NoError(t, err, empty)
			require.Empty(t, renderedTuples(t, rel, names), empty)

			some := "[:find ?e ?c :where [?e :p _] [?y :q _] " + count + "]"
			rel, err = d.Query(some)
			require.NoError(t, err, some)
			require.Equal(t, []string{"e0 1"}, renderedTuples(t, rel, names), some)
		})
	}
}

// A group the find does not read is an existence verdict under aggregation as
// under projection (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §3): a
// count over one group beside an unread group is answered when the unread
// group has a tuple and empty when it has none.
func TestFindBoundaryTakesTheVerdictUnderAggregation(t *testing.T) {
	e0, y0 := named("e0"), named("y0")
	facts := []fact{
		{e0, ":p", int64(1)},
		{y0, ":q", int64(1)},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, facts)

			some := "[:find (count ?e) :where [?e :p _] [?y :q _]]"
			rel, err := d.Query(some)
			require.NoError(t, err, some)
			require.Equal(t, []string{"1"}, renderedTuples(t, rel, names), some)

			empty := "[:find (count ?e) :where [?e :p _] [?y :absent _]]"
			rel, err = d.Query(empty)
			require.NoError(t, err, empty)
			require.Empty(t, renderedTuples(t, rel, names), empty)
		})
	}
}

// A group a subquery's product opened and never pulled is read whole at the
// find boundary.
func TestProductOverAnEmptyGroupLeavesTheOtherGroupReadable(t *testing.T) {
	e0, y0 := named("e0"), named("y0")
	facts := []fact{
		{e0, ":p", int64(1)},
		{y0, ":q", int64(1)},
	}
	count := `[(q [:find (count ?s) :in $ ?e :where [?s :r ?e]] $ ?e) [[?c]]]`
	text := "[:find ?e ?y ?c :where [?e :absent _] [?y :q _] " + count + "]"
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, facts)
			rel, err := d.Query(text)
			require.NoError(t, err, text)
			require.Empty(t, renderedTuples(t, rel, names), text)
		})
	}
}
