package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A group a subquery marked for replay and never read is read whole at the
// find boundary: a result the group has no part in is empty, with no error,
// and one it has a part in is answered.
func TestFindBoundaryReadsAGroupMarkedForReplay(t *testing.T) {
	entities := map[string][]fact{
		"e0": {{":p", int64(1)}},
		"s0": {{":r", ref("e0")}},
		"y0": {{":q", int64(1)}},
	}
	count := `[(q [:find (count ?s) :in $ ?e :where [?s :r ?e]] $ ?e) [[?c]]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)

			empty := "[:find ?e ?c :where [?e :absent _] [?y :q _] " + count + "]"
			rel, err := d.Query(empty)
			require.NoError(t, err, empty)
			require.Empty(t, renderedRows(t, rel, names), empty)

			some := "[:find ?e ?c :where [?e :p _] [?y :q _] " + count + "]"
			rel, err = d.Query(some)
			require.NoError(t, err, some)
			require.Equal(t, []string{"e0 1"}, renderedRows(t, rel, names), some)
		})
	}
}

// A group a subquery's product opened and never pulled is read whole at the
// find boundary.
func TestProductOverAnEmptyGroupLeavesTheOtherGroupReadable(t *testing.T) {
	entities := map[string][]fact{
		"e0": {{":p", int64(1)}},
		"y0": {{":q", int64(1)}},
	}
	count := `[(q [:find (count ?s) :in $ ?e :where [?s :r ?e]] $ ?e) [[?c]]]`
	text := "[:find ?e ?y ?c :where [?e :absent _] [?y :q _] " + count + "]"
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			rel, err := d.Query(text)
			require.NoError(t, err, text)
			require.Empty(t, renderedRows(t, rel, names), text)
		})
	}
}
