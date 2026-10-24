package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// A subquery's collection argument binds its members as a group inside the
// scope, a relation and so a set, and the inner find aggregates over all of
// them per combination of the call (docs/proposals/RELATION_INPUT_PROOF.md
// §8): the count is one count over every member, an empty collection binds
// nothing and the clause answers no tuple, and a member named twice is one
// member.

// holders are the entities whose :t/r a collection subquery counts under,
// one at 1, two at 2 and one at 3, beside two entities carrying :t/k.
var holders = map[string][]fact{
	"a":  {{":t/r", int64(1)}},
	"b":  {{":t/r", int64(2)}},
	"c":  {{":t/r", int64(2)}},
	"d":  {{":t/r", int64(3)}},
	"z1": {{":t/k", ":x"}},
	"z2": {{":t/k", ":y"}},
}

// countOverCollection is the query passing a ground vector into a counting
// subquery as its collection, answering each :t/k beside the count of the
// entities whose :t/r is a member.
const countOverCollection = `[:find ?k ?c :where [?z :t/k ?k] [(ground %s) ?ns] [(q [:find (count ?e) :in $ [?n ...] :where [?e :t/r ?n]] $ ?ns) [[?c]]]]`

// The aggregate over a collection argument is over all its members at once:
// the members 1, 2 and 3 are held by four entities, so each :t/k is answered
// beside 4.
func TestSubquery_ACollectionArgumentAggregatesOverItsMembers(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, holders)
			rel, err := d.Query(fmt.Sprintf(countOverCollection, "[1 2 3]"))
			require.NoError(t, err)
			require.Equal(t, []string{":x 4", ":y 4"}, renderedRows(t, rel, names))
		})
	}
}

// An empty collection argument binds nothing, so the subquery clause answers
// no tuple and the query answers no row, without a panic.
func TestSubquery_AnEmptyCollectionArgumentAnswersNothing(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, holders)
			var rows []string
			require.NotPanics(t, func() {
				rel, err := d.Query(fmt.Sprintf(countOverCollection, "[]"))
				require.NoError(t, err)
				rows = renderedRows(t, rel, names)
			})
			require.Empty(t, rows)
		})
	}
}

// A collection argument naming a member twice is the set of its members: the
// members 1, 1 and 2 are held by three entities, so each :t/k is answered
// beside 3, without a panic.
func TestSubquery_ACollectionArgumentNamingAMemberTwiceCountsItOnce(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, holders)
			var rows []string
			require.NotPanics(t, func() {
				rel, err := d.Query(fmt.Sprintf(countOverCollection, "[1 1 2]"))
				require.NoError(t, err)
				rows = renderedRows(t, rel, names)
			})
			require.Equal(t, []string{":x 3", ":y 3"}, rows)
		})
	}
}
