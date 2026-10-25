package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The default branch of an or-default over a subquery binds each of its
// symbols by name, whatever the order the subquery's binding names them in:
// a ground of 7 and 8 into [?u ?v] gives ?u 7 and ?v 8 under both optimizer
// paths. x carries no :t/c, so it takes the defaults, and y's max and min of
// its one :t/c are both 5.
func TestOrDefaultLowering_DefaultsBindBySymbol(t *testing.T) {
	x, y := named("x"), named("y")
	facts := []fact{
		{x, ":t/b", int64(1)},
		{y, ":t/b", int64(1)}, {y, ":t/c", int64(5)},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, facts)
			rel, err := d.Query(`[:find ?e ?v ?u :where [?e :t/b _] (or-default-join [[?e] ?v ?u] [(q [:find (max ?w) (min ?w) :in $ ?e :where [?e :t/c ?w]] $ ?e) [[?v ?u]]] [(ground [7 8]) [?u ?v]])]`)
			require.NoError(t, err)
			require.Equal(t, []string{"x 8 7", "y 5 5"}, renderedTuples(t, rel, names))
		})
	}
}
