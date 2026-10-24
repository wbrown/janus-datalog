package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A plan is served by the prepared text, and a float constant and an integer
// constant are two texts (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §5.2,
// docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §6): on one database holding
// :t/a 5.0 on one entity and :t/a 5 on another, the float constant answers
// the first and the integer constant answers the second, whichever runs
// first, with the plan cache on as with it off.

// numberedEntities carry :t/a as a float on f and as an integer on i.
var numberedEntities = map[string][]fact{
	"f": {{":t/a", 5.0}},
	"i": {{":t/a", int64(5)}},
}

func TestPlanCache_DistinguishesAFloatFromAnInteger(t *testing.T) {
	for _, mode := range optimizerModes {
		for _, cached := range []bool{true, false} {
			name := mode.name + "/plan cache on"
			if !cached {
				name = mode.name + "/plan cache off"
			}
			t.Run(name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				if !cached {
					d.SetPlanCache(nil)
				}
				names := namedEntities(t, d, numberedEntities)

				rel, err := d.Query(`[:find ?e :where [?e :t/a 5.0]]`)
				require.NoError(t, err)
				require.Equal(t, []string{"f"}, renderedRows(t, rel, names), "the float constant answers the float-valued entity")

				rel, err = d.Query(`[:find ?e :where [?e :t/a 5]]`)
				require.NoError(t, err)
				require.Equal(t, []string{"i"}, renderedRows(t, rel, names), "the integer constant answers the integer-valued entity, run after the float's query")
			})
		}
	}
}
