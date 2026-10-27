package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A data pattern of fewer than three elements leaves the positions its text
// omits unbound: [?e :t/n] answers what [?e :t/n _] answers, and [?e] what
// [?e _ _] answers, with or without a source named, on both planner paths.
func TestQuery_AShortPatternLeavesItsTrailingPositionsUnbound(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, []fact{
				{named("n1"), ":t/n", int64(1)},
				{named("n1"), ":t/m", int64(2)},
				{named("n2"), ":t/n", int64(3)},
				{named("m1"), ":t/m", int64(4)},
			})
			cases := []struct {
				name   string
				short  string
				blanks string
			}{
				{"the value omitted", `[:find ?e :where [?e :t/n]]`, `[:find ?e :where [?e :t/n _]]`},
				{"the attribute and the value omitted", `[:find ?e :where [?e]]`, `[:find ?e :where [?e _ _]]`},
				{"the source named and the value omitted", `[:find ?e :where [$ ?e :t/n]]`, `[:find ?e :where [$ ?e :t/n _]]`},
			}
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					padded, err := d.Query(c.blanks)
					require.NoError(t, err, c.blanks)
					want := renderedTuples(t, padded, names)
					require.NotEmpty(t, want, "the padded pattern answers entities, so the comparison compares something")
					short, err := d.Query(c.short)
					require.NoError(t, err, c.short)
					require.ElementsMatch(t, want, renderedTuples(t, short, names), "the short pattern answers what the padded pattern answers")
				})
			}
		})
	}
}
