package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A find element renders at its own position through the query path: the
// k-th find element naming a variable reads the k-th position of the result
// carrying it, under a sort on a :where-bound key too, where the sorted
// result is stripped to the find before the pulls render.
func TestFindPull_RendersAtItsOwnPositionBesideThePlainVariable(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			s, err := schema.NewBuilder().Attribute(":t/name").Type(schema.TypeString).Add().Build()
			require.NoError(t, err)
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
			a := datalog.NewIdentity("position:a")
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(a, kw(":t/name"), "n"))
			_, err = tx.Commit()
			require.NoError(t, err)
			named := map[string]interface{}{"t/name": "n"}

			beside, err := executor.CollectTuples(d.Query(`[:find ?e (pull ?e [:t/name]) :where [?e :t/name "n"]]`))
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{a, named}}, beside, "the plain variable then its pull")

			before, err := executor.CollectTuples(d.Query(`[:find (pull ?e [:t/name]) ?e :where [?e :t/name "n"]]`))
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{named, a}}, before, "the pull then the plain variable")

			sorted, err := executor.CollectTuples(d.Query(`[:find ?e (pull ?e [:t/name]) :where [?e :t/name ?n] :order-by [[?n :asc]]]`))
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{a, named}}, sorted, "under a sort on a :where-bound key, after the strip to the find")
		})
	}
}
