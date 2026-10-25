package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// An executor a caller builds over a refusing database's matcher refuses an
// attribute the schema does not declare and answers a query naming declared
// attributes alone, as the database's own executor does, under both planner
// paths and whether or not the caller passes the database's planner options
// (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §5.5, the roster paragraph).
func TestNewExecutor_OverARefusingMatcherRefusesAndAnswers(t *testing.T) {
	s, err := schema.NewBuilder().Attribute(":t/name").Type(schema.TypeString).Add().Build()
	require.NoError(t, err)
	typo, err := parser.ParseQuery(`[:find ?e :where [?e :t/nope _]]`)
	require.NoError(t, err)
	declared, err := parser.ParseQuery(`[:find ?v :where [?e :t/name ?v]]`)
	require.NoError(t, err)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, RefuseUnknownAttributes: true})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(datalog.NewIdentity("e"), kw(":t/name"), "n"))
			_, err := tx.Commit()
			require.NoError(t, err)
			executors := []struct {
				name string
				exec *executor.Executor
			}{
				{"the database's own", d.NewExecutor()},
				{"built by the caller", executor.NewExecutor(d.Matcher(), d)},
				{"built by the caller with the database's options", executor.NewExecutorWithOptions(d.Matcher(), d, mode.plannerOptions())},
			}
			for _, e := range executors {
				t.Run(e.name, func(t *testing.T) {
					var refused error
					require.NotPanics(t, func() {
						_, refused = executor.CollectTuples(e.exec.Execute(typo))
					})
					requireUnknown(t, refused, kw(":t/nope"))
					var tuples [][]interface{}
					var answered error
					require.NotPanics(t, func() {
						tuples, answered = executor.CollectTuples(e.exec.Execute(declared))
					})
					require.NoError(t, answered)
					require.Equal(t, [][]interface{}{{"n"}}, tuples)
				})
			}
		})
	}
}
