package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A pattern's answer carries its entity variable among its identity symbols
// on the executor's routes as on the matcher's: a pattern sharing a symbol
// with a group read as a stream answers through the window, and an attribute
// fetch over a group holding its entity fuses into lookups, which answer an
// entity only where it is an identity. Each route's answer is read through
// the query's final relation, with the route's own event as the precondition
// that the route ran, on the legs the route exists on: the window on every
// leg, the fused fetch where fusion is on.
func TestAnExecutorRoutesAnswerCarriesThePatternsEntityVariable(t *testing.T) {
	a, b := datalog.NewIdentity("route:a"), datalog.NewIdentity("route:b")
	for _, tc := range []struct {
		name   string
		query  string
		inputs []interface{}
		ran    func(annotations.Event) bool
		legs   func(optimizerMode) bool
	}{
		{"a pattern binding its entity over an expression's stream answers through the window",
			`[:find ?e ?x :where [?x :t/n ?n] [(+ ?n 1) ?m] [?e :t/count ?m]]`, nil,
			func(e annotations.Event) bool {
				return e.Name == annotations.ScopeWindow && e.Data[annotations.KeyForm] == "pattern"
			},
			func(optimizerMode) bool { return true }},
		{"an attribute fetch over a collection input fuses into lookups",
			`[:find ?e ?z :in $ [?e ...] :where [?e :t/a ?z]]`, []interface{}{[]datalog.Identity{a, b}},
			func(e annotations.Event) bool { return e.Name == annotations.PatternFusedFetch },
			func(m optimizerMode) bool { return m.fusion }},
	} {
		for _, mode := range legsWhere(optimizerModes, tc.legs) {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				var events []annotations.Event
				db := createOptimizerModeDB(t, mode, DatabaseOptions{
					AnnotationHandler: func(e annotations.Event) { events = append(events, e) },
				})
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(a, datalog.NewKeyword(":t/n"), int64(1)))
				require.NoError(t, tx.Add(a, datalog.NewKeyword(":t/a"), "x"))
				require.NoError(t, tx.Add(b, datalog.NewKeyword(":t/count"), int64(2)))
				require.NoError(t, tx.Add(b, datalog.NewKeyword(":t/a"), "y"))
				_, err := tx.Commit()
				require.NoError(t, err)

				rel, err := db.Query(tc.query, tc.inputs...)
				require.NoError(t, err)
				tuples, err := executor.CollectTuples(rel, nil)
				require.NoError(t, err)
				require.NotEmpty(t, tuples, "the query answers the fixture's facts")
				ran := false
				for _, e := range events {
					ran = ran || tc.ran(e)
				}
				require.True(t, ran, "the route ran")
				require.Contains(t, rel.Properties().Identities, datalog.NewSymbol("?e"))
			})
		}
	}
}
