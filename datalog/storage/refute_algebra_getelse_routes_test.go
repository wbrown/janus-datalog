package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A get-else that reads a named memory source answers the source's lookup,
// the datom with the latest transaction (PatternMatcher.LookupAttribute),
// under both optimizer paths (optimizer_modes_test.go), on every route that
// names the source: the clause itself, a subquery whose call passes the
// source as its $ (RenameSources), and a rule body.
func TestRefuteAlgebra_GetElseOnAMemorySourceAnswersItsLookupOnEveryRoute(t *testing.T) {
	one := datalog.NewIdentity("refute-algebra:one")
	name, attr := kw(":t/name"), kw(":t/a")
	first := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	second := datalog.ElementID{Lamport: 2, ReplicaID: 1}
	facts := []datalog.Datom{
		{E: one, A: name, V: "one", Tx: first},
		{E: one, A: attr, V: "old", Tx: first},
		{E: one, A: attr, V: "new", Tx: second},
	}
	cases := []struct {
		name   string
		text   string
		inputs []interface{}
	}{
		{
			name: "the clause names $mem",
			text: `[:find ?e ?v :in $ $mem :where [$mem ?e :t/name _] [(get-else $mem ?e :t/a "none") ?v]]`,
		},
		{
			name: "a subquery call passes $mem as its $",
			text: `[:find ?e ?v :in $ $mem :where [(q [:find ?e ?v :in $ :where [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]] $mem) [[?e ?v]]]]`,
		},
		{
			name:   "a rule body names $mem",
			text:   `[:find ?e ?v :in $ $mem % :where (valued ?e ?v)]`,
			inputs: []interface{}{`[[(valued ?e ?v) [$mem ?e :t/name _] [(get-else $mem ?e :t/a "none") ?v]]]`},
		},
	}
	for _, mode := range legsFor(optimizerModes, algebraOption) {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				args := append([]interface{}{WithSources(map[query.Symbol]executor.PatternMatcher{
					datalog.NewSymbol("$mem"): logDatabase(t, facts),
				})}, c.inputs...)
				got, err := executor.CollectTuples(d.Query(c.text, args...))
				require.NoError(t, err)
				require.ElementsMatch(t, [][]interface{}{{one, "new"}}, got)
			})
		}
	}
}
