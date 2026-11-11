package db_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// databaseOfLog opens the database a log's EDN dump loads: one datom per line,
// each with the ElementID and op it carries (storage.FormatDatomEDN). The
// database closes when the test ends.
func databaseOfLog(t *testing.T, log []datalog.Datom) *db.DB {
	t.Helper()
	lines := make([]string, len(log))
	for i := range log {
		lines[i] = storage.FormatDatomEDN(&log[i])
	}
	d, err := db.Open([]byte(strings.Join(lines, "\n")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	return d
}

// A memory database holding a log, passed as a named source, answers every
// reader from its current view. The later of two values at an attribute
// supersedes the earlier, a value whose latest datom is a remove resolves to
// nothing, and a set member added and then removed is withdrawn. get-else,
// get-some and missing? look the attribute up in that view.
func TestAMemoryDatabaseNamedAsASourceAnswersItsResolvedView(t *testing.T) {
	one := datalog.NewIdentity("memory-source:one")
	two := datalog.NewIdentity("memory-source:two")
	three := datalog.NewIdentity("memory-source:three")
	name, a, b, tags := datalog.NewKeyword(":t/name"), datalog.NewKeyword(":t/a"), datalog.NewKeyword(":t/b"), datalog.NewKeyword(":t/tags")
	at := func(lamport uint64) datalog.ElementID { return datalog.ElementID{Lamport: lamport, ReplicaID: 1} }
	held := databaseOfLog(t, []datalog.Datom{
		{E: one, A: name, V: "one", Tx: at(1)},
		{E: one, A: a, V: "old", Tx: at(2)},
		{E: one, A: a, V: "new", Tx: at(3)},
		{E: two, A: name, V: "two", Tx: at(4)},
		{E: two, A: b, V: "x", Tx: at(5)},
		{E: two, A: b, V: "x", Tx: at(6), Op: datalog.OpCRDTRemove},
		{E: three, A: name, V: "three", Tx: at(7)},
		{E: three, A: tags, V: "red", Tx: at(8), Op: datalog.OpCRDTAdd},
		{E: three, A: tags, V: "blue", Tx: at(9), Op: datalog.OpCRDTAdd},
		{E: three, A: tags, V: "red", Tx: at(10), Op: datalog.OpCRDTRemove},
	})
	cases := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{"a value a later one supersedes",
			`[:find ?e ?v :in $ $m :where [$m ?e :t/a ?v]]`,
			[][]interface{}{{one, "new"}}},
		{"a value whose latest datom is a remove",
			`[:find ?e ?v :in $ $m :where [$m ?e :t/b ?v]]`,
			nil},
		{"a set member added and then removed",
			`[:find ?e ?v :in $ $m :where [$m ?e :t/tags ?v]]`,
			[][]interface{}{{three, "blue"}}},
		{"get-else over a removed value",
			`[:find ?e ?v :in $ $m :where [$m ?e :t/name _] [(get-else $m ?e :t/b "none") ?v]]`,
			[][]interface{}{{one, "none"}, {two, "none"}, {three, "none"}}},
		{"get-else over a superseded value",
			`[:find ?e ?v :in $ $m :where [$m ?e :t/name _] [(get-else $m ?e :t/a "none") ?v]]`,
			[][]interface{}{{one, "new"}, {two, "none"}, {three, "none"}}},
		{"get-some over a removed value and a superseded one",
			`[:find ?e ?attr ?v :in $ $m :where [$m ?e :t/name _] [(get-some $m ?e :t/b :t/a) [?attr ?v]]]`,
			[][]interface{}{{one, a, "new"}}},
		{"missing? over a removed value",
			`[:find ?e :in $ $m :where [$m ?e :t/name _] [(missing? $m ?e :t/b)]]`,
			[][]interface{}{{one}, {two}, {three}}},
	}

	for _, mode := range legsFor(t, optimizerModes, algebraOption) {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				host, err := db.OpenMemory(db.WithPlannerOptions(mode.plannerOptions()))
				require.NoError(t, err)
				defer func() { require.NoError(t, host.Close()) }()
				got, err := executor.CollectTuples(host.Query(c.query,
					db.WithSources(map[datalog.Symbol]db.PatternMatcher{datalog.NewSymbol("$m"): held})))
				require.NoError(t, err)
				require.ElementsMatch(t, c.want, got)
			})
		}
	}
}
