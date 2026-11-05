package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A database opened with no schema answers whether a match on it answers its
// lookup's one value by the cardinality its readers resolve the attribute
// by: true in the latest view for an attribute a dump's ops infer
// cardinality-one and for one first written after the import, which has no
// definition and resolves by LWW, and false for one the ops infer
// cardinality-many and on its history and as-of handles. Where it answers
// true, a get-else answers alike on both optimizer paths.
func TestASchemalessDatabaseAnswersWhetherAMatchAnswersItsLookup(t *testing.T) {
	one := datalog.NewIdentity("schemaless-answer:one")
	imported := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	dump := strings.Join([]string{
		FormatDatomEDN(&datalog.Datom{E: one, A: kw(":s/name"), V: "one", Tx: imported}),
		FormatDatomEDN(&datalog.Datom{E: one, A: kw(":s/inferred-one"), V: "kept", Tx: imported}),
		FormatDatomEDN(&datalog.Datom{E: one, A: kw(":s/inferred-many"), V: "red", Tx: imported, Op: datalog.OpCRDTAdd}),
		FormatDatomEDN(&datalog.Datom{E: one, A: kw(":s/inferred-many"), V: "blue", Tx: imported, Op: datalog.OpCRDTAdd}),
	}, "\n")

	answers := make([][][]interface{}, len(optimizerModes))
	for i, mode := range optimizerModes {
		d := createOptimizerModeDB(t, mode, DatabaseOptions{})
		require.NoError(t, d.Import(strings.NewReader(dump)))
		tx := d.NewTransaction()
		require.NoError(t, tx.Add(one, kw(":s/later"), "first"))
		require.NoError(t, tx.Add(one, kw(":s/later"), "second"))
		_, err := tx.Commit()
		require.NoError(t, err)

		latest, ok := d.Matcher().(*PatternMatcher)
		require.True(t, ok)
		require.True(t, latest.CanFuseAttributeFetch(datalog.SymDollar, kw(":s/inferred-one")),
			"%s: an attribute the ops infer cardinality-one", mode.name)
		require.True(t, latest.CanFuseAttributeFetch(datalog.SymDollar, kw(":s/later")),
			"%s: an attribute with no definition resolves by LWW in both readers", mode.name)
		require.False(t, latest.CanFuseAttributeFetch(datalog.SymDollar, kw(":s/inferred-many")),
			"%s: an attribute the ops infer cardinality-many", mode.name)

		history, ok := d.History().Matcher().(*PatternMatcher)
		require.True(t, ok)
		require.False(t, history.CanFuseAttributeFetch(datalog.SymDollar, kw(":s/later")),
			"%s: a history handle matches every datom", mode.name)
		asOf, ok := d.AsOf(imported).Matcher().(*PatternMatcher)
		require.True(t, ok)
		require.False(t, asOf.CanFuseAttributeFetch(datalog.SymDollar, kw(":s/inferred-one")),
			"%s: an as-of handle resolves as of its point", mode.name)

		answers[i], err = executor.CollectTuples(d.Query(`[:find ?e ?kept ?later
			:where [?e :s/name _]
			       [(get-else $ ?e :s/inferred-one "none") ?kept]
			       [(get-else $ ?e :s/later "none") ?later]]`))
		require.NoError(t, err, mode.name)
	}
	require.ElementsMatch(t, [][]interface{}{{one, "kept", "second"}}, answers[0])
	for i := 1; i < len(answers); i++ {
		require.ElementsMatch(t, answers[0], answers[i],
			"%s answers otherwise than %s", optimizerModes[i].name, optimizerModes[0].name)
	}
}
