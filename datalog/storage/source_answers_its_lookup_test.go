package storage

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A database opened with no schema answers whether a match on it answers its
// lookup's one value by the cardinality its readers resolve the attribute
// by: true in the latest view and on an as-of handle for an attribute a
// dump's ops infer cardinality-one and for one first written after the
// import, which has no definition and resolves by LWW, and false for one the
// ops infer cardinality-many and on its history handle. Where it answers
// true, a get-else answers alike on both optimizer paths, at the head and as
// of the import.
func TestASchemalessDatabaseAnswersWhetherAMatchAnswersItsLookup(t *testing.T) {
	one := datalog.NewIdentity("schemaless-answer:one")
	imported := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	dump := strings.Join([]string{
		FormatDatomEDN(&datalog.Datom{E: one, A: kw(":s/name"), V: "one", Tx: imported}),
		FormatDatomEDN(&datalog.Datom{E: one, A: kw(":s/inferred-one"), V: "kept", Tx: imported}),
		FormatDatomEDN(&datalog.Datom{E: one, A: kw(":s/inferred-many"), V: "red", Tx: imported, Op: datalog.OpCRDTAdd}),
		FormatDatomEDN(&datalog.Datom{E: one, A: kw(":s/inferred-many"), V: "blue", Tx: imported, Op: datalog.OpCRDTAdd}),
	}, "\n")
	const getElse = `[:find ?e ?kept ?later
		:where [?e :s/name _]
		       [(get-else $ ?e :s/inferred-one "none") ?kept]
		       [(get-else $ ?e :s/later "none") ?later]]`

	type configuration struct {
		name         string
		mode         optimizerMode
		disableCache bool
	}
	var configurations []configuration
	for _, mode := range legsFor(optimizerModes, algebraOption, fusionOption) {
		for _, disableCache := range []bool{false, true} {
			configurations = append(configurations, configuration{
				name:         fmt.Sprintf("%s/cache_disabled_%t", mode.name, disableCache),
				mode:         mode,
				disableCache: disableCache,
			})
		}
	}

	answers := make([][][]interface{}, len(configurations))
	asOfAnswers := make([][][]interface{}, len(configurations))
	for i, c := range configurations {
		d := createOptimizerModeDB(t, c.mode, DatabaseOptions{DisableCache: c.disableCache})
		require.NoError(t, d.Import(strings.NewReader(dump)))
		tx := d.NewTransaction()
		require.NoError(t, tx.Add(one, kw(":s/later"), "first"))
		require.NoError(t, tx.Add(one, kw(":s/later"), "second"))
		_, err := tx.Commit()
		require.NoError(t, err)

		latest, ok := d.Matcher().(*PatternMatcher)
		require.True(t, ok)
		require.True(t, latest.CanFuseAttributeFetch(datalog.SymDollar, kw(":s/inferred-one")),
			"%s: an attribute the ops infer cardinality-one", c.name)
		require.True(t, latest.CanFuseAttributeFetch(datalog.SymDollar, kw(":s/later")),
			"%s: an attribute with no definition resolves by LWW in both readers", c.name)
		require.False(t, latest.CanFuseAttributeFetch(datalog.SymDollar, kw(":s/inferred-many")),
			"%s: an attribute the ops infer cardinality-many", c.name)

		history, ok := d.History().Matcher().(*PatternMatcher)
		require.True(t, ok)
		require.False(t, history.CanFuseAttributeFetch(datalog.SymDollar, kw(":s/later")),
			"%s: a history handle matches every datom", c.name)
		asOf, ok := d.AsOf(imported).Matcher().(*PatternMatcher)
		require.True(t, ok)
		require.True(t, asOf.CanFuseAttributeFetch(datalog.SymDollar, kw(":s/inferred-one")),
			"%s: an as-of handle's lookup answers the value as of its point", c.name)

		answers[i], err = executor.CollectTuples(d.Query(getElse))
		require.NoError(t, err, c.name)
		asOfAnswers[i], err = executor.CollectTuples(d.AsOf(imported).Query(getElse))
		require.NoError(t, err, c.name)
	}
	require.ElementsMatch(t, [][]interface{}{{one, "kept", "second"}}, answers[0])
	require.ElementsMatch(t, [][]interface{}{{one, "kept", "none"}}, asOfAnswers[0])
	for i := 1; i < len(answers); i++ {
		require.ElementsMatch(t, answers[0], answers[i],
			"%s answers otherwise than %s", configurations[i].name, configurations[0].name)
		require.ElementsMatch(t, asOfAnswers[0], asOfAnswers[i],
			"as of the import, %s answers otherwise than %s", configurations[i].name, configurations[0].name)
	}
}
