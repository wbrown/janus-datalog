package storage

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A get-else answers alike with the algebra optimizer on and off where a
// match on the source it reads answers otherwise than the source's lookup: a
// cardinality-many attribute and an attribute no schema declares, each held
// twice by one entity on a database's $, and one query text run on a
// database's plan cache with $ the database and then with $ a memory source
// holding two datoms at the attribute.
func TestAGetElseAnswersAlikeUnderBothOptimizerPathsWhereAMatchAnswersOtherwise(t *testing.T) {
	one := datalog.NewIdentity("scan-answers:one")
	declared := func() *schema.Schema {
		s := schema.NewSchema()
		s.Add(&schema.AttributeDefinition{Ident: kw(":t/name"), ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
		s.Add(&schema.AttributeDefinition{Ident: kw(":t/a"), ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
		s.Add(&schema.AttributeDefinition{Ident: kw(":t/tags"), ValueType: schema.TypeString, Cardinality: schema.CardinalityMany})
		return s
	}

	twice := []struct {
		name   string
		schema func() *schema.Schema
		attr   string
	}{
		{"a cardinality-many attribute on the database's $", declared, ":t/tags"},
		{"an attribute no schema declares on the database's $", nil, ":t/misc"},
	}
	for _, c := range twice {
		t.Run(c.name, func(t *testing.T) {
			legs := legsFor(optimizerModes, algebraOption, fusionOption)
			answers := make([][][]interface{}, len(legs))
			for i, mode := range legs {
				opts := DatabaseOptions{}
				if c.schema != nil {
					opts.Schema = c.schema()
				}
				d := createOptimizerModeDB(t, mode, opts)
				tx := d.NewTransaction()
				require.NoError(t, tx.Add(one, kw(":t/name"), "one"))
				require.NoError(t, tx.Add(one, kw(c.attr), "red"))
				require.NoError(t, tx.Add(one, kw(c.attr), "blue"))
				_, err := tx.Commit()
				require.NoError(t, err)
				answers[i], err = executor.CollectTuples(d.Query(
					`[:find ?e ?v :where [?e :t/name _] [(get-else $ ?e ` + c.attr + ` "none") ?v]]`))
				require.NoError(t, err, mode.name)
			}
			for i := 1; i < len(answers); i++ {
				require.ElementsMatch(t, setValuesOrdered(answers[0]), setValuesOrdered(answers[i]),
					"%s answers otherwise than %s", legs[i].name, legs[0].name)
			}
		})
	}

	t.Run("one query text on the database's plan cache, $ the database and then a memory source", func(t *testing.T) {
		for _, mode := range legsFor(optimizerModes, algebraOption, fusionOption) {
			t.Run(mode.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: declared()})
				tx := d.NewTransaction()
				require.NoError(t, tx.Add(one, kw(":t/name"), "one"))
				require.NoError(t, tx.Add(one, kw(":t/a"), "stored"))
				_, err := tx.Commit()
				require.NoError(t, err)

				text := `[:find ?e ?v :where [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]`
				fromDatabase, err := executor.CollectTuples(d.Query(text))
				require.NoError(t, err)
				require.ElementsMatch(t, [][]interface{}{{one, "stored"}}, fromDatabase)

				first := datalog.ElementID{Lamport: 1, ReplicaID: 1}
				second := datalog.ElementID{Lamport: 2, ReplicaID: 1}
				memory := logDatabase(t, []datalog.Datom{
					{E: one, A: kw(":t/name"), V: "one", Tx: first},
					{E: one, A: kw(":t/a"), V: "old", Tx: first},
					{E: one, A: kw(":t/a"), V: "new", Tx: second},
				})

				fromMemory, err := executor.CollectTuples(d.Query(text,
					WithSources(map[query.Symbol]executor.PatternMatcher{datalog.SymDollar: memory})))
				require.NoError(t, err)
				require.ElementsMatch(t, [][]interface{}{{one, "new"}}, fromMemory,
					"the memory source's lookup answers the datom with the latest transaction")
			})
		}
	})
}

// One query text run on a database's $, and then with $ the database's
// history handle through WithSources, answers on both optimizer paths what
// the get-else's lookup answers. The query runs on the database both times,
// so both runs read its plan cache. The history handle's match answers every
// datom at the attribute, and its lookup answers the latest value.
func TestAGetElseAnswersAlikeWhenThePlanCacheServesItsTextOnAHistoryHandle(t *testing.T) {
	one := datalog.NewIdentity("history-plan-cache:one")
	text := `[:find ?e ?v :where [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]`
	for _, mode := range legsFor(optimizerModes, algebraOption, fusionOption) {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: getElseFixtureSchema(schema.CardinalityOne)})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(one, kw(":t/name"), "one"))
			require.NoError(t, tx.Add(one, kw(":t/a"), "old"))
			_, err := tx.Commit()
			require.NoError(t, err)
			tx = d.NewTransaction()
			require.NoError(t, tx.Add(one, kw(":t/a"), "new"))
			_, err = tx.Commit()
			require.NoError(t, err)

			fromDatabase, err := executor.CollectTuples(d.Query(text))
			require.NoError(t, err)
			require.ElementsMatch(t, [][]interface{}{{one, "new"}}, fromDatabase)

			fromHistory, err := executor.CollectTuples(d.Query(text,
				WithSources(map[query.Symbol]executor.PatternMatcher{datalog.SymDollar: d.History()})))
			require.NoError(t, err)
			require.ElementsMatch(t, [][]interface{}{{one, "new"}}, fromHistory,
				"the get-else on the history handle answers what the handle's lookup answers, the latest value")
		})
	}
}

// One query text run twice on a database answers alike on both optimizer
// paths when SetSchema declares the get-else's attribute cardinality-many
// between the runs. The install leaves the database's plan cache as it was.
// After it, the match answers one tuple per member, and the lookup answers
// the set as one value.
func TestAGetElseAnswersAlikeAfterSetSchemaDeclaresItsAttributeCardinalityMany(t *testing.T) {
	one := datalog.NewIdentity("set-schema-cardinality:one")
	text := `[:find ?e ?v :where [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]`
	legs := legsFor(optimizerModes, algebraOption, fusionOption)
	answers := make([][][]interface{}, len(legs))
	for i, mode := range legs {
		d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: getElseFixtureSchema(schema.CardinalityOne)})
		tx := d.NewTransaction()
		require.NoError(t, tx.Add(one, kw(":t/name"), "one"))
		require.NoError(t, tx.Add(one, kw(":t/a"), "red"))
		_, err := tx.Commit()
		require.NoError(t, err)
		beforeInstall, err := executor.CollectTuples(d.Query(text))
		require.NoError(t, err, mode.name)
		require.ElementsMatch(t, [][]interface{}{{one, "red"}}, beforeInstall, mode.name)

		require.NoError(t, d.SetSchema(getElseFixtureSchema(schema.CardinalityMany)))
		tx = d.NewTransaction()
		require.NoError(t, tx.Add(one, kw(":t/a"), "blue"))
		require.NoError(t, tx.Add(one, kw(":t/a"), "green"))
		_, err = tx.Commit()
		require.NoError(t, err)
		answers[i], err = executor.CollectTuples(d.Query(text))
		require.NoError(t, err, mode.name)
	}
	for i := 1; i < len(answers); i++ {
		require.ElementsMatch(t, setValuesOrdered(answers[0]), setValuesOrdered(answers[i]),
			"%s answers otherwise than %s after the install", legs[i].name, legs[0].name)
	}
}

// A get-else inside an aggregating subquery answers alike on both optimizer
// paths when the call passes, at the inner query's $, a source declaring the
// attribute cardinality-many, while the outer $ declares it cardinality-one.
// Decorrelation folds the inner query into the outer one, and the inner
// get-else reads the source the call passes.
func TestAGetElseInAnAggregatingSubqueryAnswersAlikeOnTheSourceTheCallPasses(t *testing.T) {
	one := datalog.NewIdentity("aggregate-call-source:one")
	cases := []struct {
		name string
		text string
	}{
		{"the call passes the source at the inner $",
			`[:find ?e ?n :in $ $src :where [?e :t/name _]
			  [(q [:find (count ?v) :in $ ?x :where [?x :t/name _] [(get-else $ ?x :t/a "none") ?v]] $src ?e) [[?n]]]]`},
		{"the call passes one source at two inner positions",
			`[:find ?e ?n :in $ $src :where [?e :t/name _]
			  [(q [:find (count ?v) :in $ $b ?x :where [$b ?x :t/name _] [(get-else $ ?x :t/a "none") ?v]] $src $src ?e) [[?n]]]]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			legs := legsFor(optimizerModes, algebraOption, aggregationOption)
			answers := make([][][]interface{}, len(legs))
			for i, mode := range legs {
				outer := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: getElseFixtureSchema(schema.CardinalityOne)})
				tx := outer.NewTransaction()
				require.NoError(t, tx.Add(one, kw(":t/name"), "one"))
				require.NoError(t, tx.Add(one, kw(":t/a"), "only"))
				_, err := tx.Commit()
				require.NoError(t, err)

				source := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: getElseFixtureSchema(schema.CardinalityMany)})
				tx = source.NewTransaction()
				require.NoError(t, tx.Add(one, kw(":t/name"), "one"))
				require.NoError(t, tx.Add(one, kw(":t/a"), "red"))
				require.NoError(t, tx.Add(one, kw(":t/a"), "blue"))
				_, err = tx.Commit()
				require.NoError(t, err)

				answers[i], err = executor.CollectTuples(outer.Query(c.text,
					WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$src"): source})))
				require.NoError(t, err, mode.name)
			}
			for i := 1; i < len(answers); i++ {
				require.ElementsMatch(t, answers[0], answers[i],
					"%s answers otherwise than %s", legs[i].name, legs[0].name)
			}
		})
	}
}

// getElseFixtureSchema declares :t/name cardinality-one and :t/a under the
// cardinality given, both strings.
func getElseFixtureSchema(aCardinality datalog.Keyword) *schema.Schema {
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: kw(":t/name"), ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
	s.Add(&schema.AttributeDefinition{Ident: kw(":t/a"), ValueType: schema.TypeString, Cardinality: aCardinality})
	return s
}

// setValuesOrdered answers the tuples with the members of each set value in
// CompareValues order. A get-else on a cardinality-many attribute answers the
// set as one []interface{} value, and the lookup answers its members in no
// fixed order, so two answers holding one set compare equal once ordered.
func setValuesOrdered(tuples [][]interface{}) [][]interface{} {
	ordered := make([][]interface{}, len(tuples))
	for i, tuple := range tuples {
		ordered[i] = make([]interface{}, len(tuple))
		for j, value := range tuple {
			members, isSet := value.([]interface{})
			if !isSet {
				ordered[i][j] = value
				continue
			}
			sorted := slices.Clone(members)
			slices.SortFunc(sorted, datalog.CompareValues)
			ordered[i][j] = sorted
		}
	}
	return ordered
}
