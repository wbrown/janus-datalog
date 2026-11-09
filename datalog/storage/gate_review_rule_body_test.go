package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// gateReviewRuleBodyDatabase opens a database on the leg given, declaring
// :t/name and :t/a cardinality-one, holding :t/name "one" and :t/a "old" at
// one entity in a first transaction and :t/a "new" in a second.
func gateReviewRuleBodyDatabase(t *testing.T, mode optimizerMode, one datalog.Identity) *Database {
	t.Helper()
	d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: gateReviewSchema(schema.CardinalityOne, nil)})
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(one, datalog.NewKeyword(":t/name"), "one"))
	require.NoError(t, tx.Add(one, datalog.NewKeyword(":t/a"), "old"))
	_, err := tx.Commit()
	require.NoError(t, err)
	tx = d.NewTransaction()
	require.NoError(t, tx.Add(one, datalog.NewKeyword(":t/a"), "new"))
	_, err = tx.Commit()
	require.NoError(t, err)
	return d
}

// A bound rule invocation whose body holds a get-else answers, on every leg,
// what the reference leg answers, the get-else's lookup, when one database
// runs its text a second time on a source whose match no longer answers the
// lookup's one value per entity. Each case runs the text once on the
// database's latest view, then once on the route named; the control runs the
// second route alone.
func TestGateReview_ABoundRuleBodysGetElseAnswersAlikeOnASecondRunOfItsText(t *testing.T) {
	one := datalog.NewIdentity("gate-review-rule:one")
	src := datalog.NewSymbol("$src")
	viaSrc := `[:find ?e ?v :in $ $src % :where [?e :t/name _] (ga ?e ?v)]`
	viaSrcRules := `[[(ga ?e ?v) [$src ?e :t/name _] [(get-else $src ?e :t/a "none") ?v]]]`
	viaDollar := `[:find ?e ?v :in $ % :where [?e :t/name _] (ga ?e ?v)]`
	viaDollarRules := `[[(ga ?e ?v) [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]]`
	cases := []struct {
		name   string
		text   string
		rules  string
		first  func(d *Database, rs *query.RuleSet) (executor.Relation, error)
		second func(t *testing.T, d *Database, rs *query.RuleSet) (executor.Relation, error)
	}{
		{"$src the history handle through WithSources, after $src the database", viaSrc, viaSrcRules,
			func(d *Database, rs *query.RuleSet) (executor.Relation, error) {
				return d.Query(viaSrc, WithSources(map[query.Symbol]executor.PatternMatcher{src: d}), rs)
			},
			func(t *testing.T, d *Database, rs *query.RuleSet) (executor.Relation, error) {
				return d.Query(viaSrc, WithSources(map[query.Symbol]executor.PatternMatcher{src: d.History()}), rs)
			}},
		{"$ the history handle through WithSources, after $ the database", viaDollar, viaDollarRules,
			func(d *Database, rs *query.RuleSet) (executor.Relation, error) { return d.Query(viaDollar, rs) },
			func(t *testing.T, d *Database, rs *query.RuleSet) (executor.Relation, error) {
				return d.Query(viaDollar, WithSources(map[query.Symbol]executor.PatternMatcher{datalog.SymDollar: d.History()}), rs)
			}},
		{"the history handle's Query, after the database's Query", viaDollar, viaDollarRules,
			func(d *Database, rs *query.RuleSet) (executor.Relation, error) { return d.Query(viaDollar, rs) },
			func(t *testing.T, d *Database, rs *query.RuleSet) (executor.Relation, error) {
				return d.History().Query(viaDollar, rs)
			}},
		{"control: the history handle's Query on a database that ran nothing before", viaDollar, viaDollarRules,
			nil,
			func(t *testing.T, d *Database, rs *query.RuleSet) (executor.Relation, error) {
				return d.History().Query(viaDollar, rs)
			}},
		{"the database's Query, after SetSchema declares :t/a cardinality-many", viaDollar, viaDollarRules,
			func(d *Database, rs *query.RuleSet) (executor.Relation, error) { return d.Query(viaDollar, rs) },
			func(t *testing.T, d *Database, rs *query.RuleSet) (executor.Relation, error) {
				require.NoError(t, d.SetSchema(gateReviewSchema(schema.CardinalityMany, nil)))
				tx := d.NewTransaction()
				require.NoError(t, tx.Add(one, datalog.NewKeyword(":t/a"), "blue"))
				_, err := tx.Commit()
				require.NoError(t, err)
				return d.Query(viaDollar, rs)
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// secondRun answers the case's second run on a database on the leg,
			// after the first run where the case has one.
			secondRun := func(mode optimizerMode) [][]interface{} {
				d := gateReviewRuleBodyDatabase(t, mode, one)
				rs, err := parser.ParseRules(c.rules)
				require.NoError(t, err)
				if c.first != nil {
					first, err := gateReviewAnswer(c.first(d, rs))
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{one, "new"}}, first, "the first run, %s", mode.name)
				}
				second, err := gateReviewAnswer(c.second(t, d, rs))
				require.NoError(t, err)
				return second
			}
			want := secondRun(referenceMode)
			for _, mode := range optimizerModes {
				require.Equal(t, want, secondRun(mode), "%s answers the second run otherwise than the reference leg", mode.name)
			}
		})
	}
}
