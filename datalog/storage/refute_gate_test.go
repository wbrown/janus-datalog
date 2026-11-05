package storage

import (
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// refuteGateEntry is one public entry point a rule set's get-else reaches its
// source by: the text and rules, an optional first run on the database's
// latest view, and the second run whose answer is compared.
type refuteGateEntry struct {
	name   string
	text   string
	rules  string
	first  bool
	second func(d *Database, text string, rules string) ([][]interface{}, error)
}

func refuteGateQueryOn(view func(d *Database) *Database) func(d *Database, text, rules string) ([][]interface{}, error) {
	return func(d *Database, text, rules string) ([][]interface{}, error) {
		rs, err := parser.ParseRules(rules)
		if err != nil {
			return nil, err
		}
		return gateReviewAnswer(view(d).Query(text, rs))
	}
}

func refuteGateAnalyzeOn(view func(d *Database) *Database) func(d *Database, text, rules string) ([][]interface{}, error) {
	return func(d *Database, text, rules string) ([][]interface{}, error) {
		rs, err := parser.ParseRules(rules)
		if err != nil {
			return nil, err
		}
		result, err := view(d).Analyze(text, rs)
		if err != nil {
			return nil, err
		}
		return gateReviewAnswer(result.Result, nil)
	}
}

func refuteGateHistory(d *Database) *Database { return d.History() }

func refuteGateLastAsOf(h gateReviewHistory) func(d *Database) *Database {
	return func(d *Database) *Database { return d.AsOf(h.txIDs[len(h.txIDs)-1]) }
}

// Over generated histories, each entry point's second run answers on the
// optimizer-on path, through a plan cache holding the text's plan from a run
// on the latest view, what the optimizer-off path answers; a fresh
// optimizer-on database that ran nothing answers alike too (the control).
func TestRefuteGate_ACacheServedRuleBodyAnswersAsTheUnoptimizedPathOnEveryEntryPoint(t *testing.T) {
	bound := `[[(ga ?e ?v) [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]]`
	boundText := `[:find ?e ?v :in $ % :where [?e :t/name _] (ga ?e ?v)]`
	required := `[[(top ?e ?v) [?e :t/name _] (gb ?e ?v)] [(gb [?e] ?v) [(get-else $ ?e :t/a "none") ?v]]]`
	requiredText := `[:find ?e ?v :in $ % :where (top ?e ?v)]`
	free := `[[(ga ?e ?v) [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]]`
	freeText := `[:find ?e ?v :in $ % :where (ga ?e ?v)]`
	cached := map[string]int{}
	fresh := map[string]int{}
	firstCached := map[string]string{}
	for i, c := range generateGateReviewCases(424242, 60) {
		entries := []refuteGateEntry{
			{"bound invocation, History().Query after Query", boundText, bound, true, refuteGateQueryOn(refuteGateHistory)},
			{"bound invocation, History().Analyze after Query", boundText, bound, true, refuteGateAnalyzeOn(refuteGateHistory)},
			{"required head from a free invocation, History().Query after Query", requiredText, required, true, refuteGateQueryOn(refuteGateHistory)},
			{"free invocation, History().Query after Query", freeText, free, true, refuteGateQueryOn(refuteGateHistory)},
		}
		for _, e := range entries {
			off := buildGateReviewHistory(t, c, false)
			want, wantErr := e.second(off.db, e.text, e.rules)
			on := buildGateReviewHistory(t, c, true)
			if e.first {
				rs, err := parser.ParseRules(e.rules)
				require.NoError(t, err)
				latest, err := gateReviewAnswer(on.db.Query(e.text, rs))
				require.NoError(t, err, e.name)
				require.NotEmpty(t, latest, e.name)
			}
			got, gotErr := e.second(on.db, e.text, e.rules)
			if (wantErr == nil) != (gotErr == nil) || !reflect.DeepEqual(want, got) {
				cached[e.name]++
				if _, seen := firstCached[e.name]; !seen {
					firstCached[e.name] = fmt.Sprintf("case %d %v: off %v %v, on %v %v", i, c, want, wantErr, got, gotErr)
				}
			}
			control := buildGateReviewHistory(t, c, true)
			controlGot, controlErr := e.second(control.db, e.text, e.rules)
			if (wantErr == nil) != (controlErr == nil) || !reflect.DeepEqual(want, controlGot) {
				fresh[e.name]++
			}
		}
		asOf := buildGateReviewHistory(t, c, true)
		asOfOff := buildGateReviewHistory(t, c, false)
		rs, err := parser.ParseRules(bound)
		require.NoError(t, err)
		latest, err := gateReviewAnswer(asOf.db.Query(boundText, rs))
		require.NoError(t, err)
		require.NotEmpty(t, latest)
		got, gotErr := refuteGateQueryOn(refuteGateLastAsOf(asOf))(asOf.db, boundText, bound)
		want, wantErr := refuteGateQueryOn(refuteGateLastAsOf(asOfOff))(asOfOff.db, boundText, bound)
		if (wantErr == nil) != (gotErr == nil) || !reflect.DeepEqual(want, got) {
			cached["control: bound invocation, AsOf(last).Query after Query"]++
		}
	}
	require.Empty(t, fresh, "entry points where a fresh optimizer-on database answers otherwise than the optimizer-off path")
	require.Empty(t, cached, "entry points where the cache-served optimizer-on run answers otherwise than the optimizer-off path, first instance of each: %v", firstCached)
}

// After SetSchema declares :t/a cardinality-many and a value is asserted, a
// bound invocation's get-else answers on the optimizer-on path what it
// answers on the optimizer-off path, whether or not the database ran the
// text before the declaration.
func TestRefuteGate_ABoundRuleBodyAnswersAlikeAfterSetSchemaWithAndWithoutAnEarlierRun(t *testing.T) {
	one := datalog.NewIdentity("refute-gate-schema:one")
	text := `[:find ?e ?v :in $ % :where [?e :t/name _] (ga ?e ?v)]`
	rules := `[[(ga ?e ?v) [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]]`
	cases := []struct {
		name     string
		ranFirst bool
	}{
		{"control: no run before the declaration", false},
		{"a run on the latest view before the declaration", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var answers [2][][]interface{}
			for i, algebra := range []bool{false, true} {
				d := gateReviewRuleBodyDatabase(t, algebra, one)
				rs, err := parser.ParseRules(rules)
				require.NoError(t, err)
				if c.ranFirst {
					latest, err := gateReviewAnswer(d.Query(text, rs))
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{one, "new"}}, latest)
				}
				require.NoError(t, d.SetSchema(gateReviewSchema(schema.CardinalityMany, nil)))
				tx := d.NewTransaction()
				require.NoError(t, tx.Add(one, datalog.NewKeyword(":t/a"), "blue"))
				_, err = tx.Commit()
				require.NoError(t, err)
				answers[i], err = gateReviewAnswer(d.Query(text, rs))
				require.NoError(t, err)
			}
			require.Equal(t, answers[0], answers[1], "optimizer-off answer first, optimizer-on second")
		})
	}
}

// On the latest view the get-else scan rewrite applies to the rule body of
// each invocation shape the differential above runs.
func TestRefuteGate_TheRewriteAppliesToEachInvocationShapesBody(t *testing.T) {
	cases := []struct {
		name  string
		text  string
		rules string
	}{
		{"bound invocation", `[:find ?e ?v :in $ % :where [?e :t/name _] (ga ?e ?v)]`, `[[(ga ?e ?v) [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]]`},
		{"required head from a free invocation", `[:find ?e ?v :in $ % :where (top ?e ?v)]`, `[[(top ?e ?v) [?e :t/name _] (gb ?e ?v)] [(gb [?e] ?v) [(get-else $ ?e :t/a "none") ?v]]]`},
		{"free invocation", `[:find ?e ?v :in $ % :where (ga ?e ?v)]`, `[[(ga ?e ?v) [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			applied := 0
			opts := DefaultPlannerOptions()
			opts.Handler = func(e annotations.Event) {
				if e.Name == annotations.AlgebraGetElseScanApply {
					applied++
				}
			}
			d, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory-trees", Schema: gateReviewSchema(schema.CardinalityOne, nil), PlannerOptions: &opts})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, d.Close()) })
			rs, err := parser.ParseRules(c.rules)
			require.NoError(t, err)
			got, err := gateReviewAnswer(d.Query(c.text, rs))
			require.NoError(t, err)
			require.Empty(t, got)
			require.Positive(t, applied)
		})
	}
}

// Each rule-body route's text runs twice on one database with $src the
// database, SetSchema declaring :t/a cardinality-many and a value asserted
// between the runs; the second run answers alike on both optimizer paths.
func TestRefuteGate_EachRuleBodyRouteAnswersAlikeAfterSetSchemaBetweenRuns(t *testing.T) {
	for _, route := range gateReviewRoutes {
		if route.rules == "" {
			continue
		}
		t.Run(route.name, func(t *testing.T) {
			var answers [2][][]interface{}
			for i, algebra := range []bool{false, true} {
				c := gateReviewCase{cardinality: schema.CardinalityOne, txs: 1, installAfter: -1, ops: []gateReviewOp{{tx: 0, entity: 0, value: "x"}, {tx: 0, entity: 1, value: "y"}}}
				h := buildGateReviewHistory(t, c, algebra)
				rs, err := parser.ParseRules(route.rules)
				require.NoError(t, err)
				run := func() ([][]interface{}, error) {
					return gateReviewAnswer(h.db.Query(route.text, WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$src"): h.db}), rs))
				}
				first, err := run()
				require.NoError(t, err)
				require.NotEmpty(t, first)
				require.NoError(t, h.db.SetSchema(gateReviewSchema(schema.CardinalityMany, nil)))
				tx := h.db.NewTransaction()
				require.NoError(t, tx.Add(h.entities[0], datalog.NewKeyword(":t/a"), "z"))
				_, err = tx.Commit()
				require.NoError(t, err)
				answers[i], err = run()
				require.NoError(t, err)
			}
			require.Equal(t, answers[0], answers[1], "optimizer-off answer first, optimizer-on second")
		})
	}
}

// QueryInto on the history handle, after the database's Query ran the text,
// answers a bound invocation's get-else alike on both optimizer paths.
func TestRefuteGate_QueryIntoOnTheHistoryHandleAnswersAlikeAfterAQueryRanTheText(t *testing.T) {
	one := datalog.NewIdentity("refute-gate-into:one")
	text := `[:find ?e ?v :in $ % :where [?e :t/name _] (ga ?e ?v)]`
	rules := `[[(ga ?e ?v) [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]]`
	type answer struct {
		E datalog.Identity `datalog:"?e"`
		V string           `datalog:"?v"`
	}
	cases := []struct {
		name     string
		ranFirst bool
	}{
		{"control: no run before", false},
		{"after the database's Query", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var answers [2][]string
			for i, algebra := range []bool{false, true} {
				d := gateReviewRuleBodyDatabase(t, algebra, one)
				rs, err := parser.ParseRules(rules)
				require.NoError(t, err)
				if c.ranFirst {
					latest, err := gateReviewAnswer(d.Query(text, rs))
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{one, "new"}}, latest)
				}
				var got []answer
				require.NoError(t, d.History().QueryInto(&got, text, rs))
				for _, a := range got {
					require.Equal(t, one, a.E)
					answers[i] = append(answers[i], a.V)
				}
				slices.Sort(answers[i])
			}
			require.Equal(t, answers[0], answers[1], "optimizer-off answer first, optimizer-on second")
		})
	}
}
