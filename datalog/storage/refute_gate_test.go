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

// Over generated histories, each entry point's second run answers on every
// leg, through a plan cache holding the text's plan from a run on the latest
// view, what the reference leg answers; a fresh database on the leg that ran
// nothing answers alike too (the control).
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
			reference := buildGateReviewHistory(t, c, referenceMode)
			want, wantErr := e.second(reference.db, e.text, e.rules)
			for _, mode := range optimizerModes {
				key := mode.name + " / " + e.name
				leg := buildGateReviewHistory(t, c, mode)
				if e.first {
					rs, err := parser.ParseRules(e.rules)
					require.NoError(t, err)
					latest, err := gateReviewAnswer(leg.db.Query(e.text, rs))
					require.NoError(t, err, key)
					require.NotEmpty(t, latest, key)
				}
				got, gotErr := e.second(leg.db, e.text, e.rules)
				if (wantErr == nil) != (gotErr == nil) || !reflect.DeepEqual(want, got) {
					cached[key]++
					if _, seen := firstCached[key]; !seen {
						firstCached[key] = fmt.Sprintf("case %d %v: reference %v %v, leg %v %v", i, c, want, wantErr, got, gotErr)
					}
				}
				control := buildGateReviewHistory(t, c, mode)
				controlGot, controlErr := e.second(control.db, e.text, e.rules)
				if (wantErr == nil) != (controlErr == nil) || !reflect.DeepEqual(want, controlGot) {
					fresh[key]++
				}
			}
		}
		asOfReference := buildGateReviewHistory(t, c, referenceMode)
		want, wantErr := refuteGateQueryOn(refuteGateLastAsOf(asOfReference))(asOfReference.db, boundText, bound)
		for _, mode := range optimizerModes {
			asOf := buildGateReviewHistory(t, c, mode)
			rs, err := parser.ParseRules(bound)
			require.NoError(t, err)
			latest, err := gateReviewAnswer(asOf.db.Query(boundText, rs))
			require.NoError(t, err)
			require.NotEmpty(t, latest)
			got, gotErr := refuteGateQueryOn(refuteGateLastAsOf(asOf))(asOf.db, boundText, bound)
			if (wantErr == nil) != (gotErr == nil) || !reflect.DeepEqual(want, got) {
				cached[mode.name+" / control: bound invocation, AsOf(last).Query after Query"]++
			}
		}
	}
	require.Empty(t, fresh, "legs and entry points where a fresh database answers otherwise than the reference leg")
	require.Empty(t, cached, "legs and entry points where the cache-served run answers otherwise than the reference leg, first instance of each: %v", firstCached)
}

// After SetSchema declares :t/a cardinality-many and a value is asserted, a
// bound invocation's get-else answers on every leg what the reference leg
// answers, whether or not the database ran the text before the declaration.
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
			// answer runs the case on a database on the leg: the first run where
			// the case has one, the declaration, the asserted value, and the
			// query after them.
			answer := func(mode optimizerMode) [][]interface{} {
				d := gateReviewRuleBodyDatabase(t, mode, one)
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
				got, err := gateReviewAnswer(d.Query(text, rs))
				require.NoError(t, err)
				return got
			}
			want := answer(referenceMode)
			for _, mode := range optimizerModes {
				require.Equal(t, want, answer(mode), "%s answers otherwise than the reference leg", mode.name)
			}
		})
	}
}

// On the algebra legs, with $src the latest view, the get-else scan rewrite
// applies to the rule body of each invocation shape the differential above
// runs.
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
	for _, mode := range legsWhere(optimizerModes, func(m optimizerMode) bool { return m.algebra }) {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				applied := 0
				d := createOptimizerModeDB(t, mode, DatabaseOptions{
					Schema: gateReviewSchema(schema.CardinalityOne, nil),
					AnnotationHandler: func(e annotations.Event) {
						if e.Name == annotations.AlgebraGetElseScanApply {
							applied++
						}
					},
				})
				rs, err := parser.ParseRules(c.rules)
				require.NoError(t, err)
				got, err := gateReviewAnswer(d.Query(c.text, rs))
				require.NoError(t, err)
				require.Empty(t, got)
				require.Positive(t, applied)
			})
		}
	}
}

// Each rule-body route's text runs twice on one database with $src the
// database, SetSchema declaring :t/a cardinality-many and a value asserted
// between the runs; the second run answers on every leg what it answers on
// the reference leg.
func TestRefuteGate_EachRuleBodyRouteAnswersAlikeAfterSetSchemaBetweenRuns(t *testing.T) {
	c := gateReviewCase{cardinality: schema.CardinalityOne, txs: 1, installAfter: -1, ops: []gateReviewOp{{tx: 0, entity: 0, value: "x"}, {tx: 0, entity: 1, value: "y"}}}
	for _, route := range gateReviewRoutes {
		if route.rules == "" {
			continue
		}
		t.Run(route.name, func(t *testing.T) {
			// secondRun builds the history on the leg, runs the route's text,
			// installs the cardinality-many declaration, asserts a second value
			// and answers the text's second run.
			secondRun := func(mode optimizerMode) [][]interface{} {
				h := buildGateReviewHistory(t, c, mode)
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
				second, err := run()
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

// QueryInto on the history handle, after the database's Query ran the text,
// answers a bound invocation's get-else on every leg as on the reference leg.
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
			// values answers the case on a database on the leg: the first run
			// where the case has one, then QueryInto on the history handle,
			// each tuple's value, sorted.
			values := func(mode optimizerMode) []string {
				d := gateReviewRuleBodyDatabase(t, mode, one)
				rs, err := parser.ParseRules(rules)
				require.NoError(t, err)
				if c.ranFirst {
					latest, err := gateReviewAnswer(d.Query(text, rs))
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{one, "new"}}, latest)
				}
				var got []answer
				require.NoError(t, d.History().QueryInto(&got, text, rs))
				var vs []string
				for _, a := range got {
					require.Equal(t, one, a.E)
					vs = append(vs, a.V)
				}
				slices.Sort(vs)
				return vs
			}
			want := values(referenceMode)
			for _, mode := range optimizerModes {
				require.Equal(t, want, values(mode), "%s answers otherwise than the reference leg", mode.name)
			}
		})
	}
}
