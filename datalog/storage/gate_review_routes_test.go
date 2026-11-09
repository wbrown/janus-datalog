package storage

import (
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// gateReviewOp is one generated write: an assertion or a removal of a value
// of :t/a at an entity, in the transaction it belongs to.
type gateReviewOp struct {
	tx     int
	entity int
	remove bool
	value  string
}

// gateReviewCase is one generated history of :t/a: the cardinality and
// uniqueness its schema declares (none for no definition), the writes, and
// an optional cardinality SetSchema installs after a transaction.
type gateReviewCase struct {
	cardinality   datalog.Keyword
	unique        datalog.Keyword
	ops           []gateReviewOp
	txs           int
	installAfter  int
	installedCard datalog.Keyword
}

func (c gateReviewCase) String() string {
	return fmt.Sprintf("card=%v unique=%v txs=%d install=%d:%v ops=%v", c.cardinality, c.unique, c.txs, c.installAfter, c.installedCard, c.ops)
}

func gateReviewSchema(card, unique datalog.Keyword) *schema.Schema {
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: datalog.NewKeyword(":t/name"), ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
	if card != nil {
		s.Add(&schema.AttributeDefinition{Ident: datalog.NewKeyword(":t/a"), ValueType: schema.TypeString, Cardinality: card, Unique: unique})
	}
	return s
}

// generateGateReviewCases draws histories from a seeded generator covering
// no definition, cardinality one, one under each unique rule, and many;
// assertions and removals, two writes at one entity in one transaction, and
// a SetSchema changing the cardinality between transactions.
func generateGateReviewCases(seed int64, n int) []gateReviewCase {
	r := rand.New(rand.NewSource(seed))
	cards := []datalog.Keyword{nil, schema.CardinalityOne, schema.CardinalityOne, schema.CardinalityOne, schema.CardinalityMany}
	uniques := []datalog.Keyword{nil, nil, schema.UniqueIdentity, schema.UniqueValue, nil}
	values := []string{"x", "y", "z"}
	var cases []gateReviewCase
	for i := 0; i < n; i++ {
		k := r.Intn(len(cards))
		c := gateReviewCase{cardinality: cards[k], unique: uniques[k], txs: 1 + r.Intn(3), installAfter: -1}
		for tx := 0; tx < c.txs; tx++ {
			writes := 1 + r.Intn(4)
			for w := 0; w < writes; w++ {
				c.ops = append(c.ops, gateReviewOp{tx: tx, entity: r.Intn(3), remove: tx > 0 && r.Intn(4) == 0, value: values[r.Intn(len(values))]})
			}
		}
		if c.txs > 1 && c.unique == nil && c.cardinality != nil && r.Intn(3) == 0 {
			c.installAfter = r.Intn(c.txs - 1)
			if c.cardinality == schema.CardinalityOne {
				c.installedCard = schema.CardinalityMany
			} else {
				c.installedCard = schema.CardinalityOne
			}
		}
		cases = append(cases, c)
	}
	return cases
}

// gateReviewHistory is a database holding a generated history, its entities
// and the transaction each write landed in.
type gateReviewHistory struct {
	db       *Database
	entities []datalog.Identity
	txIDs    []datalog.ElementID
}

// buildGateReviewHistory writes c into a database on the leg given, under
// the leg's planner options.
func buildGateReviewHistory(t *testing.T, c gateReviewCase, mode optimizerMode) gateReviewHistory {
	t.Helper()
	d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: gateReviewSchema(c.cardinality, c.unique)})
	e0 := datalog.NewIdentity("gate-review:e0")
	e1 := datalog.NewIdentity("gate-review:e1")
	e2 := datalog.NewIdentity("gate-review:e2")
	h := gateReviewHistory{db: d, entities: []datalog.Identity{e0, e1, e2}}
	name := datalog.NewKeyword(":t/name")
	a := datalog.NewKeyword(":t/a")
	for tx := 0; tx < c.txs; tx++ {
		w := d.NewTransaction()
		if tx == 0 {
			require.NoError(t, w.Add(e0, name, "e0"))
			require.NoError(t, w.Add(e1, name, "e1"))
			require.NoError(t, w.Add(e2, name, "e2"))
		}
		for _, op := range c.ops {
			if op.tx != tx {
				continue
			}
			if op.remove {
				require.NoError(t, w.Remove(h.entities[op.entity], a, op.value))
			} else {
				require.NoError(t, w.Add(h.entities[op.entity], a, op.value))
			}
		}
		id, err := w.Commit()
		require.NoError(t, err)
		h.txIDs = append(h.txIDs, id)
		if tx == c.installAfter {
			require.NoError(t, d.SetSchema(gateReviewSchema(c.installedCard, nil)))
		}
	}
	return h
}

// gateReviewRoutes are the routes a get-else on the source $src reaches its
// source by: named at top level, a subquery's $, an aggregating subquery's $,
// an or-join branch, a not-join body and a rule body.
var gateReviewRoutes = []struct {
	name  string
	text  string
	rules string
}{
	{"named", `[:find ?e ?v :in $ $src :where [?e :t/name _] [(get-else $src ?e :t/a "none") ?v]]`, ""},
	{"subquery $", `[:find ?e ?v :in $ $src :where [?e :t/name _] [(q [:find ?w :in $ ?x :where [?x :t/name _] [(get-else $ ?x :t/a "none") ?w]] $src ?e) [[?v]]]]`, ""},
	{"aggregating subquery $", `[:find ?e ?n :in $ $src :where [?e :t/name _] [(q [:find (count ?w) :in $ ?x :where [?x :t/name _] [(get-else $ ?x :t/a "none") ?w]] $src ?e) [[?n]]]]`, ""},
	{"or-join branch", `[:find ?e ?v :in $ $src :where [?e :t/name _] (or-join [?e ?v] (and [$src ?e :t/name _] [(get-else $src ?e :t/a "none") ?v]))]`, ""},
	{"not-join body", `[:find ?e :in $ $src :where [?e :t/name _] (not-join [?e] [$src ?e :t/name _] [(get-else $src ?e :t/a "none") ?v] [(= ?v "x")])]`, ""},
	{"rule body", `[:find ?e ?v :in $ $src % :where [?e :t/name _] (ga ?e ?v)]`, `[[(ga ?e ?v) [$src ?e :t/name _] [(get-else $src ?e :t/a "none") ?v]]]`},
	{"free rule invocation", `[:find ?e ?v :in $ $src % :where (ga ?e ?v)]`, `[[(ga ?e ?v) [$src ?e :t/name _] [(get-else $src ?e :t/a "none") ?v]]]`},
	{"nested subquery $", `[:find ?e ?v :in $ $src :where [?e :t/name _] [(q [:find ?w :in $ ?x :where [(q [:find ?u :in $ ?y :where [?y :t/name _] [(get-else $ ?y :t/a "none") ?u]] $ ?x) [[?w]]]] $src ?e) [[?v]]]]`, ""},
	{"or-join inside a subquery", `[:find ?e ?v :in $ $src :where [?e :t/name _] [(q [:find ?w :in $ ?x :where (or-join [?x ?w] (and [?x :t/name _] [(get-else $ ?x :t/a "none") ?w]))] $src ?e) [[?w2]]] [(identity ?w2) ?v]]`, ""},
	{"aggregating subquery inside a subquery", `[:find ?e ?n :in $ $src :where [?e :t/name _] [(q [:find ?m :in $ ?x :where [(q [:find (count ?w) :in $ ?y :where [?y :t/name _] [(get-else $ ?y :t/a "none") ?w]] $ ?x) [[?m]]]] $src ?e) [[?n]]]]`, ""},
	{"rule body under a subquery", `[:find ?e ?v :in $ $src % :where [?e :t/name _] [(q [:find ?w :in $ % ?x :where [?x :t/name _] (gd ?x ?w)] $src % ?e) [[?v]]]]`, `[[(gd ?e ?v) [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]]`},
}

// gateReviewViews are the handles of a history bound at $src: the database,
// its history handle, and an as-of handle at each transaction. They run in
// this order on one database, so one query text runs on each through the
// database's plan cache.
func gateReviewViews(h gateReviewHistory) []struct {
	name string
	view *Database
} {
	views := []struct {
		name string
		view *Database
	}{{"latest", h.db}, {"history", h.db.History()}}
	for i, id := range h.txIDs {
		views = append(views, struct {
			name string
			view *Database
		}{fmt.Sprintf("as-of tx%d", i), h.db.AsOf(id)})
	}
	views = append(views, struct {
		name string
		view *Database
	}{"latest again", h.db})
	return views
}

// gateReviewAnswer is a query's answer as sorted tuples, each set value
// sorted, or the error it failed with.
func gateReviewAnswer(rel executor.Relation, err error) ([][]interface{}, error) {
	tuples, err := executor.CollectTuples(rel, err)
	if err != nil {
		return nil, err
	}
	for _, tuple := range tuples {
		for j, value := range tuple {
			if members, isSet := value.([]interface{}); isSet {
				sorted := slices.Clone(members)
				slices.SortFunc(sorted, datalog.CompareValues)
				tuple[j] = sorted
			}
		}
	}
	slices.SortFunc(tuples, func(x, y []interface{}) int {
		for i := range x {
			if c := datalog.CompareValues(x[i], y[i]); c != 0 {
				return c
			}
		}
		return 0
	})
	return tuples, nil
}

// gateReviewRun is one answer of a route's text with $src one view of a
// history: the view's name, and the tuples or the error.
type gateReviewRun struct {
	view   string
	tuples [][]interface{}
	err    error
}

// gateReviewAnswers runs every route's text over every view of h in order,
// through h's database and its plan cache, and records each answer by route
// and view.
func gateReviewAnswers(t *testing.T, h gateReviewHistory) [][]gateReviewRun {
	t.Helper()
	views := gateReviewViews(h)
	answers := make([][]gateReviewRun, len(gateReviewRoutes))
	for r, route := range gateReviewRoutes {
		answers[r] = make([]gateReviewRun, len(views))
		for v, view := range views {
			inputs := []interface{}{WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$src"): view.view})}
			if route.rules != "" {
				rs, err := parser.ParseRules(route.rules)
				require.NoError(t, err)
				inputs = append(inputs, rs)
			}
			tuples, err := gateReviewAnswer(h.db.Query(route.text, inputs...))
			answers[r][v] = gateReviewRun{view: view.name, tuples: tuples, err: err}
		}
	}
	return answers
}

// Every route's get-else on $src answers on every leg what it answers on the
// reference leg, the per-tuple lookup, over generated histories, with $src
// each handle in turn through one database's plan cache.
func TestGateReview_GetElseOnEveryRouteAnswersAlikeOnBothOptimizerPaths(t *testing.T) {
	diverged := map[string]int{}
	first := map[string]string{}
	for i, c := range generateGateReviewCases(20261103, 120) {
		want := gateReviewAnswers(t, buildGateReviewHistory(t, c, referenceMode))
		for _, mode := range optimizerModes {
			got := gateReviewAnswers(t, buildGateReviewHistory(t, c, mode))
			for r, route := range gateReviewRoutes {
				for v := range got[r] {
					w, g := want[r][v], got[r][v]
					viewKind := g.view
					if len(viewKind) > 5 && viewKind[:5] == "as-of" {
						viewKind = "as-of"
					}
					key := mode.name + " / " + route.name + " / " + viewKind
					if (w.err == nil) != (g.err == nil) || !reflect.DeepEqual(w.tuples, g.tuples) {
						diverged[key]++
						if _, seen := first[key]; !seen {
							first[key] = fmt.Sprintf("case %d %v: reference %v %v, leg %v %v", i, c, w.tuples, w.err, g.tuples, g.err)
						}
					}
				}
			}
		}
	}
	require.Empty(t, diverged, "legs, routes and views answering otherwise than the reference leg, first instance of each: %v", first)
}

// The plan cache serves a plan made on one handle to another only where the
// other's sources answer the plan's questions alike: one database on each leg
// runs the text over each handle in turn, and every answer equals the answer
// a fresh reference-leg database, with no plan served, gives for that handle.
func TestGateReview_ThePlanCacheServesNoPlanAHandleAnswersOtherwise(t *testing.T) {
	text := `[:find ?e ?v :where [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]`
	for i, c := range generateGateReviewCases(7, 60) {
		for _, mode := range optimizerModes {
			leg := buildGateReviewHistory(t, c, mode)
			views := gateReviewViews(leg)
			for v := range views {
				reference := buildGateReviewHistory(t, c, referenceMode)
				referenceViews := gateReviewViews(reference)
				want, wantErr := gateReviewAnswer(reference.db.Query(text, WithSources(map[query.Symbol]executor.PatternMatcher{datalog.SymDollar: referenceViews[v].view})))
				got, gotErr := gateReviewAnswer(leg.db.Query(text, WithSources(map[query.Symbol]executor.PatternMatcher{datalog.SymDollar: views[v].view})))
				where := fmt.Sprintf("case %d %v leg %s view %q", i, c, mode.name, views[v].name)
				require.Equal(t, wantErr == nil, gotErr == nil, "%s: reference err %v, leg err %v", where, wantErr, gotErr)
				require.Equal(t, want, got, where)
			}
		}
	}
}

// Every route answers its query, on every leg, over a history whose $src is
// the history handle, so the differential above compares answers and not two
// refusals.
func TestGateReview_EveryRouteAnswers(t *testing.T) {
	c := gateReviewCase{cardinality: schema.CardinalityOne, txs: 2, installAfter: -1, ops: []gateReviewOp{{tx: 0, entity: 0, value: "x"}, {tx: 1, entity: 0, value: "y"}}}
	for _, mode := range optimizerModes {
		h := buildGateReviewHistory(t, c, mode)
		for _, route := range gateReviewRoutes {
			inputs := []interface{}{WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$src"): h.db.History()})}
			if route.rules != "" {
				rs, err := parser.ParseRules(route.rules)
				require.NoError(t, err)
				inputs = append(inputs, rs)
			}
			got, err := gateReviewAnswer(h.db.Query(route.text, inputs...))
			require.NoError(t, err, "%s, %s", route.name, mode.name)
			require.NotEmpty(t, got, "%s, %s", route.name, mode.name)
		}
	}
}

// On the algebra legs the get-else scan rewrite applies on every route with
// $src the latest view, so the differential above compares a rewritten plan
// with the per-tuple lookup on each route.
func TestGateReview_TheRewriteAppliesOnEveryRoute(t *testing.T) {
	for _, mode := range legsWhere(optimizerModes, func(m optimizerMode) bool { return m.algebra }) {
		for _, route := range gateReviewRoutes {
			applied := 0
			d := createOptimizerModeDB(t, mode, DatabaseOptions{
				Schema: gateReviewSchema(schema.CardinalityOne, nil),
				AnnotationHandler: func(e annotations.Event) {
					if e.Name == annotations.AlgebraGetElseScanApply {
						applied++
					}
				},
			})
			inputs := []interface{}{WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$src"): d})}
			if route.rules != "" {
				rs, err := parser.ParseRules(route.rules)
				require.NoError(t, err)
				inputs = append(inputs, rs)
			}
			got, err := gateReviewAnswer(d.Query(route.text, inputs...))
			require.NoError(t, err, "%s, %s", route.name, mode.name)
			require.Empty(t, got, "%s, %s", route.name, mode.name)
			require.Positive(t, applied, "%s, %s: the rewrite applies", route.name, mode.name)
		}
	}
}

// Every route's text runs twice on one database with $src the database,
// with SetSchema declaring :t/a cardinality-many and a second value asserted
// between the runs; the second run answers on every leg what it answers on
// the reference leg.
func TestGateReview_EveryRouteAnswersAlikeAfterSetSchemaBetweenRuns(t *testing.T) {
	c := gateReviewCase{cardinality: schema.CardinalityOne, txs: 1, installAfter: -1, ops: []gateReviewOp{{tx: 0, entity: 0, value: "x"}, {tx: 0, entity: 1, value: "y"}}}
	diverged := map[string]string{}
	for _, route := range gateReviewRoutes {
		// secondRun builds the history on the leg, runs the route's text,
		// installs the cardinality-many declaration, asserts a second value and
		// answers the text's second run.
		secondRun := func(mode optimizerMode) ([][]interface{}, error) {
			h := buildGateReviewHistory(t, c, mode)
			run := func() ([][]interface{}, error) {
				inputs := []interface{}{WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$src"): h.db})}
				if route.rules != "" {
					rs, err := parser.ParseRules(route.rules)
					require.NoError(t, err)
					inputs = append(inputs, rs)
				}
				return gateReviewAnswer(h.db.Query(route.text, inputs...))
			}
			first, err := run()
			require.NoError(t, err, "%s, %s", route.name, mode.name)
			require.NotEmpty(t, first, "%s, %s", route.name, mode.name)
			require.NoError(t, h.db.SetSchema(gateReviewSchema(schema.CardinalityMany, nil)))
			tx := h.db.NewTransaction()
			require.NoError(t, tx.Add(h.entities[0], datalog.NewKeyword(":t/a"), "z"))
			_, err = tx.Commit()
			require.NoError(t, err)
			return run()
		}
		want, wantErr := secondRun(referenceMode)
		for _, mode := range optimizerModes {
			got, gotErr := secondRun(mode)
			if (wantErr == nil) != (gotErr == nil) || !reflect.DeepEqual(want, got) {
				diverged[mode.name+" / "+route.name] = fmt.Sprintf("reference %v %v, leg %v %v", want, wantErr, got, gotErr)
			}
		}
	}
	require.Empty(t, diverged)
}
