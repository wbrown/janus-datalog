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

func buildGateReviewHistory(t *testing.T, c gateReviewCase, algebra bool) gateReviewHistory {
	t.Helper()
	opts := DefaultPlannerOptions()
	opts.EnableAlgebraOptimizer = algebra
	d, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory-trees", Schema: gateReviewSchema(c.cardinality, c.unique), PlannerOptions: &opts})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
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

// Every route's get-else on $src answers with the algebra optimizer on what
// it answers with it off, the per-tuple lookup, over generated histories,
// with $src each handle in turn through one database's plan cache.
func TestGateReview_GetElseOnEveryRouteAnswersAlikeOnBothOptimizerPaths(t *testing.T) {
	diverged := map[string]int{}
	first := map[string]string{}
	for i, c := range generateGateReviewCases(20261103, 120) {
		on := buildGateReviewHistory(t, c, true)
		off := buildGateReviewHistory(t, c, false)
		onViews, offViews := gateReviewViews(on), gateReviewViews(off)
		for _, route := range gateReviewRoutes {
			for v := range onViews {
				run := func(h gateReviewHistory, view *Database) ([][]interface{}, error) {
					inputs := []interface{}{WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$src"): view})}
					if route.rules != "" {
						rs, err := parser.ParseRules(route.rules)
						require.NoError(t, err)
						inputs = append(inputs, rs)
					}
					return gateReviewAnswer(h.db.Query(route.text, inputs...))
				}
				want, wantErr := run(off, offViews[v].view)
				got, gotErr := run(on, onViews[v].view)
				viewKind := onViews[v].name
				if len(viewKind) > 5 && viewKind[:5] == "as-of" {
					viewKind = "as-of"
				}
				key := route.name + " / " + viewKind
				if (wantErr == nil) != (gotErr == nil) || !reflect.DeepEqual(want, got) {
					diverged[key]++
					if _, seen := first[key]; !seen {
						first[key] = fmt.Sprintf("case %d %v: off %v %v, on %v %v", i, c, want, wantErr, got, gotErr)
					}
				}
			}
		}
	}
	require.Empty(t, diverged, "routes and views where the optimizer-on path answers otherwise than the optimizer-off path, first instance of each: %v", first)
}

// The plan cache serves a plan made on one handle to another only where the
// other's sources answer the plan's questions alike: one database runs each
// route's text over each handle in turn, and every answer equals the answer
// a fresh database with no plan cache gives on the optimizer-off path.
func TestGateReview_ThePlanCacheServesNoPlanAHandleAnswersOtherwise(t *testing.T) {
	for i, c := range generateGateReviewCases(7, 60) {
		on := buildGateReviewHistory(t, c, true)
		views := gateReviewViews(on)
		text := `[:find ?e ?v :where [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]`
		{
			for v := range views {
				off := buildGateReviewHistory(t, c, false)
				offViews := gateReviewViews(off)
				want, wantErr := gateReviewAnswer(off.db.Query(text, WithSources(map[query.Symbol]executor.PatternMatcher{datalog.SymDollar: offViews[v].view})))
				got, gotErr := gateReviewAnswer(on.db.Query(text, WithSources(map[query.Symbol]executor.PatternMatcher{datalog.SymDollar: views[v].view})))
				where := fmt.Sprintf("case %d %v view %q", i, c, views[v].name)
				require.Equal(t, wantErr == nil, gotErr == nil, "%s: off err %v, on err %v", where, wantErr, gotErr)
				require.Equal(t, want, got, where)
			}
		}
	}
}

// Every route answers its query, on each optimizer path, over a history
// whose $src is the latest view, so the differential above compares answers
// and not two refusals.
func TestGateReview_EveryRouteAnswers(t *testing.T) {
	c := gateReviewCase{cardinality: schema.CardinalityOne, txs: 2, installAfter: -1, ops: []gateReviewOp{{tx: 0, entity: 0, value: "x"}, {tx: 1, entity: 0, value: "y"}}}
	for _, algebra := range []bool{false, true} {
		h := buildGateReviewHistory(t, c, algebra)
		for _, route := range gateReviewRoutes {
			inputs := []interface{}{WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$src"): h.db.History()})}
			if route.rules != "" {
				rs, err := parser.ParseRules(route.rules)
				require.NoError(t, err)
				inputs = append(inputs, rs)
			}
			got, err := gateReviewAnswer(h.db.Query(route.text, inputs...))
			require.NoError(t, err, "%s, algebra %v", route.name, algebra)
			require.NotEmpty(t, got, "%s, algebra %v", route.name, algebra)
		}
	}
}

// On the optimizer-on path the get-else scan rewrite applies on every route
// with $src the latest view, so the differential above compares a rewritten
// plan with the per-tuple lookup on each route.
func TestGateReview_TheRewriteAppliesOnEveryRoute(t *testing.T) {
	for _, route := range gateReviewRoutes {
		applied := 0
		opts := DefaultPlannerOptions()
		opts.Handler = func(e annotations.Event) {
			if e.Name == annotations.AlgebraGetElseScanApply {
				applied++
			}
		}
		d, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory-trees", Schema: gateReviewSchema(schema.CardinalityOne, nil), PlannerOptions: &opts})
		require.NoError(t, err)
		inputs := []interface{}{WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$src"): d})}
		if route.rules != "" {
			rs, err := parser.ParseRules(route.rules)
			require.NoError(t, err)
			inputs = append(inputs, rs)
		}
		got, err := gateReviewAnswer(d.Query(route.text, inputs...))
		require.NoError(t, err, route.name)
		require.Empty(t, got, route.name)
		require.Positive(t, applied, "%s: the rewrite applies", route.name)
		require.NoError(t, d.Close())
	}
}

// Every route's text runs twice on one database with $src the database,
// with SetSchema declaring :t/a cardinality-many and a second value asserted
// between the runs; the second run answers on the optimizer-on path what it
// answers on the optimizer-off path.
func TestGateReview_EveryRouteAnswersAlikeAfterSetSchemaBetweenRuns(t *testing.T) {
	diverged := map[string]string{}
	for _, route := range gateReviewRoutes {
		var answers [2][][]interface{}
		var errs [2]error
		for i, algebra := range []bool{false, true} {
			c := gateReviewCase{cardinality: schema.CardinalityOne, txs: 1, installAfter: -1, ops: []gateReviewOp{{tx: 0, entity: 0, value: "x"}, {tx: 0, entity: 1, value: "y"}}}
			h := buildGateReviewHistory(t, c, algebra)
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
			require.NoError(t, err, route.name)
			require.NotEmpty(t, first, route.name)
			require.NoError(t, h.db.SetSchema(gateReviewSchema(schema.CardinalityMany, nil)))
			tx := h.db.NewTransaction()
			require.NoError(t, tx.Add(h.entities[0], datalog.NewKeyword(":t/a"), "z"))
			_, err = tx.Commit()
			require.NoError(t, err)
			answers[i], errs[i] = run()
		}
		if (errs[0] == nil) != (errs[1] == nil) || !reflect.DeepEqual(answers[0], answers[1]) {
			diverged[route.name] = fmt.Sprintf("off %v %v, on %v %v", answers[0], errs[0], answers[1], errs[1])
		}
	}
	require.Empty(t, diverged)
}
