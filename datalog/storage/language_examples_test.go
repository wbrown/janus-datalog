package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/qb"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Explain plans the prepared query under the symbols the inputs bind, so its
// plan is the plan an execution with those inputs runs
// (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §5.1, §5.2).
func TestExplain_PlanIsTheExecutedPlanUnderTheInputsBoundSymbols(t *testing.T) {
	cases := []struct {
		name, text string
		input      interface{}
	}{
		{"a scalar input", `[:find ?n ?m :in $ ?x :where [?e :a ?x] [?e :b ?f] [?f :c ?n] [?f :a ?m] [?g :b ?f]]`, int64(1)},
		{"a collection input", `[:find ?n :in $ [?x ...] :where [?e :a ?x] [?e :b ?f] [?f :c ?n] [?g :b ?f] [?g :a ?k]]`, []interface{}{int64(1)}},
		{"a scalar input beside a subquery", `[:find (count ?n) :in $ ?x :where [?e :a ?x] [?e :b ?f] [?f :c ?n] [(q [:find (max ?w) :in $ ?f :where [?f :c ?w]] $ ?f) [[?mx]]]]`, int64(1)},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(datalog.NewIdentity("p"), kw(":a"), int64(1)))
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					explained, err := d.Explain(c.text, c.input)
					require.NoError(t, err)
					q, err := parser.ParseQuery(c.text)
					require.NoError(t, err)
					prepared, err := query.Prepare(q, nil)
					require.NoError(t, err)
					bound := map[query.Symbol]bool{}
					for _, in := range query.BindingInputs(q.In) {
						switch in := in.(type) {
						case query.ScalarInput:
							bound[in.Symbol] = true
						case query.CollectionInput:
							bound[in.Symbol] = true
						}
					}
					executed, err := d.NewExecutor().GetPlanner().PlanQuery(prepared, query.Bindings{Symbols: bound}, nil)
					require.NoError(t, err)
					require.Equal(t, executed.String(), explained.String(), "Explain's plan is the plan the execution runs")
				})
			}
		})
	}
}

// A subquery the builder builds over each inner :in shape, none, a tuple
// input and a relation input, renders to text the parser reads back and
// answers what the text answers (docs/reference/QUERY_BUILDER.md).
func TestQueryBuilderSubqueries_RenderReparseAndAnswerAsTheirText(t *testing.T) {
	n, m := kw(":n"), kw(":m")
	a, b := datalog.NewIdentity("a"), datalog.NewIdentity("b")
	e, v, w, x, c := qb.NewVar("e"), qb.NewVar("v"), qb.NewVar("w"), qb.NewVar("x"), qb.NewVar("c")
	builds := []struct {
		name  string
		build func() *qb.QueryBuilder
		want  [][]interface{}
	}{
		{"an inner query with no :in", func() *qb.QueryBuilder {
			inner := qb.Query().Find(qb.Count(x)).Where(qb.Pat(x, qb.Kw(":n"), qb.NewVar("q")))
			return qb.Query().Find(e, c).Where(qb.Pat(e, qb.Kw(":n"), v), qb.Subquery(inner).BindRelation(c))
		}, [][]interface{}{{a, int64(2)}, {b, int64(2)}}},
		{"an inner tuple input", func() *qb.QueryBuilder {
			inner := qb.Query().Find(x).In(qb.DB, qb.Tuple(v, w)).Where(qb.Pat(x, qb.Kw(":n"), v), qb.Pat(x, qb.Kw(":m"), w))
			return qb.Query().Find(e, c).Where(qb.Pat(e, qb.Kw(":n"), v), qb.Pat(e, qb.Kw(":m"), w), qb.Subquery(inner, v, w).BindCollection(c))
		}, [][]interface{}{{a, a}, {b, b}}},
		{"an inner relation input", func() *qb.QueryBuilder {
			inner := qb.Query().Find(x).In(qb.DB, qb.Relation(v, w)).Where(qb.Pat(x, qb.Kw(":n"), v), qb.Pat(x, qb.Kw(":m"), w))
			return qb.Query().Find(e, c).Where(qb.Pat(e, qb.Kw(":n"), v), qb.Pat(e, qb.Kw(":m"), w), qb.Subquery(inner, v, w).BindCollection(c))
		}, [][]interface{}{{a, a}, {b, b}}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(a, n, int64(1)))
			require.NoError(t, tx.Add(a, m, int64(10)))
			require.NoError(t, tx.Add(b, n, int64(2)))
			require.NoError(t, tx.Add(b, m, int64(20)))
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, bd := range builds {
				t.Run(bd.name, func(t *testing.T) {
					built, err := bd.build().Build()
					require.NoError(t, err)
					text := built.String()
					_, err = parser.ParseQuery(text)
					require.NoError(t, err, "the built query's text parses back: %s", text)
					fromBuilt, err := executor.CollectTuples(d.Query(built))
					require.NoError(t, err)
					require.ElementsMatch(t, bd.want, fromBuilt)
					fromText, err := executor.CollectTuples(d.Query(text))
					require.NoError(t, err)
					require.ElementsMatch(t, bd.want, fromText, "the text answers what the built query answers")
				})
			}
		})
	}
}

// A symbol at two positions of one pattern selects the datoms whose
// positions agree, a cycle of two patterns over two symbols answers the
// entity that reaches itself, a relation ground naming one symbol twice binds
// the tuples whose values agree, and a branch invoking a required position on
// a local nothing in the branch binds is refused
// (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §2.4, §2.5).
func TestRepeatedSymbols_UnifyAndAnUnboundRequiredPositionIsRefused(t *testing.T) {
	a, b := datalog.NewIdentity("a"), datalog.NewIdentity("b")
	answers := []struct {
		name, text string
		want       [][]interface{}
	}{
		{"one pattern naming a symbol twice", `[:find ?e :where [?e :b ?e]]`, [][]interface{}{{a}}},
		{"two patterns closing a cycle", `[:find ?e :where [?e :b ?v] [?v :b ?e]]`, [][]interface{}{{a}}},
		{"a relation ground naming a symbol twice", `[:find ?x :where [?x :b ?y] [(ground [[1 1] [1 2]]) [[?p ?p]]]]`, [][]interface{}{{a}, {b}}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(a, kw(":b"), a))
			require.NoError(t, tx.Add(b, kw(":b"), a))
			require.NoError(t, tx.Add(a, kw(":c"), int64(1)))
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, c := range answers {
				t.Run(c.name, func(t *testing.T) {
					tuples, err := executor.CollectTuples(d.Query(c.text))
					require.NoError(t, err)
					require.ElementsMatch(t, c.want, tuples)
				})
			}
			t.Run("a branch invoking a required position on a local nothing binds", func(t *testing.T) {
				rs := mustRuleSet(t, `[[(s [?x] ?y) [?x :c ?y]]]`)
				_, err := executor.CollectTuples(d.Query(`[:find ?x :in $ % :where [?x :c _] (or [?x :c 9] (and [?x :b ?z] (s ?e ?e)))]`, rs))
				require.Error(t, err, "the branch reads ?e at a required position, and neither the branch nor the header binds it")
			})
		})
	}
}

// A byte vector, an instant and a whole-valued float as a built query's
// constants answer the entity carrying them, and the query's rendering,
// parsed back, answers the same
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §6).
func TestBytesAndInstantConstants_AnswerFromTheBuiltQueryAndItsRendering(t *testing.T) {
	blob := []byte{0xde, 0xad, 0xbe, 0xef, 0x01}
	at := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	e := datalog.NewIdentity("e1")
	x := qb.NewVar("x")
	constants := []struct {
		name  string
		attr  datalog.Keyword
		value interface{}
	}{
		{"a byte vector", kw(":b/v"), blob},
		{"an instant", kw(":b/t"), at},
		{"a whole-valued float", kw(":b/f"), float64(3)},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(e, kw(":b/v"), blob))
			require.NoError(t, tx.Add(e, kw(":b/t"), at))
			require.NoError(t, tx.Add(e, kw(":b/f"), float64(3)))
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, c := range constants {
				t.Run(c.name, func(t *testing.T) {
					built := qb.Query().Find(x).Where(qb.Pat(x, c.attr, c.value)).MustBuild()
					fromBuilt, err := executor.CollectTuples(d.Query(built))
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{e}}, fromBuilt)
					parsed, err := parser.ParseQuery(built.String())
					require.NoError(t, err, "the built query's text parses back: %s", built.String())
					fromText, err := executor.CollectTuples(d.Query(parsed))
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{e}}, fromText, "the rendering answers what the built query answers")
				})
			}
		})
	}
}

// A subquery over an inner query declaring no :in is passed the $ an omitted
// :in defaults to and answers, and a call passing nothing passes fewer inputs
// than the inner :in binds and is refused on the count
// (docs/reference/QUERY_BUILDER.md, the subquery section).
func TestSubqueryOverAnInnerQueryWithNoIn_IsPassedTheDefaultSource(t *testing.T) {
	p := datalog.NewIdentity("p1")
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(p, kw(":p/name"), "alice"))
			_, err := tx.Commit()
			require.NoError(t, err)
			tuples, err := executor.CollectTuples(d.Query(`[:find ?x ?n :where [?x :p/name _] [(q [:find (count ?y) :where [?y :p/name _]] $) [[?n]]]]`))
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{p, int64(1)}}, tuples)
			_, err = executor.CollectTuples(d.Query(`[:find ?x ?n :where [?x :p/name _] [(q [:find (count ?y) :where [?y :p/name _]]) [[?n]]]]`))
			require.Error(t, err, "a call passing nothing passes fewer inputs than the inner :in binds")
		})
	}
}

// A subquery's inner query takes the sources its call passes: a pattern on an
// inner source symbol, the $ of an omitted or declared :in included, reads the
// source the call names at that symbol's place, at any depth of nesting
// (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §2.2 and §2.5).
func TestSubquery_ACallsSourceArgumentIsTheInnerSource(t *testing.T) {
	cases := []struct{ name, text, want string }{
		{"an inner :in $t given $other", `[:find ?n :in $ $other :where [(q [:find ?n :in $t :where [$t ?e :p/name ?n]] $other) [[?n]]]]`, "away"},
		{"an inner :in $ given $other", `[:find ?n :in $ $other :where [(q [:find ?n :in $ :where [?e :p/name ?n]] $other) [[?n]]]]`, "away"},
		{"an omitted inner :in given $other", `[:find ?n :in $ $other :where [(q [:find ?n :where [?e :p/name ?n]] $other) [[?n]]]]`, "away"},
		{"an inner :in $t given $", `[:find ?n :in $ $other :where [(q [:find ?n :in $t :where [$t ?e :p/name ?n]] $) [[?n]]]]`, "home"},
		{"an inner call passing its $t onward", `[:find ?n :in $ $other :where [(q [:find ?n :in $t :where [(q [:find ?n :in $u :where [$u ?e :p/name ?n]] $t) [[?n]]]] $other) [[?n]]]]`, "away"},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			home := createOptimizerModeDB(t, mode, DatabaseOptions{})
			away := createOptimizerModeDB(t, mode, DatabaseOptions{})
			for d, name := range map[*Database]string{home: "home", away: "away"} {
				tx := d.NewTransaction()
				require.NoError(t, tx.Add(datalog.NewIdentity("p1"), kw(":p/name"), name))
				_, err := tx.Commit()
				require.NoError(t, err)
			}
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					tuples, err := executor.CollectTuples(home.Query(c.text, WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$other"): away})))
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{c.want}}, tuples)
				})
			}
		})
	}
}

// Explain renders an invocation in its phase with the variable arguments the
// phase binds before it, and the execution rewrites the rule set for that
// binding (docs/reference/RULES.md, the observability and evaluation
// sections): a required position a ground binds, and a free predicate's
// first position bound by an or's output or by a pattern, each shown bound
// at that one argument and adorned bound-free at execution.
func TestExplain_RendersAnInvocationWithTheArgumentsThePhaseBindsBeforeIt(t *testing.T) {
	const succ = `[[(succ [?n] ?m) [(+ ?n 1) ?m]]]`
	cases := []struct {
		name, text, rules string
		bound             query.Symbol
		want              []string
	}{
		{"a required position a ground binds", `[:find ?m :in $ % :where (succ ?n ?m) [(ground 5) ?n]]`, succ, datalog.NewSymbol("?n"), []string{"6"}},
		{"a first position an or's output binds", `[:find ?x ?y :in $ % :where (or [?x :edge ?z] [?z :edge ?x]) (reach ?x ?y)]`, closureRules, datalog.NewSymbol("?x"), chainClosure(4)},
		{"a first position a pattern binds", `[:find ?x ?y :in $ % :where [?x :type :node] (reach ?x ?y)]`, closureRules, datalog.NewSymbol("?x"), chainClosure(4)},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					adorned := &rewriteAdornments{}
					d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: adorned.handle})
					g := writeGraph(t, d, chain(4))
					rs := mustRuleSet(t, c.rules)
					plan, err := d.Explain(c.text, rs)
					require.NoError(t, err)
					var bound [][]query.Symbol
					for _, phase := range plan.Phases {
						for _, inv := range phase.Invocations {
							bound = append(bound, inv.Bound)
						}
					}
					require.Equal(t, [][]query.Symbol{{c.bound}}, bound, "the one invocation is rendered with the argument the phase binds before it")
					rel, err := d.Query(c.text, rs)
					require.NoError(t, err)
					require.ElementsMatch(t, c.want, g.tuples(t, rel))
					require.Equal(t, [][]bool{{true, false}}, adorned.made(), "the execution rewrites the predicate for the bound first position")
				})
			}
		})
	}
}
