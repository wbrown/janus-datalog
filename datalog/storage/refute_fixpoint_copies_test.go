package storage

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// copyFact is one fact of the copies fixture, written in order.
type copyFact struct {
	e datalog.Identity
	a string
	v interface{}
}

type copyPair = [2]interface{}
type copyPairs = map[copyPair]bool

// copiesFixture is a graph with a cycle, a two-cycle, a dead end and jump
// edges, each identity minted once.
func copiesFixture() ([]datalog.Identity, []copyFact) {
	a := datalog.NewIdentity("copies/a")
	b := datalog.NewIdentity("copies/b")
	c := datalog.NewIdentity("copies/c")
	d := datalog.NewIdentity("copies/d")
	e := datalog.NewIdentity("copies/e")
	f := datalog.NewIdentity("copies/f")
	g := datalog.NewIdentity("copies/g")
	h := datalog.NewIdentity("copies/h")
	nodes := []datalog.Identity{a, b, c, d, e, f, g, h}
	var facts []copyFact
	for i, n := range nodes {
		facts = append(facts, copyFact{n, ":name", string(rune('a' + i))})
	}
	for _, edge := range [][2]datalog.Identity{{a, b}, {b, c}, {c, d}, {d, b}, {c, e}, {e, f}, {f, e}, {g, a}, {f, h}} {
		facts = append(facts, copyFact{edge[0], ":edge", edge[1]})
	}
	for _, jump := range [][2]datalog.Identity{{h, g}, {c, a}, {b, d}} {
		facts = append(facts, copyFact{jump[0], ":jump", jump[1]})
	}
	return nodes, facts
}

func copyAttr(facts []copyFact, attr string) copyPairs {
	out := copyPairs{}
	for _, f := range facts {
		if f.a == attr {
			out[copyPair{f.e, f.v}] = true
		}
	}
	return out
}

// copyFixpoint iterates step from the empty set until it changes nothing.
func copyFixpoint(step func(copyPairs) copyPairs) copyPairs {
	cur := copyPairs{}
	for {
		next := step(cur)
		if len(next) == len(cur) {
			return next
		}
		cur = next
	}
}

func copiesSchema() *schema.Schema {
	return schema.NewBuilder().
		Attribute(":edge").Type(schema.TypeRef).Many().Add().
		Attribute(":jump").Type(schema.TypeRef).Many().Add().
		Attribute(":never").Type(schema.TypeRef).Add().
		Attribute(":name").Type(schema.TypeString).Add().
		MustBuild()
}

// A rule set whose recursive body holds a lower invocation under a not or a
// fallback answers the naive fixpoint through Query, bound by a scalar
// input, by a collection input and under a windowed form, and every lower
// entry of the plan Explain shows for the bound invocation is marked on a
// predicate and invokes that predicate's delta once.
func TestRefuteFixpoint_LowerOccurrencesUnderNotAndFallback(t *testing.T) {
	nodes, facts := copiesFixture()
	edge, jump := copyAttr(facts, ":edge"), copyAttr(facts, ":jump")
	cases := []struct {
		name  string
		rules string
		want  func() copyPairs
	}{
		{
			name: "a lower occurrence inside a not",
			rules: `[[(near ?a ?b) [?a :jump ?b]]
			         [(far ?a ?b) [?a :edge ?b] (not (near ?b ?a))]
			         [(far ?a ?c) [?a :edge ?b] (far ?b ?c) (not (near ?c ?a))]]`,
			want: func() copyPairs {
				return copyFixpoint(func(r copyPairs) copyPairs {
					out := copyPairs{}
					for p := range edge {
						if !jump[copyPair{p[1], p[0]}] {
							out[p] = true
						}
						for q := range r {
							if q[0] == p[1] && !jump[copyPair{q[1], p[0]}] {
								out[copyPair{p[0], q[1]}] = true
							}
						}
					}
					return out
				})
			},
		},
		{
			name: "a lower occurrence inside a fallback branch",
			rules: `[[(near ?a ?b) [?a :jump ?b]]
			         [(far ?a ?k) [?a :edge ?b] (or-default-join [[?a] ?k] (near ?a ?k) [(ground 0) ?k])]
			         [(far ?a ?c) [?a :edge ?b] (far ?b ?c)]]`,
			want: func() copyPairs {
				return copyFixpoint(func(r copyPairs) copyPairs {
					out := copyPairs{}
					for p := range edge {
						fell := true
						for q := range jump {
							if q[0] == p[0] {
								out[copyPair{p[0], q[1]}] = true
								fell = false
							}
						}
						if fell {
							out[copyPair{p[0], int64(0)}] = true
						}
						for q := range r {
							if q[0] == p[1] {
								out[copyPair{p[0], q[1]}] = true
							}
						}
					}
					return out
				})
			},
		},
	}
	for _, tc := range cases {
		for _, mode := range optimizerModes {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: copiesSchema()})
				tx := d.NewTransaction()
				for _, f := range facts {
					require.NoError(t, tx.Add(f.e, kw(f.a), f.v))
				}
				_, err := tx.Commit()
				require.NoError(t, err)
				names := readEntityNames(t, d)
				render := func(s copyPairs) string {
					var out []string
					for p := range s {
						out = append(out, fmt.Sprintf("%v>%v", nameOf(names, p[0]), nameOf(names, p[1])))
					}
					sort.Strings(out)
					return strings.Join(out, " ")
				}
				rules := mustRuleSet(t, tc.rules)
				want := tc.want()
				for _, n := range nodes {
					rel, err := d.Query(`[:find ?x ?y :in $ % ?x :where (far ?x ?y)]`, rules, n)
					require.NoError(t, err)
					got := valuePairsOf(t, rel)
					wantAt := copyPairs{}
					for p := range want {
						if p[0] == n {
							wantAt[p] = true
						}
					}
					require.Equal(t, render(wantAt), render(got), "bound at %s", names[n])
				}
				anyNodes := make([]interface{}, len(nodes))
				for i, n := range nodes {
					anyNodes[i] = n
				}
				rel, err := d.Query(`[:find ?x ?y :in $ % [?x ...] :where (far ?x ?y)]`, rules, anyNodes)
				require.NoError(t, err)
				require.Equal(t, render(want), render(valuePairsOf(t, rel)), "collection input")
				rel, err = d.Query(`[:find ?x ?y :in $ % :where [?x :name _] (or-join [[?x] ?y] (far ?x ?y) (and [?x :never ?y]))]`, rules)
				require.NoError(t, err)
				require.Equal(t, render(want), render(valuePairsOf(t, rel)), "windowed")

				plan, err := d.Explain(`[:find ?y :in $ % ?x :where (far ?x ?y)]`, rules, nodes[0])
				require.NoError(t, err)
				for inv, ev := range plan.Evaluations {
					for _, stratum := range ev.Strata {
						for _, body := range stratum.Bodies.Lower {
							var deltas []query.Symbol
							for _, called := range query.Invocations(body.Plan.Query.Where) {
								if query.IsDeltaSymbol(called.Name) {
									deltas = append(deltas, called.Name)
								}
							}
							require.NotNil(t, body.Marked, "%v: stratum %v: the lower entry of %s is marked on a predicate", inv, stratum.Stratum.Predicates, body.Rule.Name)
						require.Equal(t, []query.Symbol{query.DeltaSymbol(body.Marked)}, deltas, "%v: stratum %v: the lower entry of %s invokes the delta of the predicate it is marked on, once", inv, stratum.Stratum.Predicates, body.Rule.Name)
						}
					}
				}
			})
		}
	}
}

// Through Explain over the storage route, every delta copy of an evaluation
// carries its delta invocation, and a copy whose occurrence stands at the top
// level opens with it; a copy whose occurrence stands inside an or-join
// branch, an or or a subquery carries it inside the form or the call, whose
// own plan the delta leads (docs/proposals/DELTA_LEADS_ITS_COPY_PROOF.md,
// T2), and the body opens as the rank and the score place the form or the
// call.
func TestRefuteFixpoint_FirstClauseOfEachDeltaCopyThroughExplain(t *testing.T) {
	cases := []struct{ name, rules string }{
		{"two occurrences at the top level", `[[(reach ?a ?b) [?a :edge ?b]]
		                                       [(reach ?a ?c) (reach ?a ?b) (reach ?b ?c)]]`},
		{"an occurrence inside an or-join branch", `[[(reach ?a ?b) [?a :edge ?b]]
		                                            [(reach ?a ?c) [?a :edge ?b] (or-join [?b ?c] (reach ?b ?c) [?b :jump ?c])]]`},
		{"an occurrence inside an or", `[[(reach ?a ?b) [?a :edge ?b]]
		                                 [(reach ?a ?c) [?a :edge ?b] (or (reach ?b ?c) [?b :jump ?c])]]`},
		{"an occurrence inside a subquery", `[[(reach ?x ?y) [?x :edge ?y]]
		                                     [(reach ?x ?z) [?x :edge ?y] [(q [:find ?w :in $ % ?y :where (reach ?y ?w)] $ % ?y) [[?z] ...]]]]`},
	}
	for _, tc := range cases {
		for _, mode := range optimizerModes {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: copiesSchema()})
				rules := mustRuleSet(t, tc.rules)
				_, err := d.Query(`[:find ?x ?y :in $ % :where (reach ?x ?y)]`, rules)
				require.NoError(t, err)
				plan, err := d.Explain(`[:find ?x ?y :in $ % :where (reach ?x ?y)]`, rules)
				require.NoError(t, err)
				copies := 0
				for _, ev := range plan.Evaluations {
					for _, stratum := range ev.Strata {
						for _, body := range stratum.Bodies.Delta {
							copies++
							delta := query.DeltaSymbol(body.Marked)
							carried := false
							for _, inv := range query.Invocations(body.Plan.Query.Where) {
								if inv.Name == delta {
									carried = true
								}
							}
							require.True(t, carried, "the copy marked on %s carries %s", body.Marked, delta)
							topLevel := false
							for _, clause := range body.Plan.Query.Where {
								if inv, ok := clause.(*query.RuleInvocation); ok && inv.Name == delta {
									topLevel = true
								}
							}
							if topLevel {
								first := body.Plan.Phases[0].Query.Where[0]
								inv, ok := first.(*query.RuleInvocation)
								require.True(t, ok && inv.Name == delta, "the copy marked on %s opens with %v, want %s", body.Marked, first, delta)
							}
						}
					}
				}
				require.NotZero(t, copies, "the evaluation holds a delta copy")
			})
		}
	}
}

func nameOf(names map[datalog.Identity]string, v interface{}) string {
	if id, ok := v.(datalog.Identity); ok {
		return names[id]
	}
	return fmt.Sprintf("%v", v)
}

func valuePairsOf(t *testing.T, rel executor.Relation) copyPairs {
	t.Helper()
	out := copyPairs{}
	require.NoError(t, executor.ForEach(rel, func(tuple executor.Tuple) error {
		out[copyPair{tuple[0], tuple[1]}] = true
		return nil
	}))
	return out
}

// readEntityNames reads each entity's :name back from the database.
func readEntityNames(t *testing.T, d *Database) map[datalog.Identity]string {
	t.Helper()
	rel, err := d.Query(`[:find ?e ?n :where [?e :name ?n]]`)
	require.NoError(t, err)
	names := map[datalog.Identity]string{}
	require.NoError(t, executor.ForEach(rel, func(tuple executor.Tuple) error {
		names[tuple[0].(datalog.Identity)] = tuple[1].(string)
		return nil
	}))
	return names
}
