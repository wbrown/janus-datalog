package storage

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A stratum evaluated semi-naively derives what the naive evaluation of its
// rules derives, whichever copies an iteration runs
// (docs/proposals/DELTA_COPY_RUNS_ON_ITS_DELTA_PROOF.md T1): every recursive
// rule set below is answered through Query free, bound at each position to
// each node, under a windowed form, and under a not, and each answer is the
// one the naive fixpoint of the same rules over the same facts gives.

// fixpointFact is one fact of the fixture, written in order.
type fixpointFact struct {
	e datalog.Identity
	a string
	v interface{}
}

// fixpointFixture is a graph with a cycle, a branch, a dead end, a jump edge
// and a parent relation: each identity minted once.
type fixpointFixture struct {
	nodes []datalog.Identity
	facts []fixpointFact
}

func newFixpointFixture() fixpointFixture {
	a := datalog.NewIdentity("fixpoint/a")
	b := datalog.NewIdentity("fixpoint/b")
	c := datalog.NewIdentity("fixpoint/c")
	dd := datalog.NewIdentity("fixpoint/d")
	e := datalog.NewIdentity("fixpoint/e")
	f := datalog.NewIdentity("fixpoint/f")
	g := datalog.NewIdentity("fixpoint/g")
	h := datalog.NewIdentity("fixpoint/h")
	nodes := []datalog.Identity{a, b, c, dd, e, f, g, h}
	names := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	var facts []fixpointFact
	for i, n := range nodes {
		facts = append(facts, fixpointFact{n, ":name", names[i]})
	}
	for _, edge := range [][2]datalog.Identity{
		{a, b}, {b, c}, {c, dd}, {dd, b}, {c, e}, {e, f}, {g, a}, {f, h},
	} {
		facts = append(facts, fixpointFact{edge[0], ":edge", edge[1]})
	}
	facts = append(facts,
		fixpointFact{h, ":jump", g},
		fixpointFact{e, ":bad", true},
	)
	for _, child := range [][2]datalog.Identity{
		{a, b}, {a, c}, {b, dd}, {c, e}, {dd, f}, {e, g}, {g, h},
	} {
		facts = append(facts, fixpointFact{child[0], ":child", child[1]})
	}
	return fixpointFixture{nodes: nodes, facts: facts}
}

// pairs reads the fixture's facts of attribute a as pairs.
func (fx fixpointFixture) pairs(a string) map[[2]datalog.Identity]bool {
	out := map[[2]datalog.Identity]bool{}
	for _, f := range fx.facts {
		if f.a == a {
			out[[2]datalog.Identity{f.e, f.v.(datalog.Identity)}] = true
		}
	}
	return out
}

// marked reads the entities carrying attribute a.
func (fx fixpointFixture) marked(a string) map[datalog.Identity]bool {
	out := map[datalog.Identity]bool{}
	for _, f := range fx.facts {
		if f.a == a {
			out[f.e] = true
		}
	}
	return out
}

type pairSet = map[[2]datalog.Identity]bool

// compose is {(x, z) : (x, y) in l, (y, z) in r}.
func compose(l, r pairSet) pairSet {
	out := pairSet{}
	for p := range l {
		for q := range r {
			if p[1] == q[0] {
				out[[2]datalog.Identity{p[0], q[1]}] = true
			}
		}
	}
	return out
}

func union(sets ...pairSet) pairSet {
	out := pairSet{}
	for _, s := range sets {
		for p := range s {
			out[p] = true
		}
	}
	return out
}

func inverse(s pairSet) pairSet {
	out := pairSet{}
	for p := range s {
		out[[2]datalog.Identity{p[1], p[0]}] = true
	}
	return out
}

// naive iterates step from the empty relation until it changes nothing.
func naive(step func(pairSet) pairSet) pairSet {
	cur := pairSet{}
	for {
		next := step(cur)
		if len(next) == len(cur) {
			return next
		}
		cur = next
	}
}

type fixpointCase struct {
	name      string
	rules     string
	predicate string
	want      func(fx fixpointFixture) pairSet
}

func fixpointCases() []fixpointCase {
	return []fixpointCase{
		{
			name: "right-linear closure",
			rules: `[[(reach ?a ?b) [?a :edge ?b]]
			         [(reach ?a ?c) [?a :edge ?b] (reach ?b ?c)]]`,
			predicate: "reach",
			want: func(fx fixpointFixture) pairSet {
				e := fx.pairs(":edge")
				return naive(func(r pairSet) pairSet { return union(e, compose(e, r)) })
			},
		},
		{
			name: "left-linear closure",
			rules: `[[(reach ?a ?b) [?a :edge ?b]]
			         [(reach ?a ?c) (reach ?a ?b) [?b :edge ?c]]]`,
			predicate: "reach",
			want: func(fx fixpointFixture) pairSet {
				e := fx.pairs(":edge")
				return naive(func(r pairSet) pairSet { return union(e, compose(r, e)) })
			},
		},
		{
			name: "two occurrences of the recursive predicate",
			rules: `[[(reach ?a ?b) [?a :edge ?b]]
			         [(reach ?a ?c) (reach ?a ?b) (reach ?b ?c)]]`,
			predicate: "reach",
			want: func(fx fixpointFixture) pairSet {
				e := fx.pairs(":edge")
				return naive(func(r pairSet) pairSet { return union(e, compose(r, r)) })
			},
		},
		{
			name: "mutual recursion, odd paths",
			rules: `[[(odd ?a ?b) [?a :edge ?b]]
			         [(odd ?a ?c) [?a :edge ?b] (even ?b ?c)]
			         [(even ?a ?c) [?a :edge ?b] (odd ?b ?c)]]`,
			predicate: "odd",
			want: func(fx fixpointFixture) pairSet {
				e := fx.pairs(":edge")
				odd, even := pairSet{}, pairSet{}
				for {
					nOdd := union(e, compose(e, even))
					nEven := compose(e, odd)
					if len(nOdd) == len(odd) && len(nEven) == len(even) {
						return odd
					}
					odd, even = nOdd, nEven
				}
			},
		},
		{
			name: "mutual recursion, two predicates in one body",
			rules: `[[(p ?a ?b) [?a :edge ?b]]
			         [(q ?a ?b) [?a :jump ?b]]
			         [(p ?a ?c) (q ?a ?b) (p ?b ?c)]
			         [(q ?a ?c) (p ?a ?b) (q ?b ?c)]]`,
			predicate: "p",
			want: func(fx fixpointFixture) pairSet {
				e, j := fx.pairs(":edge"), fx.pairs(":jump")
				p, q := pairSet{}, pairSet{}
				for {
					np := union(e, compose(q, p))
					nq := union(j, compose(p, q))
					if len(np) == len(p) && len(nq) == len(q) {
						return p
					}
					p, q = np, nq
				}
			},
		},
		{
			name: "same generation",
			rules: `[[(sg ?x ?y) [?p :child ?x] [?p :child ?y]]
			         [(sg ?x ?y) [?a :child ?x] (sg ?a ?b) [?b :child ?y]]]`,
			predicate: "sg",
			want: func(fx fixpointFixture) pairSet {
				c := fx.pairs(":child")
				base := compose(inverse(c), c)
				return naive(func(s pairSet) pairSet { return union(base, compose(compose(inverse(c), s), c)) })
			},
		},
		{
			name: "recursion inside an or-join branch",
			rules: `[[(reach ?a ?b) [?a :edge ?b]]
			         [(reach ?a ?c) [?a :edge ?b] (or-join [?b ?c] (reach ?b ?c) [?b :jump ?c])]]`,
			predicate: "reach",
			want: func(fx fixpointFixture) pairSet {
				e, j := fx.pairs(":edge"), fx.pairs(":jump")
				return naive(func(r pairSet) pairSet { return union(e, compose(e, union(r, j))) })
			},
		},
		{
			name: "negation of a lower recursive predicate",
			rules: `[[(tainted ?x) [?x :bad true]]
			         [(tainted ?y) (tainted ?x) [?x :edge ?y]]
			         [(clean ?a ?b) [?a :edge ?b] (not (tainted ?b))]
			         [(clean ?a ?c) (clean ?a ?b) [?b :edge ?c] (not (tainted ?c))]]`,
			predicate: "clean",
			want: func(fx fixpointFixture) pairSet {
				e := fx.pairs(":edge")
				tainted := fx.marked(":bad")
				for {
					grown := false
					for p := range e {
						if tainted[p[0]] && !tainted[p[1]] {
							tainted[p[1]] = true
							grown = true
						}
					}
					if !grown {
						break
					}
				}
				ok := pairSet{}
				for p := range e {
					if !tainted[p[1]] {
						ok[p] = true
					}
				}
				return naive(func(r pairSet) pairSet { return union(ok, compose(r, ok)) })
			},
		},
		{
			name: "lower recursive predicate at the top of a recursive body",
			rules: `[[(step ?a ?b) [?a :edge ?b]]
			         [(step ?a ?c) (step ?a ?b) [?b :jump ?c]]
			         [(walk ?a ?b) (step ?a ?b)]
			         [(walk ?a ?c) (step ?a ?b) (walk ?b ?c)]]`,
			predicate: "walk",
			want: func(fx fixpointFixture) pairSet {
				e, j := fx.pairs(":edge"), fx.pairs(":jump")
				step := naive(func(s pairSet) pairSet { return union(e, compose(s, j)) })
				return naive(func(w pairSet) pairSet { return union(step, compose(step, w)) })
			},
		},
	}
}

func TestFixpoint_EverySemiNaiveAnswerIsTheNaiveFixpoint(t *testing.T) {
	fx := newFixpointFixture()
	for _, tc := range fixpointCases() {
		for _, mode := range legsFor(optimizerModes, sharingOption, symmetricOption, fusionOption) {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				counts := &ruleEventCounts{}
				var windows atomic.Int64
				handle := func(e annotations.Event) {
					if e.Name == annotations.ScopeWindow {
						windows.Add(1)
					}
					counts.handle(e)
				}
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: fixpointSchema(), AnnotationHandler: handle})
				tx := d.NewTransaction()
				for _, f := range fx.facts {
					require.NoError(t, tx.Add(f.e, kw(f.a), f.v))
				}
				_, err := tx.Commit()
				require.NoError(t, err)
				names := readNames(t, d)
				rules := mustRuleSet(t, tc.rules)
				want := tc.want(fx)
				render := func(s pairSet) string {
					var out []string
					for p := range s {
						out = append(out, names[p[0]]+">"+names[p[1]])
					}
					sort.Strings(out)
					return strings.Join(out, " ")
				}
				check := func(label string, got, want pairSet) {
					t.Helper()
					require.Equal(t, render(want), render(got), "%s", label)
					require.Equal(t, want, got, "%s", label)
				}

				free := fmt.Sprintf(`[:find ?x ?y :in $ %% :where (%s ?x ?y)]`, tc.predicate)
				rel, err := d.Query(free, rules)
				require.NoError(t, err)
				check("free", identityPairsOf(t, rel, 0, 1, nil), want)

				for _, n := range fx.nodes {
					bound := fmt.Sprintf(`[:find ?y :in $ %% ?x :where (%s ?x ?y)]`, tc.predicate)
					rel, err := d.Query(bound, rules, n)
					require.NoError(t, err)
					check("bound first at "+names[n], identityPairsOf(t, rel, -1, 0, &n), restrict(want, &n, nil))

					bound = fmt.Sprintf(`[:find ?x :in $ %% ?y :where (%s ?x ?y)]`, tc.predicate)
					rel, err = d.Query(bound, rules, n)
					require.NoError(t, err)
					check("bound second at "+names[n], identityPairsOf(t, rel, 0, -1, &n), restrict(want, nil, &n))
				}

				windowed := fmt.Sprintf(`[:find ?x ?y :in $ %% :where [?x :name _] (or-join [[?x] ?y] (%s ?x ?y) (and [?x :never ?y]))]`, tc.predicate)
				counts.reset()
				windows.Store(0)
				rel, err = d.Query(windowed, rules)
				require.NoError(t, err)
				check("windowed by its first position", identityPairsOf(t, rel, 0, 1, nil), want)
				require.Greater(t, windows.Load(), int64(1), "the form reads its subject in more than one window; the events:\n%s", counts.events())
				require.True(t, counts.numberedPerWindow(), "the events:\n%s", counts.events())

				twice := fmt.Sprintf(`[:find ?x ?z :in $ %% :where [?x :name _]
				                        (or-join [[?x] ?z]
				                          (and [?x :edge ?y] [?y :edge ?w] (%[1]s ?w ?z))
				                          (and [?x :jump ?y] [?y :edge ?w] (%[1]s ?w ?z)))]`, tc.predicate)
				rel, err = d.Query(twice, rules)
				require.NoError(t, err)
				step := union(fx.pairs(":edge"), fx.pairs(":jump"))
				check("one frame text in two branches", identityPairsOf(t, rel, 0, 1, nil), compose(compose(step, fx.pairs(":edge")), want))

				negated := fmt.Sprintf(`[:find ?x ?y :in $ %% :where [?x :name _] [?y :name _] (not (%s ?x ?y))]`, tc.predicate)
				rel, err = d.Query(negated, rules)
				require.NoError(t, err)
				complement := pairSet{}
				for _, x := range fx.nodes {
					for _, y := range fx.nodes {
						if !want[[2]datalog.Identity{x, y}] {
							complement[[2]datalog.Identity{x, y}] = true
						}
					}
				}
				check("under a not", identityPairsOf(t, rel, 0, 1, nil), complement)
			})
		}
	}
}

// fixpointSchema declares the fixture's attributes, the graph's three
// relations many-valued.
func fixpointSchema() *schema.Schema {
	return schema.NewBuilder().
		Attribute(":edge").Type(schema.TypeRef).Many().Add().
		Attribute(":jump").Type(schema.TypeRef).Many().Add().
		Attribute(":child").Type(schema.TypeRef).Many().Add().
		Attribute(":bad").Type(schema.TypeBoolean).Add().
		Attribute(":never").Type(schema.TypeRef).Add().
		Attribute(":name").Type(schema.TypeString).Add().
		MustBuild()
}

// readNames reads each entity's :name back from the database.
func readNames(t *testing.T, d *Database) map[datalog.Identity]string {
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

// identityPairsOf reads a result into pairs: position x and y of each tuple, a
// position of -1 standing for the fixed identity.
func identityPairsOf(t *testing.T, rel executor.Relation, x, y int, fixed *datalog.Identity) pairSet {
	t.Helper()
	out := pairSet{}
	require.NoError(t, executor.ForEach(rel, func(tuple executor.Tuple) error {
		var p [2]datalog.Identity
		if x < 0 {
			p[0] = *fixed
		} else {
			p[0] = tuple[x].(datalog.Identity)
		}
		if y < 0 {
			p[1] = *fixed
		} else {
			p[1] = tuple[y].(datalog.Identity)
		}
		out[p] = true
		return nil
	}))
	return out
}

func restrict(s pairSet, first, second *datalog.Identity) pairSet {
	out := pairSet{}
	for p := range s {
		if first != nil && p[0] != *first {
			continue
		}
		if second != nil && p[1] != *second {
			continue
		}
		out[p] = true
	}
	return out
}
