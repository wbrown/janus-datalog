package storage

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// The planner's rewrite fixtures with a kept site, a site the rewrite leaves
// under the predicate's own name, each evaluated over the fixed graph and over
// random graphs under every binding shape of the root: free; bound at its
// first position, at its second and at both through the inputs; bound at its
// first through a pattern; under a pattern over every node keyed on either
// position or on both, which takes the windowed route; invoked twice in a
// chain; and negated under a not-join. Each answers a reference computed in
// Go, so every shape agrees with every other whatever copies the rewrite
// emits (docs/proposals/A_PREDICATE_DERIVED_WHOLE_IS_READ_WHOLE_PROOF.md T1,
// T4). Each rule text is the planner test's fixture named beside it.

// keptSiteSchema declares the graph's attributes: the edge attributes as sets
// of refs, the type and the marks as keywords, the name as a string.
func keptSiteSchema() *schema.Schema {
	b := schema.NewBuilder()
	for _, edges := range []string{":edge", ":page/a", ":page/b", ":page/next"} {
		b = b.Attribute(edges).Type(schema.TypeRef).Many().Add()
	}
	for _, mark := range []string{":type", ":page/c", ":page/marked", ":page/kind", ":color"} {
		b = b.Attribute(mark).Type(schema.TypeKeyword).Add()
	}
	return b.Attribute(":name").Type(schema.TypeString).Add().MustBuild()
}

// keptSiteMarks is the nodes each mark attribute is written on.
type keptSiteMarks struct {
	c, marked, kinded, colored map[string]bool
}

// keptSiteGraph is a graph with the marks the fixtures read.
type keptSiteGraph struct {
	spec  graphSpec
	marks keptSiteMarks
}

// fixedKeptSiteGraph is the chain n1 → … → n8 with the edges n8 → n3 and
// n2 → n6, so a node reaches nodes along two paths and around a cycle, with
// :page/c on the even nodes, :page/marked on the odd ones, :page/kind on the
// first four and :color on three.
func fixedKeptSiteGraph() keptSiteGraph {
	spec := chain(8)
	spec.edges = append(spec.edges, [2]string{"n8", "n3"}, [2]string{"n2", "n6"})
	return keptSiteGraph{spec: spec, marks: keptSiteMarks{
		c:       map[string]bool{"n2": true, "n4": true, "n6": true, "n8": true},
		marked:  map[string]bool{"n1": true, "n3": true, "n5": true, "n7": true},
		kinded:  map[string]bool{"n1": true, "n2": true, "n3": true, "n4": true},
		colored: map[string]bool{"n1": true, "n4": true, "n7": true},
	}}
}

// randomKeptSiteGraph draws a graph from seed (randomGraph) and each mark on
// each node by a coin, from a source other than the graph's.
func randomKeptSiteGraph(seed int64) keptSiteGraph {
	spec := randomGraph(seed)
	r := rand.New(rand.NewSource(^seed))
	marks := keptSiteMarks{c: map[string]bool{}, marked: map[string]bool{}, kinded: map[string]bool{}, colored: map[string]bool{}}
	for _, node := range spec.nodes {
		for _, mark := range []map[string]bool{marks.c, marks.marked, marks.kinded, marks.colored} {
			if r.Intn(2) == 0 {
				mark[node] = true
			}
		}
	}
	return keptSiteGraph{spec: spec, marks: marks}
}

// writeKeptSiteAttributes commits the attributes the fixtures read beside
// :type, :edge and :name: every edge again under :page/a, :page/b and
// :page/next, and each mark on its nodes.
func writeKeptSiteAttributes(t testing.TB, d *Database, g keptSiteGraph) {
	t.Helper()
	tx := d.NewTransaction()
	for _, edge := range g.spec.edges {
		for _, attribute := range []string{":page/a", ":page/b", ":page/next"} {
			require.NoError(t, tx.Add(nodeID(edge[0]), kw(attribute), nodeID(edge[1])))
		}
	}
	for attribute, nodes := range map[string]map[string]bool{":page/c": g.marks.c, ":page/marked": g.marks.marked, ":page/kind": g.marks.kinded, ":color": g.marks.colored} {
		for node := range nodes {
			require.NoError(t, tx.Add(nodeID(node), kw(attribute), kw(":mark")))
		}
	}
	_, err := tx.Commit()
	require.NoError(t, err)
}

// edgesOf is spec's edges as a set of pairs.
func edgesOf(spec graphSpec) map[[2]string]bool {
	edges := make(map[[2]string]bool, len(spec.edges))
	for _, edge := range spec.edges {
		edges[edge] = true
	}
	return edges
}

// joinedOf is the pairs a, c with a, b in first and b, c in second.
func joinedOf(first, second map[[2]string]bool) map[[2]string]bool {
	out := map[[2]string]bool{}
	for ab := range first {
		for bc := range second {
			if ab[1] == bc[0] {
				out[[2]string{ab[0], bc[1]}] = true
			}
		}
	}
	return out
}

// negatedSiteReference is q of the negated-site fixture by naive iteration,
// :page/a and :page/b both being the graph's edges: a pair an edge joins; a
// pair x, z with an edge x → y, q(y, w), w under :page/c and an edge w → z;
// and a pair x, z with an edge x → w, w not under :page/c and an edge w → z.
func negatedSiteReference(g keptSiteGraph) map[[2]string]bool {
	q := edgesOf(g.spec)
	for grew := true; grew; {
		grew = false
		for _, xy := range g.spec.edges {
			for _, wz := range g.spec.edges {
				w := wz[0]
				through := q[[2]string{xy[1], w}] && g.marks.c[w]
				around := xy[1] == w && !g.marks.c[w]
				if (through || around) && !q[[2]string{xy[0], wz[1]}] {
					q[[2]string{xy[0], wz[1]}] = true
					grew = true
				}
			}
		}
	}
	return q
}

// fallbackSiteReference is walk of the fallback-site fixture by naive
// iteration over :page/next, the graph's edges: a pair an edge joins; a pair
// a, c with an edge a → b, walk(b, c) and c under :page/marked; and a pair a,
// c with an edge a → b, b under :page/marked or :page/kind, and walk(b, c).
func fallbackSiteReference(g keptSiteGraph) map[[2]string]bool {
	walk := edgesOf(g.spec)
	for grew := true; grew; {
		grew = false
		for _, ab := range g.spec.edges {
			b := ab[1]
			for _, c := range g.spec.nodes {
				if !walk[[2]string{b, c}] || walk[[2]string{ab[0], c}] {
					continue
				}
				if g.marks.marked[c] || g.marks.marked[b] || g.marks.kinded[b] {
					walk[[2]string{ab[0], c}] = true
					grew = true
				}
			}
		}
	}
	return walk
}

// pathsOfTwoOrMore is every pair a path of at least two edges joins, the
// closure joined with an edge, which p3 of the free-site fixture derives: p0
// and p2 are the closure, and p3 reads a p2 pair and an edge after it under a
// count that holds for every such pair.
func pathsOfTwoOrMore(g keptSiteGraph) map[[2]string]bool {
	return joinedOf(closureOf(g.spec), edgesOf(g.spec))
}

// keptSiteFixture is one planner fixture: its rule set, its root, and the
// reference the root derives over a graph.
type keptSiteFixture struct {
	name, rules, root string
	reference         func(g keptSiteGraph) map[[2]string]bool
}

var keptSiteFixtures = []keptSiteFixture{
	// TestMagic_ANegatedSiteWhoseAdornmentWouldNotStratifyIsLeftUnadorned
	{"a negated site left unadorned",
		`[[(q ?x ?z) [?x :page/a ?z]]
		  [(q ?x ?z) [?x :page/a ?y] (q ?y ?w) (s ?w) [?w :page/b ?z]]
		  [(q ?x ?z) [?x :page/a ?w] (not (s ?w)) [?w :page/b ?z]]
		  [(s ?w) [?w :page/c _]]]`,
		"q", negatedSiteReference},
	// TestMagic_AFallbackSiteWhoseAdornmentWouldNotStratifyIsLeftUnadorned
	{"a fallback site left unadorned",
		`[[(marked ?b) [?b :page/marked _]]
		  [(walk ?a ?c) [?a :page/next ?c]]
		  [(walk ?a ?c) [?a :page/next ?b] (walk ?b ?c) (marked ?c)]
		  [(walk ?a ?c) [?a :page/next ?b] (or-default-join [[?b]] (marked ?b) [?b :page/kind _]) (walk ?b ?c)]]`,
		"walk", fallbackSiteReference},
	// TestMagic_AFreeSiteUnderAFallbackWhoseAllFreeNameWouldNotStratifyKeepsTheOriginalRules
	{"a free site under a fallback keeping the original rules",
		`[[(nb [?x] ?y) [?x :edge ?y]]
		  [(p0 ?a ?b) [?a :edge ?b]]
		  [(p0 ?a ?b) [?a :edge ?m] (p0 ?m ?b)]
		  [(p0 ?a ?b) [?a :color _] (or-join [[?a] ?b] (nb ?a ?b) [?a :edge ?b])]
		  [(p2 ?a ?b) [?a :edge ?b]]
		  [(p2 ?a ?b) [?a :edge ?m] (p0 ?m ?b)]
		  [(p2 ?a ?b) (p2 ?a ?b) (or-default-join [[?b] ?k] (and (p0 ?b ?z) [(ground 1) ?k]) [(ground 0) ?k]) [(= ?k 1)]]
		  [(p3 ?a ?b) (p2 ?a ?m) [?m :edge ?b] [(q [:find (count ?z) :in $ % ?x :where [?x :edge ?w] (p0 ?w ?z)] $ % ?a) [[?n]]] [(> ?n 0)]]]`,
		"p3", pathsOfTwoOrMore},
	// TestMagic_AnAggregatingSubqueryBeforeTheRecursionIsNotRead
	{"an aggregating subquery before the recursion",
		`[[(low ?a ?b) [?a :edge ?b]]
		  [(p ?a ?b) [?a :edge ?b]]
		  [(p ?a ?c) [?a :edge _] [(q [:find ?w (count ?z) :in $ % ?x :where (low ?x ?w) (low ?w ?z)] $ % ?a) [[?v ?n]]] [(identity ?v) ?m] (p ?m ?c)]]`,
		"p", func(g keptSiteGraph) map[[2]string]bool { return closureOf(g.spec) }},
	// TestMagic_ASubqueryWhoseReadNoLongerBindsItsFindIsNotRead
	{"a subquery whose read no longer binds its find",
		`[[(low ?a ?b) [?a :edge ?b]]
		  [(p ?a ?b) [?a :edge ?b]]
		  [(p ?a ?c) [?a :type :node] [(q [:find ?w :in $ % ?x :where [(q [:find ?u (count ?z) :in $ % ?y :where (low ?y ?u) (low ?u ?z)] $ % ?x) [[?w ?n]]]] $ % ?a) [[?m]]] (p ?m ?c)]]`,
		"p", func(g keptSiteGraph) map[[2]string]bool { return closureOf(g.spec) }},
}

// keptSiteTrials is the random graphs each fixture is evaluated over, beside
// the fixed one.
const keptSiteTrials = 25

func TestRules_TheKeptSiteFixturesAnswerTheReferenceUnderEveryBinding(t *testing.T) {
	graphs := []keptSiteGraph{fixedKeptSiteGraph()}
	for trial := int64(0); trial < keptSiteTrials; trial++ {
		graphs = append(graphs, randomKeptSiteGraph(trial))
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			witnessed := map[string]int{}
			for i, kg := range graphs {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: keptSiteSchema()})
				g := writeGraph(t, d, kg.spec)
				writeNames(t, d, kg.spec)
				writeKeptSiteAttributes(t, d, kg)
				for _, fixture := range keptSiteFixtures {
					want := fixture.reference(kg)
					if len(want) > 0 {
						witnessed[fixture.name]++
					}
					rules := mustRuleSet(t, fixture.rules)
					root := fixture.root
					label := fmt.Sprintf("graph %d, %s, edges %v, marks %+v", i, fixture.name, kg.spec.edges, kg.marks)

					rel, err := d.Query(`[:find ?a ?b :in $ % :where (`+root+` ?a ?b)]`, rules)
					require.NoError(t, err, label)
					require.ElementsMatch(t, pairsRendered(want), g.tuples(t, rel), "%s: free", label)

					var pairs []interface{}
					var unreached []string
					for _, x := range kg.spec.nodes {
						if len(reachedFrom(want, x)) == 0 {
							unreached = append(unreached, x)
						}
						rel, err := d.Query(`[:find ?b :in $ % ?a :where (`+root+` ?a ?b)]`, rules, nodeID(x))
						require.NoError(t, err, label)
						require.ElementsMatch(t, reachedFrom(want, x), g.tuples(t, rel), "%s: bound first to %s", label, x)
						rel, err = d.Query(`[:find ?a :in $ % ?b :where (`+root+` ?a ?b)]`, rules, nodeID(x))
						require.NoError(t, err, label)
						require.ElementsMatch(t, reaching(want, x), g.tuples(t, rel), "%s: bound second to %s", label, x)
						rel, err = d.Query(`[:find ?b :in $ % ?n :where [?a :name ?n] (`+root+` ?a ?b)]`, rules, x)
						require.NoError(t, err, label)
						require.ElementsMatch(t, reachedFrom(want, x), g.tuples(t, rel), "%s: bound first through a pattern to %s", label, x)
						for _, y := range kg.spec.nodes {
							pairs = append(pairs, []interface{}{nodeID(x), nodeID(y)})
							rel, err = d.Query(`[:find ?a ?b :in $ % ?a ?b :where (`+root+` ?a ?b)]`, rules, nodeID(x), nodeID(y))
							require.NoError(t, err, label)
							var held []string
							if want[[2]string{x, y}] {
								held = []string{x + ">" + y}
							}
							require.ElementsMatch(t, held, g.tuples(t, rel), "%s: bound at both to %s, %s", label, x, y)
						}
					}

					rel, err = d.Query(`[:find ?a ?b :in $ % [[?a ?b]] :where (`+root+` ?a ?b)]`, rules, pairs)
					require.NoError(t, err, label)
					require.ElementsMatch(t, pairsRendered(want), g.tuples(t, rel), "%s: bound at both through a relation input of every pair", label)
					rel, err = d.Query(`[:find ?x ?z :in $ % :where [?x :type :node] (`+root+` ?x ?z)]`, rules)
					require.NoError(t, err, label)
					require.ElementsMatch(t, pairsRendered(want), g.tuples(t, rel), "%s: under a pattern keyed on the first position", label)
					rel, err = d.Query(`[:find ?x ?z :in $ % :where [?z :type :node] (`+root+` ?x ?z)]`, rules)
					require.NoError(t, err, label)
					require.ElementsMatch(t, pairsRendered(want), g.tuples(t, rel), "%s: under a pattern keyed on the second position", label)
					rel, err = d.Query(`[:find ?x ?z :in $ % :where [?x :type :node] [?z :type :node] (`+root+` ?x ?z)]`, rules)
					require.NoError(t, err, label)
					require.ElementsMatch(t, pairsRendered(want), g.tuples(t, rel), "%s: under two patterns keyed on both positions", label)
					rel, err = d.Query(`[:find ?a ?c :in $ % :where (`+root+` ?a ?b) (`+root+` ?b ?c)]`, rules)
					require.NoError(t, err, label)
					require.ElementsMatch(t, pairsRendered(joinedOf(want, want)), g.tuples(t, rel), "%s: invoked twice in a chain", label)
					rel, err = d.Query(`[:find ?x :in $ % :where [?x :type :node] (not-join [?x] (`+root+` ?x _))]`, rules)
					require.NoError(t, err, label)
					require.ElementsMatch(t, unreached, g.tuples(t, rel), "%s: negated under a not-join over every node", label)
				}
			}
			for _, fixture := range keptSiteFixtures {
				require.NotZero(t, witnessed[fixture.name], "%s derives pairs on some graph for the contract to be testable", fixture.name)
			}
		})
	}
}
