package storage

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// The rule evaluator against a reference it does not compute: the closure of
// a random graph by Warshall's algorithm in Go, and the odd and even
// reachability of the same graph by a walk over node and parity. Each trial
// draws its graph from the trial's own seed, so every leg of the matrix sees
// the same graphs.

// graphSchema declares a random graph's attributes: :edge a set of refs,
// :type a keyword, :name a string.
func graphSchema() *schema.Schema {
	return schema.NewBuilder().
		Attribute(":edge").Type(schema.TypeRef).Many().Add().
		Attribute(":type").Type(schema.TypeKeyword).Add().
		Attribute(":name").Type(schema.TypeString).Add().
		MustBuild()
}

// randomGraph draws a graph from seed: three to eight nodes, and between n
// and 3n draws of a directed edge, each kept once.
func randomGraph(seed int64) graphSpec {
	r := rand.New(rand.NewSource(seed))
	n := 3 + r.Intn(6)
	var spec graphSpec
	for i := 0; i < n; i++ {
		spec.nodes = append(spec.nodes, nodeName(i))
	}
	seen := map[[2]string]bool{}
	for k := 0; k < n+r.Intn(2*n); k++ {
		edge := [2]string{spec.nodes[r.Intn(n)], spec.nodes[r.Intn(n)]}
		if seen[edge] {
			continue
		}
		seen[edge] = true
		spec.edges = append(spec.edges, edge)
	}
	return spec
}

// closureOf is spec's transitive closure by Warshall's algorithm.
func closureOf(spec graphSpec) map[[2]string]bool {
	reach := map[[2]string]bool{}
	for _, edge := range spec.edges {
		reach[edge] = true
	}
	for _, k := range spec.nodes {
		for _, i := range spec.nodes {
			for _, j := range spec.nodes {
				if reach[[2]string{i, k}] && reach[[2]string{k, j}] {
					reach[[2]string{i, j}] = true
				}
			}
		}
	}
	return reach
}

// parityClosureOf is spec's reachability by path length: odd holds a pair a
// path of odd length joins, even a pair a path of even length of at least two
// joins, found by walking states of node and parity from each node.
func parityClosureOf(spec graphSpec) (odd, even map[[2]string]bool) {
	next := map[string][]string{}
	for _, edge := range spec.edges {
		next[edge[0]] = append(next[edge[0]], edge[1])
	}
	odd, even = map[[2]string]bool{}, map[[2]string]bool{}
	type state struct {
		node string
		odd  bool
	}
	for _, start := range spec.nodes {
		seen := map[state]bool{}
		var frontier []state
		for _, to := range next[start] {
			frontier = append(frontier, state{to, true})
		}
		for len(frontier) > 0 {
			s := frontier[0]
			frontier = frontier[1:]
			if seen[s] {
				continue
			}
			seen[s] = true
			if s.odd {
				odd[[2]string{start, s.node}] = true
			} else {
				even[[2]string{start, s.node}] = true
			}
			for _, to := range next[s.node] {
				frontier = append(frontier, state{to, !s.odd})
			}
		}
	}
	return odd, even
}

// complementOf is every ordered pair of spec's nodes that pairs does not hold.
func complementOf(spec graphSpec, pairs map[[2]string]bool) map[[2]string]bool {
	out := map[[2]string]bool{}
	for _, a := range spec.nodes {
		for _, b := range spec.nodes {
			if !pairs[[2]string{a, b}] {
				out[[2]string{a, b}] = true
			}
		}
	}
	return out
}

// pairsRendered renders pairs as the graph renders a two-column result.
func pairsRendered(pairs map[[2]string]bool) []string {
	var out []string
	for pair := range pairs {
		out = append(out, pair[0]+">"+pair[1])
	}
	return out
}

// reachedFrom is the second component of each pair whose first is x.
func reachedFrom(pairs map[[2]string]bool, x string) []string {
	var out []string
	for pair := range pairs {
		if pair[0] == x {
			out = append(out, pair[1])
		}
	}
	return out
}

// reaching is the first component of each pair whose second is y.
func reaching(pairs map[[2]string]bool, y string) []string {
	var out []string
	for pair := range pairs {
		if pair[1] == y {
			out = append(out, pair[0])
		}
	}
	return out
}

// writeNames records each node's name under :name, which a query binding a
// node through a pattern selects it by.
func writeNames(t testing.TB, d *Database, spec graphSpec) {
	t.Helper()
	tx := d.NewTransaction()
	for _, name := range spec.nodes {
		require.NoError(t, tx.Add(nodeID(name), kw(":name"), name))
	}
	_, err := tx.Commit()
	require.NoError(t, err)
}

const edgeClosure = `[(e ?a ?b) [?a :edge ?b]] [(e ?a ?c) [?a :edge ?b] (e ?b ?c)]`

// closureForms are rule sets each deriving r: the closure of :edge, or, where
// complement holds, its complement over the nodes.
var closureForms = []struct {
	name, rules string
	complement  bool
}{
	{"linear", `[[(r ?a ?b) [?a :edge ?b]] [(r ?a ?c) [?a :edge ?b] (r ?b ?c)]]`, false},
	{"nonlinear", `[[(r ?a ?b) [?a :edge ?b]] [(r ?a ?c) (r ?a ?b) (r ?b ?c)]]`, false},
	{"left-linear", `[[(r ?a ?b) [?a :edge ?b]] [(r ?a ?c) (r ?a ?b) [?b :edge ?c]]]`, false},
	{"or", `[[(r ?a ?b) [?a :edge ?b]] [(r ?a ?c) (or (and [?a :edge ?b] (r ?b ?c)) (and (r ?a ?b) (r ?b ?c)))]]`, false},
	{"mutual", `[[(r ?a ?b) [?a :edge ?b]] [(r ?a ?c) (s ?a ?b) (r ?b ?c)] [(s ?a ?b) (r ?a ?b)]]`, false},
	{"or-join", `[[(r ?a ?c) (or-join [?a ?c] [?a :edge ?c] (and [?a :edge ?b] (r ?b ?c)))]]`, false},
	{"not", `[` + edgeClosure + ` [(r ?a ?b) [?a :type :node] [?b :type :node] (not (e ?a ?b))]]`, true},
	{"not-join", `[` + edgeClosure + ` [(r ?a ?b) [?a :type :node] [?b :type :node] (not-join [?a ?b] (e ?a ?b))]]`, true},
	{"or-default", `[` + edgeClosure + ` [(r ?a ?b) [?a :type :node] [?b :type :node] (or-default (and (e ?a ?b) [(ground 1) ?k]) [(ground 0) ?k]) [(= ?k 0)]]]`, true},
	{"subquery", `[` + edgeClosure + ` [(r ?a ?b) [?a :type :node] [?b :type :node] (or-default [(q [:find (count ?z) :in $ % ?a ?b :where (e ?a ?b) [(ground 1) ?z]] $ % ?a ?b) [[?n]]] [(ground 0) ?n]) [(= ?n 0)]]]`, true},
}

// Every form of the closure, and of its complement, derives on a random graph
// what the reference computes: whole, bound at either position through an
// input, bound at the first position through a pattern, and under a
// negation.
func TestRules_ClosureFormsOnRandomGraphsMatchTheReference(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			for trial := 0; trial < 25; trial++ {
				spec := randomGraph(int64(trial))
				closure := closureOf(spec)
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: graphSchema()})
				g := writeGraph(t, d, spec)
				writeNames(t, d, spec)
				for _, form := range closureForms {
					want := closure
					if form.complement {
						want = complementOf(spec, closure)
					}
					rs := mustRuleSet(t, form.rules)
					label := fmt.Sprintf("trial %d, %s, edges %v", trial, form.name, spec.edges)
					rel, err := d.Query(`[:find ?x ?y :in $ % :where (r ?x ?y)]`, rs)
					require.NoError(t, err, label)
					require.ElementsMatch(t, pairsRendered(want), g.tuples(t, rel), "%s: whole", label)
					for _, x := range spec.nodes {
						rel, err := d.Query(`[:find ?y :in $ % ?x :where (r ?x ?y)]`, rs, nodeID(x))
						require.NoError(t, err, label)
						require.ElementsMatch(t, reachedFrom(want, x), g.tuples(t, rel), "%s: bound first to %s", label, x)
						rel, err = d.Query(`[:find ?x :in $ % ?y :where (r ?x ?y)]`, rs, nodeID(x))
						require.NoError(t, err, label)
						require.ElementsMatch(t, reaching(want, x), g.tuples(t, rel), "%s: bound second to %s", label, x)
						rel, err = d.Query(`[:find ?y :in $ % ?n :where [?x :name ?n] (r ?x ?y)]`, rs, x)
						require.NoError(t, err, label)
						require.ElementsMatch(t, reachedFrom(want, x), g.tuples(t, rel), "%s: bound first through a pattern to %s", label, x)
						rel, err = d.Query(`[:find ?y :in $ % ?x :where [?y :type :node] (not (r ?x ?y))]`, rs, nodeID(x))
						require.NoError(t, err, label)
						require.ElementsMatch(t, reachedFrom(complementOf(spec, want), x), g.tuples(t, rel), "%s: negated from %s", label, x)
					}
				}
			}
		})
	}
}

// Mutual recursion by parity derives on a random graph what the reference
// computes: odd, the pairs a path of odd length joins, and even, the pairs a
// path of even length joins, whole and bound at the first position.
func TestRules_ParityRecursionOnRandomGraphsMatchesTheReference(t *testing.T) {
	const rules = `[[(odd ?a ?b) [?a :edge ?b]]
	                [(odd ?a ?c) [?a :edge ?b] (even ?b ?c)]
	                [(even ?a ?c) [?a :edge ?b] (odd ?b ?c)]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			for trial := 0; trial < 25; trial++ {
				spec := randomGraph(int64(trial))
				odd, even := parityClosureOf(spec)
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: graphSchema()})
				g := writeGraph(t, d, spec)
				rs := mustRuleSet(t, rules)
				label := fmt.Sprintf("trial %d, edges %v", trial, spec.edges)
				for _, parity := range []struct {
					name string
					want map[[2]string]bool
				}{{"odd", odd}, {"even", even}} {
					rel, err := d.Query(fmt.Sprintf(`[:find ?x ?y :in $ %% :where (%s ?x ?y)]`, parity.name), rs)
					require.NoError(t, err, label)
					require.ElementsMatch(t, pairsRendered(parity.want), g.tuples(t, rel), "%s: %s whole", label, parity.name)
					for _, x := range spec.nodes {
						rel, err := d.Query(fmt.Sprintf(`[:find ?y :in $ %% ?x :where (%s ?x ?y)]`, parity.name), rs, nodeID(x))
						require.NoError(t, err, label)
						require.ElementsMatch(t, reachedFrom(parity.want, x), g.tuples(t, rel), "%s: %s from %s", label, parity.name, x)
					}
				}
			}
		})
	}
}

// Queries run concurrently over one database derive what the same queries
// derive one at a time: sixteen goroutines each run the closure and its
// complement, whole and bound, twenty times, and every result is the serial
// one.
func TestRules_ConcurrentQueriesDeriveWhatSerialQueriesDerive(t *testing.T) {
	const rules = `[[(r ?a ?b) [?a :edge ?b]]
	                [(r ?a ?c) [?a :edge ?b] (r ?b ?c)]
	                [(u ?a ?b) [?a :type :node] [?b :type :node] (not (r ?a ?b))]]`
	var spec graphSpec
	for i := 0; i < 40; i++ {
		spec.nodes = append(spec.nodes, nodeName(i))
	}
	for i := 0; i < 40; i++ {
		spec.edges = append(spec.edges, [2]string{nodeName(i), nodeName((i*7 + 3) % 40)})
		if i%3 == 0 {
			spec.edges = append(spec.edges, [2]string{nodeName(i), nodeName((i + 1) % 40)})
		}
	}
	queries := []struct {
		text   string
		inputs []interface{}
	}{
		{`[:find ?a ?b :in $ % :where (r ?a ?b)]`, nil},
		{`[:find ?b :in $ % ?a :where (r ?a ?b)]`, []interface{}{nodeID(nodeName(5))}},
		{`[:find ?a ?b :in $ % :where (u ?a ?b)]`, nil},
		{`[:find ?b :in $ % ?a :where (u ?a ?b)]`, []interface{}{nodeID(nodeName(5))}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: graphSchema()})
			g := writeGraph(t, d, spec)
			rs := mustRuleSet(t, rules)
			serial := make([][]string, len(queries))
			for i, q := range queries {
				rel, err := d.Query(q.text, append([]interface{}{rs}, q.inputs...)...)
				require.NoError(t, err)
				serial[i] = g.tuples(t, rel)
				sort.Strings(serial[i])
				require.NotEmpty(t, serial[i], "query %d derives something serially", i)
			}

			var wg sync.WaitGroup
			var mu sync.Mutex
			var disagreements []string
			for w := 0; w < 16; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for k := 0; k < 20; k++ {
						i := (w + k) % len(queries)
						rel, err := d.Query(queries[i].text, append([]interface{}{rs}, queries[i].inputs...)...)
						var got []string
						if err == nil {
							got, err = g.render(rel)
						}
						if err != nil {
							mu.Lock()
							disagreements = append(disagreements, fmt.Sprintf("worker %d run %d query %d: %v", w, k, i, err))
							mu.Unlock()
							continue
						}
						sort.Strings(got)
						if strings.Join(got, " ") != strings.Join(serial[i], " ") {
							mu.Lock()
							disagreements = append(disagreements, fmt.Sprintf("worker %d run %d query %d: %d tuples, serial %d", w, k, i, len(got), len(serial[i])))
							mu.Unlock()
						}
					}
				}(w)
			}
			wg.Wait()
			require.Empty(t, disagreements)
		})
	}
}

// intGraph is a random graph whose nodes carry integer ids, a colour and a
// seed mark, and whose edges are entities carrying their endpoints' ids, with
// its closure by Warshall's algorithm.
type intGraph struct {
	nodes, seeds, reds []int64
	edge, reach        map[[2]int64]bool
}

// randomIntGraph draws an intGraph from seed: n nodes, a third of them red
// and a quarter seeds, and m edges.
func randomIntGraph(seed int64, n, m int) intGraph {
	r := rand.New(rand.NewSource(seed))
	g := intGraph{edge: map[[2]int64]bool{}, reach: map[[2]int64]bool{}}
	for i := 0; i < n; i++ {
		g.nodes = append(g.nodes, int64(i))
		if r.Intn(3) == 0 {
			g.reds = append(g.reds, int64(i))
		}
		if r.Intn(4) == 0 {
			g.seeds = append(g.seeds, int64(i))
		}
	}
	for j := 0; j < m; j++ {
		g.edge[[2]int64{int64(r.Intn(n)), int64(r.Intn(n))}] = true
	}
	for pair := range g.edge {
		g.reach[pair] = true
	}
	for _, k := range g.nodes {
		for _, i := range g.nodes {
			for _, j := range g.nodes {
				if g.reach[[2]int64{i, k}] && g.reach[[2]int64{k, j}] {
					g.reach[[2]int64{i, j}] = true
				}
			}
		}
	}
	return g
}

// writeIntGraph commits g into d: each node under :node/id, :node/color and,
// for a seed, :node/seed, and each edge as an entity under :edge/from and
// :edge/to.
func writeIntGraph(t testing.TB, d *Database, g intGraph) {
	t.Helper()
	reds := map[int64]bool{}
	for _, n := range g.reds {
		reds[n] = true
	}
	seeds := map[int64]bool{}
	for _, n := range g.seeds {
		seeds[n] = true
	}
	tx := d.NewTransaction()
	for _, n := range g.nodes {
		e := datalog.NewIdentity(fmt.Sprintf("node%d", n))
		require.NoError(t, tx.Add(e, kw(":node/id"), n))
		colour := "blue"
		if reds[n] {
			colour = "red"
		}
		require.NoError(t, tx.Add(e, kw(":node/color"), colour))
		if seeds[n] {
			require.NoError(t, tx.Add(e, kw(":node/seed"), true))
		}
	}
	i := 0
	for pair := range g.edge {
		e := datalog.NewIdentity(fmt.Sprintf("edge%d", i))
		i++
		require.NoError(t, tx.Add(e, kw(":edge/from"), pair[0]))
		require.NoError(t, tx.Add(e, kw(":edge/to"), pair[1]))
	}
	_, err := tx.Commit()
	require.NoError(t, err)
}

// Every shape a query invokes the closure in derives on a random graph what
// the reference computes: bound through a pattern at either position, under
// a negation with a repeated variable, with a repeated variable, with a
// constant at either position and as a verdict, inside an or, bound by two
// patterns, bound by an input beside a pattern, through a collection input,
// under a not-join, free beside bound, inside a subquery, and inside an
// or-join.
func TestRules_QueryLevelShapesOnRandomGraphsMatchTheReference(t *testing.T) {
	const rules = `[[(e ?a ?b) [?x :edge/from ?a] [?x :edge/to ?b]]
	                [(reach ?a ?b) (e ?a ?b)]
	                [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)]]`
	pair := func(a, b int64) string { return fmt.Sprintf("%d>%d", a, b) }
	one := func(a int64) string { return fmt.Sprintf("%d", a) }
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			for seed := int64(1); seed <= 6; seed++ {
				g := randomIntGraph(seed, 12, 18)
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				writeIntGraph(t, d, g)
				rs := mustRuleSet(t, rules)
				reds := map[int64]bool{}
				for _, n := range g.reds {
					reds[n] = true
				}

				shapes := []struct {
					label, text string
					inputs      []interface{}
					want        []string
				}{}
				add := func(label, text string, inputs []interface{}, want []string) {
					shapes = append(shapes, struct {
						label, text string
						inputs      []interface{}
						want        []string
					}{label, text, inputs, want})
				}

				var want []string
				for p := range g.reach {
					want = append(want, pair(p[0], p[1]))
				}
				add("whole", `[:find ?a ?b :in $ % :where (reach ?a ?b)]`, nil, want)

				want = nil
				for _, s := range g.seeds {
					for p := range g.reach {
						if p[0] == s {
							want = append(want, pair(s, p[1]))
						}
					}
				}
				add("bound first through a pattern", `[:find ?s ?y :in $ % :where [?n :node/seed true] [?n :node/id ?s] (reach ?s ?y)]`, nil, want)

				want = nil
				for _, s := range g.seeds {
					for p := range g.reach {
						if p[1] == s {
							want = append(want, pair(p[0], s))
						}
					}
				}
				add("bound second through a pattern", `[:find ?y ?s :in $ % :where [?n :node/seed true] [?n :node/id ?s] (reach ?y ?s)]`, nil, want)

				var onCycle, offCycle []string
				for _, n := range g.nodes {
					if g.reach[[2]int64{n, n}] {
						onCycle = append(onCycle, one(n))
					} else {
						offCycle = append(offCycle, one(n))
					}
				}
				add("negated with a repeated variable", `[:find ?s :in $ % :where [?n :node/id ?s] (not (reach ?s ?s))]`, nil, offCycle)
				add("repeated variable", `[:find ?s :in $ % :where [?n :node/id ?s] (reach ?s ?s)]`, nil, onCycle)
				add("repeated variable unbound", `[:find ?s :in $ % :where (reach ?s ?s)]`, nil, onCycle)

				want = nil
				for p := range g.reach {
					if p[0] == 3 {
						want = append(want, one(p[1]))
					}
				}
				add("constant first", `[:find ?y :in $ % :where (reach 3 ?y)]`, nil, want)
				want = nil
				for p := range g.reach {
					if p[1] == 3 {
						want = append(want, one(p[0]))
					}
				}
				add("constant second", `[:find ?y :in $ % :where (reach ?y 3)]`, nil, want)
				want = nil
				if g.reach[[2]int64{3, 5}] {
					for _, n := range g.nodes {
						want = append(want, one(n))
					}
				}
				add("verdict", `[:find ?s :in $ % :where [?n :node/id ?s] (reach 3 5)]`, nil, want)

				set := map[string]bool{}
				for _, s := range g.seeds {
					for p := range g.reach {
						if p[0] == s {
							set[pair(s, p[1])] = true
						}
					}
					for p := range g.edge {
						if p[1] == s {
							set[pair(s, p[0])] = true
						}
					}
				}
				want = nil
				for k := range set {
					want = append(want, k)
				}
				add("inside an or", `[:find ?s ?y :in $ % :where [?n :node/seed true] [?n :node/id ?s] (or (reach ?s ?y) (e ?y ?s))]`, nil, want)

				want = nil
				for _, s := range g.seeds {
					for _, r := range g.reds {
						if g.reach[[2]int64{s, r}] {
							want = append(want, pair(s, r))
						}
					}
				}
				add("bound by two patterns", `[:find ?s ?t :in $ % :where [?n :node/seed true] [?n :node/id ?s] [?m :node/color "red"] [?m :node/id ?t] (reach ?s ?t)]`, nil, want)

				for _, x := range []int64{0, 3, 7} {
					want = nil
					for _, r := range g.reds {
						if g.reach[[2]int64{x, r}] {
							want = append(want, one(r))
						}
					}
					add(fmt.Sprintf("bound by an input beside a pattern, from %d", x), `[:find ?t :in $ % ?s :where [?m :node/color "red"] [?m :node/id ?t] (reach ?s ?t)]`, []interface{}{x}, want)
				}

				want = nil
				for p := range g.reach {
					if p[0] == 1 || p[0] == 4 || p[0] == 9 {
						want = append(want, pair(p[0], p[1]))
					}
				}
				add("through a collection input", `[:find ?s ?y :in $ % [?s ...] :where (reach ?s ?y)]`, []interface{}{[]interface{}{int64(1), int64(4), int64(9)}}, want)

				want = nil
				for _, s := range g.seeds {
					reachesRed := false
					for _, r := range g.reds {
						if g.reach[[2]int64{s, r}] {
							reachesRed = true
						}
					}
					if !reachesRed {
						want = append(want, one(s))
					}
				}
				add("under a not-join", `[:find ?s :in $ % :where [?n :node/seed true] [?n :node/id ?s] (not-join [?s] [?m :node/color "red"] [?m :node/id ?t] (reach ?s ?t))]`, nil, want)

				set = map[string]bool{}
				for p := range g.reach {
					for q := range g.reach {
						if p[1] == q[0] {
							set[fmt.Sprintf("%d>%d>%d", p[0], p[1], q[1])] = true
						}
					}
				}
				want = nil
				for k := range set {
					want = append(want, k)
				}
				add("free beside bound", `[:find ?a ?b ?c :in $ % :where (reach ?a ?b) (reach ?b ?c)]`, nil, want)

				want = nil
				for _, s := range g.seeds {
					n := 0
					for p := range g.reach {
						if p[0] == s {
							n++
						}
					}
					if n > 0 {
						want = append(want, fmt.Sprintf("%d>%d", s, n))
					}
				}
				add("inside a subquery", `[:find ?s ?k :in $ % :where [?n :node/seed true] [?n :node/id ?s] [(q [:find (count ?y) :in $ % ?s :where (reach ?s ?y)] $ % ?s) [[?k]]]]`, nil, want)

				want = nil
				for _, s := range g.seeds {
					for p := range g.reach {
						if p[0] == s {
							want = append(want, one(s))
							break
						}
					}
				}
				add("inside an or-join", `[:find ?s :in $ % :where [?n :node/seed true] [?n :node/id ?s] (or-join [?s] (reach ?s _) (and (e ?s ?z) (reach ?z _)))]`, nil, want)

				for _, shape := range shapes {
					rel, err := d.Query(shape.text, append([]interface{}{rs}, shape.inputs...)...)
					require.NoError(t, err, "seed %d: %s", seed, shape.label)
					require.ElementsMatch(t, shape.want, graph{}.tuples(t, rel), "seed %d: %s", seed, shape.label)
				}
			}
		})
	}
}
