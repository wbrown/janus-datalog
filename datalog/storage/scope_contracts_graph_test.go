package storage

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// The contracts of docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §7 over
// the two recorded graphs in testdata, scope_review_f4.txt of two hundred
// entities and scope_review_f5.txt of three thousand: streaming subjects of
// one not-join and of several, the forms, invocations and subqueries run
// over them, headers and inputs the environment binds, and the fallback's
// prefetch, each answered as a reference over the graph's own facts answers
// it, on every backend and mode, at window caps from one tuple to the
// executor's default. Each entity is named e<i> and carries :k :x, a :p ref
// on some, a :q ref on some, and :r on some. A check whose tuple count the
// record of the graph states asserts that count beside the reference.

// scopeGraph is a recorded graph: per entity index, the index of its :p
// target, of its :q target, and its :r, each -1 where the entity carries none.
type scopeGraph struct {
	p, q []int
	r    []int64
}

// scopeName is the name of the i-th entity.
func scopeName(i int) string { return fmt.Sprintf("e%d", i) }

// scopeIndex is the index of the entity named e<i>.
func scopeIndex(t testing.TB, name string) int {
	t.Helper()
	i, err := strconv.Atoi(strings.TrimPrefix(name, "e"))
	require.NoError(t, err, name)
	return i
}

// loadScopeGraph reads a data file of lines "E A V": E an entity named
// e<i>, A one of :k, :p, :q and :r, and V the keyword :x, a ref written
// @e<j>, or an integer.
func loadScopeGraph(t testing.TB, path string) scopeGraph {
	t.Helper()
	text, err := os.ReadFile(path)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(text)), "\n")
	n := 0
	for _, line := range lines {
		if i := scopeIndex(t, strings.Fields(line)[0]); i >= n {
			n = i + 1
		}
	}
	g := scopeGraph{p: make([]int, n), q: make([]int, n), r: make([]int64, n)}
	for i := range g.p {
		g.p[i], g.q[i], g.r[i] = -1, -1, -1
	}
	for _, line := range lines {
		fields := strings.Fields(line)
		require.Len(t, fields, 3, line)
		e := scopeIndex(t, fields[0])
		switch fields[1] {
		case ":k":
			require.Equal(t, ":x", fields[2], line)
		case ":p":
			g.p[e] = scopeIndex(t, strings.TrimPrefix(fields[2], "@"))
		case ":q":
			g.q[e] = scopeIndex(t, strings.TrimPrefix(fields[2], "@"))
		case ":r":
			v, err := strconv.ParseInt(fields[2], 10, 64)
			require.NoError(t, err, line)
			g.r[e] = v
		default:
			t.Fatalf("%s: an attribute the recorded graphs do not carry", line)
		}
	}
	return g
}

// facts is the graph as commitFacts commits it, each entity's facts in index
// order: its :k, then its :p and :q refs and its :r where it carries them.
func (g scopeGraph) facts() []fact {
	entities := make([]namedEntity, len(g.p))
	for i := range entities {
		entities[i] = named(scopeName(i))
	}
	var facts []fact
	for i, e := range entities {
		facts = append(facts, fact{e, ":k", kw(":x")})
		if g.p[i] >= 0 {
			facts = append(facts, fact{e, ":p", entities[g.p[i]]})
		}
		if g.q[i] >= 0 {
			facts = append(facts, fact{e, ":q", entities[g.q[i]]})
		}
		if g.r[i] >= 0 {
			facts = append(facts, fact{e, ":r", g.r[i]})
		}
	}
	return facts
}

// those lists the entities keep holds of, in index order.
func (g scopeGraph) those(keep func(i int) bool) []int {
	var out []int
	for i := range g.p {
		if keep(i) {
			out = append(out, i)
		}
	}
	return out
}

// reach is the set of entities i reaches through one or more :p refs.
func (g scopeGraph) reach(i int) map[int]bool {
	reached := map[int]bool{}
	for x := g.p[i]; x >= 0 && !reached[x]; x = g.p[x] {
		reached[x] = true
	}
	return reached
}

// scopeTuple renders a reference tuple as renderedTuples renders a result's:
// an int is an entity's index, rendered as its name, and any other value is
// printed.
func scopeTuple(parts ...interface{}) string {
	rendered := make([]string, len(parts))
	for i, part := range parts {
		if index, ok := part.(int); ok {
			rendered[i] = scopeName(index)
		} else {
			rendered[i] = fmt.Sprint(part)
		}
	}
	return strings.Join(rendered, " ")
}

// scopeReach derives reach, the closure of :p.
const scopeReach = `[[(reach ?a ?b) [?a :p ?b]] [(reach ?a ?c) [?a :p ?b] (reach ?b ?c)]]`

// scopeGraphSchema declares the graph's attributes, each ref cardinality one.
func scopeGraphSchema(t testing.TB) *schema.Schema {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(":k").Type(schema.TypeKeyword).Add().
		Attribute(":r").Type(schema.TypeLong).Add().
		Attribute(":p").Type(schema.TypeRef).Add().
		Attribute(":q").Type(schema.TypeRef).Add().
		Build()
	require.NoError(t, err)
	return s
}

// scopeCheck is one query over the graph, whether it takes the reach rules,
// its inputs after them, the tuples the reference gives, the tuple count the
// record states or -1 where it states none, and the count of
// RulesStratumBegin events its execution emits or -1 where the check reads
// none.
type scopeCheck struct {
	name   string
	text   string
	rules  bool
	inputs []interface{}
	want   []string
	tuples int
	strata int
}

// recordedGraphCaps are the window caps the two-hundred-entity graph's
// queries run at: the executor's default, under which a subject is one
// window; one tuple, a window per tuple; and thirty-two, several windows.
var recordedGraphCaps = []int{0, 1, 32}

// runScopeChecks runs every check on each backend and mode at each cap,
// asserting the reference's tuples, the recorded count where the record
// states one, and the stratum count where a check reads it.
func runScopeChecks(t *testing.T, g scopeGraph, caps []int, checks []scopeCheck) {
	t.Helper()
	facts := g.facts()
	rules := mustRuleSet(t, scopeReach)
	for _, mode := range optimizerModes {
		for _, windowCap := range caps {
			t.Run(fmt.Sprintf("%s/cap_%d", mode.name, windowCap), func(t *testing.T) {
				var strata atomic.Int32
				opts := mode.plannerOptions()
				opts.ScopeWindowCap = windowCap
				d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts, AnnotationHandler: func(e annotations.Event) {
					if e.Name == annotations.RulesStratumBegin {
						strata.Add(1)
					}
				}})
				names := commitFacts(t, d, facts)
				for _, c := range checks {
					t.Run(c.name, func(t *testing.T) {
						var inputs []interface{}
						if c.rules {
							inputs = append(inputs, rules)
						}
						inputs = append(inputs, c.inputs...)
						strata.Store(0)
						rel, err := d.Query(c.text, inputs...)
						require.NoError(t, err)
						got := renderedTuples(t, rel, names)
						t.Logf("%d tuples", len(got))
						require.ElementsMatch(t, c.want, got)
						if c.tuples >= 0 {
							require.Len(t, got, c.tuples, "the record's count")
						}
						if c.strata >= 0 {
							require.Equal(t, int32(c.strata), strata.Load(), "one evaluation per invocation, continued across the windows")
						}
					})
				}
			})
		}
	}
}

// Over the two-hundred-entity graph: a pattern naming one variable twice, the
// fold over three streaming groups sharing no symbol, a pattern naming one
// variable twice over a streaming group, a bound invocation over one as a
// clause, as a union branch and inside a counting subquery, a union with an
// invocation branch and a carried output over a product subject, headers and
// inputs the environment alone binds, a binding naming its own argument, a
// collection argument across windows with a member twice and none, the
// product of disjoint streaming groups at the find, forms nested inside
// branches, an invocation with a repeated argument, a bound pattern with an
// input-bound value, and the zero-default count idiom. A not-join whose body
// reads an entity's :q as itself drops nothing of these groups and makes
// each a streaming subject.
func TestScopeForms_OverTheRecordedGraphAnswerTheRecordAndTheReference(t *testing.T) {
	g := loadScopeGraph(t, "testdata/scope_review_f4.txt")
	selfQ := func(i int) bool { return g.q[i] == i }
	ones := g.those(func(i int) bool { return g.r[i] == 1 && !selfQ(i) })
	twos := g.those(func(i int) bool { return g.r[i] == 2 && !selfQ(i) })
	threes := g.those(func(i int) bool { return g.r[i] == 3 && !selfQ(i) })
	onesNoP := g.those(func(i int) bool { return g.r[i] == 1 && g.p[i] < 0 })
	threesNoP := g.those(func(i int) bool { return g.r[i] == 3 && g.p[i] < 0 })
	for _, group := range [][]int{ones, twos, threes, onesNoP, threesNoP} {
		require.NotEmpty(t, group, "the graph fills every group a subject is drawn from")
	}

	var selfLoops []string
	for e := range g.p {
		if g.p[e] == e {
			selfLoops = append(selfLoops, scopeTuple(e))
		}
	}

	var fold []string
	for _, a := range ones {
		for _, b := range threes {
			for _, c := range onesNoP {
				if g.p[b] == c || g.p[a] == b || g.p[b] == a {
					fold = append(fold, scopeTuple(a, b, c))
				}
			}
		}
	}

	var selfTargets []string
	for _, x := range twos {
		if y := g.q[x]; y >= 0 && g.p[y] == y {
			selfTargets = append(selfTargets, scopeTuple(x, y))
		}
	}

	var closure, closureOrQ, fans, cyclic, zeroDefault []string
	for _, x := range ones {
		reached := g.reach(x)
		union := map[int]bool{}
		for y := range reached {
			closure = append(closure, scopeTuple(x, y))
			union[y] = true
		}
		for y := range g.p {
			if g.q[y] == x {
				union[y] = true
			}
		}
		for y := range union {
			closureOrQ = append(closureOrQ, scopeTuple(x, y))
		}
		if len(reached) > 0 {
			fans = append(fans, scopeTuple(x, int64(len(reached))))
		}
		if reached[x] {
			cyclic = append(cyclic, scopeTuple(x))
		}
		zeroDefault = append(zeroDefault, scopeTuple(x, int64(len(reached))))
	}

	var carried, reachesThree, product []string
	for _, a := range ones {
		reached := g.reach(a)
		for _, w := range threes {
			product = append(product, scopeTuple(a, w))
			if reached[w] || (g.p[a] >= 0 && g.q[g.p[a]] == w) {
				carried = append(carried, scopeTuple(a, w))
			}
		}
		for _, w := range threesNoP {
			if reached[w] {
				reachesThree = append(reachesThree, scopeTuple(a, w))
			}
		}
	}

	both, noQ := -1, -1
	for i := range g.p {
		if both < 0 && g.p[i] >= 0 && g.q[i] >= 0 {
			both = i
		}
		if noQ < 0 && g.q[i] < 0 {
			noQ = i
		}
	}
	require.True(t, both >= 0 && noQ >= 0, "the graph holds an entity carrying both refs and one carrying no :q")
	bothID, noQID := datalog.NewIdentity(scopeName(both)), datalog.NewIdentity(scopeName(noQ))
	var allOnes, onesButTarget, onesFlagged []string
	for _, e := range g.those(func(i int) bool { return g.r[i] == 1 }) {
		allOnes = append(allOnes, scopeTuple(e))
		if g.p[both] != e {
			onesButTarget = append(onesButTarget, scopeTuple(e))
		}
		flag := int64(0)
		if g.p[both] == e {
			flag = 1
		}
		onesFlagged = append(onesFlagged, scopeTuple(e, flag))
	}
	noQValue := g.p[noQ]
	if noQValue < 0 {
		noQValue = noQ
	}

	inP := make([]int, len(g.p))
	for z := range g.p {
		if g.p[z] >= 0 {
			inP[g.p[z]]++
		}
	}
	hub := 0
	for i := range inP {
		if inP[i] > inP[hub] {
			hub = i
		}
	}
	hubID := datalog.NewIdentity(scopeName(hub))
	perTarget := map[int]int64{}
	for z := range g.p {
		if g.p[z] == hub && g.q[z] >= 0 {
			perTarget[g.q[z]]++
		}
	}
	var hubCounts []string
	for e, n := range perTarget {
		hubCounts = append(hubCounts, scopeTuple(e, n))
	}
	require.NotEmpty(t, hubCounts, "some entity referring to the hub carries a :q")

	members := []struct {
		t  int64
		ns []int64
	}{{1, []int64{1, 2}}, {2, []int64{2, 3}}, {3, nil}, {4, []int64{3}}, {5, []int64{1, 2}}}
	var held []string
	for _, m := range members {
		for h := range g.p {
			for _, n := range m.ns {
				if g.r[h] == n {
					held = append(held, scopeTuple(m.t, h))
					break
				}
			}
		}
	}

	var countBeside []string
	for _, a := range ones {
		countBeside = append(countBeside, scopeTuple(a, int64(len(g.those(func(i int) bool { return g.r[i] == 3 })))))
	}

	var nestedNot, nestedUnion, nestedFallback, nestedCount []string
	for _, x := range g.those(func(i int) bool { return g.r[i] == 1 }) {
		if (g.p[x] >= 0 && g.q[g.p[x]] < 0) || g.q[x] >= 0 {
			nestedNot = append(nestedNot, scopeTuple(x))
		}
		values := map[int64]bool{}
		if g.p[x] >= 0 {
			values[g.r[g.p[x]]] = true
		}
		if g.q[x] >= 0 {
			values[g.r[g.q[x]]] = true
		}
		if len(values) == 0 {
			nestedUnion = append(nestedUnion, scopeTuple(x, int64(-1)))
		}
		for v := range values {
			nestedUnion = append(nestedUnion, scopeTuple(x, v))
		}
		v := g.p[x]
		if v < 0 {
			v = x
		}
		if g.q[v] < 0 {
			nestedFallback = append(nestedFallback, scopeTuple(x))
		}
		var n int64
		for z := range g.p {
			if g.p[z] == x && g.q[z] < 0 {
				n++
			}
		}
		if n > 0 {
			nestedCount = append(nestedCount, scopeTuple(x, n))
		}
	}

	target := -1
	for _, e := range ones {
		if g.p[e] >= 0 {
			target = g.p[e]
			break
		}
	}
	require.True(t, target >= 0, "some entity of the subject carries a :p")
	targetID := datalog.NewIdentity(scopeName(target))
	var pointing []string
	for _, e := range ones {
		if g.p[e] == target {
			pointing = append(pointing, scopeTuple(e))
		}
	}

	checks := []scopeCheck{
		{"a pattern naming one variable twice", `[:find ?e :where [?e :p ?e]]`, false, nil, selfLoops, 67, -1},
		{"the fold over three streaming groups sharing no symbol", `[:find ?a ?b ?c :where [?a :r 1] (not-join [?a] [?a :q ?a]) [?b :r 3] (not-join [?b] [?b :q ?b]) [?c :r 1] (not-join [?c] [?c :p _]) (or-join [[?a ?b ?c]] [?b :p ?c] [?a :p ?b] [?b :p ?a])]`, false, nil, fold, 400, -1},
		{"a pattern naming one variable twice over a streaming group", `[:find ?x ?y :where [?x :r 2] (not-join [?x] [?x :q ?x]) [?x :q ?y] [?y :p ?y]]`, false, nil, selfTargets, 23, -1},
		{"a bound invocation over a streaming group", `[:find ?x ?y :in $ % :where [?x :r 1] (not-join [?x] [?x :q ?x]) (reach ?x ?y)]`, true, nil, closure, 40, 2},
		{"a bound invocation as a union branch over a streaming group", `[:find ?x ?y :in $ % :where [?x :r 1] (not-join [?x] [?x :q ?x]) (or-join [[?x] ?y] (reach ?x ?y) [?y :q ?x])]`, true, nil, closureOrQ, -1, 2},
		{"a bound invocation inside a counting subquery over a streaming group", `[:find ?x ?n :in $ % :where [?x :r 1] (not-join [?x] [?x :q ?x]) [(q [:find (count ?y) :in $ % ?x :where (reach ?x ?y)] $ % ?x) [[?n]]]]`, true, nil, fans, 33, 2},
		{"a union with an invocation branch and a carried output over a product subject", `[:find ?a ?w :in $ % :where [?a :r 1] (not-join [?a] [?a :q ?a]) [?w :r 3] (not-join [?w] [?w :q ?w]) (or-join [[?a] ?w] (reach ?a ?w) (and [?a :p ?x] [?x :q ?w]))]`, true, nil, carried, 10, -1},
		{"a not-join header the environment alone binds, deciding against", `[:find ?e :in $ ?x :where [?e :r 1] (not-join [?x] [?x :q _])]`, false, []interface{}{bothID}, nil, -1, -1},
		{"a not-join header the environment alone binds, deciding for", `[:find ?e :in $ ?x :where [?e :r 1] (not-join [?x] [?x :q _])]`, false, []interface{}{noQID}, allOnes, -1, -1},
		{"a not-join header of an input and a data variable", `[:find ?e :in $ ?x :where [?e :r 1] (not-join [?x ?e] [?x :p ?e])]`, false, []interface{}{bothID}, onesButTarget, -1, -1},
		{"a fallback whose required run the environment alone binds", `[:find ?v :in $ ?x :where (or-default-join [[?x] ?v] [?x :p ?v] [(identity ?x) ?v])]`, false, []interface{}{bothID}, []string{scopeTuple(g.p[both])}, -1, -1},
		{"a fallback whose required run the environment alone binds, defaulting", `[:find ?v :in $ ?x :where (or-default-join [[?x] ?v] [?x :p ?v] [(identity ?x) ?v])]`, false, []interface{}{noQID}, []string{scopeTuple(noQValue)}, -1, -1},
		{"a fallback whose required run holds an input and a data variable", `[:find ?e ?v :in $ ?x :where [?e :r 1] (or-default-join [[?x ?e] ?v] (and [?x :p ?e] [(ground 1) ?v]) [(ground 0) ?v])]`, false, []interface{}{bothID}, onesFlagged, -1, -1},
		{"a counting subquery over an input and a data variable", `[:find ?e ?n :in $ ?x :where [?e :k :x] [(q [:find (count ?z) :in $ ?x ?e :where [?z :p ?x] [?z :q ?e]] $ ?x ?e) [[?n]]]]`, false, []interface{}{hubID}, hubCounts, -1, -1},
		{"a subquery binding naming its own argument", `[:find ?e :where [?e :k :x] [(q [:find ?y :in $ ?x :where [?x :p ?y]] $ ?e) [[?e]]]]`, false, nil, selfLoops, 67, -1},
		{"a collection argument across windows with a member twice and none", `[:find ?t ?h :where [(ground [[1 [1 2]] [2 [2 3]] [3 []] [4 [3]] [5 [1 2]]]) [[?t ?ns]]] [(q [:find ?h :in $ [?n ...] :where [?h :r ?n]] $ ?ns) [[?h] ...]]]`, false, nil, held, -1, -1},
		{"the product of two disjoint streaming groups at the find", `[:find ?a ?b :where [?a :r 1] (not-join [?a] [?a :q ?a]) [?b :r 3] (not-join [?b] [?b :q ?b])]`, false, nil, product, 2500, -1},
		{"a streaming group beside an uncorrelated count at the find", `[:find ?a ?n :where [?a :r 1] (not-join [?a] [?a :q ?a]) [(q [:find (count ?z) :in $ :where [?z :r 3]] $) [[?n]]]]`, false, nil, countBeside, -1, -1},
		{"a not inside a union branch", `[:find ?x :where [?x :r 1] (or-join [?x] (and [?x :p ?y] (not [?y :q _])) [?x :q _])]`, false, nil, nestedNot, -1, -1},
		{"a union inside a fallback branch", `[:find ?x ?v :where [?x :r 1] (or-default-join [[?x] ?v] (and (or-join [[?x] ?y] [?x :p ?y] [?x :q ?y]) [?y :r ?v]) [(ground -1) ?v])]`, false, nil, nestedUnion, -1, -1},
		{"a fallback inside a not-join body", `[:find ?x :where [?x :r 1] (not-join [?x] (or-default-join [[?x] ?v] [?x :p ?v] [(identity ?x) ?v]) [?v :q _])]`, false, nil, nestedFallback, -1, -1},
		{"a not-join inside a counting subquery", `[:find ?x ?n :where [?x :r 1] [(q [:find (count ?z) :in $ ?x :where [?z :p ?x] (not-join [?z] [?z :q _])] $ ?x) [[?n]]]]`, false, nil, nestedCount, -1, -1},
		{"an invocation with a repeated argument over a streaming group", `[:find ?a :in $ % :where [?a :r 1] (not-join [?a] [?a :q ?a]) (reach ?a ?a)]`, true, nil, cyclic, 16, -1},
		{"an invocation over two streaming groups", `[:find ?a ?b :in $ % :where [?a :r 1] (not-join [?a] [?a :q ?a]) [?b :r 3] (not-join [?b] [?b :p _]) (reach ?a ?b)]`, true, nil, reachesThree, -1, -1},
		{"a bound pattern with an input-bound value over a streaming group", `[:find ?e :in $ ?w :where [?e :r 1] (not-join [?e] [?e :q ?e]) [?e :p ?w]]`, false, []interface{}{targetID}, pointing, -1, -1},
		{"a bound pattern with an input-bound value, counted", `[:find (count ?e) :in $ ?w :where [?e :r 1] (not-join [?e] [?e :q ?e]) [?e :p ?w]]`, false, []interface{}{targetID}, []string{scopeTuple(int64(len(pointing)))}, -1, -1},
		{"a bound pattern with an input-bound value under a predicate", `[:find ?e :in $ ?w :where [?e :r 1] (not-join [?e] [?e :q ?e]) [?e :p ?t] [(= ?t ?w)]]`, false, []interface{}{targetID}, pointing, -1, -1},
		{"the zero-default count idiom with a rule inside its subquery", `[:find ?g ?n :in $ % :where [?g :r 1] (or-default [(q [:find (count ?x) :in $ % ?g :where (reach ?g ?x)] $ % ?g) [[?n]]] [(ground 0) ?n])]`, true, nil, zeroDefault, -1, 2},
	}
	runScopeChecks(t, g, recordedGraphCaps, checks)
}

// Over the three-thousand-entity graph, a not-join's streaming result
// followed by a pattern and a fallback over the pattern's value, by a bound
// pattern on that value, and by a counting subquery, at the executor's
// default cap and at seven tuples.
func TestScopeForms_OverThreeThousandEntitiesAnswerTheReference(t *testing.T) {
	g := loadScopeGraph(t, "testdata/scope_review_f5.txt")
	subject := g.those(func(i int) bool { return g.q[i] != i })
	require.NotEmpty(t, subject, "the graph fills the subject")
	var fallback, through, counted []string
	for _, x := range subject {
		if y := g.p[x]; y >= 0 {
			if g.q[y] >= 0 {
				fallback = append(fallback, scopeTuple(x, y, g.q[y]))
				through = append(through, scopeTuple(x, g.q[y]))
			} else {
				fallback = append(fallback, scopeTuple(x, y, ":none"))
			}
		}
		var n int64
		for z := range g.p {
			if g.p[z] == x {
				n++
			}
		}
		if n > 0 {
			counted = append(counted, scopeTuple(x, n))
		}
	}
	checks := []scopeCheck{
		{"a not-join, a pattern and a fallback over its value", `[:find ?x ?y ?v :where [?x :k :x] (not-join [?x] [?x :q ?x]) [?x :p ?y] (or-default-join [[?y] ?v] [?y :q ?v] [(ground :none) ?v])]`, false, nil, fallback, -1, -1},
		{"a not-join, a pattern and a bound pattern on its value", `[:find ?x ?w :where [?x :k :x] (not-join [?x] [?x :q ?x]) [?x :p ?y] [?y :q ?w]]`, false, nil, through, -1, -1},
		{"a not-join and a counting subquery", `[:find ?x ?n :where [?x :k :x] (not-join [?x] [?x :q ?x]) [(q [:find (count ?z) :in $ ?x :where [?z :p ?x]] $ ?x) [[?n]]]]`, false, nil, counted, -1, -1},
	}
	runScopeChecks(t, g, []int{0, 7}, checks)
}

// A header symbol nothing binds, over a streaming subject, is refused on both
// planner paths naming the symbol, under a not-join header and as a
// subquery's argument, and no window runs.
func TestScopeForms_AHeaderSymbolNothingBindsOverAStreamingSubjectIsRefused(t *testing.T) {
	g := loadScopeGraph(t, "testdata/scope_review_f4.txt")
	texts := []string{
		`[:find ?e :where [?e :r 1] (not-join [?e] [?e :q ?e]) (not-join [?e ?zz] [?zz :p ?e])]`,
		`[:find ?e ?n :where [?e :r 1] (not-join [?e] [?e :q ?e]) [(q [:find (count ?z) :in $ ?k :where [?z :p ?k]] $ ?zz) [[?n]]]]`,
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			commitFacts(t, d, g.facts())
			for _, text := range texts {
				_, err := executor.CollectTuples(d.Query(text))
				require.Error(t, err, text)
				require.Contains(t, err.Error(), "cannot order clauses", text)
				require.Contains(t, err.Error(), "?zz", text)
			}
		})
	}
}

// The fallback's prefetch answers as the scan path answers, whatever the
// cache holds, over the two-hundred-entity graph under a schema declaring the
// refs cardinality one: with the option on the trigger fires, and the tuples
// equal the option's off tuples and the reference, through a fallback over
// every entity, a union whose required value the subject carries, a union
// whose required value never agrees, and a fallback whose output the subject
// carries.
func TestFallbackPrefetch_AnswersAsTheScanPathAndFires(t *testing.T) {
	g := loadScopeGraph(t, "testdata/scope_review_f4.txt")
	var every, required, carried []string
	for e := range g.p {
		if g.p[e] >= 0 {
			every = append(every, scopeTuple(e, g.p[e]))
		} else {
			every = append(every, scopeTuple(e, ":none"))
		}
		required = append(required, scopeTuple(e, g.r[e]))
		if g.q[e] >= 0 && g.q[e] == g.p[e] {
			carried = append(carried, scopeTuple(e, g.q[e]))
		}
	}
	checks := []scopeCheck{
		{"a fallback over every entity", `[:find ?e ?v :where [?e :k :x] (or-default-join [[?e] ?v] [?e :p ?v] [(ground :none) ?v])]`, false, nil, every, 200, -1},
		{"a union whose required value the subject carries", `[:find ?e ?n :where [?e :r ?n] (or-join [[?e ?n]] [?e :r ?n])]`, false, nil, required, 200, -1},
		{"a union whose required value never agrees", `[:find ?e ?n :where [?e :r ?n] [(+ ?n 10) ?m] (or-join [[?e ?m]] [?e :r ?m])]`, false, nil, nil, -1, -1},
		{"a fallback whose output the subject carries", `[:find ?e ?v :where [?e :q ?v] (or-default-join [[?e] ?v] [?e :p ?v] [(ground :none) ?v])]`, false, nil, carried, 0, -1},
	}
	for _, mode := range optimizerModes {
		for _, prefetch := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/prefetch_%v", mode.name, prefetch), func(t *testing.T) {
				var triggers atomic.Int32
				opts := mode.plannerOptions()
				opts.EnableEntityPrefetch = prefetch
				d := openBackendDB(t, mode.backend, DatabaseOptions{Schema: scopeGraphSchema(t), PlannerOptions: &opts, AnnotationHandler: func(e annotations.Event) {
					if e.Name == annotations.PrefetchTrigger {
						triggers.Add(1)
					}
				}})
				names := commitFacts(t, d, g.facts())
				for _, c := range checks {
					t.Run(c.name, func(t *testing.T) {
						rel, err := d.Query(c.text)
						require.NoError(t, err)
						got := renderedTuples(t, rel, names)
						require.ElementsMatch(t, c.want, got)
						if c.tuples >= 0 {
							require.Len(t, got, c.tuples, "the record's count")
						}
					})
				}
				if prefetch {
					require.GreaterOrEqual(t, triggers.Load(), int32(1), "the trigger fires with the option on")
				} else {
					require.Equal(t, int32(0), triggers.Load(), "no prefetch runs with the option off")
				}
			})
		}
	}
}
