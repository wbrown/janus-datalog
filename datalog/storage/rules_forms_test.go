package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// diamond is the graph a → b, a → c, b → d, c → d, d → e.
func diamond() graphSpec {
	return graphSpec{
		nodes: []string{"a", "b", "c", "d", "e"},
		edges: [][2]string{{"a", "b"}, {"a", "c"}, {"b", "d"}, {"c", "d"}, {"d", "e"}},
	}
}

// A rule body derives through every clause form a query body does: an
// arithmetic counter and a depth count that recurse on a number, get-else
// and missing?, an aggregate subquery, a constant bound inside the body, and
// an invocation with a constant argument or with no variable at all. Each
// derives on the diamond what the form states, whole and bound.
func TestRules_BodyForms(t *testing.T) {
	const closure = `[(r ?a ?b) [?a :edge ?b]] [(r ?a ?c) [?a :edge ?b] (r ?b ?c)]`
	cases := []struct {
		name, rules, text string
		inputs            []interface{}
		want              []string
	}{
		{"counter",
			`[[(cnt ?a ?n) [?a :name "a"] [(ground 0) ?n]] [(cnt ?a ?m) (cnt ?a ?n) [(< ?n 5)] [(+ ?n 1) ?m]]]`,
			`[:find ?n :in $ % :where (cnt _ ?n)]`, nil,
			[]string{"0", "1", "2", "3", "4", "5"}},
		{"depth",
			`[[(dep ?a ?d) [?a :name "a"] [(ground 0) ?d]] [(dep ?b ?d2) (dep ?a ?d) [?a :edge ?b] [(+ ?d 1) ?d2]]]`,
			`[:find ?b ?d :in $ % :where (dep ?b ?d)]`, nil,
			[]string{"a>0", "b>1", "c>1", "d>2", "e>3"}},
		{"get-else",
			`[[(lbl ?a ?l) [?a :type :node] [(get-else $ ?a :label "none") ?l]] [(lbl2 ?a ?l) (lbl ?a ?l)]]`,
			`[:find ?a ?l :in $ % :where (lbl2 ?a ?l)]`, nil,
			[]string{"a>none", "b>none", "c>none", "d>none", "e>none"}},
		{"missing?",
			`[[(leaf ?a) [?a :type :node] [(missing? $ ?a :edge)]]]`,
			`[:find ?a :in $ % :where (leaf ?a)]`, nil,
			[]string{"e"}},
		{"aggregate subquery",
			`[` + closure + ` [(fan ?a ?n) [?a :type :node] [(q [:find (count ?y) :in $ % ?a :where (r ?a ?y)] $ % ?a) [[?n]]]]]`,
			`[:find ?a ?n :in $ % :where (fan ?a ?n)]`, nil,
			[]string{"a>4", "b>2", "c>2", "d>1"}},
		{"bound counter",
			`[[(cnt ?a ?n) [?a :type :node] [(ground 0) ?n]] [(cnt ?a ?m) (cnt ?a ?n) [(< ?n 3)] [(+ ?n 1) ?m]]]`,
			`[:find ?n :in $ % ?a :where (cnt ?a ?n)]`, []interface{}{nodeID("b")},
			[]string{"0", "1", "2", "3"}},
		{"bound depth",
			`[[(dep ?a ?b ?d) [?a :edge ?b] [(ground 1) ?d]] [(dep ?a ?c ?d2) (dep ?a ?b ?d) [?b :edge ?c] [(+ ?d 1) ?d2]]]`,
			`[:find ?c ?d :in $ % ?a :where (dep ?a ?c ?d)]`, []interface{}{nodeID("a")},
			[]string{"b>1", "c>1", "d>2", "e>3"}},
		{"constant in the body",
			`[` + closure + ` [(toE ?a) (r ?a ?e) [?e :name "e"]]]`,
			`[:find ?a :in $ % :where (toE ?a)]`, nil,
			[]string{"a", "b", "c", "d"}},
		{"constant argument",
			`[` + closure + `]`,
			`[:find ?a :in $ % ?e :where (r ?a ?e)]`, []interface{}{nodeID("e")},
			[]string{"a", "b", "c", "d"}},
		{"verdict",
			`[` + closure + `]`,
			`[:find ?a :in $ % ?x ?e :where [?a :type :node] (r ?x ?e)]`, []interface{}{nodeID("a"), nodeID("e")},
			[]string{"a", "b", "c", "d", "e"}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: graphSchema()})
			spec := diamond()
			g := writeGraph(t, d, spec)
			writeNames(t, d, spec)
			for _, c := range cases {
				rs := mustRuleSet(t, c.rules)
				rel, err := d.Query(c.text, append([]interface{}{rs}, c.inputs...)...)
				require.NoError(t, err, c.name)
				require.ElementsMatch(t, c.want, g.tuples(t, rel), c.name)
			}
		})
	}
}

// writeValues records :val 1 to n on n entities.
func writeValues(t testing.TB, d *Database, n int) {
	t.Helper()
	tx := d.NewTransaction()
	for i := 1; i <= n; i++ {
		require.NoError(t, tx.Add(datalog.NewIdentity(fmt.Sprintf("val/%d", i)), kw(":val"), int64(i)))
	}
	_, err := tx.Commit()
	require.NoError(t, err)
}

// A predicate's required position is bound by whatever provides the
// variable before the invocation runs: an or, a subquery's collection
// binding through identity or directly, an earlier invocation of the same
// predicate, a pattern before a negation holding a constant, or an input. A
// variable repeated at the required position and the free one binds the
// free one to the bound value, which the successor never equals.
func TestRules_RequiredBindingBoundThroughEachBinder(t *testing.T) {
	const rules = `[[(succ [?n] ?m) [(+ ?n 1) ?m]]]`
	cases := []struct {
		name, text string
		inputs     []interface{}
		want       []string
	}{
		{"an or", `[:find ?m :in $ % :where (succ ?n ?m) (or [?e :val ?n] [?e :num ?n])]`, nil, []string{"2", "3", "4"}},
		{"a subquery collection through identity", `[:find ?m :in $ % :where (succ ?n ?m) [(q [:find ?v :where [_ :val ?v]] $) [[?v] ...]] [(identity ?v) ?n]]`, nil, []string{"2", "3", "4"}},
		{"a subquery collection", `[:find ?m :in $ % :where (succ ?n ?m) [(q [:find ?v :where [_ :val ?v]] $) [[?n] ...]]]`, nil, []string{"2", "3", "4"}},
		{"an earlier invocation", `[:find ?k :in $ % :where [?e :val ?v] (succ ?v ?m) (succ ?m ?k)]`, nil, []string{"3", "4", "5"}},
		{"a pattern before a negation with a constant", `[:find ?v :in $ % :where [?e :val ?v] (not (succ ?v 3))]`, nil, []string{"1", "3"}},
		{"an input", `[:find ?m :in $ % ?n :where (succ ?n ?m)]`, []interface{}{int64(7)}, []string{"8"}},
		{"a variable repeated at both positions", `[:find ?v :in $ % :where [?e :val ?v] (succ ?v ?v)]`, nil, nil},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			writeValues(t, d, 3)
			rs := mustRuleSet(t, rules)
			for _, c := range cases {
				rel, err := d.Query(c.text, append([]interface{}{rs}, c.inputs...)...)
				require.NoError(t, err, c.name)
				require.ElementsMatch(t, c.want, graph{}.tuples(t, rel), c.name)
			}
		})
	}
}

// One query text run under two rule sets, one requiring the invoked
// predicate's first position and one not, derives the same tuples under
// each, in either order: the plan serving one never serves the other.
func TestRules_PlanCacheServesEachRuleSetItsOwnPlan(t *testing.T) {
	const text = `[:find ?v ?m :in $ % :where (succ ?v ?m) [?e :val ?v]]`
	free := `[[(succ ?n ?m) [?e :val ?n] [(+ ?n 1) ?m]]]`
	required := `[[(succ [?n] ?m) [(+ ?n 1) ?m]]]`
	want := []string{"1>2", "2>3", "3>4"}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			writeValues(t, d, 3)
			for i, rules := range []string{free, required, free, required} {
				rel, err := d.Query(text, mustRuleSet(t, rules))
				require.NoError(t, err, "run %d", i)
				require.ElementsMatch(t, want, graph{}.tuples(t, rel), "run %d", i)
			}
		})
	}
}

// Join-project insertion changes no result of a query over rules: the
// closure whole, a group bound before the invocation with a pattern after
// it, an aggregate over the invocation joined with a rule that computes a
// value, and a negated invocation with a blank.
func TestRules_JoinProjectInsertionKeepsResults(t *testing.T) {
	const rules = `[[(reach ?a ?b) [?a :edge ?b]] [(reach ?a ?c) [?a :edge ?b] (reach ?b ?c)]
	                [(lbl ?x ?l) [?x :type ?t] [(str ?t) ?l]]]`
	cases := []struct {
		name, text string
		want       []string
	}{
		{"whole", `[:find ?x ?y :in $ % :where (reach ?x ?y)]`, chainClosure(4)},
		{"bound before, pattern after", `[:find ?y :in $ % :where [?x :type :node] (reach ?x ?y) [?y :type ?t]]`, []string{"n2", "n3", "n4"}},
		{"aggregate over the invocation and a computing rule", `[:find (count ?y) :in $ % :where (reach ?x ?y) (lbl ?y ?l) [?x :type :node]]`, []string{"6"}},
		{"negated with a blank", `[:find ?x :in $ % :where [?x :type :node] (not (reach ?x _))]`, []string{"n4"}},
	}
	for _, mode := range optimizerModes {
		for _, insertion := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/insertion=%v", mode.name, insertion), func(t *testing.T) {
				opts := mode.plannerOptions()
				opts.EnableJoinProjectInsertion = insertion
				d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts})
				g := writeGraph(t, d, chain(4))
				rs := mustRuleSet(t, rules)
				for _, c := range cases {
					rel, err := d.Query(c.text, rs)
					require.NoError(t, err, c.name)
					require.ElementsMatch(t, c.want, g.tuples(t, rel), c.name)
				}
			})
		}
	}
}

// stratumTotals reads, from the stratum-complete events of a query, the
// most iterations any stratum ran and the tuples every stratum derived.
type stratumTotals struct {
	mu         sync.Mutex
	iterations int
	tuples     int
}

func (s *stratumTotals) handle(e annotations.Event) {
	if e.Name != annotations.RulesStratumComplete {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if iterations := e.Data["iterations"].(int); iterations > s.iterations {
		s.iterations = iterations
	}
	s.tuples += e.Data["tuples"].(int)
}

func (s *stratumTotals) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.iterations, s.tuples = 0, 0
}

func (s *stratumTotals) read() (iterations, tuples int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.iterations, s.tuples
}

// A bound at the need admits the evaluation and a bound one below it
// refuses it: for the closure of a chain whole and bound to its head, the
// iterations the stratum runs and the tuples the evaluation derives are read
// from the stratum-complete events of an unbounded run, a database bounded
// at those numbers derives the same tuples, and one bounded one below each
// returns the limit error naming the limit and what was reached. The events
// count a seed's tuples among a stratum's, and the tuple bound does not,
// since a seed is given and not derived, so the bound query's one seeded
// tuple is taken off the need.
func TestRules_BoundsAtTheirBoundaries(t *testing.T) {
	queries := []struct {
		name, text string
		inputs     []interface{}
		seeded     int
	}{
		{"whole", closureQuery, nil, 0},
		{"bound to the head", `[:find ?x ?y :in $ % ?x :where (reach ?x ?y)]`, []interface{}{nodeID("n1")}, 1},
	}
	for _, mode := range optimizerModes {
		for _, q := range queries {
			t.Run(mode.name+"/"+q.name, func(t *testing.T) {
				totals := &stratumTotals{}
				unboundedOpts := mode.plannerOptions()
				unbounded := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &unboundedOpts, AnnotationHandler: totals.handle})
				g := writeGraph(t, unbounded, chain(11))
				rs := mustRuleSet(t, closureRules)
				rel, err := unbounded.Query(q.text, append([]interface{}{rs}, q.inputs...)...)
				require.NoError(t, err)
				want := g.tuples(t, rel)
				require.NotEmpty(t, want)
				iterations, held := totals.read()
				tuples := held - q.seeded
				require.Greater(t, iterations, 1, "the chain's closure takes more than one iteration")
				require.GreaterOrEqual(t, tuples, len(want), "the evaluation derives at least what it returns")

				atNeed := mode.plannerOptions()
				atNeed.MaxRuleIterations = iterations
				atNeed.MaxDerivedTuples = tuples
				admitted := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &atNeed})
				g = writeGraph(t, admitted, chain(11))
				rel, err = admitted.Query(q.text, append([]interface{}{rs}, q.inputs...)...)
				require.NoError(t, err, "bounded at the need, the evaluation runs")
				require.ElementsMatch(t, want, g.tuples(t, rel))

				fewerIterations := mode.plannerOptions()
				fewerIterations.MaxRuleIterations = iterations - 1
				refused := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &fewerIterations})
				writeGraph(t, refused, chain(11))
				_, err = refused.Query(q.text, append([]interface{}{rs}, q.inputs...)...)
				var iterationLimit *query.RuleIterationLimitError
				require.ErrorAs(t, err, &iterationLimit, "one iteration below the need is refused")
				require.Equal(t, iterations-1, iterationLimit.Limit)
				require.Equal(t, iterations, iterationLimit.Reached)

				fewerTuples := mode.plannerOptions()
				fewerTuples.MaxDerivedTuples = tuples - 1
				refused = openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &fewerTuples})
				writeGraph(t, refused, chain(11))
				_, err = refused.Query(q.text, append([]interface{}{rs}, q.inputs...)...)
				var tupleLimit *query.DerivedTupleLimitError
				require.ErrorAs(t, err, &tupleLimit, "one tuple below the need is refused")
				require.Equal(t, tuples-1, tupleLimit.Limit)
				require.Greater(t, tupleLimit.Reached, tuples-1)
			})
		}
	}
}

// markedCycle is the graph 1 → 2 → 3 → 4 → 2 on nodes 0 to 5, with 2 and 4
// red.
func markedCycle() intGraph {
	g := intGraph{edge: map[[2]int64]bool{}, reach: map[[2]int64]bool{}}
	for i := int64(0); i < 6; i++ {
		g.nodes = append(g.nodes, i)
	}
	g.reds = []int64{2, 4}
	for _, edge := range [][2]int64{{1, 2}, {2, 3}, {3, 4}, {4, 2}} {
		g.edge[edge] = true
		g.reach[edge] = true
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

// A ground expression in an or branch beside the recursive invocation adds
// the grounded value as a destination: on the marked cycle, reach holds the
// closure and, for a branch grounding 0 under a red successor, the pair of
// every node on a path to a red node with 0, whole and bound at either
// position or both. The branch's shape, the order of the branches, a
// duplicated branch and the red node found by invocation or by pattern
// change nothing.
func TestRules_GroundInsideAnOrBranchBesideTheRecursion(t *testing.T) {
	const base = `[(e ?a ?b) [?x :edge/from ?a] [?x :edge/to ?b]] [(red ?a) [?n :node/id ?a] [?n :node/color "red"]] [(reach ?a ?b) (e ?a ?b)]`
	branches := []struct {
		name, or string
		grounds  bool
	}{
		{"invocation and ground", `(or (reach ?b ?c) (and (red ?b) [(ground 0) ?c]))`, true},
		{"pattern and ground", `(or (reach ?b ?c) (and [?n :node/id ?b] [?n :node/color "red"] [(ground 0) ?c]))`, true},
		{"ground branch first", `(or (and (red ?b) [(ground 0) ?c]) (reach ?b ?c))`, true},
		{"ground branch twice", `(or (reach ?b ?c) (and (red ?b) [(ground 0) ?c]) (and (red ?b) [(ground 0) ?c]))`, true},
		{"ground before the invocation", `(or (reach ?b ?c) (and [(ground 0) ?c] (red ?b)))`, true},
		{"the grounded node by pattern", `(or (reach ?b ?c) (and (red ?b) [?n :node/id ?c] [(= ?c 0)]))`, true},
		{"a lower invocation", `(or (reach ?b ?c) (e ?b ?c))`, false},
	}
	g := markedCycle()
	pair := func(a, b int64) string { return fmt.Sprintf("%d>%d", a, b) }
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			writeIntGraph(t, d, g)
			for _, branch := range branches {
				rs := mustRuleSet(t, `[`+base+` [(reach ?a ?c) (e ?a ?b) `+branch.or+`]]`)
				var whole []string
				for p := range g.reach {
					whole = append(whole, pair(p[0], p[1]))
				}
				if branch.grounds {
					for _, a := range []int64{1, 2, 3, 4} {
						whole = append(whole, pair(a, 0))
					}
				}
				var fromOne, toZero, oneToZero []string
				for _, p := range whole {
					var a, c int64
					_, err := fmt.Sscanf(p, "%d>%d", &a, &c)
					require.NoError(t, err)
					if a == 1 {
						fromOne = append(fromOne, p)
					}
					if c == 0 {
						toZero = append(toZero, p)
					}
					if a == 1 && c == 0 {
						oneToZero = append(oneToZero, p)
					}
				}
				shapes := []struct {
					name, text string
					inputs     []interface{}
					want       []string
				}{
					{"whole", `[:find ?a ?c :in $ % :where (reach ?a ?c)]`, nil, whole},
					{"bound first", `[:find ?a ?c :in $ % ?a :where (reach ?a ?c)]`, []interface{}{int64(1)}, fromOne},
					{"bound second", `[:find ?a ?c :in $ % ?c :where (reach ?a ?c)]`, []interface{}{int64(0)}, toZero},
					{"bound both", `[:find ?a ?c :in $ % ?a ?c :where (reach ?a ?c)]`, []interface{}{int64(1), int64(0)}, oneToZero},
				}
				for _, shape := range shapes {
					rel, err := d.Query(shape.text, append([]interface{}{rs}, shape.inputs...)...)
					require.NoError(t, err, "%s: %s", branch.name, shape.name)
					require.ElementsMatch(t, shape.want, graph{}.tuples(t, rel), "%s: %s", branch.name, shape.name)
				}
			}
		})
	}
}

// The order rules are written in changes nothing a set derives: a seeded
// closure in the shape the rewrite mints, with a ground seed, a recursive
// seed rule, two seed-forwarding rules and a guarded edge rule, derives the
// edges reachable from the seed in every order of its rules, and with one
// seed-forwarding rule alone.
func TestRules_RuleOrderDoesNotChangeWhatIsDerived(t *testing.T) {
	orderings := []struct {
		name, rules string
	}{
		{"seed first", `[[(mr ?a) [(ground 1) ?a]] [(mr ?b) (mr ?a) (ae ?a ?b)] [(me ?a) (mr ?a)] [(me ?b) (mr ?a) (ae ?a ?b)] [(ae ?a ?b) (me ?a) [?x :edge/from ?a] [?x :edge/to ?b]]]`},
		{"recursive seed rule first", `[[(mr ?b) (mr ?a) (ae ?a ?b)] [(ae ?a ?b) (me ?a) [?x :edge/from ?a] [?x :edge/to ?b]] [(me ?a) (mr ?a)] [(me ?b) (mr ?a) (ae ?a ?b)] [(mr ?a) [(ground 1) ?a]]]`},
		{"forwarding rules swapped", `[[(mr ?a) [(ground 1) ?a]] [(mr ?b) (mr ?a) (ae ?a ?b)] [(me ?b) (mr ?a) (ae ?a ?b)] [(me ?a) (mr ?a)] [(ae ?a ?b) (me ?a) [?x :edge/from ?a] [?x :edge/to ?b]]]`},
		{"forwarding rules swapped, recursive seed rule first", `[[(mr ?b) (mr ?a) (ae ?a ?b)] [(mr ?a) [(ground 1) ?a]] [(me ?b) (mr ?a) (ae ?a ?b)] [(me ?a) (mr ?a)] [(ae ?a ?b) (me ?a) [?x :edge/from ?a] [?x :edge/to ?b]]]`},
		{"one forwarding rule", `[[(mr ?a) [(ground 1) ?a]] [(mr ?b) (mr ?a) (ae ?a ?b)] [(me ?a) (mr ?a)] [(ae ?a ?b) (me ?a) [?x :edge/from ?a] [?x :edge/to ?b]]]`},
	}
	g := markedCycle()
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			writeIntGraph(t, d, g)
			for _, ordering := range orderings {
				rs := mustRuleSet(t, ordering.rules)
				rel, err := d.Query(`[:find ?a :in $ % :where (mr ?a)]`, rs)
				require.NoError(t, err, ordering.name)
				require.ElementsMatch(t, []string{"1", "2", "3", "4"}, graph{}.tuples(t, rel), "%s: the seed reaches every node on the cycle", ordering.name)
				rel, err = d.Query(`[:find ?a ?b :in $ % :where (ae ?a ?b)]`, rs)
				require.NoError(t, err, ordering.name)
				require.ElementsMatch(t, []string{"1>2", "2>3", "3>4", "4>2"}, graph{}.tuples(t, rel), "%s: every edge is reachable from the seed", ordering.name)
			}
		})
	}
}
