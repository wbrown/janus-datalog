package storage

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A rule set rides the % input of Query as a *query.RuleSet or as its text.
// The query's invocations read the relations the fixpoint derives, under
// every clause form, with bound, constant and blank arguments, on the one
// snapshot the query reads, and the bounds and the naming errors reach the
// caller through Query.

// closureRules derives reach, the transitive closure of :edge.
const closureRules = `[[(reach ?a ?b) [?a :edge ?b]]
                       [(reach ?a ?c) [?a :edge ?b] (reach ?b ?c)]]`

const closureQuery = `[:find ?x ?y :in $ % :where (reach ?x ?y)]`

// mustRuleSet parses text as a rule set.
func mustRuleSet(t testing.TB, text string) *query.RuleSet {
	t.Helper()
	rules, err := parser.ParseRules(text)
	require.NoError(t, err)
	return rules
}

// graphSpec names a graph: its nodes, each typed :node, and its directed
// edges, each a pair of node names.
type graphSpec struct {
	nodes []string
	edges [][2]string
}

// chain is the graph n1 → n2 → … → n<n>.
func chain(n int) graphSpec {
	var spec graphSpec
	for i := 1; i <= n; i++ {
		spec.nodes = append(spec.nodes, nodeName(i))
		if i > 1 {
			spec.edges = append(spec.edges, [2]string{nodeName(i - 1), nodeName(i)})
		}
	}
	return spec
}

// cycle is chain(n) closed by the edge n<n> → n1.
func cycle(n int) graphSpec {
	spec := chain(n)
	spec.edges = append(spec.edges, [2]string{nodeName(n), nodeName(1)})
	return spec
}

func nodeName(i int) string { return fmt.Sprintf("n%d", i) }

// nodeID is the identity of the node with this name.
func nodeID(name string) datalog.Identity { return datalog.NewIdentity("node/" + name) }

// chainClosure is the closure of chain(n) by hand: each node reaches every
// later node.
func chainClosure(n int) []string {
	var pairs []string
	for i := 1; i <= n; i++ {
		for j := i + 1; j <= n; j++ {
			pairs = append(pairs, nodeName(i)+">"+nodeName(j))
		}
	}
	return pairs
}

// cycleClosure is the closure of cycle(n) by hand: each node reaches every
// node, itself included.
func cycleClosure(n int) []string {
	var pairs []string
	for i := 1; i <= n; i++ {
		for j := 1; j <= n; j++ {
			pairs = append(pairs, nodeName(i)+">"+nodeName(j))
		}
	}
	return pairs
}

// graph reads node names back from the identities a result carries.
type graph struct {
	names map[datalog.Identity]string
}

// graphBatch is the most datoms writeGraph commits in one transaction: the
// largest benchmark graph in one transaction exceeds what Badger accepts in
// one request.
const graphBatch = 10000

// writeGraph commits spec into d.
func writeGraph(t testing.TB, d *Database, spec graphSpec) graph {
	t.Helper()
	g := graph{names: make(map[datalog.Identity]string, len(spec.nodes))}
	tx := d.NewTransaction()
	written := 0
	add := func(e datalog.Identity, a datalog.Keyword, v interface{}) {
		require.NoError(t, tx.Add(e, a, v))
		written++
		if written == graphBatch {
			_, err := tx.Commit()
			require.NoError(t, err)
			tx = d.NewTransaction()
			written = 0
		}
	}
	for _, name := range spec.nodes {
		g.names[nodeID(name)] = name
		add(nodeID(name), kw(":type"), kw(":node"))
	}
	for _, edge := range spec.edges {
		add(nodeID(edge[0]), kw(":edge"), nodeID(edge[1]))
	}
	_, err := tx.Commit()
	require.NoError(t, err)
	return g
}

// tuples reads a result into one string per tuple, its values joined by ">",
// each value a node's name where it is an identity of the graph.
func (g graph) tuples(t testing.TB, rel executor.Relation) []string {
	t.Helper()
	var rendered []string
	require.NoError(t, executor.ForEach(rel, func(tuple executor.Tuple) error {
		parts := make([]string, len(tuple))
		for i, v := range tuple {
			if id, ok := v.(datalog.Identity); ok {
				name, named := g.names[id]
				require.True(t, named, "result identity %s names no node of the fixture", id)
				parts[i] = name
			} else {
				parts[i] = fmt.Sprint(v)
			}
		}
		rendered = append(rendered, strings.Join(parts, ">"))
		return nil
	}))
	return rendered
}

func TestRules_TransitiveClosureOnChain(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			g := writeGraph(t, d, chain(10))
			rel, err := d.Query(closureQuery, mustRuleSet(t, closureRules))
			require.NoError(t, err)
			require.ElementsMatch(t, chainClosure(10), g.tuples(t, rel))
		})
	}
}

func TestRules_TransitiveClosureOnCycle(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			g := writeGraph(t, d, cycle(10))
			rel, err := d.Query(closureQuery, mustRuleSet(t, closureRules))
			require.NoError(t, err)
			require.ElementsMatch(t, cycleClosure(10), g.tuples(t, rel))
		})
	}
}

func TestRules_StratifiedNegation(t *testing.T) {
	const rules = `[[(reach ?a ?b) [?a :edge ?b]]
	                [(reach ?a ?c) [?a :edge ?b] (reach ?b ?c)]
	                [(unreached ?n) [?n :type :node] (not (reach _ ?n))]]`
	spec := chain(5)
	spec.nodes = append(spec.nodes, "n6", "n7")
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			g := writeGraph(t, d, spec)
			rel, err := d.Query(`[:find ?n :in $ % :where (unreached ?n)]`, mustRuleSet(t, rules))
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"n1", "n6", "n7"}, g.tuples(t, rel))
		})
	}
}

func TestRules_InvocationInsideSubquery(t *testing.T) {
	const text = `[:find ?x ?n :in $ %
	               :where [?x :edge _]
	                      [(q [:find (count ?y) :in $ % ?x :where (reach ?x ?y)] $ % ?x) [[?n]]]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			g := writeGraph(t, d, chain(5))
			rel, err := d.Query(text, mustRuleSet(t, closureRules))
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"n1>4", "n2>3", "n3>2", "n4>1"}, g.tuples(t, rel))
		})
	}
}

func TestRules_InvocationInsideOrJoinBranch(t *testing.T) {
	const text = `[:find ?y :in $ % ?s :where (or-join [?y] (reach ?s ?y) [?y :edge ?s])]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			g := writeGraph(t, d, chain(5))
			rel, err := d.Query(text, mustRuleSet(t, closureRules), nodeID("n3"))
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"n2", "n4", "n5"}, g.tuples(t, rel))
		})
	}
}

// TestRules_InvocationInsideOrBranch: an invocation stands in a plain or
// branch as a pattern does. Both branches bind ?s and ?y, so the union is
// the nodes n3 reaches and the node whose edge reaches n3.
func TestRules_InvocationInsideOrBranch(t *testing.T) {
	const text = `[:find ?y :in $ % ?s :where (or (reach ?s ?y) [?y :edge ?s])]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			g := writeGraph(t, d, chain(5))
			rel, err := d.Query(text, mustRuleSet(t, closureRules), nodeID("n3"))
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"n2", "n4", "n5"}, g.tuples(t, rel))
		})
	}
}

func TestRules_InvocationUnderNotJoin(t *testing.T) {
	const text = `[:find ?n :in $ % :where [?n :type :node] (not-join [?n] (reach _ ?n))]`
	spec := chain(5)
	spec.nodes = append(spec.nodes, "n6")
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			g := writeGraph(t, d, spec)
			rel, err := d.Query(text, mustRuleSet(t, closureRules))
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"n1", "n6"}, g.tuples(t, rel))
		})
	}
}

func TestRules_BoundArgumentThroughInput(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			g := writeGraph(t, d, chain(5))
			rel, err := d.Query(`[:find ?y :in $ % ?start :where (reach ?start ?y)]`, mustRuleSet(t, closureRules), nodeID("n3"))
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"n4", "n5"}, g.tuples(t, rel))
		})
	}
}

// TestRules_BoundInvocationDerivesOnlyWhatItReaches binds the start of the
// closure over ten chains through an input and caps the derived tuples
// between one chain's closure and the graph's: the invocation derives the
// chain its start reaches and nothing of the other nine.
func TestRules_BoundInvocationDerivesOnlyWhatItReaches(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			opts := mode.plannerOptions()
			opts.MaxDerivedTuples = 100
			d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts})
			g := writeGraph(t, d, chains(100))
			rel, err := d.Query(`[:find ?y :in $ % ?start :where (reach ?start ?y)]`, mustRuleSet(t, closureRules), nodeID("n1"))
			require.NoError(t, err, "a bound invocation derives only what its start reaches")
			require.ElementsMatch(t, []string{"n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9", "n10", "n11"}, g.tuples(t, rel))
		})
	}
}

// TestRules_BoundInvocationByConstantDerivesOnlyWhatItReaches is the same
// contract with the start a constant argument of the invocation.
func TestRules_BoundInvocationByConstantDerivesOnlyWhatItReaches(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			opts := mode.plannerOptions()
			opts.MaxDerivedTuples = 100
			d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts})
			g := writeGraph(t, d, chains(100))
			rel, err := d.Query(`[:find ?y :in $ % :where (reach #id "node/n12" ?y)]`, mustRuleSet(t, closureRules))
			require.NoError(t, err, "a constant start derives only what it reaches")
			require.ElementsMatch(t, []string{"n13", "n14", "n15", "n16", "n17", "n18", "n19", "n20", "n21", "n22"}, g.tuples(t, rel))
		})
	}
}

// TestRules_RequiredBinding: a predicate requiring its first position bound,
// invoked before the expression that binds it in the written order. Query
// orders the invocation after its provider, and the plan-only surfaces plan
// the same order: the invocation's phase has the required variable bound.
func TestRules_RequiredBinding(t *testing.T) {
	const rules = `[[(succ [?n] ?m) [(+ ?n 1) ?m]]]`
	const text = `[:find ?m :in $ % :where (succ ?n ?m) [(ground 5) ?n]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			rs := mustRuleSet(t, rules)

			tuples, err := executor.CollectTuples(d.Query(text, rs))
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(6)}}, tuples)

			plan, err := d.Explain(text, rs)
			require.NoError(t, err)
			bound := false
			for _, phase := range plan.Phases {
				for _, inv := range phase.Invocations {
					bound = bound || query.ContainsSymbol(inv.Bound, datalog.NewSymbol("?n"))
				}
			}
			require.True(t, bound, "the plan binds ?n before the invocation:\n%s", plan)

			explanation, err := d.ExplainAlgebra(text, rs)
			require.NoError(t, err)
			bound = false
			for _, phase := range explanation.Plan.Phases {
				for _, inv := range phase.Invocations {
					bound = bound || query.ContainsSymbol(inv.Bound, datalog.NewSymbol("?n"))
				}
			}
			require.True(t, bound, "the algebra explanation's plan binds ?n before the invocation")

			analysis, err := d.Analyze(text, rs)
			require.NoError(t, err)
			tuples, err = executor.CollectTuples(analysis.Result, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(6)}}, tuples)
		})
	}
}

func TestRules_ConstantAndBlankArguments(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			g := writeGraph(t, d, chain(5))
			rules := mustRuleSet(t, closureRules)

			rel, err := d.Query(`[:find ?y :in $ % :where (reach #id "node/n3" ?y)]`, rules)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"n4", "n5"}, g.tuples(t, rel))

			rel, err = d.Query(`[:find ?x :in $ % :where (reach ?x _)]`, rules)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"n1", "n2", "n3", "n4"}, g.tuples(t, rel))
		})
	}
}

func TestRules_RulesInputAsText(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			g := writeGraph(t, d, chain(4))
			rel, err := d.Query(closureQuery, closureRules)
			require.NoError(t, err)
			require.ElementsMatch(t, chainClosure(4), g.tuples(t, rel))
		})
	}
}

// TestRules_RulesTextResolvesOnce: rule text given at % is parsed once per
// text.
func TestRules_RulesTextResolvesOnce(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			q, err := parser.ParseQuery(closureQuery)
			require.NoError(t, err)
			_, first, err := d.convertInputsToRelations(q, []interface{}{closureRules})
			require.NoError(t, err)
			_, second, err := d.convertInputsToRelations(q, []interface{}{closureRules})
			require.NoError(t, err)
			require.Same(t, first, second, "the same rule text resolves to one rule set")
		})
	}
}

func TestRules_RulesInputOfAnotherTypeRefused(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			_, err := d.Query(closureQuery, 42)
			require.ErrorContains(t, err, "int")
			require.ErrorContains(t, err, "%")
		})
	}
}

func TestRules_BodyAttributeRefused(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, _ := refusingDB(t, mode)
			rules := mustRuleSet(t, `[[(held ?e ?v) [?e :t/strnig ?v]]]`)
			_, err := d.Query(`[:find ?v :in $ % :where (held _ ?v)]`, rules)
			requireUnknown(t, err, kw(":t/strnig"))
		})
	}
}

func TestRules_IterationLimit(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			opts := mode.plannerOptions()
			opts.MaxRuleIterations = 2
			d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts})
			writeGraph(t, d, chain(10))
			_, err := d.Query(closureQuery, mustRuleSet(t, closureRules))
			var limit *query.RuleIterationLimitError
			require.ErrorAs(t, err, &limit)
			require.Equal(t, []query.Symbol{datalog.NewSymbol("reach")}, limit.Predicates)
			require.Equal(t, 2, limit.Limit)
			require.Equal(t, 3, limit.Reached)
		})
	}
}

func TestRules_DerivedTupleLimit(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			opts := mode.plannerOptions()
			opts.MaxDerivedTuples = 7
			d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts})
			writeGraph(t, d, chain(5))
			_, err := d.Query(closureQuery, mustRuleSet(t, closureRules))
			var limit *query.DerivedTupleLimitError
			require.ErrorAs(t, err, &limit)
			require.Equal(t, []query.Symbol{datalog.NewSymbol("reach")}, limit.Predicates)
			require.Equal(t, 7, limit.Limit)
			require.Greater(t, limit.Reached, 7)
		})
	}
}

func TestRules_UnknownRuleNamed(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			_, err := d.Query(`[:find ?x ?y :in $ % :where (reahc ?x ?y)]`, mustRuleSet(t, closureRules))
			var unknown *query.UnknownRuleError
			require.ErrorAs(t, err, &unknown)
			require.Equal(t, datalog.NewSymbol("reahc"), unknown.Name)
			require.Equal(t, []query.Symbol{datalog.NewSymbol("reach")}, unknown.Defined)
		})
	}
}

func TestRules_ArityMismatchNamed(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			_, err := d.Query(`[:find ?x :in $ % :where (reach ?x)]`, mustRuleSet(t, closureRules))
			var arity *query.RuleArityError
			require.ErrorAs(t, err, &arity)
			require.Equal(t, datalog.NewSymbol("reach"), arity.Name)
			require.Equal(t, 2, arity.Expected)
			require.Equal(t, 1, arity.Got)
		})
	}
}

// TestRules_EvaluatesUnderOneSnapshot commits an edge from the first
// iteration event of a closure. The closure never reads it; the next query
// does.
func TestRules_EvaluatesUnderOneSnapshot(t *testing.T) {
	spec := chain(5)
	spec.nodes = append(spec.nodes, "n6")
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var d *Database
			var once sync.Once
			var writeErr error
			d = createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: func(e annotations.Event) {
				if e.Name != annotations.RulesIteration {
					return
				}
				once.Do(func() {
					tx := d.NewTransaction()
					if err := tx.Add(nodeID("n5"), kw(":edge"), nodeID("n6")); err != nil {
						writeErr = err
						return
					}
					_, writeErr = tx.Commit()
				})
			}})
			g := writeGraph(t, d, spec)
			rules := mustRuleSet(t, closureRules)

			rel, err := d.Query(closureQuery, rules)
			require.NoError(t, err)
			closure := g.tuples(t, rel)
			require.NoError(t, writeErr)
			require.ElementsMatch(t, chainClosure(5), closure)

			rel, err = d.Query(closureQuery, rules)
			require.NoError(t, err)
			require.ElementsMatch(t, chainClosure(6), g.tuples(t, rel))
		})
	}
}

// TestRules_InvocationInsideOrDefaultBranch: an invocation stands in an
// or-default and an or-default-join branch as a pattern does. A node that
// reaches others takes its reachable nodes from the first branch, and a node
// that reaches none takes the default.
func TestRules_InvocationInsideOrDefaultBranch(t *testing.T) {
	queries := []struct{ name, text string }{
		{"or-default", `[:find ?n ?m :in $ % :where [?n :type :node] (or-default (reach ?n ?m) [(ground :dead-end) ?m])]`},
		{"or-default-join", `[:find ?n ?m :in $ % :where [?n :type :node] (or-default-join [[?n] ?m] (reach ?n ?m) [(ground :dead-end) ?m])]`},
	}
	want := append(chainClosure(3), "n3>:dead-end")
	for _, q := range queries {
		for _, mode := range optimizerModes {
			t.Run(q.name+"/"+mode.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				g := writeGraph(t, d, chain(3))
				rel, err := d.Query(q.text, mustRuleSet(t, closureRules))
				require.NoError(t, err)
				require.ElementsMatch(t, want, g.tuples(t, rel))
			})
		}
	}
}
