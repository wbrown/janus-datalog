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
	rendered, err := g.render(rel)
	require.NoError(t, err)
	return rendered
}

// render reads a result into one string per tuple, its values joined by ">",
// each value a node's name where it is an identity of the graph. An identity
// of no node is an error, so a goroutine that is not the test's can render.
func (g graph) render(rel executor.Relation) ([]string, error) {
	var rendered []string
	err := executor.ForEach(rel, func(tuple executor.Tuple) error {
		parts := make([]string, len(tuple))
		for i, v := range tuple {
			if id, ok := v.(datalog.Identity); ok {
				name, named := g.names[id]
				if !named {
					return fmt.Errorf("result identity %s names no node of the fixture", id)
				}
				parts[i] = name
			} else {
				parts[i] = fmt.Sprint(v)
			}
		}
		rendered = append(rendered, strings.Join(parts, ">"))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rendered, nil
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

// TestRules_InvocationInsideOrJoinBranch: an invocation stands in an or-join
// branch as a pattern does, and the input both branches read reaches them
// through the required run (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §2.2).
func TestRules_InvocationInsideOrJoinBranch(t *testing.T) {
	const text = `[:find ?y :in $ % ?s :where (or-join [[?s] ?y] (reach ?s ?y) [?y :edge ?s])]`
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

// TestRules_InvocationInsideABranchBoundByTheBranchsClauseDerivesWhatItReaches
// is the same contract for a closure whose recursion sits inside an or-join
// branch, bound by the branch's own pattern: the rewrite adorns the
// recursion from that pattern, so the invocation derives the chain its start
// reaches and nothing of the other nine, with the rule's required run named
// and under a flat header (NESTED_SCOPES_AS_QUERIES.md §5.5).
func TestRules_InvocationInsideABranchBoundByTheBranchsClauseDerivesWhatItReaches(t *testing.T) {
	shapes := []struct {
		name  string
		rules string
	}{
		{"the required run named", `[[(reach [?x] ?y) (or-join [[?x] ?y] [?x :edge ?y] (and [?x :edge ?z] (reach ?z ?y)))]]`},
		{"a flat header", `[[(reach ?x ?y) (or-join [?x ?y] [?x :edge ?y] (and [?x :edge ?z] (reach ?z ?y)))]]`},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			for _, mode := range optimizerModes {
				t.Run(mode.name, func(t *testing.T) {
					opts := mode.plannerOptions()
					opts.MaxDerivedTuples = 100
					d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts})
					g := writeGraph(t, d, chains(100))
					rel, err := d.Query(`[:find ?y :in $ % ?start :where (reach ?start ?y)]`, mustRuleSet(t, shape.rules), nodeID("n1"))
					require.NoError(t, err, "the recursion inside the branch is adorned from the branch's pattern and derives what the start reaches")
					require.ElementsMatch(t, []string{"n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9", "n10", "n11"}, g.tuples(t, rel))
				})
			}
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

// A query declares % once. A query declaring it twice is refused, naming the
// input, and neither rule set given is read.
func TestRules_SecondRulesInputRefused(t *testing.T) {
	const other = `[[(reach ?a ?b) [?a :type ?b]]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			writeGraph(t, d, chain(3))
			_, err := d.Query(`[:find ?x ?y :in $ % % :where (reach ?x ?y)]`, mustRuleSet(t, closureRules), mustRuleSet(t, other))
			require.Error(t, err, "a query declares % once")
			require.ErrorContains(t, err, "%")
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

// A rule's variables belong to the rule. A query input that shares a name with
// one binds nothing inside the body: the invocation's arguments are the only
// way the query's bindings reach a rule.
func TestRules_QueryInputSharingARuleVariableNameBindsNothingInTheBody(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			g := writeGraph(t, d, chain(5))
			rules := mustRuleSet(t, closureRules)
			for _, input := range []string{"?start", "?a", "?b", "?c"} {
				text := fmt.Sprintf(`[:find ?y :in $ %% %s :where (reach %s ?y)]`, input, input)
				rel, err := d.Query(text, rules, nodeID("n1"))
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"n2", "n3", "n4", "n5"}, g.tuples(t, rel),
					"the input %s names the start and nothing inside the rule", input)
			}
			rel, err := d.Query(`[:find ?x ?y :in $ % ?b :where (reach ?x ?y)]`, rules, nodeID("n1"))
			require.NoError(t, err)
			require.ElementsMatch(t, chainClosure(5), g.tuples(t, rel),
				"an input the query never reads binds nothing inside the rule")
		})
	}
}

// A rule whose recursion sits inside an or or an or-join branch derives what
// its other branch states, whole and bound, as the same rule split into two
// derives it.
func TestRules_RecursionInsideAnOrBranchDerivesTheOtherBranch(t *testing.T) {
	sets := []struct{ name, rules string }{
		{"or", `[[(r ?a ?c) (or [?a :edge ?c] (and [?a :edge ?b] (r ?b ?c)))]]`},
		{"or-join", `[[(r ?a ?c) (or-join [?a ?c] [?a :edge ?c] (and [?a :edge ?b] (r ?b ?c)))]]`},
	}
	for _, set := range sets {
		for _, mode := range optimizerModes {
			t.Run(set.name+"/"+mode.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				g := writeGraph(t, d, chain(4))
				rules := mustRuleSet(t, set.rules)
				rel, err := d.Query(`[:find ?x ?y :in $ % :where (r ?x ?y)]`, rules)
				require.NoError(t, err)
				require.ElementsMatch(t, chainClosure(4), g.tuples(t, rel), "the whole closure")
				rel, err = d.Query(`[:find ?y :in $ % ?s :where (r ?s ?y)]`, rules, nodeID("n1"))
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"n2", "n3", "n4"}, g.tuples(t, rel), "the closure from n1")
			})
		}
	}
}

// An or-join's required run is the branch's correlation with its scope under
// the delta mark and under the rewrite. A body's or-join declares the edge's
// end, bound by the pattern before it, as its required run: queried whole,
// with the branch reading the end through an expression alone, it derives
// the closure, and bound at the start, with the branch passing the end to
// the recursion, it derives the chain the start reaches and nothing of the
// other nine.
func TestRules_OrJoinWithARequiredRunInARecursiveBody(t *testing.T) {
	const readingTheEnd = `[[(r ?a ?b) [?a :edge ?b]]
	                        [(r ?a ?c) [?a :edge ?b] (or-join [[?b] ?c] (and [(identity ?b) ?k] (r ?k ?c)) [?b :edge ?c])]]`
	const passingTheEnd = `[[(r ?a ?b) [?a :edge ?b]]
	                        [(r ?a ?c) [?a :edge ?b] (or-join [[?b] ?c] (r ?b ?c) [?b :edge ?c])]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			g := writeGraph(t, d, chain(4))
			rel, err := d.Query(`[:find ?x ?y :in $ % :where (r ?x ?y)]`, mustRuleSet(t, readingTheEnd))
			require.NoError(t, err)
			require.ElementsMatch(t, chainClosure(4), g.tuples(t, rel), "the whole closure")

			opts := mode.plannerOptions()
			opts.MaxDerivedTuples = 100
			bounded := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts})
			g = writeGraph(t, bounded, chains(100))
			rel, err = bounded.Query(`[:find ?y :in $ % ?start :where (r ?start ?y)]`, mustRuleSet(t, passingTheEnd), nodeID("n1"))
			require.NoError(t, err, "the branch's invocation is bound by the required run, so the start's chain is derived and nothing of the other nine")
			require.ElementsMatch(t, []string{"n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9", "n10", "n11"}, g.tuples(t, rel), "the closure from n1")
		})
	}
}

// A bound invocation derives what the whole invocation derives at that
// binding, when the bound position is one that only a branch of the recursive
// rule produces: here n4 alone is marked, so n3 reaches :end through the
// recursive rule's other branch, and n1 and n2 reach :end only through the
// recursion, while nothing reaches :end through the base rule.
func TestRules_BoundInvocationKeepsWhatOnlyTheRecursiveRulesOtherBranchProduces(t *testing.T) {
	const rules = `[[(reach ?a ?b) [?a :edge ?b]]
	                [(reach ?a ?c) [?a :edge ?b] (or (reach ?b ?c) (and [?b :marked true] [(ground :end) ?c]))]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			g := writeGraph(t, d, chain(4))
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(nodeID("n4"), kw(":marked"), true))
			_, err := tx.Commit()
			require.NoError(t, err)
			rs := mustRuleSet(t, rules)
			rel, err := d.Query(`[:find ?x ?y :in $ % :where (reach ?x ?y)]`, rs)
			require.NoError(t, err)
			require.ElementsMatch(t, append(chainClosure(4), "n1>:end", "n2>:end", "n3>:end"), g.tuples(t, rel), "the whole closure")
			rel, err = d.Query(`[:find ?x :in $ % ?end :where (reach ?x ?end)]`, rs, kw(":end"))
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"n1", "n2", "n3"}, g.tuples(t, rel), "the nodes reaching :end, bound at the second position")
		})
	}
}

// scanSignalStore runs the signal it is armed with once, after the first scan
// it serves from then on returns, whether the scan comes through the store or
// through a session it opened. A scan before arm runs nothing, and so does a
// scan the signal itself makes.
type scanSignalStore struct {
	Store
	mu     sync.Mutex
	signal func()
}

func (s *scanSignalStore) arm(signal func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.signal = signal
}

func (s *scanSignalStore) fire() {
	s.mu.Lock()
	signal := s.signal
	s.signal = nil
	s.mu.Unlock()
	if signal != nil {
		signal()
	}
}

func (s *scanSignalStore) Scan(bound ScanBound) (Iterator, error) {
	iter, err := s.Store.Scan(bound)
	s.fire()
	return iter, err
}

func (s *scanSignalStore) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	iter, err := s.Store.ScanKeysOnly(bound)
	s.fire()
	return iter, err
}

func (s *scanSignalStore) NewReadSession() (ReadSession, error) {
	session, err := s.Store.NewReadSession()
	if err != nil {
		return nil, err
	}
	return &scanSignalSession{ReadSession: session, store: s}, nil
}

type scanSignalSession struct {
	ReadSession
	store *scanSignalStore
}

func (s *scanSignalSession) Scan(bound ScanBound) (Iterator, error) {
	iter, err := s.ReadSession.Scan(bound)
	s.store.fire()
	return iter, err
}

func (s *scanSignalSession) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	iter, err := s.ReadSession.ScanKeysOnly(bound)
	s.store.fire()
	return iter, err
}

// Analyze evaluates rules under one snapshot, as Query does: an edge into the
// chain's head, committed after the evaluation's first scan, is in no
// iteration's reading, and the next query reads it. An evaluation reading
// the edge in a later iteration alone would carry every pair from n0 past
// n1 and no pair (n0, n1).
func TestRules_AnalyzeEvaluatesUnderOneSnapshot(t *testing.T) {
	spec := chain(5)
	spec.nodes = append(spec.nodes, "n0")
	withHead := append(chainClosure(5), "n0>n1", "n0>n2", "n0>n3", "n0>n4", "n0>n5")
	for _, algebra := range []bool{false, true} {
		t.Run(fmt.Sprintf("algebra_%v", algebra), func(t *testing.T) {
			opts := DefaultPlannerOptions()
			opts.EnableAlgebraOptimizer = algebra
			store := &scanSignalStore{Store: NewBadgerEmulatorStore(&BinaryKeyEncoder{})}
			d, err := NewDatabaseWithOptions(DatabaseOptions{Store: store, PlannerOptions: &opts})
			require.NoError(t, err)
			defer d.Close()
			g := writeGraph(t, d, spec)
			rules := mustRuleSet(t, closureRules)

			var writeErr error
			store.arm(func() {
				tx := d.NewTransaction()
				if err := tx.Add(nodeID("n0"), kw(":edge"), nodeID("n1")); err != nil {
					writeErr = err
					return
				}
				_, writeErr = tx.Commit()
			})
			analysis, err := d.Analyze(closureQuery, rules)
			require.NoError(t, err)
			require.NoError(t, writeErr)
			require.ElementsMatch(t, chainClosure(5), g.tuples(t, analysis.Result), "the closure of the snapshot the evaluation opened on")

			rel, err := d.Query(closureQuery, rules)
			require.NoError(t, err)
			require.ElementsMatch(t, withHead, g.tuples(t, rel))
		})
	}
}

// A bound invocation runs wherever the whole invocation runs. The marked chain
// is n1 → n2 → n3 → n4 with n2 marked. The first rule set reads a variable in
// a clause written before the clause binding it; the second reads a lower
// predicate inside an or-default of a recursive rule.
func TestRules_BoundInvocationRunsWhereverTheWholeInvocationRuns(t *testing.T) {
	cases := []struct {
		name, rules, whole, bound string
		wantWhole, wantBound      []string
	}{
		{
			"a clause before the invocation reading a later binding",
			`[[(reach ?a ?b) [?a :edge ?b]]
			  [(reach ?a ?c) [?a :edge ?b] (reach ?b ?c)]
			  [(t ?a ?c) (not-join [?w] [?w :marked true]) (reach ?a ?c) [?w :edge ?c]]]`,
			`[:find ?x ?y :in $ % :where (t ?x ?y)]`,
			`[:find ?y :in $ % ?s :where (t ?s ?y)]`,
			[]string{"n1>n2", "n1>n4", "n2>n4", "n3>n4"},
			[]string{"n2", "n4"},
		},
		{
			"an or-default over a lower predicate in a recursive rule",
			`[[(marked ?n) [?n :marked true]]
			  [(walk ?a ?b ?l) [?a :edge ?b] [(ground :e) ?l]]
			  [(walk ?a ?c ?l) [?a :edge ?b] (or-default (and (marked ?b) [(ground :m) ?l]) [(ground :u) ?l]) (walk ?b ?c _)]]`,
			`[:find ?x ?y ?l :in $ % :where (walk ?x ?y ?l)]`,
			`[:find ?y ?l :in $ % ?s :where (walk ?s ?y ?l)]`,
			[]string{"n1>n2>:e", "n2>n3>:e", "n3>n4>:e", "n1>n3>:m", "n1>n4>:m", "n2>n4>:u"},
			[]string{"n2>:e", "n3>:m", "n4>:m"},
		},
	}
	for _, c := range cases {
		for _, mode := range optimizerModes {
			t.Run(c.name+"/"+mode.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				g := writeGraph(t, d, chain(4))
				tx := d.NewTransaction()
				require.NoError(t, tx.Add(nodeID("n2"), kw(":marked"), true))
				_, err := tx.Commit()
				require.NoError(t, err)
				rs := mustRuleSet(t, c.rules)

				rel, err := d.Query(c.whole, rs)
				require.NoError(t, err)
				require.ElementsMatch(t, c.wantWhole, g.tuples(t, rel), "the whole invocation")

				rel, err = d.Query(c.bound, rs, nodeID("n1"))
				require.NoError(t, err, "the invocation bound to n1 runs as the whole one does")
				require.ElementsMatch(t, c.wantBound, g.tuples(t, rel), "the invocation bound to n1")
			})
		}
	}
}

// A predicate that invokes a required-binding predicate evaluates whole
// through one rewrite: the required position is bound inside the invoking
// rule's own body, from which the required predicate is adorned. The chain
// of four carries ranks 1 to 4 with n2 marked. The first case invokes such a
// predicate free, rewritten with its head all free
// (NESTED_SCOPES_AS_QUERIES.md §5.5). The second binds a recursive predicate
// one of whose rules invokes a lower predicate after the recursion, so that
// predicate's magic rule reads the adorned recursion, and another of whose
// rules negates it, where adorning the negated site would close a cycle
// through the negation: that site is left unadorned, the rewrite is
// accepted, and the query answers (NON_MONOTONE_FORMS_PROOF.md T6).
func TestRules_PredicateInvokingARequiredBindingPredicateEvaluatesWhole(t *testing.T) {
	cases := []struct {
		name, rules, text string
		inputs            []interface{}
		want              []string
		rewrites          [][]bool
	}{
		{
			"free invocation",
			`[[(succ [?n] ?m) [(+ ?n 1) ?m]]
			  [(next ?a ?r) [?a :rank ?k] (succ ?k ?r)]]`,
			`[:find ?a ?r :in $ % :where (next ?a ?r)]`,
			nil,
			[]string{"n1>2", "n2>3", "n3>4", "n4>5"},
			[][]bool{{false, false}},
		},
		{
			"bound invocation whose negated site is left unadorned",
			`[[(marked ?n) [?n :marked true]]
			  [(succ [?n] ?m) [(+ ?n 1) ?m]]
			  [(walk ?a ?b) [?a :edge ?b]]
			  [(walk ?a ?c) [?a :edge ?b] (walk ?b ?c) (marked ?b) [?b :rank ?k] (succ ?k ?j) [(< ?j 100)]]
			  [(walk ?a ?c) [?a :edge ?b] (not (marked ?b)) [?b :rank ?k] (succ ?k ?j) [(< ?j 100)] (walk ?b ?c)]]`,
			`[:find ?y :in $ % ?s :where (walk ?s ?y)]`,
			[]interface{}{nodeID("n2")},
			[]string{"n3", "n4"},
			[][]bool{{true, false}},
		},
	}
	for _, c := range cases {
		for _, mode := range optimizerModes {
			t.Run(c.name+"/"+mode.name, func(t *testing.T) {
				adorned := &rewriteAdornments{}
				d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: adorned.handle})
				g := writeGraph(t, d, chain(4))
				tx := d.NewTransaction()
				for i := 1; i <= 4; i++ {
					require.NoError(t, tx.Add(nodeID(nodeName(i)), kw(":rank"), int64(i)))
				}
				require.NoError(t, tx.Add(nodeID("n2"), kw(":marked"), true))
				_, err := tx.Commit()
				require.NoError(t, err)

				rs := mustRuleSet(t, c.rules)
				inputs := append([]interface{}{rs}, c.inputs...)
				rel, err := d.Query(c.text, inputs...)
				require.NoError(t, err, "the predicate evaluates whole")
				require.ElementsMatch(t, c.want, g.tuples(t, rel))
				require.Equal(t, c.rewrites, adorned.made(), "one rewrite, under the invocation's adornment")
			})
		}
	}
}

// A free invocation of a predicate whose strata hold a required one is
// rewritten with its head all free, so one rewrite and one evaluation derive
// the predicate whole, the required predicate adorned from the clauses before
// its invocation, and a required predicate recursive with a free one builds
// (NESTED_SCOPES_AS_QUERIES.md §5.5): walk, free, invokes the required hop
// after the pattern binding its argument, and hop invokes walk back.
func TestFreeInvocationRewritesTheRequiredPredicatesOfItsStratum(t *testing.T) {
	const mutual = `[[(hop [?x] ?y) [?x :edge ?y]]
	                 [(hop [?x] ?y) [?x :edge ?m] (walk ?m ?y)]
	                 [(walk ?a ?b) [?a :edge _] (hop ?a ?b)]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			adorned := &rewriteAdornments{}
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: adorned.handle})
			g := writeGraph(t, d, chain(4))
			rel, err := d.Query(`[:find ?a ?b :in $ % :where (walk ?a ?b)]`, mustRuleSet(t, mutual))
			require.NoError(t, err, "the free invocation of walk derives it whole")
			require.ElementsMatch(t, chainClosure(4), g.tuples(t, rel))
			require.Equal(t, [][]bool{{false, false}}, adorned.made(), "one rewrite, the free invocation with its head all free")
		})
	}
}

// Every phase of a plan parses back to itself: a correlated count over an
// invocation, which the optimizer decorrelates into an aggregate, and a plan
// with a projection inserted between its joins included.
func TestRules_EveryPhaseParsesBackToItself(t *testing.T) {
	queries := []string{
		`[:find ?x ?n :in $ % :where [?x :edge _] [(q [:find (count ?y) :in $ % ?x :where [?x :edge _] (reach ?x ?y)] $ % ?x) [[?n]]]]`,
		`[:find ?y :in $ % :where [?x :type :node] (reach ?x ?y) [?y :type ?t]]`,
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			opts := mode.plannerOptions()
			opts.EnableJoinProjectInsertion = true
			d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts})
			writeGraph(t, d, chain(4))
			rules := mustRuleSet(t, closureRules)
			for _, text := range queries {
				plan, err := d.Explain(text, rules)
				require.NoError(t, err)
				for i, phase := range plan.Phases {
					rendered := phase.Query.String()
					back, err := parser.ParseQuery(rendered)
					require.NoError(t, err, "phase %d of %s parses back:\n%s", i, text, rendered)
					require.Equal(t, rendered, back.String(), "phase %d of %s renders the same after a round trip", i, text)
				}
			}
		})
	}
}

// Explain's record of an invocation names every argument the clauses before
// it bind, an or and an or-join among the binders, since execution seeds the
// rewrite from those bindings.
func TestRules_ExplainBoundNamesAnArgumentAnOrBinds(t *testing.T) {
	queries := []struct{ name, text string }{
		{"or", `[:find ?x ?y :in $ % :where (or [?x :edge ?z] [?z :edge ?x]) (reach ?x ?y)]`},
		{"or-join", `[:find ?x ?y :in $ % :where (or-join [?x ?z] [?x :edge ?z] [?z :edge ?x]) (reach ?x ?y)]`},
	}
	for _, q := range queries {
		for _, mode := range optimizerModes {
			t.Run(q.name+"/"+mode.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				writeGraph(t, d, chain(3))
				plan, err := d.Explain(q.text, mustRuleSet(t, closureRules))
				require.NoError(t, err)
				var invocations int
				for _, phase := range plan.Phases {
					for _, inv := range phase.Invocations {
						invocations++
						require.True(t, query.ContainsSymbol(inv.Bound, datalog.NewSymbol("?x")),
							"the %s before the invocation binds ?x:\n%s", q.name, plan)
					}
				}
				require.Equal(t, 1, invocations, "the plan records the one invocation")
			})
		}
	}
}

// ruleEventCounts counts the rule events a query emits, from the parallel
// workers the engine does not serialize, and keeps the trail of those
// events in the order they arrived: the rewrites, the strata begun, the
// tuples that entered a delta, and whether every iteration event continued
// its stratum's numbering or restarted it at zero, a window's first
// iteration.
type ruleEventCounts struct {
	mu                        sync.Mutex
	rewrites, strata, entered int
	last                      map[query.Symbol]int // per stratum, by its first predicate, the last iteration numbered
	misnumbered               bool
	trail                     []string
}

func (c *ruleEventCounts) handle(e annotations.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch e.Name {
	case annotations.RulesMagicSets:
		c.rewrites++
		c.trail = append(c.trail, fmt.Sprintf("%s %v %v", e.Name, e.Data["predicate"], e.Data["adornment"]))
	case annotations.RulesStratumBegin:
		c.strata++
		c.trail = append(c.trail, fmt.Sprintf("%s %v", e.Name, e.Data["predicates"]))
	case annotations.RulesIteration:
		for _, size := range e.Data["delta"].(map[query.Symbol]int) {
			c.entered += size
		}
		stratum := e.Data["predicates"].([]query.Symbol)[0]
		iteration := e.Data["iteration"].(int)
		if last, seen := c.last[stratum]; seen && iteration != 0 && iteration != last+1 {
			c.misnumbered = true
		}
		if c.last == nil {
			c.last = map[query.Symbol]int{}
		}
		c.last[stratum] = iteration
		c.trail = append(c.trail, fmt.Sprintf("%s %v iteration %v", e.Name, e.Data["predicates"], iteration))
	case annotations.RulesStratumComplete:
		c.trail = append(c.trail, fmt.Sprintf("%s %v", e.Name, e.Data["predicates"]))
	case annotations.ScopeWindow:
		c.trail = append(c.trail, fmt.Sprintf("%s %v ordinal %v tuples %v", e.Name, e.Data[annotations.KeyForm], e.Data["ordinal"], e.Data["tuple_count"]))
	case annotations.ScopeBranchBuild:
		c.trail = append(c.trail, fmt.Sprintf("%s %v branch %v size %v", e.Name, e.Data[annotations.KeyForm], e.Data["branch"], e.Data["branch_size"]))
	}
}

func (c *ruleEventCounts) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rewrites, c.strata, c.entered = 0, 0, 0
	c.last, c.misnumbered = nil, false
	c.trail = nil
}

// snapshot is the rewrites, the strata begun and the tuples that entered a
// delta.
func (c *ruleEventCounts) snapshot() [3]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return [3]int{c.rewrites, c.strata, c.entered}
}

// numberedPerWindow reports whether every iteration event continued its
// stratum's numbering or restarted it at zero.
func (c *ruleEventCounts) numberedPerWindow() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.misnumbered
}

// events renders the trail, one event per line.
func (c *ruleEventCounts) events() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	rendered := ""
	for _, line := range c.trail {
		rendered += line + "\n"
	}
	return rendered
}

// An invocation evaluates its predicate once per query, whatever clause form
// holds it: the rewrites, the strata begun and the tuples that enter a delta
// under not, not-join, or, or-join or or-default are the counts a top-level
// invocation bound at the same positions produces, so each tuple is derived
// once however the form's subject is split into windows, and each window's
// iterations number from zero
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.2 and §4.9). The chain
// of ten with its sink bound gives every form the same seed. An invocation
// inside a form is bound
// by the form's header (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §2.2): the
// headers of the not, the not-join, the or-join and the or-default bind both
// positions, so their baseline is bound at both, and the or's inferred header
// `[[?s] ?x]` binds ?s alone (§2.3), so its baseline is bound at ?s alone. A
// rule inside a subquery inside a branch costs the same one fixpoint: the
// subquery runs once over the relation of its argument combinations, so its
// invocation, bound at both positions by the inner query's inputs, is seeded
// once from every combination.
func TestRules_InvocationUnderAFormEvaluatesOncePerQuery(t *testing.T) {
	const boundAtBoth = `[:find ?x :in $ % ?s :where [?x :type :node] (reach ?x ?s)]`
	const boundAtSink = `[:find ?x :in $ % ?s :where (reach ?x ?s)]`
	forms := []struct {
		name, text, baseline string
		want                 []string
	}{
		{"not", `[:find ?x :in $ % ?s :where [?x :type :node] (not (reach ?x ?s))]`, boundAtBoth, []string{"n10"}},
		{"not-join", `[:find ?x :in $ % ?s :where [?x :type :node] (not-join [?x ?s] (reach ?x ?s))]`, boundAtBoth, []string{"n10"}},
		{"or", `[:find ?x :in $ % ?s :where [?x :type :node] (or (reach ?x ?s) [?x :marked true])]`, boundAtSink, chainReaching(10)},
		{"or-join", `[:find ?x :in $ % ?s :where [?x :type :node] (or-join [[?x ?s]] (reach ?x ?s) (and [?x :marked true] [?x :edge ?s]))]`, boundAtBoth, chainReaching(10)},
		{"or-default", `[:find ?x ?k :in $ % ?s :where [?x :type :node] (or-default (and (reach ?x ?s) [(ground 1) ?k]) [(ground 0) ?k])]`, boundAtBoth, append(chainReachingWith(10, ">1"), "n10>0")},
		{"subquery in a branch", `[:find ?x ?k :in $ % ?s :where [?x :type :node] (or-default [(q [:find (count ?y) :in $ % ?x ?s :where (reach ?x ?s) [?x :edge ?y]] $ % ?x ?s) [[?k]]] [(ground 0) ?k])]`, boundAtBoth, append(chainReachingWith(10, ">1"), "n10>0")},
	}
	for _, form := range forms {
		for _, mode := range optimizerModes {
			t.Run(form.name+"/"+mode.name, func(t *testing.T) {
				counts := &ruleEventCounts{}
				d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: counts.handle})
				g := writeGraph(t, d, chain(10))
				rules := mustRuleSet(t, closureRules)

				rel, err := d.Query(form.baseline, rules, nodeID("n10"))
				require.NoError(t, err)
				require.ElementsMatch(t, chainReaching(10), g.tuples(t, rel))
				atBaseline := counts.snapshot()
				require.Equal(t, 1, atBaseline[0], "the top-level invocation rewrites once")

				counts.reset()
				rel, err = d.Query(form.text, rules, nodeID("n10"))
				require.NoError(t, err)
				require.ElementsMatch(t, form.want, g.tuples(t, rel))
				require.Equal(t, atBaseline, counts.snapshot(), "rewrites, strata and the tuples entering a delta under %s, against the top-level invocation's; the events:\n%s", form.name, counts.events())
				require.True(t, counts.numberedPerWindow(), "each window's iterations number from zero under %s; the events:\n%s", form.name, counts.events())
			})
		}
	}
}

// chainReaching lists the nodes of chain(n) that reach its sink: every node
// but the sink.
func chainReaching(n int) []string {
	var names []string
	for i := 1; i < n; i++ {
		names = append(names, nodeName(i))
	}
	return names
}

// chainReachingWith is chainReaching with suffix appended to each name.
func chainReachingWith(n int, suffix string) []string {
	names := chainReaching(n)
	for i := range names {
		names[i] += suffix
	}
	return names
}

// A rule whose only invocation of its own stratum sits inside a subquery
// whose inner query does not aggregate derives the closure on both planner
// paths: the subquery joins the inner query's result with the scope over its
// combinations, monotone as a branch is. Bound at a node, the invocation
// derives what that node reaches.
func TestRules_InvocationOfTheOwnStratumInsideANonAggregatingSubqueryDerivesTheClosure(t *testing.T) {
	const rules = `[[(reach ?x ?y) [?x :edge ?y]]
	                [(reach ?x ?z) [?x :edge ?y] [(q [:find ?w :in $ % ?y :where (reach ?y ?w)] $ % ?y) [[?z] ...]]]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			g := writeGraph(t, d, chain(4))
			rs := mustRuleSet(t, rules)
			rel, err := d.Query(`[:find ?x ?z :in $ % :where (reach ?x ?z)]`, rs)
			require.NoError(t, err)
			require.ElementsMatch(t, chainClosure(4), g.tuples(t, rel))
			rel, err = d.Query(`[:find ?z :in $ % ?x :where (reach ?x ?z)]`, rs, nodeID("n2"))
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"n3", "n4"}, g.tuples(t, rel))
		})
	}
}

// Two invocations of one predicate under one form, bound at the same position
// to different nodes, each read what their own node reaches: on the chain of
// six, n5 and n2 both reach n6, and n2 is not reached from n5, so a not over
// both invocations excludes the pair.
func TestRules_TwoInvocationsUnderAFormKeepTheirOwnSeeds(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			g := writeGraph(t, d, chain(6))
			rel, err := d.Query(`[:find ?x ?z :in $ % ?x ?z
			                      :where [?x :type :node] [?z :type :node]
			                             (not (reach ?x ?w) (reach ?z ?w))]`,
				mustRuleSet(t, closureRules), nodeID("n5"), nodeID("n2"))
			require.NoError(t, err)
			require.Empty(t, g.tuples(t, rel), "n5 and n2 both reach n6, so the not excludes the pair")
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
