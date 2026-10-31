package storage

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// rewriteAdornments records the binding pattern of every rewrite the
// evaluation made, from the rules/magic-sets events.
type rewriteAdornments struct {
	mu    sync.Mutex
	bound [][]bool
}

func (r *rewriteAdornments) handle(e annotations.Event) {
	if e.Name != annotations.RulesMagicSets {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bound = append(r.bound, append([]bool(nil), e.Data["adornment"].([]bool)...))
}

func (r *rewriteAdornments) made() [][]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]bool(nil), r.bound...)
}

// A top-level invocation bound at one position and followed by a scan of the
// attribute its other position reads is rewritten for the bound position
// alone, so the fixpoint derives what the binding reaches and never the
// binding paired with the scan's every value (REWRITE_ORDER_PROOF.md T5).
func TestRules_ABoundInvocationBeforeAScanIsRewrittenForItsBindingAlone(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			adorned := &rewriteAdornments{}
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: adorned.handle})
			g := writeGraph(t, d, chain(10))
			rel, err := d.Query(`[:find ?c :in $ % ?start :where (reach ?start ?b) [?b :edge ?c]]`, mustRuleSet(t, closureRules), nodeID("n1"))
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"n3", "n4", "n5", "n6", "n7", "n8", "n9", "n10"}, g.tuples(t, rel))
			require.Equal(t, [][]bool{{true, false}}, adorned.made(), "one rewrite, for the start alone")
		})
	}
}

// A top-level invocation bound at one position beside a pattern with a value
// constant providing its other position is rewritten for both positions: the
// pattern has visible selectivity and is planned first, and the top level
// binds an invocation's position from every group in hand
// (REWRITE_ORDER_PROOF.md T5, F5), where a rule body's rewrite binds from
// the seed component alone
// (A_CLAUSE_OUTSIDE_THE_SEED_COMPONENT_BINDS_NOTHING_PROOF.md §7).
func TestRules_ATopLevelInvocationBesideAValueConstantPatternIsRewrittenForBothPositions(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			adorned := &rewriteAdornments{}
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: adorned.handle})
			g := writeGraph(t, d, chain(10))
			rel, err := d.Query(`[:find ?b :in $ % ?start :where [?b :type :node] (reach ?start ?b)]`, mustRuleSet(t, closureRules), nodeID("n1"))
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9", "n10"}, g.tuples(t, rel))
			require.Equal(t, [][]bool{{true, true}}, adorned.made(), "one rewrite, for the start and the nodes in hand")
		})
	}
}

// A rule body written with its recursive invocation before the pattern that
// binds the invocation's argument is rewritten in the planner's order, so a
// bound invocation derives, on every backend, the chain its start reaches and
// never the closure whole (REWRITE_ORDER_PROOF.md T4, NESTED §5.5).
func TestRules_ABodyWrittenBeforeItsBinderDerivesWhatTheBindingReaches(t *testing.T) {
	const invocationFirst = `[[(reach ?a ?b) [?a :edge ?b]]
	                          [(reach ?a ?c) (reach ?b ?c) [?a :edge ?b]]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			opts := mode.plannerOptions()
			opts.MaxDerivedTuples = 100
			d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts})
			g := writeGraph(t, d, chains(100))
			rel, err := d.Query(`[:find ?y :in $ % ?start :where (reach ?start ?y)]`, mustRuleSet(t, invocationFirst), nodeID("n1"))
			require.NoError(t, err, "a bound invocation derives only what its start reaches, whichever way the body is written")
			require.ElementsMatch(t, []string{"n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9", "n10", "n11"}, g.tuples(t, rel))
		})
	}
}

// A predicate the evaluation derives whole is read whole at every
// invocation of it, so the evaluation derives no adorned copy of it
// (docs/proposals/A_PREDICATE_DERIVED_WHOLE_IS_READ_WHOLE_PROOF.md, T1, T3):
// over the set where p's body invokes s bound and r free, and r reads s
// whole, the query bound at p's first position answers what the free query
// answers at that binding, and the strata it begins hold no adorned copy of
// s, which would be a stratum of its own.
func TestRules_ABoundInvocationOfAPredicateDerivedWholeDerivesNoAdornedCopy(t *testing.T) {
	const wholeRules = `[[(p ?x ?z) [?x :edge ?y] (s ?y ?z) (r ?k)]
	                     [(r ?k) [?k :type :node] (s ?k _)]
	                     [(s ?a ?b) [?a :edge ?b]]]`
	adornedS := query.AdornedSymbol(datalog.NewSymbol("s"), []bool{true, false})
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			begun := &strataBegun{}
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: begun.handle})
			g := writeGraph(t, d, chain(10))
			bound, err := d.Query(`[:find ?z :in $ % ?start :where (p ?start ?z)]`, mustRuleSet(t, wholeRules), nodeID("n1"))
			require.NoError(t, err)
			answered := g.tuples(t, bound)
			strata := begun.perStratum()
			whole, err := d.Query(`[:find ?x ?z :in $ % :where (p ?x ?z)]`, mustRuleSet(t, wholeRules))
			require.NoError(t, err)
			var fromFirst []string
			for _, pair := range g.tuples(t, whole) {
				if strings.HasPrefix(pair, "n1>") {
					fromFirst = append(fromFirst, strings.TrimPrefix(pair, "n1>"))
				}
			}
			require.NotEmpty(t, fromFirst, "the first node reaches a node two edges on")
			require.ElementsMatch(t, fromFirst, answered, "the bound query answers what the free query answers at the binding")
			require.NotEmpty(t, strata, "the bound query begins its strata")
			require.NotContains(t, strata, adornedS, "s is derived whole for r and read whole at p's site, so no stratum derives an adorned copy of s: %v", strata)
		})
	}
}
