package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/annotations"
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
