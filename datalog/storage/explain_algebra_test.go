package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/algebra"
)

// Pins for Database.ExplainAlgebra: the record of what planning does to a
// query, returned as values — the compiled algebra, every rewrite decision,
// the optimized tree, the rewritten Datalog, and the physical plan. The
// annotation events remain the streaming view; nothing here scrapes them.

// TestExplainAlgebraDecorrelation pins the optimizer-enabled shape on a
// correlated grouped-aggregate query: every field populated, and the rewrite
// records carrying the decorrelation decision as a value.
func TestExplainAlgebraDecorrelation(t *testing.T) {
	db := createOptimizerModeDB(t, optimizerMode{"algebra_on", true})

	expl, err := db.ExplainAlgebra(`[:find ?s ?mx
	  :where
	  [?s :scenario/name ?n]
	  [(q [:find (max ?h) :in $ ?s :where [?p :price/scenario ?s] [?p :price/high ?h]] $ ?s) [[?mx]]]]`)
	require.NoError(t, err)

	require.True(t, expl.OptimizerEnabled)
	require.NotNil(t, expl.Original)
	require.NotNil(t, expl.Compiled)
	require.NotNil(t, expl.Optimized)
	require.NotNil(t, expl.Rewritten)
	require.NotNil(t, expl.Plan)
	require.NotEmpty(t, expl.Rewrites)

	var sawApplied bool
	for _, r := range expl.Rewrites {
		if r.Pass == "decorrelation" && r.Action == algebra.RewriteApplied {
			sawApplied = true
		}
	}
	require.True(t, sawApplied, "the correlated aggregate decorrelates; rewrites=%v", expl.Rewrites)

	rendered := expl.String()
	require.Contains(t, rendered, "Compiled algebra:")
	require.Contains(t, rendered, "Rewrites (")
	require.Contains(t, rendered, "Realized Query Plan:")
}

// TestExplainAlgebraGetElse pins the provenance of the formerly-silent
// get-else rewrite through the public surface.
func TestExplainAlgebraGetElse(t *testing.T) {
	db := createOptimizerModeDB(t, optimizerMode{"algebra_on", true})

	expl, err := db.ExplainAlgebra(`[:find ?e ?title
	  :where
	  [?e :entity/type :entity.type/project]
	  [(get-else $ ?e :project/title "") ?title]]`)
	require.NoError(t, err)

	var sawApplied bool
	for _, r := range expl.Rewrites {
		if r.Pass == "get-else-scan-rewrite" && r.Action == algebra.RewriteApplied {
			sawApplied = true
		}
	}
	require.True(t, sawApplied, "the get-else scan rewrite reports its decision; rewrites=%v", expl.Rewrites)
}

// TestExplainAlgebraDeclineReasons pins that declined rewrites surface the
// failed precondition as a value.
func TestExplainAlgebraDeclineReasons(t *testing.T) {
	db := createOptimizerModeDB(t, optimizerMode{"algebra_on", true})

	expl, err := db.ExplainAlgebra(`[:find ?e ?v
	  :where
	  [?e :thing/kind "widget"]
	  [(q [:find ?x :in $ ?e :where [?e :thing/value ?x]] $ ?e) [[?v] ...]]]`)
	require.NoError(t, err)

	var reason string
	for _, r := range expl.Rewrites {
		if r.Pass == "decorrelation" && r.Action == algebra.RewriteDeclined {
			reason = r.Reason
		}
	}
	require.Equal(t, "pure DataPattern query — indexed lookup is faster", reason,
		"rewrites=%v", expl.Rewrites)
}

// TestExplainAlgebraOptimizerDisabled pins the baseline shape: the algebra of
// a query is a fact about the query, so the compiled tree is still returned;
// no passes run, and the plan is exactly what this database would use.
func TestExplainAlgebraOptimizerDisabled(t *testing.T) {
	db := createOptimizerModeDB(t, optimizerMode{"algebra_off", false})

	expl, err := db.ExplainAlgebra(`[:find ?e ?n :where [?e :scenario/name ?n]]`)
	require.NoError(t, err)

	require.False(t, expl.OptimizerEnabled)
	require.NotNil(t, expl.Compiled)
	require.Empty(t, expl.CompileError)
	require.Empty(t, expl.Rewrites)
	require.Nil(t, expl.Optimized)
	require.NotNil(t, expl.Rewritten)
	require.NotNil(t, expl.Plan)
	require.Contains(t, expl.String(), "Optimizer: disabled")
}

// TestExplainAlgebraValidatesInputs mirrors Explain's input validation: a
// query whose :in demands inputs errors without them.
func TestExplainAlgebraValidatesInputs(t *testing.T) {
	db := createOptimizerModeDB(t, optimizerMode{"algebra_on", true})

	_, err := db.ExplainAlgebra(`[:find ?e :in $ ?n :where [?e :scenario/name ?n]]`)
	require.Error(t, err)

	expl, err := db.ExplainAlgebra(`[:find ?e :in $ ?n :where [?e :scenario/name ?n]]`, "alpha")
	require.NoError(t, err)
	require.NotNil(t, expl.Plan)
}
