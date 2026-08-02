package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/algebra"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Pins for Database.ExplainAlgebra: the record of what planning does to a
// query, returned as values — the compiled algebra, every rewrite decision,
// the optimized tree, the rewritten Datalog, and the physical plan. The
// annotation events remain the streaming view; nothing here scrapes them.

// TestExplainAlgebraDecorrelation pins the optimizer-enabled shape on a
// correlated grouped-aggregate query: every field populated, and the rewrite
// records carrying the decorrelation decision as a value.
func TestExplainAlgebraDecorrelation(t *testing.T) {
	for _, mode := range pinnedOptimizerModes(true) {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

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
		})
	}
}

// findAntiJoinNodes walks an algebra tree and collects every AntiJoin node.
func findAntiJoinNodes(node *algebra.Node) []*algebra.AntiJoin {
	if node == nil {
		return nil
	}
	var found []*algebra.AntiJoin
	if data, ok := node.Data.(*algebra.AntiJoin); ok {
		found = append(found, data)
	}
	for _, child := range node.Children {
		found = append(found, findAntiJoinNodes(child)...)
	}
	return found
}

// TestExplainAlgebraNotJoinEnvironmentHeader pins the compiled structure the
// environment-header ruling produces (NOTJOIN_HEADER_ENV_BINDING_DERIVATION):
// a not-join declaring an :in-bound symbol compiles to an AntiJoin carrying
// the symbol in its join set, and the tree's root free requirements name it —
// the query's demand on its environment, represented as freeness, never as a
// value in the tree.
func TestExplainAlgebraNotJoinEnvironmentHeader(t *testing.T) {
	flag := datalog.NewSymbol("?flag")
	shapes := map[string]string{
		"pattern-provided": `[:find ?e
		  :in $ ?flag
		  :where
		  [?e :entity/kind "thing"]
		  (not-join [?e ?flag] [?e :entity/flag ?flag])]`,
		"predicate-consumed": `[:find ?e
		  :in $ ?flag
		  :where
		  [?e :entity/kind "thing"]
		  (not-join [?e ?flag] [?e :entity/flag ?f] [(= ?f ?flag)])]`,
	}

	for name, queryText := range shapes {
		t.Run(name, func(t *testing.T) {
			for _, mode := range pinnedOptimizerModes(true) {
				t.Run(mode.name, func(t *testing.T) {
					db := createOptimizerModeDB(t, mode, DatabaseOptions{})

					expl, err := db.ExplainAlgebra(queryText, "hot")
					require.NoError(t, err)
					require.Empty(t, expl.CompileError)
					require.NotNil(t, expl.Compiled)

					antiJoins := findAntiJoinNodes(expl.Compiled)
					require.Len(t, antiJoins, 1)
					require.True(t, query.ContainsSymbol(antiJoins[0].JoinSymbols, flag),
						"the declared environment symbol rides the AntiJoin join set; got %v", antiJoins[0].JoinSymbols)

					analysis, err := algebra.Analyze(expl.Compiled)
					require.NoError(t, err)
					require.True(t, query.ContainsSymbol(analysis[expl.Compiled].Required, flag),
						"the root's free requirements carry the environment demand; got %v", analysis[expl.Compiled].Required)
				})
			}
		})
	}
}

// TestExplainAlgebraGetElse pins the provenance of the get-else rewrite
// through the public surface.
func TestExplainAlgebraGetElse(t *testing.T) {
	for _, mode := range pinnedOptimizerModes(true) {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

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
		})
	}
}

// TestExplainAlgebraDeclineReasons pins that declined rewrites surface the
// failed precondition as a value.
func TestExplainAlgebraDeclineReasons(t *testing.T) {
	for _, mode := range pinnedOptimizerModes(true) {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

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
		})
	}
}

// TestExplainAlgebraOptimizerDisabled pins the baseline shape: the algebra of
// a query is a fact about the query, so the compiled tree is still returned;
// no passes run, and the plan is exactly what this database would use.
func TestExplainAlgebraOptimizerDisabled(t *testing.T) {
	for _, mode := range pinnedOptimizerModes(false) {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

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
		})
	}
}

// TestExplainAlgebraValidatesInputs mirrors Explain's input validation: a
// query whose :in demands inputs errors without them.
func TestExplainAlgebraValidatesInputs(t *testing.T) {
	for _, mode := range pinnedOptimizerModes(true) {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			_, err := db.ExplainAlgebra(`[:find ?e :in $ ?n :where [?e :scenario/name ?n]]`)
			require.Error(t, err)

			expl, err := db.ExplainAlgebra(`[:find ?e :in $ ?n :where [?e :scenario/name ?n]]`, "alpha")
			require.NoError(t, err)
			require.NotNil(t, expl.Plan)
		})
	}
}
