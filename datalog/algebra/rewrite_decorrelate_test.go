package algebra

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestDecorrelation_SimpleAggregate verifies the basic decorrelation:
// LateralJoin with aggregated subquery → decorrelated subquery with GROUP BY.
func TestDecorrelation_SimpleAggregate(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?count
	  :where
	  [?e :entity/type :entity.type/scenario]
	  (or [(q [:find (count ?t)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/status :status/complete]]
	          $ ?e) [[?count]]]
	      [(ground 0) ?count])]`)
	require.NoError(t, err)

	// Compile to algebra
	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Before decorrelation:\n%s", root.String())

	// Verify LateralJoin exists before optimization
	lj := findLateralJoin(root)
	require.NotNil(t, lj, "should have LateralJoin before optimization")
	ljData := lj.Data.(*LateralJoin)
	t.Logf("LateralJoin correlation vars: %v", ljData.CorrelationVars)

	// Apply decorrelation
	optimizer := NewOptimizer(DecorrelationPass())
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	t.Logf("After decorrelation:\n%s", optimized.String())

	// Verify no LateralJoin remains
	ljAfter := findLateralJoin(optimized)
	assert.Nil(t, ljAfter, "LateralJoin should be eliminated by decorrelation")

	// Decompile back to clauses
	clauses, err := Decompile(optimized)
	require.NoError(t, err)
	t.Logf("Decompiled %d clauses:", len(clauses))
	for i, c := range clauses {
		t.Logf("  [%d] %T: %s", i, c, c.String())
	}

	// Verify the decorrelated subquery has the correlation var in :find
	for _, c := range clauses {
		if sp, ok := c.(*query.SubqueryPattern); ok {
			t.Logf("SubqueryPattern found: %s", sp.Query.String())
			// The decorrelated query should have ?s (or ?e) in :find
			hasGroupBy := false
			for _, fe := range sp.Query.Find {
				if fv, ok := fe.(query.FindVariable); ok {
					t.Logf("  Find variable: %s", fv.Symbol.String())
					hasGroupBy = true
				}
			}
			assert.True(t, hasGroupBy, "decorrelated query should have grouping variable in :find")

			// The decorrelated query should NOT have ?s in :in (only $)
			for _, in := range sp.Query.In {
				if si, ok := in.(query.ScalarInput); ok {
					t.Errorf("decorrelated query should not have scalar input %s", si.Symbol.String())
				}
			}
		}

		// Check if it's wrapped in an OR (for defaults)
		if oc, ok := c.(*query.OrClause); ok {
			t.Logf("OR clause with %d branches", len(oc.Branches))
			for i, branch := range oc.Branches {
				t.Logf("  Branch %d: %d clauses", i, len(branch))
				for _, bc := range branch {
					t.Logf("    %T: %s", bc, bc.String())
				}
			}
		}
	}
}

// TestDecorrelation_NonAggregateUnchanged verifies that non-aggregate
// subqueries are NOT decorrelated (they'd produce wrong results).
func TestDecorrelation_NonAggregateUnchanged(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?name
	  :where
	  [?e :entity/type :entity.type/scenario]
	  [(q [:find ?name
	       :in $ ?s
	       :where [?s :scenario/name ?name]]
	      $ ?e) [[?name]]]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	// Verify LateralJoin exists
	lj := findLateralJoin(root)
	require.NotNil(t, lj, "should have LateralJoin")

	// Apply decorrelation — should leave it unchanged
	optimizer := NewOptimizer(DecorrelationPass())
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)

	// LateralJoin should still be there (non-aggregate, can't decorrelate)
	ljAfter := findLateralJoin(optimized)
	assert.NotNil(t, ljAfter, "non-aggregate LateralJoin should remain")
}

// TestDecorrelation_MultipleSubqueries verifies that multiple correlated
// subqueries in the same query are all decorrelated.
func TestDecorrelation_MultipleSubqueries(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?count ?total
	  :where
	  [?e :entity/type :entity.type/scenario]
	  (or [(q [:find (count ?t)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/status :status/complete]]
	          $ ?e) [[?count]]]
	      [(ground 0) ?count])
	  (or [(q [:find (sum ?v)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/value ?v]]
	          $ ?e) [[?total]]]
	      [(ground 0) ?total])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Before:\n%s", root.String())

	// Count LateralJoins before
	ljCount := countNodes(root, RuleLateralJoin)
	t.Logf("LateralJoins before: %d", ljCount)

	optimizer := NewOptimizer(DecorrelationPass())
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	t.Logf("After:\n%s", optimized.String())

	// Count LateralJoins after — should be fewer
	ljCountAfter := countNodes(optimized, RuleLateralJoin)
	t.Logf("LateralJoins after: %d", ljCountAfter)
	assert.Less(t, ljCountAfter, ljCount, "decorrelation should reduce LateralJoin count")
}

// countNodes counts nodes with a given Op in the tree.
func countNodes(n *Node, op string) int {
	if n == nil {
		return 0
	}
	count := 0
	if n.Op == op {
		count++
	}
	for _, child := range n.Children {
		count += countNodes(child, op)
	}
	return count
}
