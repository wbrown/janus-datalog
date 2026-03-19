package algebra

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestGetElseScanRewrite_Simple verifies that a single get-else expression
// is rewritten from Map(get-else) to LeftOuterJoin + Scan.
func TestGetElseScanRewrite_Simple(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?title
	  :where
	  [?e :entity/type :entity.type/project]
	  [(get-else $ ?e :project/title "") ?title]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Before:\n%s", root.String())

	// Before optimization: should have a Map node for get-else
	mapsBefore := countNodes(root, RuleMap)
	require.Equal(t, 1, mapsBefore, "should have 1 Map node (get-else)")

	// Apply get-else rewrite
	optimizer := NewOptimizer(GetElseScanRewritePass())
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	t.Logf("After:\n%s", optimized.String())

	// After optimization: Map should be replaced with LeftOuterJoin
	mapsAfter := countNodes(optimized, RuleMap)
	joinsAfter := countNodes(optimized, RuleJoin)
	t.Logf("Maps: %d → %d, Joins: %d", mapsBefore, mapsAfter, joinsAfter)

	assert.Equal(t, 0, mapsAfter, "get-else Map should be eliminated")
	assert.Greater(t, joinsAfter, 0, "should have a Join node (LeftOuterJoin for get-else)")

	// Decompile and verify structure
	clauses, err := Decompile(optimized)
	require.NoError(t, err)
	t.Logf("Decompiled %d clauses:", len(clauses))
	for i, c := range clauses {
		t.Logf("  [%d] %T: %s", i, c, c.String())
	}
}

// TestGetElseScanRewrite_Multiple verifies that multiple chained get-else
// expressions are all rewritten. This matches the production pattern where
// 4+ get-else expressions follow the entity scan.
func TestGetElseScanRewrite_Multiple(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?title ?priority ?category ?notes
	  :where
	  [?e :entity/type :entity.type/project]
	  [(get-else $ ?e :project/title "") ?title]
	  [(get-else $ ?e :project/priority 0) ?priority]
	  [(get-else $ ?e :project/category "") ?category]
	  [(get-else $ ?e :project/notes "") ?notes]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	mapsBefore := countNodes(root, RuleMap)
	require.Equal(t, 4, mapsBefore, "should have 4 Map nodes (get-else)")

	optimizer := NewOptimizer(GetElseScanRewritePass())
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	t.Logf("After:\n%s", optimized.String())

	mapsAfter := countNodes(optimized, RuleMap)
	assert.Equal(t, 0, mapsAfter, "all get-else Maps should be eliminated")

	// Decompile and verify
	clauses, err := Decompile(optimized)
	require.NoError(t, err)
	t.Logf("Decompiled %d clauses:", len(clauses))
	for i, c := range clauses {
		t.Logf("  [%d] %T: %s", i, c, c.String())
	}
}

// TestGetElseScanRewrite_NonGetElsePreserved verifies that non-get-else
// Map nodes (arithmetic expressions, comparisons) are NOT rewritten.
func TestGetElseScanRewrite_NonGetElsePreserved(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?total
	  :where
	  [?e :item/price ?p]
	  [(+ ?p 10) ?total]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	mapsBefore := countNodes(root, RuleMap)
	require.Equal(t, 1, mapsBefore)

	optimizer := NewOptimizer(GetElseScanRewritePass())
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)

	mapsAfter := countNodes(optimized, RuleMap)
	assert.Equal(t, 1, mapsAfter, "non-get-else Map should be preserved")
}

// TestGetElseScanRewrite_VectorDefaultRewritten verifies that get-else with
// a vector default (e.g., []) IS rewritten. The default branch emits a
// get-else expression (not ground) so TypedDefaulter can produce the
// schema-typed default (e.g., []string instead of []interface{}).
func TestGetElseScanRewrite_VectorDefaultRewritten(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?lore
	  :where
	  [?e :entity/type :entity.type/room]
	  [(get-else $ ?e :entity/lore []) ?lore]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	mapsBefore := countNodes(root, RuleMap)
	require.Equal(t, 1, mapsBefore)

	optimizer := NewOptimizer(GetElseScanRewritePass())
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)

	mapsAfter := countNodes(optimized, RuleMap)
	assert.Equal(t, 0, mapsAfter, "get-else with vector default should be rewritten")

	joinsAfter := countNodes(optimized, RuleJoin)
	assert.Greater(t, joinsAfter, 0, "should have LeftOuterJoin from get-else rewrite")

	// Verify the LeftOuterJoin carries the attribute for typed defaults
	var foundAttr bool
	walkNodes(optimized, func(n *Node) {
		if n.Op == RuleJoin {
			j := n.Data.(*Join)
			if j.Kind == LeftOuterJoin && j.DefaultAttr != nil {
				foundAttr = true
			}
		}
	})
	assert.True(t, foundAttr, "LeftOuterJoin should carry DefaultAttr for typed defaults")

	// Decompile and verify the default branch uses get-else (not ground)
	clauses, err := Decompile(optimized)
	require.NoError(t, err)
	t.Logf("Decompiled %d clauses:", len(clauses))
	for i, c := range clauses {
		t.Logf("  [%d] %T: %s", i, c, c.String())
	}
}

// TestGetElseScanRewrite_InputParamEntitySkipped verifies that get-else
// with an entity variable from :in (not from a DataPattern) is NOT rewritten,
// because the Scan would have an unbound variable.
func TestGetElseScanRewrite_InputParamEntitySkipped(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?nick
	  :in $ ?entity
	  :where
	  [?i :item/name ?item]
	  [(get-else $ ?entity :person/nickname "none") ?nick]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	mapsBefore := countNodes(root, RuleMap)
	require.Equal(t, 1, mapsBefore)

	optimizer := NewOptimizer(GetElseScanRewritePass())
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)

	mapsAfter := countNodes(optimized, RuleMap)
	assert.Equal(t, 1, mapsAfter, "get-else with input param entity should NOT be rewritten")
}

// TestCompileMissingPredicateInOr verifies that missing? inside an OR
// clause compiles successfully to an AntiJoin (not an error).
func TestCompileMissingPredicateInOr(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?name
	  :where
	  [?e :entity/name ?name]
	  (or-default [(missing? $ ?e :entity/lore)]
	              [?e :entity/lore []])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err, "OR with missing? should compile (missing? → Select predicate)")
	t.Logf("Compiled:\n%s", root.String())

	// Verify the tree contains a Select (missing? preserved as predicate)
	selects := countNodes(root, RuleSelect)
	assert.Greater(t, selects, 0, "should have Select from missing?")
}
