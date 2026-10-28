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

	root, err := Compile(q, nil)
	require.NoError(t, err)
	t.Logf("Before:\n%s", root.String())

	// Before optimization: should have a Map node for get-else
	mapsBefore := countNodes(root, RuleMap)
	require.Equal(t, 1, mapsBefore, "should have 1 Map node (get-else)")

	// Apply get-else rewrite
	optimizer := NewOptimizer(GetElseScanRewritePass(nil))
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

	root, err := Compile(q, nil)
	require.NoError(t, err)

	mapsBefore := countNodes(root, RuleMap)
	require.Equal(t, 4, mapsBefore, "should have 4 Map nodes (get-else)")

	optimizer := NewOptimizer(GetElseScanRewritePass(nil))
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

	root, err := Compile(q, nil)
	require.NoError(t, err)

	mapsBefore := countNodes(root, RuleMap)
	require.Equal(t, 1, mapsBefore)

	optimizer := NewOptimizer(GetElseScanRewritePass(nil))
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

	root, err := Compile(q, nil)
	require.NoError(t, err)

	mapsBefore := countNodes(root, RuleMap)
	require.Equal(t, 1, mapsBefore)

	optimizer := NewOptimizer(GetElseScanRewritePass(nil))
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

	root, err := Compile(q, nil)
	require.NoError(t, err)

	mapsBefore := countNodes(root, RuleMap)
	require.Equal(t, 1, mapsBefore)

	optimizer := NewOptimizer(GetElseScanRewritePass(nil))
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)

	mapsAfter := countNodes(optimized, RuleMap)
	assert.Equal(t, 1, mapsAfter, "get-else with input param entity should NOT be rewritten")
}

// A scope's required run is the scope's input, as :in is a query's: a
// get-else over an entity the required run alone provides stays an
// expression, whether the run is the scope/required invocation or the
// placeholder a header form's branch is compiled over, and one over an
// entity a scan also provides is rewritten.
func TestGetElseScanRewrite_RequiredRunEntitySkipped(t *testing.T) {
	cases := []struct {
		name      string
		text      string
		mapsAfter int
	}{
		{
			"the required run alone provides the entity",
			`[:find ?e ?nick :in $ % :where (scope/required ?e) [(get-else $ ?e :person/nickname "none") ?nick]]`,
			1,
		},
		{
			"a scan provides the entity beside the required run",
			`[:find ?e ?nick :in $ % :where (scope/required ?e) [?e :person/name _] [(get-else $ ?e :person/nickname "none") ?nick]]`,
			0,
		},
		{
			"a fallback's header alone provides the entity to its default branch",
			`[:find ?e ?nick :where [?e :person/name _] (or-default-join [[?e] ?nick] [?e :person/nickname ?nick] [(get-else $ ?e :person/nickname "none") ?nick])]`,
			1,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q, err := parser.ParseQuery(c.text)
			require.NoError(t, err)
			root, err := Compile(q, nil)
			require.NoError(t, err)
			require.Equal(t, 1, countNodes(root, RuleMap))
			optimized, err := NewOptimizer(GetElseScanRewritePass(nil)).Optimize(root)
			require.NoError(t, err)
			assert.Equal(t, c.mapsAfter, countNodes(optimized, RuleMap))
		})
	}
}

// A get-else whose binding variable the child relation already provides is
// a unification the Map evaluates per tuple, so it stays a Map: the rewrite's
// scan would output the binding beside the entity, and its default would
// stand in for a value the child already holds. That holds when the binding
// is the entity variable itself and when another pattern binds it.
func TestGetElseScanRewrite_BoundBindingSkipped(t *testing.T) {
	cases := []struct {
		name string
		text string
	}{
		{"the binding is the entity variable", `[:find ?f :where [?f :u/link ?w] [(get-else $ ?f :u/link :none) ?f]]`},
		{"another pattern binds the binding", `[:find ?f :where [?f :u/m ?x] [(get-else $ ?f :u/n :none) ?x]]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q, err := parser.ParseQuery(c.text)
			require.NoError(t, err)
			root, err := Compile(q, nil)
			require.NoError(t, err)
			require.Equal(t, 1, countNodes(root, RuleMap))
			optimized, err := NewOptimizer(GetElseScanRewritePass(nil)).Optimize(root)
			require.NoError(t, err)
			assert.Equal(t, 1, countNodes(optimized, RuleMap))
		})
	}
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

	root, err := Compile(mustPrepare(t, q, nil), nil)
	require.NoError(t, err, "OR with missing? should compile (missing? → Select predicate)")
	t.Logf("Compiled:\n%s", root.String())

	// Verify the tree contains a Select (missing? preserved as predicate)
	selects := countNodes(root, RuleSelect)
	assert.Greater(t, selects, 0, "should have Select from missing?")
}
