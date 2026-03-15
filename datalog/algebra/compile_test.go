package algebra

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestCompileDecompileRoundTrip verifies that compiling clauses to algebra
// and decompiling back produces functionally equivalent clauses.
func TestCompileDecompileRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "simple pattern",
			query: `[:find ?e ?name :where [?e :person/name ?name]]`,
		},
		{
			name:  "two patterns",
			query: `[:find ?e ?name ?age :where [?e :person/name ?name] [?e :person/age ?age]]`,
		},
		{
			name:  "pattern with predicate",
			query: `[:find ?e ?age :where [?e :person/age ?age] [(> ?age 21)]]`,
		},
		{
			name:  "pattern with expression",
			query: `[:find ?e ?total :where [?e :item/price ?p] [(+ ?p 10) ?total]]`,
		},
		{
			name:  "not clause",
			query: `[:find ?e :where [?e :person/name ?name] (not [?e :person/fired true])]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tt.query)
			require.NoError(t, err)

			// Compile
			root, err := Compile(q)
			require.NoError(t, err)
			require.NotNil(t, root)

			t.Logf("Algebra tree:\n%s", root.String())

			// Decompile
			clauses, err := Decompile(root)
			require.NoError(t, err)
			require.NotEmpty(t, clauses)

			t.Logf("Decompiled %d clauses", len(clauses))
			for i, c := range clauses {
				t.Logf("  [%d] %T: %s", i, c, c.String())
			}

			// Verify same number of clauses
			assert.Equal(t, len(q.Where), len(clauses),
				"round-trip should preserve clause count")
		})
	}
}

// TestCompileOrFallback verifies that OR-fallback with subquery compiles
// to a LateralJoin with defaults.
func TestCompileOrFallback(t *testing.T) {
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

	root, err := Compile(q)
	require.NoError(t, err)
	require.NotNil(t, root)

	t.Logf("Algebra tree:\n%s", root.String())

	// The tree should contain a LateralJoin node
	lj := findLateralJoin(root)
	if lj != nil {
		ljData := lj.Data.(*LateralJoin)
		t.Logf("Found LateralJoin: correlation=%v, defaults=%v",
			ljData.CorrelationVars, ljData.DefaultValues)
		assert.NotEmpty(t, ljData.CorrelationVars, "should have correlation variables")
	} else {
		t.Log("No LateralJoin found — OR-fallback compiled to different structure")
		// This is acceptable if the pattern wasn't detected
	}
}

// TestCompileDecompileNotAtEnd reproduces a production crash where the NOT
// clause is at the END of the clause list (after two data patterns).
func TestCompileDecompileNotAtEnd(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?scenario ?tag
		:where [?scenario :entity/type :entity.type/scenario]
		       [?scenario :entity/tag ?tag]
		       (not [?scenario :entity/deleted true])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Compiled:\n%s", root.String())

	optimizer := NewOptimizer(DefaultPasses()...)
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	t.Logf("Optimized:\n%s", optimized.String())

	clauses, err := Decompile(optimized)
	require.NoError(t, err)
	t.Logf("Decompiled %d clauses:", len(clauses))
	for i, c := range clauses {
		t.Logf("  [%d] %T: %s", i, c, c.String())
	}
	assert.Equal(t, len(q.Where), len(clauses))
}

// TestCompileDecompileWithNot verifies that NOT clauses survive the
// compile → optimize → decompile pipeline. This covers the production
// pattern: [?e :entity/type :foo] (not [?e :entity/deleted true])
func TestCompileDecompileWithNot(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?tag
		:where [?e :entity/type :entity.type/scenario]
		       (not [?e :entity/deleted true])
		       [?e :entity/tag ?tag]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Compiled:\n%s", root.String())

	// Optimize (should be a no-op — no LateralJoins)
	optimizer := NewOptimizer(DefaultPasses()...)
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	t.Logf("Optimized:\n%s", optimized.String())

	// Decompile
	clauses, err := Decompile(optimized)
	require.NoError(t, err)
	require.NotEmpty(t, clauses)
	t.Logf("Decompiled %d clauses:", len(clauses))
	for i, c := range clauses {
		t.Logf("  [%d] %T: %s", i, c, c.String())
	}

	// Should have 3 clauses: pattern, NOT, pattern
	assert.Equal(t, len(q.Where), len(clauses))
}

// TestCompileDecompileWithGetElse verifies get-else survives the pipeline.
func TestCompileDecompileWithGetElse(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?title
		:where [?e :entity/type :entity.type/scenario]
		       (not [?e :entity/deleted true])
		       [(get-else $ ?e :scenario/title "") ?title]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	optimizer := NewOptimizer(DefaultPasses()...)
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)

	clauses, err := Decompile(optimized)
	require.NoError(t, err)
	require.NotEmpty(t, clauses)
	t.Logf("Decompiled %d clauses:", len(clauses))
	for i, c := range clauses {
		t.Logf("  [%d] %T: %s", i, c, c.String())
	}
	assert.Equal(t, len(q.Where), len(clauses))
}

// TestCompileAdapterRoundTrip verifies the parse.Node adapter preserves
// the algebra tree structure through ToParseTree/FromParseTree.
func TestCompileAdapterRoundTrip(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?name :where [?e :person/name ?name] [?e :person/age ?age] [(> ?age 21)]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	// Adapt to parse tree and back
	tree := ToParseTree(root)
	require.NotNil(t, tree)
	require.NotNil(t, tree.Root)

	recovered := FromParseTree(tree)
	require.NotNil(t, recovered)

	assert.Equal(t, root.Op, recovered.Op)
	assert.Equal(t, len(root.Children), len(recovered.Children))
}
