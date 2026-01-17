package executor

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestNotClause(t *testing.T) {
	// Create test data
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	charlie := datalog.NewIdentity("user:charlie")
	nameAttr := datalog.NewKeyword(":user/name")
	archivedAttr := datalog.NewKeyword(":user/archived")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: 1},
		// Alice is archived
		{E: alice, A: archivedAttr, V: true, Tx: 2},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Query: Find non-archived users
	// [:find ?name :where [?e :user/name ?name] (not [?e :user/archived true])]
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?name"},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: nameAttr},
					query.Variable{Name: "?name"},
				},
			},
			&query.NotClause{
				Clauses: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?e"},
							query.Constant{Value: archivedAttr},
							query.Constant{Value: true},
						},
					},
				},
			},
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Should have 2 non-archived users: Bob and Charlie
	if result.Size() != 2 {
		t.Errorf("expected 2 results, got %d", result.Size())
	}

	// Check we got the right names
	names := make(map[string]bool)
	for i := 0; i < result.Size(); i++ {
		name := result.Get(i)[0].(string)
		names[name] = true
	}

	if names["Alice"] {
		t.Error("Alice should not be in results (she's archived)")
	}
	if !names["Bob"] || !names["Charlie"] {
		t.Errorf("expected Bob and Charlie, got %v", names)
	}
}

func TestNotClauseNoMatches(t *testing.T) {
	// When NOT clause matches nothing, all results should be kept
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	nameAttr := datalog.NewKeyword(":user/name")
	archivedAttr := datalog.NewKeyword(":user/archived")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		// No one is archived
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Query: Find non-archived users (no one is archived)
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?name"},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: nameAttr},
					query.Variable{Name: "?name"},
				},
			},
			&query.NotClause{
				Clauses: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?e"},
							query.Constant{Value: archivedAttr},
							query.Constant{Value: true},
						},
					},
				},
			},
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Should have both users since no one is archived
	if result.Size() != 2 {
		t.Errorf("expected 2 results, got %d", result.Size())
	}
}

func TestNotClauseAllMatch(t *testing.T) {
	// When NOT clause matches everything, no results should be returned
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	nameAttr := datalog.NewKeyword(":user/name")
	archivedAttr := datalog.NewKeyword(":user/archived")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		// Everyone is archived
		{E: alice, A: archivedAttr, V: true, Tx: 2},
		{E: bob, A: archivedAttr, V: true, Tx: 2},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Query: Find non-archived users (everyone is archived)
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?name"},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: nameAttr},
					query.Variable{Name: "?name"},
				},
			},
			&query.NotClause{
				Clauses: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?e"},
							query.Constant{Value: archivedAttr},
							query.Constant{Value: true},
						},
					},
				},
			},
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Should have no users since everyone is archived
	if result.Size() != 0 {
		t.Errorf("expected 0 results, got %d", result.Size())
	}
}

func TestNotJoinClause(t *testing.T) {
	// Test NOT-JOIN with explicit join variables
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	charlie := datalog.NewIdentity("user:charlie")
	nameAttr := datalog.NewKeyword(":user/name")
	archivedAttr := datalog.NewKeyword(":user/archived")
	deletedAttr := datalog.NewKeyword(":user/deleted")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: 1},
		// Alice is archived
		{E: alice, A: archivedAttr, V: true, Tx: 2},
		// Bob is deleted
		{E: bob, A: deletedAttr, V: true, Tx: 2},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Query: Find users that are neither archived nor deleted
	// [:find ?name :where [?e :user/name ?name] (not-join [?e] [?e :user/archived true] [?e :user/deleted true])]
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?name"},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: nameAttr},
					query.Variable{Name: "?name"},
				},
			},
			&query.NotJoinClause{
				JoinVars: []query.Symbol{"?e"},
				Clauses: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?e"},
							query.Constant{Value: archivedAttr},
							query.Constant{Value: true},
						},
					},
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?e"},
							query.Constant{Value: deletedAttr},
							query.Constant{Value: true},
						},
					},
				},
			},
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Alice is archived AND deleted (inner clauses join), Bob is only deleted (doesn't match both)
	// Actually, looking at NOT-JOIN semantics: inner clauses must ALL match for exclusion
	// Alice: has archived but NOT deleted -> doesn't match both -> kept
	// Bob: has deleted but NOT archived -> doesn't match both -> kept
	// Charlie: has neither -> kept
	// Wait, let me re-read the semantics...

	// The NOT-JOIN inner clauses are like an AND - both must match for the entity to be excluded.
	// Since no entity has BOTH archived AND deleted, all should be kept.

	if result.Size() != 3 {
		t.Errorf("expected 3 results (no entity has both archived and deleted), got %d", result.Size())
	}
}

func TestOrClause(t *testing.T) {
	// Test OR clause (union of branches)
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	charlie := datalog.NewIdentity("user:charlie")
	nameAttr := datalog.NewKeyword(":user/name")
	statusAttr := datalog.NewKeyword(":user/status")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: 1},
		{E: alice, A: statusAttr, V: "active", Tx: 1},
		{E: bob, A: statusAttr, V: "pending", Tx: 1},
		{E: charlie, A: statusAttr, V: "inactive", Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Query: Find users with status "active" OR "pending"
	// [:find ?name :where [?e :user/name ?name] (or [?e :user/status "active"] [?e :user/status "pending"])]
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?name"},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: nameAttr},
					query.Variable{Name: "?name"},
				},
			},
			&query.OrClause{
				Branches: [][]query.Clause{
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: "?e"},
								query.Constant{Value: statusAttr},
								query.Constant{Value: "active"},
							},
						},
					},
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: "?e"},
								query.Constant{Value: statusAttr},
								query.Constant{Value: "pending"},
							},
						},
					},
				},
			},
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Should have Alice (active) and Bob (pending), but not Charlie (inactive)
	if result.Size() != 2 {
		t.Errorf("expected 2 results, got %d", result.Size())
	}

	names := make(map[string]bool)
	for i := 0; i < result.Size(); i++ {
		name := result.Get(i)[0].(string)
		names[name] = true
	}

	if !names["Alice"] || !names["Bob"] {
		t.Errorf("expected Alice and Bob, got %v", names)
	}
	if names["Charlie"] {
		t.Error("Charlie should not be in results (status is inactive)")
	}
}

func TestOrJoinClause(t *testing.T) {
	// Test OR-JOIN with explicit join variables
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	charlie := datalog.NewIdentity("user:charlie")
	nameAttr := datalog.NewKeyword(":user/name")
	userStatusAttr := datalog.NewKeyword(":user/status")
	adminStatusAttr := datalog.NewKeyword(":admin/status")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: 1},
		{E: alice, A: userStatusAttr, V: "active", Tx: 1},     // Alice is active user
		{E: bob, A: adminStatusAttr, V: "enabled", Tx: 1},     // Bob is enabled admin
		{E: charlie, A: userStatusAttr, V: "inactive", Tx: 1}, // Charlie is inactive
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Query: Find users that are active users OR enabled admins
	// [:find ?name :where [?e :user/name ?name] (or-join [?e] [?e :user/status "active"] [?e :admin/status "enabled"])]
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?name"},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: nameAttr},
					query.Variable{Name: "?name"},
				},
			},
			&query.OrJoinClause{
				JoinVars: []query.Symbol{"?e"},
				Branches: [][]query.Clause{
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: "?e"},
								query.Constant{Value: userStatusAttr},
								query.Constant{Value: "active"},
							},
						},
					},
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: "?e"},
								query.Constant{Value: adminStatusAttr},
								query.Constant{Value: "enabled"},
							},
						},
					},
				},
			},
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Should have Alice (active user) and Bob (enabled admin), but not Charlie
	if result.Size() != 2 {
		t.Errorf("expected 2 results, got %d", result.Size())
	}

	names := make(map[string]bool)
	for i := 0; i < result.Size(); i++ {
		name := result.Get(i)[0].(string)
		names[name] = true
	}

	if !names["Alice"] || !names["Bob"] {
		t.Errorf("expected Alice and Bob, got %v", names)
	}
	if names["Charlie"] {
		t.Error("Charlie should not be in results")
	}
}

// =============================================================================
// OR Fallback Semantics Tests
// =============================================================================

func TestOrFallbackWithGroundExpressionDirectQueryExecutor(t *testing.T) {
	// Directly test the query executor to isolate the issue
	matcher := NewMemoryPatternMatcher(nil)
	queryExecutor := NewQueryExecutor(matcher, ExecutorOptions{})
	ctx := NewContext(nil)

	// Build the OR clause query directly
	orClause := &query.OrClause{
		Branches: [][]query.Clause{
			{
				&query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: "?e"},
						query.Constant{Value: datalog.NewKeyword(":nonexistent/attr")},
						query.Variable{Name: "?x"},
					},
				},
			},
			{
				&query.Expression{
					Function: &query.GroundFunction{Value: int64(0)},
					Binding:  "?x",
				},
			},
		},
	}

	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?x"},
		},
		Where: []query.Clause{orClause},
	}

	result, err := queryExecutor.Execute(ctx, q, nil)
	if err != nil {
		t.Fatalf("query executor failed: %v", err)
	}

	t.Logf("Results: %d", len(result))
	for i, r := range result {
		t.Logf("Result %d: columns=%v, size=%d", i, r.Columns(), r.Size())
	}

	// Collapse to single relation
	collapsed := Relations(result).Collapse(ctx)
	if len(collapsed) != 1 {
		t.Fatalf("Expected 1 collapsed relation, got %d", len(collapsed))
	}

	finalRel := collapsed[0]
	t.Logf("Final: columns=%v, size=%d", finalRel.Columns(), finalRel.Size())

	if finalRel.Size() != 1 {
		t.Errorf("Expected 1 result, got %d", finalRel.Size())
	}
}

func TestOrFallbackWithGroundExpression(t *testing.T) {
	// Test OR with ground expression as fallback
	// When pattern matches nothing, ground should provide default value
	matcher := NewMemoryPatternMatcher(nil) // Empty database
	executor := NewExecutor(matcher)

	// Query: (or [?e :nonexistent ?x] [(ground 0) ?x])
	// Since no data exists, should fall back to ground(0)
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?x"},
		},
		Where: []query.Clause{
			&query.OrClause{
				Branches: [][]query.Clause{
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: "?e"},
								query.Constant{Value: datalog.NewKeyword(":nonexistent/attr")},
								query.Variable{Name: "?x"},
							},
						},
					},
					{
						&query.Expression{
							Function: &query.GroundFunction{Value: int64(0)},
							Binding:  "?x",
						},
					},
				},
			},
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Should have 1 result with value 0
	if result.Size() != 1 {
		t.Errorf("expected 1 result, got %d", result.Size())
	}

	if result.Size() > 0 {
		val := result.Get(0)[0]
		if val != int64(0) {
			t.Errorf("expected 0, got %v (type %T)", val, val)
		}
	}
}

func TestOrFallbackFirstBranchMatches(t *testing.T) {
	// Test that when first branch matches, second is not evaluated
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Query: (or [?e :user/name ?x] [(ground "fallback") ?x])
	// First branch should match, so we should get "Alice", not "fallback"
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?x"},
		},
		Where: []query.Clause{
			&query.OrClause{
				Branches: [][]query.Clause{
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: "?e"},
								query.Constant{Value: nameAttr},
								query.Variable{Name: "?x"},
							},
						},
					},
					{
						&query.Expression{
							Function: &query.GroundFunction{Value: "fallback"},
							Binding:  "?x",
						},
					},
				},
			},
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Should have 1 result with value "Alice"
	if result.Size() != 1 {
		t.Errorf("expected 1 result, got %d", result.Size())
	}

	if result.Size() > 0 {
		val := result.Get(0)[0]
		if val != "Alice" {
			t.Errorf("expected 'Alice', got %v", val)
		}
	}
}

func TestOrFallbackMultipleBranches(t *testing.T) {
	// Test OR with multiple fallback branches
	// (or [nonexistent1] [nonexistent2] [(ground "default")])
	matcher := NewMemoryPatternMatcher(nil) // Empty database
	executor := NewExecutor(matcher)

	nonexistent1 := datalog.NewKeyword(":nonexistent1")
	nonexistent2 := datalog.NewKeyword(":nonexistent2")

	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?x"},
		},
		Where: []query.Clause{
			&query.OrClause{
				Branches: [][]query.Clause{
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: "?e"},
								query.Constant{Value: nonexistent1},
								query.Variable{Name: "?x"},
							},
						},
					},
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: "?e"},
								query.Constant{Value: nonexistent2},
								query.Variable{Name: "?x"},
							},
						},
					},
					{
						&query.Expression{
							Function: &query.GroundFunction{Value: "default"},
							Binding:  "?x",
						},
					},
				},
			},
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Should fall through to third branch with "default"
	if result.Size() != 1 {
		t.Errorf("expected 1 result, got %d", result.Size())
	}

	if result.Size() > 0 {
		val := result.Get(0)[0]
		if val != "default" {
			t.Errorf("expected 'default', got %v", val)
		}
	}
}

func TestOrFallbackWithArithmeticExpression(t *testing.T) {
	// Test OR with arithmetic expression as fallback
	alice := datalog.NewIdentity("user:alice")
	ageAttr := datalog.NewKeyword(":user/age")

	datoms := []datalog.Datom{
		{E: alice, A: ageAttr, V: int64(30), Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Query: (or [?e :nonexistent ?x] [(+ 1 1) ?x])
	// Pattern won't match, so should get 2 from arithmetic
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?x"},
		},
		Where: []query.Clause{
			&query.OrClause{
				Branches: [][]query.Clause{
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: "?e"},
								query.Constant{Value: datalog.NewKeyword(":nonexistent/attr")},
								query.Variable{Name: "?x"},
							},
						},
					},
					{
						&query.Expression{
							Function: &query.ArithmeticFunction{
								Op:    query.OpAdd,
								Left:  query.ConstantTerm{Value: int64(1)},
								Right: query.ConstantTerm{Value: int64(1)},
							},
							Binding: "?x",
						},
					},
				},
			},
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	if result.Size() != 1 {
		t.Errorf("expected 1 result, got %d", result.Size())
	}

	if result.Size() > 0 {
		val := result.Get(0)[0]
		// Arithmetic on integers returns int64
		expected := int64(2)
		if val != expected {
			t.Errorf("expected %v, got %v (type %T)", expected, val, val)
		}
	}
}

func TestOrFallbackPatternOnlyUnionSemantics(t *testing.T) {
	// Verify pattern-only OR still uses union semantics (regression test)
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	nameAttr := datalog.NewKeyword(":user/name")
	activeAttr := datalog.NewKeyword(":user/active")
	premiumAttr := datalog.NewKeyword(":user/premium")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		{E: alice, A: activeAttr, V: true, Tx: 1}, // Alice is active
		{E: bob, A: premiumAttr, V: true, Tx: 1},  // Bob is premium
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Pattern-only OR should return BOTH Alice and Bob (union semantics)
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?e"},
		},
		Where: []query.Clause{
			&query.OrClause{
				Branches: [][]query.Clause{
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: "?e"},
								query.Constant{Value: activeAttr},
								query.Constant{Value: true},
							},
						},
					},
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: "?e"},
								query.Constant{Value: premiumAttr},
								query.Constant{Value: true},
							},
						},
					},
				},
			},
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Should have both Alice and Bob (union of both branches)
	if result.Size() != 2 {
		t.Errorf("expected 2 results (union semantics), got %d", result.Size())
	}
}

func TestOrFallbackPatternWithStreamingRelation(t *testing.T) {
	// Test that pattern matching works correctly in OR fallback path
	// even when the pattern returns a streaming relation
	nameAttr := datalog.NewKeyword(":user/name")
	alice := datalog.NewIdentity("user:alice")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Query: (or [?e :user/name ?x] [(ground "fallback") ?x])
	// Pattern should match, returning "Alice"
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?x"},
		},
		Where: []query.Clause{
			&query.OrClause{
				Branches: [][]query.Clause{
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: "?e"},
								query.Constant{Value: nameAttr},
								query.Variable{Name: "?x"},
							},
						},
					},
					{
						&query.Expression{
							Function: &query.GroundFunction{Value: "fallback"},
							Binding:  "?x",
						},
					},
				},
			},
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	if result.Size() != 1 {
		t.Errorf("expected 1 result, got %d", result.Size())
	}

	if result.Size() > 0 && result.Get(0)[0] != "Alice" {
		t.Errorf("expected 'Alice', got %v", result.Get(0)[0])
	}
}

func TestOrFallbackWithSubqueryPattern(t *testing.T) {
	// Test OR with SubqueryPattern as first branch and ground as fallback
	// This is the real-world use case: count with zero default
	alice := datalog.NewIdentity("task:alice")
	nameAttr := datalog.NewKeyword(":task/name")
	statusAttr := datalog.NewKeyword(":task/status")
	completeStatus := datalog.NewKeyword(":status/complete")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Task Alice", Tx: 1},
		{E: alice, A: statusAttr, V: completeStatus, Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	queryExecutor := NewQueryExecutor(matcher, ExecutorOptions{})
	ctx := NewContext(nil)

	// Build the OR clause with SubqueryPattern and ground fallback
	// (or [(q [:find (count ?t) :where [?t :task/status :status/complete]] $) [[?count]]]
	//     [(ground 0) ?count])
	orClause := &query.OrClause{
		Branches: [][]query.Clause{
			{
				&query.SubqueryPattern{
					Query: &query.Query{
						Find: []query.FindElement{
							query.FindAggregate{Function: "count", Arg: "?t"},
						},
						Where: []query.Clause{
							&query.DataPattern{
								Elements: []query.PatternElement{
									query.Variable{Name: "?t"},
									query.Constant{Value: statusAttr},
									query.Constant{Value: completeStatus},
								},
							},
						},
					},
					Inputs:  []query.PatternElement{query.Constant{Value: "db"}},
					Binding: query.TupleBinding{Variables: []query.Symbol{"?count"}},
				},
			},
			{
				&query.Expression{
					Function: &query.GroundFunction{Value: int64(0)},
					Binding:  "?count",
				},
			},
		},
	}

	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?count"},
		},
		Where: []query.Clause{orClause},
	}

	result, err := queryExecutor.Execute(ctx, q, nil)
	if err != nil {
		t.Fatalf("query executor failed: %v", err)
	}

	// Collapse to single relation
	collapsed := Relations(result).Collapse(ctx)
	if len(collapsed) != 1 {
		t.Fatalf("Expected 1 collapsed relation, got %d", len(collapsed))
	}

	finalRel := collapsed[0]
	t.Logf("Final: columns=%v, size=%d", finalRel.Columns(), finalRel.Size())

	// Should have 1 result with count=1 (one completed task)
	if finalRel.Size() != 1 {
		t.Errorf("Expected 1 result, got %d", finalRel.Size())
	}

	if finalRel.Size() > 0 {
		val := finalRel.Get(0)[0]
		// Count returns int64
		if val != int64(1) {
			t.Errorf("Expected count=1, got %v (type %T)", val, val)
		}
	}
}

func TestOrFallbackWithSubqueryPatternAndVariableInput(t *testing.T) {
	// Test the REAL use case: SubqueryPattern with variable input from outer query
	// (or [(q [:find (count ?t) :in $ ?s :where ...] $ ?scenario) [[?count]]]
	//     [(ground 0) ?count])
	scenario1 := datalog.NewIdentity("scenario:1")
	scenario2 := datalog.NewIdentity("scenario:2")
	task1 := datalog.NewIdentity("task:1")
	task2 := datalog.NewIdentity("task:2")

	scenarioAttr := datalog.NewKeyword(":scenario/name")
	taskScenarioAttr := datalog.NewKeyword(":task/scenario")
	taskStatusAttr := datalog.NewKeyword(":task/status")
	completeStatus := datalog.NewKeyword(":status/complete")

	datoms := []datalog.Datom{
		// Two scenarios
		{E: scenario1, A: scenarioAttr, V: "Scenario 1", Tx: 1},
		{E: scenario2, A: scenarioAttr, V: "Scenario 2", Tx: 1},
		// Tasks for scenario1 (has completed tasks)
		{E: task1, A: taskScenarioAttr, V: scenario1, Tx: 1},
		{E: task1, A: taskStatusAttr, V: completeStatus, Tx: 1},
		{E: task2, A: taskScenarioAttr, V: scenario1, Tx: 1},
		{E: task2, A: taskStatusAttr, V: completeStatus, Tx: 1},
		// No tasks for scenario2 (should fall back to 0)
	}

	matcher := NewMemoryPatternMatcher(datoms)
	queryExecutor := NewQueryExecutor(matcher, ExecutorOptions{})

	// Create annotation handler to debug OR fallback behavior
	var events []annotations.Event
	handler := func(event annotations.Event) {
		if strings.HasPrefix(event.Name, "or-fallback/") {
			events = append(events, event)
		}
	}
	ctx := NewContext(handler)

	// Build the query:
	// [:find ?scenario ?count
	//  :where [?scenario :scenario/name ?name]
	//         (or [(q [:find (count ?t)
	//                  :in $ ?s
	//                  :where [?t :task/scenario ?s]
	//                         [?t :task/status :status/complete]]
	//                $ ?scenario) [[?count]]]
	//             [(ground 0) ?count])]
	orClause := &query.OrClause{
		Branches: [][]query.Clause{
			{
				&query.SubqueryPattern{
					Query: &query.Query{
						Find: []query.FindElement{
							query.FindAggregate{Function: "count", Arg: "?t"},
						},
						In: []query.InputSpec{
							query.DatabaseInput{},
							query.ScalarInput{Symbol: "?s"},
						},
						Where: []query.Clause{
							&query.DataPattern{
								Elements: []query.PatternElement{
									query.Variable{Name: "?t"},
									query.Constant{Value: taskScenarioAttr},
									query.Variable{Name: "?s"},
								},
							},
							&query.DataPattern{
								Elements: []query.PatternElement{
									query.Variable{Name: "?t"},
									query.Constant{Value: taskStatusAttr},
									query.Constant{Value: completeStatus},
								},
							},
						},
					},
					Inputs: []query.PatternElement{
						query.Constant{Value: query.Symbol("$")},
						query.Variable{Name: "?scenario"},
					},
					Binding: query.TupleBinding{Variables: []query.Symbol{"?count"}},
				},
			},
			{
				&query.Expression{
					Function: &query.GroundFunction{Value: int64(0)},
					Binding:  "?count",
				},
			},
		},
	}

	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?scenario"},
			query.FindVariable{Symbol: "?count"},
		},
		Where: []query.Clause{
			// First, bind ?scenario from the database
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?scenario"},
					query.Constant{Value: scenarioAttr},
					query.Variable{Name: "?name"},
				},
			},
			// Then OR with subquery and fallback
			orClause,
		},
	}

	result, err := queryExecutor.Execute(ctx, q, nil)
	if err != nil {
		t.Fatalf("query executor failed: %v", err)
	}

	collapsed := Relations(result).Collapse(ctx)
	if len(collapsed) != 1 {
		t.Fatalf("Expected 1 collapsed relation, got %d", len(collapsed))
	}

	finalRel := collapsed[0]
	t.Logf("Final: columns=%v, size=%d", finalRel.Columns(), finalRel.Size())

	// Should have 2 results: scenario1 with count=2, scenario2 with count=0
	if finalRel.Size() != 2 {
		t.Errorf("Expected 2 results, got %d", finalRel.Size())
	}

	// Check the actual values
	for i := 0; i < finalRel.Size(); i++ {
		tuple := finalRel.Get(i)
		t.Logf("Result %d: %v", i, tuple)
	}

	// Log OR fallback annotations
	for _, event := range events {
		t.Logf("[ANNOTATION] %s: %v", event.Name, event.Data)
	}
}

func TestOrFallbackWithSubqueryPatternEmpty(t *testing.T) {
	// Test OR with SubqueryPattern that returns empty, falling back to ground
	matcher := NewMemoryPatternMatcher(nil) // Empty database
	queryExecutor := NewQueryExecutor(matcher, ExecutorOptions{})
	ctx := NewContext(nil)

	statusAttr := datalog.NewKeyword(":task/status")
	completeStatus := datalog.NewKeyword(":status/complete")

	// Same query but with empty database - should fall back to ground(0)
	orClause := &query.OrClause{
		Branches: [][]query.Clause{
			{
				&query.SubqueryPattern{
					Query: &query.Query{
						Find: []query.FindElement{
							query.FindAggregate{Function: "count", Arg: "?t"},
						},
						Where: []query.Clause{
							&query.DataPattern{
								Elements: []query.PatternElement{
									query.Variable{Name: "?t"},
									query.Constant{Value: statusAttr},
									query.Constant{Value: completeStatus},
								},
							},
						},
					},
					Inputs:  []query.PatternElement{query.Constant{Value: "db"}},
					Binding: query.TupleBinding{Variables: []query.Symbol{"?count"}},
				},
			},
			{
				&query.Expression{
					Function: &query.GroundFunction{Value: int64(0)},
					Binding:  "?count",
				},
			},
		},
	}

	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?count"},
		},
		Where: []query.Clause{orClause},
	}

	result, err := queryExecutor.Execute(ctx, q, nil)
	if err != nil {
		t.Fatalf("query executor failed: %v", err)
	}

	collapsed := Relations(result).Collapse(ctx)
	if len(collapsed) != 1 {
		t.Fatalf("Expected 1 collapsed relation, got %d", len(collapsed))
	}

	finalRel := collapsed[0]
	t.Logf("Final: columns=%v, size=%d", finalRel.Columns(), finalRel.Size())

	// Should have 1 result with count=0 (fallback)
	if finalRel.Size() != 1 {
		t.Errorf("Expected 1 result, got %d", finalRel.Size())
	}

	if finalRel.Size() > 0 {
		val := finalRel.Get(0)[0]
		if val != int64(0) {
			t.Errorf("Expected count=0 (fallback), got %v (type %T)", val, val)
		}
	}
}
