package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
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
		{E: alice, A: userStatusAttr, V: "active", Tx: 1},    // Alice is active user
		{E: bob, A: adminStatusAttr, V: "enabled", Tx: 1},    // Bob is enabled admin
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
