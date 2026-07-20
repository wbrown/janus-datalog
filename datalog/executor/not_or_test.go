package executor

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/parser"
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
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: nameAttr, V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		// Alice is archived
		{E: alice, A: archivedAttr, V: true, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Query: Find non-archived users
			// [:find ?name :where [?e :user/name ?name] (not [?e :user/archived true])]
			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?name")},
				},
				Where: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: datalog.NewSymbol("?e")},
							query.Constant{Value: nameAttr},
							query.Variable{Name: datalog.NewSymbol("?name")},
						},
					},
					&query.NotClause{
						Clauses: []query.Clause{
							&query.DataPattern{
								Elements: []query.PatternElement{
									query.Variable{Name: datalog.NewSymbol("?e")},
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
		})
	}
}

// TestNotClauseWithOrJoinBody pins NOT-over-or-join correlation: an or-join
// inside a NOT body exposes its JoinVars, and those are anti-join keys
// between the outer relation and the body. Rows whose ?v matches through the
// or-join are filtered; the rest survive.
func TestNotClauseWithOrJoinBody(t *testing.T) {
	valAttr := datalog.NewKeyword(":item/val")
	seenAttr := datalog.NewKeyword(":seen/val")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("item:1"), A: valAttr, V: int64(1), Tx: tx},
		{E: datalog.NewIdentity("item:2"), A: valAttr, V: int64(2), Tx: tx},
		{E: datalog.NewIdentity("item:3"), A: valAttr, V: int64(3), Tx: tx},
		{E: datalog.NewIdentity("seen:1"), A: seenAttr, V: int64(2), Tx: tx},
	}
	matcher := NewMemoryPatternMatcher(datoms)

	v := datalog.NewSymbol("?v")
	q := &query.Query{
		Find: []query.FindElement{query.FindVariable{Symbol: v}},
		Where: []query.Clause{
			&query.DataPattern{Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?e")},
				query.Constant{Value: valAttr},
				query.Variable{Name: v},
			}},
			&query.NotClause{Clauses: []query.Clause{
				&query.OrJoinClause{
					JoinVars: []query.Symbol{v},
					Branches: [][]query.Clause{{
						&query.DataPattern{Elements: []query.PatternElement{
							query.Variable{Name: datalog.NewSymbol("?d")},
							query.Constant{Value: seenAttr},
							query.Variable{Name: v},
						}},
					}},
				},
			}},
		},
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("execution failed: %v", err)
			}
			rows, err := CollectTuples(result, nil)
			if err != nil {
				t.Fatalf("collect failed: %v", err)
			}
			got := map[int64]bool{}
			for _, row := range rows {
				got[row[0].(int64)] = true
			}
			if len(rows) != 2 || !got[1] || !got[3] || got[2] {
				t.Errorf("expected {1 3} to survive the NOT (2 is seen), got %v", rows)
			}
		})
	}
}

// TestNotClauseWithExistentialBodyVariable pins the plain-pattern sibling of
// the or-join reproducer: (not [?d :seen/val ?v]) where ?d is existential
// (body-local) and ?v correlates with the outer relation. Rows whose ?v has
// any :seen/val match are filtered; ?d must not become a scheduling
// requirement.
func TestNotClauseWithExistentialBodyVariable(t *testing.T) {
	valAttr := datalog.NewKeyword(":item/val")
	seenAttr := datalog.NewKeyword(":seen/val")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("item:1"), A: valAttr, V: int64(1), Tx: tx},
		{E: datalog.NewIdentity("item:2"), A: valAttr, V: int64(2), Tx: tx},
		{E: datalog.NewIdentity("item:3"), A: valAttr, V: int64(3), Tx: tx},
		{E: datalog.NewIdentity("seen:1"), A: seenAttr, V: int64(2), Tx: tx},
	}
	matcher := NewMemoryPatternMatcher(datoms)

	v := datalog.NewSymbol("?v")
	q := &query.Query{
		Find: []query.FindElement{query.FindVariable{Symbol: v}},
		Where: []query.Clause{
			&query.DataPattern{Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?e")},
				query.Constant{Value: valAttr},
				query.Variable{Name: v},
			}},
			&query.NotClause{Clauses: []query.Clause{
				&query.DataPattern{Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?d")},
					query.Constant{Value: seenAttr},
					query.Variable{Name: v},
				}},
			}},
		},
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("execution failed: %v", err)
			}
			rows, err := CollectTuples(result, nil)
			if err != nil {
				t.Fatalf("collect failed: %v", err)
			}
			got := map[int64]bool{}
			for _, row := range rows {
				got[row[0].(int64)] = true
			}
			if len(rows) != 2 || !got[1] || !got[3] || got[2] {
				t.Errorf("expected {1 3} to survive the NOT (2 is seen), got %v", rows)
			}
		})
	}
}

// TestOrDefaultJoinBranchLocalAlphaEquivalence pins branch-local scoping on
// the declared interface: a branch variable outside the header is a local,
// so renaming it — including onto a name the outer relation binds — must not
// change results. The shadowing variant fails if branch evaluation leaks
// outer bindings beyond the header (name capture).
func TestOrDefaultJoinBranchLocalAlphaEquivalence(t *testing.T) {
	valAttr := datalog.NewKeyword(":item/val")
	seenAttr := datalog.NewKeyword(":seen/val")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("item:1"), A: valAttr, V: int64(1), Tx: tx},
		{E: datalog.NewIdentity("item:2"), A: valAttr, V: int64(2), Tx: tx},
		{E: datalog.NewIdentity("item:3"), A: valAttr, V: int64(3), Tx: tx},
		{E: datalog.NewIdentity("seen:1"), A: seenAttr, V: int64(2), Tx: tx},
	}

	v := datalog.NewSymbol("?v")
	flag := datalog.NewSymbol("?flag")

	// (or-default-join [[?v] ?flag]
	//   (and [<local> :seen/val ?v] [(ground true) ?flag])
	//   [(ground false) ?flag])
	buildQuery := func(local query.Symbol) *query.Query {
		return &query.Query{
			Find: []query.FindElement{
				query.FindVariable{Symbol: v},
				query.FindVariable{Symbol: flag},
			},
			Where: []query.Clause{
				&query.DataPattern{Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: valAttr},
					query.Variable{Name: v},
				}},
				&query.OrDefaultJoinClause{
					RequiredVars: []query.Symbol{v},
					OutputVars:   []query.Symbol{flag},
					Branches: [][]query.Clause{
						{
							&query.DataPattern{Elements: []query.PatternElement{
								query.Variable{Name: local},
								query.Constant{Value: seenAttr},
								query.Variable{Name: v},
							}},
							&query.Expression{
								Function: &query.GroundFunction{Value: true},
								Binding:  flag,
							},
						},
						{
							&query.Expression{
								Function: &query.GroundFunction{Value: false},
								Binding:  flag,
							},
						},
					},
				},
			},
		}
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			run := func(local query.Symbol) map[int64]bool {
				t.Helper()
				exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
				result, err := exec.Execute(buildQuery(local))
				if err != nil {
					t.Fatalf("execution failed with branch local %s: %v", local, err)
				}
				rows, err := CollectTuples(result, nil)
				if err != nil {
					t.Fatalf("collect failed with branch local %s: %v", local, err)
				}
				got := map[int64]bool{}
				for _, row := range rows {
					got[row[0].(int64)] = row[1].(bool)
				}
				if len(got) != 3 {
					t.Fatalf("expected 3 distinct ?v with branch local %s, got %v", local, rows)
				}
				return got
			}

			fresh := run(datalog.NewSymbol("?d"))
			if fresh[1] || !fresh[2] || fresh[3] {
				t.Errorf("expected only ?v=2 flagged seen with fresh local, got %v", fresh)
			}

			// Shadowing the outer ?e must not change anything: the local is scoped
			// to the branch.
			shadowing := run(datalog.NewSymbol("?e"))
			if shadowing[1] || !shadowing[2] || shadowing[3] {
				t.Errorf("expected only ?v=2 flagged seen with shadowing local, got %v", shadowing)
			}
		})
	}
}

// TestOrJoinBranchLocalAlphaEquivalence pins the same scoping invariant on
// the or-join correlated-union path: a branch variable outside the header is
// a local, and shadowing an outer name must not change the union's results.
func TestOrJoinBranchLocalAlphaEquivalence(t *testing.T) {
	valAttr := datalog.NewKeyword(":item/val")
	seenAttr := datalog.NewKeyword(":seen/val")
	alsoAttr := datalog.NewKeyword(":also/val")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("item:1"), A: valAttr, V: int64(1), Tx: tx},
		{E: datalog.NewIdentity("item:2"), A: valAttr, V: int64(2), Tx: tx},
		{E: datalog.NewIdentity("item:3"), A: valAttr, V: int64(3), Tx: tx},
		{E: datalog.NewIdentity("seen:1"), A: seenAttr, V: int64(2), Tx: tx},
		{E: datalog.NewIdentity("also:1"), A: alsoAttr, V: int64(3), Tx: tx},
	}

	v := datalog.NewSymbol("?v")

	// [?e :item/val ?v] (or-join [?v] [<local> :seen/val ?v] [<local> :also/val ?v])
	buildQuery := func(local query.Symbol) *query.Query {
		return &query.Query{
			Find: []query.FindElement{query.FindVariable{Symbol: v}},
			Where: []query.Clause{
				&query.DataPattern{Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: valAttr},
					query.Variable{Name: v},
				}},
				&query.OrJoinClause{
					JoinVars: []query.Symbol{v},
					Branches: [][]query.Clause{
						{&query.DataPattern{Elements: []query.PatternElement{
							query.Variable{Name: local},
							query.Constant{Value: seenAttr},
							query.Variable{Name: v},
						}}},
						{&query.DataPattern{Elements: []query.PatternElement{
							query.Variable{Name: local},
							query.Constant{Value: alsoAttr},
							query.Variable{Name: v},
						}}},
					},
				},
			},
		}
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			run := func(local query.Symbol) map[int64]bool {
				t.Helper()
				exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
				result, err := exec.Execute(buildQuery(local))
				if err != nil {
					t.Fatalf("execution failed with branch local %s: %v", local, err)
				}
				rows, err := CollectTuples(result, nil)
				if err != nil {
					t.Fatalf("collect failed with branch local %s: %v", local, err)
				}
				got := map[int64]bool{}
				for _, row := range rows {
					got[row[0].(int64)] = true
				}
				return got
			}

			fresh := run(datalog.NewSymbol("?d"))
			if len(fresh) != 2 || !fresh[2] || !fresh[3] {
				t.Errorf("expected {2 3} (seen or also) with fresh local, got %v", fresh)
			}

			shadowing := run(datalog.NewSymbol("?e"))
			if len(shadowing) != 2 || !shadowing[2] || !shadowing[3] {
				t.Errorf("expected {2 3} (seen or also) with shadowing local, got %v", shadowing)
			}
		})
	}
}

func TestNotClauseNoMatches(t *testing.T) {
	// When NOT clause matches nothing, all results should be kept
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	nameAttr := datalog.NewKeyword(":user/name")
	archivedAttr := datalog.NewKeyword(":user/archived")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: nameAttr, V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		// No one is archived
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Query: Find non-archived users (no one is archived)
			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?name")},
				},
				Where: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: datalog.NewSymbol("?e")},
							query.Constant{Value: nameAttr},
							query.Variable{Name: datalog.NewSymbol("?name")},
						},
					},
					&query.NotClause{
						Clauses: []query.Clause{
							&query.DataPattern{
								Elements: []query.PatternElement{
									query.Variable{Name: datalog.NewSymbol("?e")},
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
		})
	}
}

func TestNotClauseAllMatch(t *testing.T) {
	// When NOT clause matches everything, no results should be returned
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	nameAttr := datalog.NewKeyword(":user/name")
	archivedAttr := datalog.NewKeyword(":user/archived")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: nameAttr, V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		// Everyone is archived
		{E: alice, A: archivedAttr, V: true, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
		{E: bob, A: archivedAttr, V: true, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Query: Find non-archived users (everyone is archived)
			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?name")},
				},
				Where: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: datalog.NewSymbol("?e")},
							query.Constant{Value: nameAttr},
							query.Variable{Name: datalog.NewSymbol("?name")},
						},
					},
					&query.NotClause{
						Clauses: []query.Clause{
							&query.DataPattern{
								Elements: []query.PatternElement{
									query.Variable{Name: datalog.NewSymbol("?e")},
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
		})
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
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: nameAttr, V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		// Alice is archived
		{E: alice, A: archivedAttr, V: true, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
		// Bob is deleted
		{E: bob, A: deletedAttr, V: true, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Query: Find users that are neither archived nor deleted
			// [:find ?name :where [?e :user/name ?name] (not-join [?e] [?e :user/archived true] [?e :user/deleted true])]
			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?name")},
				},
				Where: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: datalog.NewSymbol("?e")},
							query.Constant{Value: nameAttr},
							query.Variable{Name: datalog.NewSymbol("?name")},
						},
					},
					&query.NotJoinClause{
						JoinVars: []query.Symbol{datalog.NewSymbol("?e")},
						Clauses: []query.Clause{
							&query.DataPattern{
								Elements: []query.PatternElement{
									query.Variable{Name: datalog.NewSymbol("?e")},
									query.Constant{Value: archivedAttr},
									query.Constant{Value: true},
								},
							},
							&query.DataPattern{
								Elements: []query.PatternElement{
									query.Variable{Name: datalog.NewSymbol("?e")},
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
		})
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
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: nameAttr, V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: statusAttr, V: "active", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: statusAttr, V: "pending", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: charlie, A: statusAttr, V: "inactive", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Query: Find users with status "active" OR "pending"
			// [:find ?name :where [?e :user/name ?name] (or [?e :user/status "active"] [?e :user/status "pending"])]
			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?name")},
				},
				Where: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: datalog.NewSymbol("?e")},
							query.Constant{Value: nameAttr},
							query.Variable{Name: datalog.NewSymbol("?name")},
						},
					},
					&query.OrClause{
						Branches: [][]query.Clause{
							{
								&query.DataPattern{
									Elements: []query.PatternElement{
										query.Variable{Name: datalog.NewSymbol("?e")},
										query.Constant{Value: statusAttr},
										query.Constant{Value: "active"},
									},
								},
							},
							{
								&query.DataPattern{
									Elements: []query.PatternElement{
										query.Variable{Name: datalog.NewSymbol("?e")},
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
		})
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
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: nameAttr, V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: userStatusAttr, V: "active", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},     // Alice is active user
		{E: bob, A: adminStatusAttr, V: "enabled", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},     // Bob is enabled admin
		{E: charlie, A: userStatusAttr, V: "inactive", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}}, // Charlie is inactive
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Query: Find users that are active users OR enabled admins
			// [:find ?name :where [?e :user/name ?name] (or-join [?e] [?e :user/status "active"] [?e :admin/status "enabled"])]
			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?name")},
				},
				Where: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: datalog.NewSymbol("?e")},
							query.Constant{Value: nameAttr},
							query.Variable{Name: datalog.NewSymbol("?name")},
						},
					},
					&query.OrJoinClause{
						JoinVars: []query.Symbol{datalog.NewSymbol("?e")},
						Branches: [][]query.Clause{
							{
								&query.DataPattern{
									Elements: []query.PatternElement{
										query.Variable{Name: datalog.NewSymbol("?e")},
										query.Constant{Value: userStatusAttr},
										query.Constant{Value: "active"},
									},
								},
							},
							{
								&query.DataPattern{
									Elements: []query.PatternElement{
										query.Variable{Name: datalog.NewSymbol("?e")},
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
		})
	}
}

// =============================================================================
// OR Fallback Semantics Tests
// =============================================================================

func TestOrFallbackWithGroundExpressionDirectQueryExecutor(t *testing.T) {
	// Directly test the query executor to isolate the issue
	matcher := NewMemoryPatternMatcher(nil)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			queryExecutor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Build the OR clause query directly
			orClause := &query.OrClause{
				Branches: [][]query.Clause{
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: datalog.NewSymbol("?e")},
								query.Constant{Value: datalog.NewKeyword(":nonexistent/attr")},
								query.Variable{Name: datalog.NewSymbol("?x")},
							},
						},
					},
					{
						&query.Expression{
							Function: &query.GroundFunction{Value: int64(0)},
							Binding:  datalog.NewSymbol("?x"),
						},
					},
				},
			}

			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?x")},
				},
				Where: []query.Clause{orClause},
			}

			result, err := queryExecutor.Execute(q)
			if err != nil {
				t.Fatalf("query executor failed: %v", err)
			}

			t.Logf("Final: symbols=%v, size=%d", result.Symbols(), result.Size())

			if result.Size() != 1 {
				t.Errorf("Expected 1 result, got %d", result.Size())
			}
		})
	}
}

func TestOrFallbackWithGroundExpression(t *testing.T) {
	// Test OR with ground expression as fallback
	// When pattern matches nothing, ground should provide default value
	matcher := NewMemoryPatternMatcher(nil) // Empty database

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Query: (or [?e :nonexistent ?x] [(ground 0) ?x])
			// Since no data exists, should fall back to ground(0)
			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?x")},
				},
				Where: []query.Clause{
					&query.OrClause{
						Branches: [][]query.Clause{
							{
								&query.DataPattern{
									Elements: []query.PatternElement{
										query.Variable{Name: datalog.NewSymbol("?e")},
										query.Constant{Value: datalog.NewKeyword(":nonexistent/attr")},
										query.Variable{Name: datalog.NewSymbol("?x")},
									},
								},
							},
							{
								&query.Expression{
									Function: &query.GroundFunction{Value: int64(0)},
									Binding:  datalog.NewSymbol("?x"),
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
		})
	}
}

func TestOrFallbackFirstBranchMatches(t *testing.T) {
	// Test that when first branch matches, second is not evaluated
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Query: (or-default [?e :user/name ?x] [(ground "fallback") ?x])
			// First branch should match, so we should get "Alice", not "fallback"
			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?x")},
				},
				Where: []query.Clause{
					&query.OrDefaultClause{
						Branches: [][]query.Clause{
							{
								&query.DataPattern{
									Elements: []query.PatternElement{
										query.Variable{Name: datalog.NewSymbol("?e")},
										query.Constant{Value: nameAttr},
										query.Variable{Name: datalog.NewSymbol("?x")},
									},
								},
							},
							{
								&query.Expression{
									Function: &query.GroundFunction{Value: "fallback"},
									Binding:  datalog.NewSymbol("?x"),
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
		})
	}
}

func TestOrFallbackMultipleBranches(t *testing.T) {
	// Test OR with multiple fallback branches
	// (or [nonexistent1] [nonexistent2] [(ground "default")])
	matcher := NewMemoryPatternMatcher(nil) // Empty database

	nonexistent1 := datalog.NewKeyword(":nonexistent1")
	nonexistent2 := datalog.NewKeyword(":nonexistent2")

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?x")},
				},
				Where: []query.Clause{
					&query.OrClause{
						Branches: [][]query.Clause{
							{
								&query.DataPattern{
									Elements: []query.PatternElement{
										query.Variable{Name: datalog.NewSymbol("?e")},
										query.Constant{Value: nonexistent1},
										query.Variable{Name: datalog.NewSymbol("?x")},
									},
								},
							},
							{
								&query.DataPattern{
									Elements: []query.PatternElement{
										query.Variable{Name: datalog.NewSymbol("?e")},
										query.Constant{Value: nonexistent2},
										query.Variable{Name: datalog.NewSymbol("?x")},
									},
								},
							},
							{
								&query.Expression{
									Function: &query.GroundFunction{Value: "default"},
									Binding:  datalog.NewSymbol("?x"),
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
		})
	}
}

func TestOrFallbackWithArithmeticExpression(t *testing.T) {
	// Test OR with arithmetic expression as fallback
	alice := datalog.NewIdentity("user:alice")
	ageAttr := datalog.NewKeyword(":user/age")

	datoms := []datalog.Datom{
		{E: alice, A: ageAttr, V: int64(30), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Query: (or [?e :nonexistent ?x] [(+ 1 1) ?x])
			// Pattern won't match, so should get 2 from arithmetic
			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?x")},
				},
				Where: []query.Clause{
					&query.OrClause{
						Branches: [][]query.Clause{
							{
								&query.DataPattern{
									Elements: []query.PatternElement{
										query.Variable{Name: datalog.NewSymbol("?e")},
										query.Constant{Value: datalog.NewKeyword(":nonexistent/attr")},
										query.Variable{Name: datalog.NewSymbol("?x")},
									},
								},
							},
							{
								&query.Expression{
									Function: &query.ArithmeticFunction{
										Op: datalog.SymAdd,
										Args: []query.Term{
											query.ConstantTerm{Value: int64(1)},
											query.ConstantTerm{Value: int64(1)},
										},
									},
									Binding: datalog.NewSymbol("?x"),
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
		})
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
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: nameAttr, V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: activeAttr, V: true, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}}, // Alice is active
		{E: bob, A: premiumAttr, V: true, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},  // Bob is premium
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Pattern-only OR should return BOTH Alice and Bob (union semantics)
			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?e")},
				},
				Where: []query.Clause{
					&query.OrClause{
						Branches: [][]query.Clause{
							{
								&query.DataPattern{
									Elements: []query.PatternElement{
										query.Variable{Name: datalog.NewSymbol("?e")},
										query.Constant{Value: activeAttr},
										query.Constant{Value: true},
									},
								},
							},
							{
								&query.DataPattern{
									Elements: []query.PatternElement{
										query.Variable{Name: datalog.NewSymbol("?e")},
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
		})
	}
}

func TestOrFallbackPatternWithStreamingRelation(t *testing.T) {
	// Test that pattern matching works correctly in OR fallback path
	// even when the pattern returns a streaming relation
	nameAttr := datalog.NewKeyword(":user/name")
	alice := datalog.NewIdentity("user:alice")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Query: (or-default [?e :user/name ?x] [(ground "fallback") ?x])
			// Pattern should match, returning "Alice"
			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?x")},
				},
				Where: []query.Clause{
					&query.OrDefaultClause{
						Branches: [][]query.Clause{
							{
								&query.DataPattern{
									Elements: []query.PatternElement{
										query.Variable{Name: datalog.NewSymbol("?e")},
										query.Constant{Value: nameAttr},
										query.Variable{Name: datalog.NewSymbol("?x")},
									},
								},
							},
							{
								&query.Expression{
									Function: &query.GroundFunction{Value: "fallback"},
									Binding:  datalog.NewSymbol("?x"),
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
		})
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
		{E: alice, A: nameAttr, V: "Task Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: statusAttr, V: completeStatus, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			queryExecutor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Build the OR clause with SubqueryPattern and ground fallback
			// (or-default [(q [:find (count ?t) :where [?t :task/status :status/complete]] $) [[?count]]]
			//             [(ground 0) ?count])
			orClause := &query.OrDefaultClause{
				Branches: [][]query.Clause{
					{
						&query.SubqueryPattern{
							Query: &query.Query{
								Find: []query.FindElement{
									query.FindAggregate{Function: datalog.SymCount, Arg: datalog.NewSymbol("?t")},
								},
								Where: []query.Clause{
									&query.DataPattern{
										Elements: []query.PatternElement{
											query.Variable{Name: datalog.NewSymbol("?t")},
											query.Constant{Value: statusAttr},
											query.Constant{Value: completeStatus},
										},
									},
								},
							},
							Inputs:  []query.PatternElement{query.Constant{Value: datalog.NewSymbol("$")}},
							Binding: query.TupleBinding{Variables: []query.Symbol{datalog.NewSymbol("?count")}},
						},
					},
					{
						&query.Expression{
							Function: &query.GroundFunction{Value: int64(0)},
							Binding:  datalog.NewSymbol("?count"),
						},
					},
				},
			}

			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?count")},
				},
				Where: []query.Clause{orClause},
			}

			result, err := queryExecutor.Execute(q)
			if err != nil {
				t.Fatalf("query executor failed: %v", err)
			}

			t.Logf("Final: symbols=%v, size=%d", result.Symbols(), result.Size())

			// Should have 1 result with count=1 (one completed task)
			if result.Size() != 1 {
				t.Errorf("Expected 1 result, got %d", result.Size())
			}

			if result.Size() > 0 {
				val := result.Get(0)[0]
				// Count returns int64
				if val != int64(1) {
					t.Errorf("Expected count=1, got %v (type %T)", val, val)
				}
			}
		})
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
		{E: scenario1, A: scenarioAttr, V: "Scenario 1", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: scenario2, A: scenarioAttr, V: "Scenario 2", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		// Tasks for scenario1 (has completed tasks)
		{E: task1, A: taskScenarioAttr, V: scenario1, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: task1, A: taskStatusAttr, V: completeStatus, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: task2, A: taskScenarioAttr, V: scenario1, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: task2, A: taskStatusAttr, V: completeStatus, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		// No tasks for scenario2 (should fall back to 0)
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			queryExecutor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

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
			orClause := &query.OrDefaultClause{
				Branches: [][]query.Clause{
					{
						&query.SubqueryPattern{
							Query: &query.Query{
								Find: []query.FindElement{
									query.FindAggregate{Function: datalog.SymCount, Arg: datalog.NewSymbol("?t")},
								},
								In: []query.InputSpec{
									query.DatabaseInput{Name: datalog.NewSymbol("$")},
									query.ScalarInput{Symbol: datalog.NewSymbol("?s")},
								},
								Where: []query.Clause{
									&query.DataPattern{
										Elements: []query.PatternElement{
											query.Variable{Name: datalog.NewSymbol("?t")},
											query.Constant{Value: taskScenarioAttr},
											query.Variable{Name: datalog.NewSymbol("?s")},
										},
									},
									&query.DataPattern{
										Elements: []query.PatternElement{
											query.Variable{Name: datalog.NewSymbol("?t")},
											query.Constant{Value: taskStatusAttr},
											query.Constant{Value: completeStatus},
										},
									},
								},
							},
							Inputs: []query.PatternElement{
								query.Constant{Value: datalog.NewSymbol("$")},
								query.Variable{Name: datalog.NewSymbol("?scenario")},
							},
							Binding: query.TupleBinding{Variables: []query.Symbol{datalog.NewSymbol("?count")}},
						},
					},
					{
						&query.Expression{
							Function: &query.GroundFunction{Value: int64(0)},
							Binding:  datalog.NewSymbol("?count"),
						},
					},
				},
			}

			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?scenario")},
					query.FindVariable{Symbol: datalog.NewSymbol("?count")},
				},
				Where: []query.Clause{
					// First, bind ?scenario from the database
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: datalog.NewSymbol("?scenario")},
							query.Constant{Value: scenarioAttr},
							query.Variable{Name: datalog.NewSymbol("?name")},
						},
					},
					// Then OR with subquery and fallback
					orClause,
				},
			}

			result, err := queryExecutor.ExecuteWithContext(ctx, q)
			if err != nil {
				t.Fatalf("query executor failed: %v", err)
			}

			t.Logf("Final: symbols=%v, size=%d", result.Symbols(), result.Size())

			// Should have 2 results: scenario1 with count=2, scenario2 with count=0
			if result.Size() != 2 {
				t.Errorf("Expected 2 results, got %d", result.Size())
			}

			// Check the actual values
			for i := 0; i < result.Size(); i++ {
				tuple := result.Get(i)
				t.Logf("Result %d: %v", i, tuple)
			}

			// Log OR fallback annotations
			for _, event := range events {
				t.Logf("[ANNOTATION] %s: %v", event.Name, event.Data)
			}
		})
	}
}

// TestOrFallbackWithPatternAndTupleGround tests the critical case where:
// - Outer relation has multiple tuples
// - First OR branch is pattern-only (matches some tuples, not others)
// - Second OR branch is tuple ground fallback
// This is the exact scenario from TestTupleGroundQBInOr
func TestOrFallbackWithPatternAndTupleGround(t *testing.T) {
	// Setup: two scenarios, only one has tasks
	scenario1 := datalog.NewIdentity("scenario:1")
	scenario2 := datalog.NewIdentity("scenario:2")
	task1 := datalog.NewIdentity("task:1")

	nameAttr := datalog.NewKeyword(":scenario/name")
	taskAttr := datalog.NewKeyword(":scenario/task")
	countAttr := datalog.NewKeyword(":task/count")

	datoms := []datalog.Datom{
		{E: scenario1, A: nameAttr, V: "Scenario One", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: scenario2, A: nameAttr, V: "Scenario Two", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: scenario1, A: taskAttr, V: task1, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: task1, A: countAttr, V: int64(5), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		// Note: scenario2 has NO tasks
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			// Add annotation handler to see what's happening
			var events []annotations.Event
			handler := func(event annotations.Event) {
				events = append(events, event)
			}
			ctx := NewContext(handler)
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Query:
			// [:find ?scenario ?name ?taskCount
			//  :where [?scenario :scenario/name ?name]
			//         (or-default (and [?scenario :scenario/task ?task]
			//                         [?task :task/count ?taskCount])
			//                    [(ground [0]) [?taskCount]])]
			orClause := &query.OrDefaultClause{
				Branches: [][]query.Clause{
					// Branch 1: pattern-only (matches scenario1, not scenario2)
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: datalog.NewSymbol("?scenario")},
								query.Constant{Value: taskAttr},
								query.Variable{Name: datalog.NewSymbol("?task")},
							},
						},
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: datalog.NewSymbol("?task")},
								query.Constant{Value: countAttr},
								query.Variable{Name: datalog.NewSymbol("?taskCount")},
							},
						},
					},
					// Branch 2: tuple ground fallback
					{
						&query.Expression{
							Function: &query.GroundFunction{Value: []interface{}{int64(0)}},
							Binding:  query.TupleBinding{Variables: []query.Symbol{datalog.NewSymbol("?taskCount")}},
						},
					},
				},
			}

			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?scenario")},
					query.FindVariable{Symbol: datalog.NewSymbol("?name")},
					query.FindVariable{Symbol: datalog.NewSymbol("?taskCount")},
				},
				Where: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: datalog.NewSymbol("?scenario")},
							query.Constant{Value: nameAttr},
							query.Variable{Name: datalog.NewSymbol("?name")},
						},
					},
					orClause,
				},
			}

			result, err := executor.ExecuteWithContext(ctx, q)
			if err != nil {
				t.Fatalf("execution failed: %v", err)
			}

			t.Logf("Result symbols: %v", result.Symbols())
			t.Logf("Result size: %d", result.Size())
			for i := 0; i < result.Size(); i++ {
				t.Logf("Tuple %d: %v", i, result.Get(i))
			}

			// Log relevant annotations
			for _, event := range events {
				// Log all events for debugging
				t.Logf("[%s] %v", event.Name, event.Data)
			}

			// Should have 2 results:
			// - scenario1 with taskCount=5 (from pattern branch)
			// - scenario2 with taskCount=0 (from tuple ground fallback)
			if result.Size() != 2 {
				t.Errorf("Expected 2 results, got %d", result.Size())
			}

			// Build result map
			resultMap := make(map[string]int64)
			for i := 0; i < result.Size(); i++ {
				tuple := result.Get(i)
				name := tuple[1].(string)
				taskCount := tuple[2].(int64)
				resultMap[name] = taskCount
			}

			// Verify scenario1 has count 5
			if count, ok := resultMap["Scenario One"]; !ok {
				t.Error("Missing Scenario One")
			} else if count != 5 {
				t.Errorf("Scenario One: expected taskCount=5, got %d", count)
			}

			// Verify scenario2 has count 0 (from fallback)
			if count, ok := resultMap["Scenario Two"]; !ok {
				t.Error("Missing Scenario Two - tuple ground fallback did not trigger")
			} else if count != 0 {
				t.Errorf("Scenario Two: expected taskCount=0, got %d", count)
			}
		})
	}
}

func TestOrFallbackWithPatternAndScalarGround(t *testing.T) {
	// Setup: two scenarios, only one has tasks
	scenario1 := datalog.NewIdentity("scenario:1")
	scenario2 := datalog.NewIdentity("scenario:2")
	task1 := datalog.NewIdentity("task:1")

	nameAttr := datalog.NewKeyword(":scenario/name")
	taskAttr := datalog.NewKeyword(":scenario/task")
	countAttr := datalog.NewKeyword(":task/count")

	datoms := []datalog.Datom{
		{E: scenario1, A: nameAttr, V: "Scenario One", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: scenario2, A: nameAttr, V: "Scenario Two", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: scenario1, A: taskAttr, V: task1, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: task1, A: countAttr, V: int64(5), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Same as TestOrFallbackWithPatternAndTupleGround but with SCALAR ground
			orClause := &query.OrDefaultClause{
				Branches: [][]query.Clause{
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: datalog.NewSymbol("?scenario")},
								query.Constant{Value: taskAttr},
								query.Variable{Name: datalog.NewSymbol("?task")},
							},
						},
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: datalog.NewSymbol("?task")},
								query.Constant{Value: countAttr},
								query.Variable{Name: datalog.NewSymbol("?taskCount")},
							},
						},
					},
					// Branch 2: SCALAR ground fallback
					{
						&query.Expression{
							Function: &query.GroundFunction{Value: int64(0)},
							Binding:  datalog.NewSymbol("?taskCount"),
						},
					},
				},
			}

			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?scenario")},
					query.FindVariable{Symbol: datalog.NewSymbol("?name")},
					query.FindVariable{Symbol: datalog.NewSymbol("?taskCount")},
				},
				Where: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: datalog.NewSymbol("?scenario")},
							query.Constant{Value: nameAttr},
							query.Variable{Name: datalog.NewSymbol("?name")},
						},
					},
					orClause,
				},
			}

			result, err := executor.Execute(q)
			if err != nil {
				t.Fatalf("execution failed: %v", err)
			}

			t.Logf("Result symbols: %v", result.Symbols())
			t.Logf("Result size: %d", result.Size())
			for i := 0; i < result.Size(); i++ {
				t.Logf("Tuple %d: %v", i, result.Get(i))
			}

			// Should have 2 results
			if result.Size() != 2 {
				t.Errorf("Expected 2 results, got %d", result.Size())
			}

			// Build result map
			resultMap := make(map[string]int64)
			for i := 0; i < result.Size(); i++ {
				tuple := result.Get(i)
				name := tuple[1].(string)
				taskCount := tuple[2].(int64)
				resultMap[name] = taskCount
			}

			if count, ok := resultMap["Scenario One"]; !ok {
				t.Error("Missing Scenario One")
			} else if count != 5 {
				t.Errorf("Scenario One: expected taskCount=5, got %d", count)
			}

			if count, ok := resultMap["Scenario Two"]; !ok {
				t.Error("Missing Scenario Two - scalar ground fallback did not trigger")
			} else if count != 0 {
				t.Errorf("Scenario Two: expected taskCount=0, got %d", count)
			}
		})
	}
}

func TestOrFallbackWithSubqueryPatternEmpty(t *testing.T) {
	// Test OR with SubqueryPattern that returns empty, falling back to ground
	matcher := NewMemoryPatternMatcher(nil) // Empty database

	statusAttr := datalog.NewKeyword(":task/status")
	completeStatus := datalog.NewKeyword(":status/complete")

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			queryExecutor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Same query but with empty database - should fall back to ground(0)
			orClause := &query.OrClause{
				Branches: [][]query.Clause{
					{
						&query.SubqueryPattern{
							Query: &query.Query{
								Find: []query.FindElement{
									query.FindAggregate{Function: datalog.SymCount, Arg: datalog.NewSymbol("?t")},
								},
								Where: []query.Clause{
									&query.DataPattern{
										Elements: []query.PatternElement{
											query.Variable{Name: datalog.NewSymbol("?t")},
											query.Constant{Value: statusAttr},
											query.Constant{Value: completeStatus},
										},
									},
								},
							},
							Inputs:  []query.PatternElement{query.Constant{Value: datalog.NewSymbol("$")}},
							Binding: query.TupleBinding{Variables: []query.Symbol{datalog.NewSymbol("?count")}},
						},
					},
					{
						&query.Expression{
							Function: &query.GroundFunction{Value: int64(0)},
							Binding:  datalog.NewSymbol("?count"),
						},
					},
				},
			}

			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?count")},
				},
				Where: []query.Clause{orClause},
			}

			result, err := queryExecutor.Execute(q)
			if err != nil {
				t.Fatalf("query executor failed: %v", err)
			}

			t.Logf("Final: symbols=%v, size=%d", result.Symbols(), result.Size())

			// Should have 1 result with count=0 (fallback)
			if result.Size() != 1 {
				t.Errorf("Expected 1 result, got %d", result.Size())
			}

			if result.Size() > 0 {
				val := result.Get(0)[0]
				if val != int64(0) {
					t.Errorf("Expected count=0 (fallback), got %v (type %T)", val, val)
				}
			}
		})
	}
}

// TestOrCorrelatedUnionWithIdentityBranch reproduces the bug where OR clauses
// drop expression-only branches due to fallback short-circuiting.
// See docs/bugs/BUG-OR-FALLBACK-DROPS-EXPRESSION-BRANCHES.md
//
// The OR has a data pattern branch (finds rooms in an area) and an identity
// expression branch (binds the area entity itself). Both branches should
// contribute results (union semantics). The bug: fallback short-circuits
// after the data pattern branch succeeds, so the identity branch is never
// evaluated.
func TestOrCorrelatedUnionWithIdentityBranch(t *testing.T) {
	area1 := datalog.NewIdentity("area:caves")
	room1 := datalog.NewIdentity("room:guard")
	room2 := datalog.NewIdentity("room:shrine")
	room3 := datalog.NewIdentity("room:depths")

	nameAttr := datalog.NewKeyword(":entity/name")
	areaAttr := datalog.NewKeyword(":entity/area")

	datoms := []datalog.Datom{
		{E: area1, A: nameAttr, V: "Coastal Caves", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: room1, A: nameAttr, V: "Guard Chamber", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: room2, A: nameAttr, V: "Shrine Hall", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: room3, A: nameAttr, V: "The Depths", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: room1, A: areaAttr, V: area1, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: room2, A: areaAttr, V: area1, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: room3, A: areaAttr, V: area1, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Query:
			// [:find ?rname
			//  :where [?self :entity/name "Guard Chamber"]
			//         [?self :entity/area ?target]
			//         (or [?related :entity/area ?target]
			//             [(identity ?target) ?related])
			//         [?related :entity/name ?rname]]
			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?rname")},
				},
				Where: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: datalog.NewSymbol("?self")},
							query.Constant{Value: nameAttr},
							query.Constant{Value: "Guard Chamber"},
						},
					},
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: datalog.NewSymbol("?self")},
							query.Constant{Value: areaAttr},
							query.Variable{Name: datalog.NewSymbol("?target")},
						},
					},
					&query.OrClause{
						Branches: [][]query.Clause{
							{
								&query.DataPattern{
									Elements: []query.PatternElement{
										query.Variable{Name: datalog.NewSymbol("?related")},
										query.Constant{Value: areaAttr},
										query.Variable{Name: datalog.NewSymbol("?target")},
									},
								},
							},
							{
								&query.Expression{
									Function: query.IdentityFunction{
										Arg: query.VariableTerm{Symbol: datalog.NewSymbol("?target")},
									},
									Binding: datalog.NewSymbol("?related"),
								},
							},
						},
					},
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: datalog.NewSymbol("?related")},
							query.Constant{Value: nameAttr},
							query.Variable{Name: datalog.NewSymbol("?rname")},
						},
					},
				},
			}

			result, err := executor.Execute(q)
			if err != nil {
				t.Fatalf("execution failed: %v", err)
			}

			names := make(map[string]bool)
			for i := 0; i < result.Size(); i++ {
				name := result.Get(i)[0].(string)
				names[name] = true
				t.Logf("result[%d]: %s", i, name)
			}

			// All 3 rooms in the area (including Guard Chamber itself) + the area entity
			if result.Size() != 4 {
				t.Errorf("expected 4 results, got %d", result.Size())
			}
			if !names["Guard Chamber"] {
				t.Error("missing 'Guard Chamber' (room in same area)")
			}
			if !names["Shrine Hall"] {
				t.Error("missing 'Shrine Hall' (room in same area)")
			}
			if !names["The Depths"] {
				t.Error("missing 'The Depths' (room in same area)")
			}
			if !names["Coastal Caves"] {
				t.Error("missing 'Coastal Caves' (area entity via identity branch)")
			}
		})
	}
}

// TestOrUnionIncludesAllBranches verifies that (or ...) with expression branches
// uses union semantics (all branches contribute), not fallback (first-match-wins).
func TestOrUnionIncludesAllBranches(t *testing.T) {
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Query:
			// [:find ?x
			//  :where (or [?e :user/name ?x]
			//             [(ground "nobody") ?x])]
			q := &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: datalog.NewSymbol("?x")},
				},
				Where: []query.Clause{
					&query.OrClause{
						Branches: [][]query.Clause{
							{
								&query.DataPattern{
									Elements: []query.PatternElement{
										query.Variable{Name: datalog.NewSymbol("?e")},
										query.Constant{Value: nameAttr},
										query.Variable{Name: datalog.NewSymbol("?x")},
									},
								},
							},
							{
								&query.Expression{
									Function: &query.GroundFunction{Value: "nobody"},
									Binding:  datalog.NewSymbol("?x"),
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

			values := make(map[string]bool)
			for i := 0; i < result.Size(); i++ {
				val := result.Get(i)[0].(string)
				values[val] = true
				t.Logf("result[%d]: %s", i, val)
			}

			// Union: both branches should contribute
			if result.Size() != 2 {
				t.Errorf("expected 2 results (union of both branches), got %d", result.Size())
			}
			if !values["Alice"] {
				t.Error("missing 'Alice' from data pattern branch")
			}
			if !values["nobody"] {
				t.Error("missing 'nobody' from ground expression branch")
			}
		})
	}
}

// TestClauseOrderIndependenceForNot pins the language contract that clause
// order must not change whether a query works, and the optimizer-transparency
// invariant that the optimizer must never change it either. Two shapes from
// docs/bugs/resolved/BUG_ALGEBRA_BRIDGE_COMPILES_IN_SOURCE_ORDER.md: a NOT
// written before the clause that binds its correlate, and a NOT written after
// a prefix that does not bind its correlate. The baseline path plans both by
// canonical clause scope; the algebra bridge orders clauses by the same
// readiness before folding (orderClausesForCompile over query.ClauseReady).
func TestClauseOrderIndependenceForNot(t *testing.T) {
	goalA := datalog.NewIdentity("goal:a")
	goalB := datalog.NewIdentity("goal:b")
	event1 := datalog.NewIdentity("event:1")
	typeAttr := datalog.NewKeyword(":entity/type")
	nameAttr := datalog.NewKeyword(":entity/name")
	eventGoalAttr := datalog.NewKeyword(":event/goal")
	goalType := datalog.NewKeyword(":type/goal")

	datoms := []datalog.Datom{
		{E: goalA, A: typeAttr, V: goalType, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: goalB, A: typeAttr, V: goalType, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: goalA, A: nameAttr, V: "alpha", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: goalB, A: nameAttr, V: "beta", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		// Only goal:a has an event; the NOT must leave exactly goal:b.
		{E: event1, A: eventGoalAttr, V: goalA, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	cases := []struct {
		name string
		text string
	}{
		{
			name: "not_before_binder",
			text: `[:find ?goal
			        :where (not [?x :event/goal ?goal])
			               [?goal :entity/type :type/goal]]`,
		},
		{
			// The prefix relation {?e ?name} shares no variable with the NOT;
			// ?goal unifies through the clauses written after it.
			name: "not_after_non_unifying_prefix",
			text: `[:find ?goal
			        :where [?e :entity/name ?name]
			               (not [?x :event/goal ?goal])
			               [?goal :entity/type :type/goal]
			               [?goal :entity/name ?name]]`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tc.text)
			if err != nil {
				t.Fatalf("failed to parse query: %v", err)
			}

			for _, mode := range optimizerModes {
				t.Run(mode.name, func(t *testing.T) {
					executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

					result, err := executor.Execute(q)
					if err != nil {
						t.Fatalf("execution failed: %v", err)
					}

					if result.Size() != 1 {
						t.Fatalf("expected exactly 1 result (goal:b), got %d", result.Size())
					}
					got, ok := result.Get(0)[0].(datalog.Identity)
					if !ok {
						t.Fatalf("expected an Identity result, got %T", result.Get(0)[0])
					}
					if !got.Equal(goalB) {
						t.Errorf("expected goal:b (no event), got %v", got)
					}
				})
			}
		})
	}
}
