package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestExecutorBasicQuery(t *testing.T) {
	// Create test data
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	friendAttr := datalog.NewKeyword(":user/friend")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: ageAttr, V: int64(30), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: friendAttr, V: bob, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
		{E: bob, A: nameAttr, V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: ageAttr, V: int64(25), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher, nil)

	// Query: Find all names
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
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Collect results by iterating (works with streaming)
	names := make(map[string]bool)
	it := result.Iterator()
	count := 0
	for it.Next() {
		name := it.Tuple()[0].(string)
		names[name] = true
		count++
	}
	it.Close()

	// Should have 2 names
	if count != 2 {
		t.Errorf("expected 2 results, got %d", count)
	}

	if !names["Alice"] || !names["Bob"] {
		t.Errorf("expected Alice and Bob, got %v", names)
	}
}

func TestExecutorJoinQuery(t *testing.T) {
	// Create test data
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	charlie := datalog.NewIdentity("user:charlie")
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	friendAttr := datalog.NewKeyword(":user/friend")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: ageAttr, V: int64(30), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: friendAttr, V: bob, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
		{E: bob, A: nameAttr, V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: ageAttr, V: int64(25), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: friendAttr, V: charlie, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher, nil)

	// Query: Find friends of friends
	// [?p1 :user/friend ?p2]
	// [?p2 :user/friend ?p3]
	// [?p1 :user/name ?name1]
	// [?p3 :user/name ?name3]
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: datalog.NewSymbol("?name1")},
			query.FindVariable{Symbol: datalog.NewSymbol("?name3")},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?p1")},
					query.Constant{Value: friendAttr},
					query.Variable{Name: datalog.NewSymbol("?p2")},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?p2")},
					query.Constant{Value: friendAttr},
					query.Variable{Name: datalog.NewSymbol("?p3")},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?p1")},
					query.Constant{Value: nameAttr},
					query.Variable{Name: datalog.NewSymbol("?name1")},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?p3")},
					query.Constant{Value: nameAttr},
					query.Variable{Name: datalog.NewSymbol("?name3")},
				},
			},
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Should find Alice -> Charlie (through Bob)
	if result.Size() != 1 {
		t.Errorf("expected 1 result, got %d", result.Size())
	}

	if result.Size() > 0 {
		tuple := result.Get(0)
		if tuple[0] != "Alice" || tuple[1] != "Charlie" {
			t.Errorf("expected [Alice Charlie], got %v", tuple)
		}
	}
}

func TestExecutorWithFilter(t *testing.T) {
	// Create test data
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	charlie := datalog.NewIdentity("user:charlie")
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: ageAttr, V: int64(30), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: nameAttr, V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: ageAttr, V: int64(25), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: charlie, A: ageAttr, V: int64(35), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher, nil)

	// Query: Find people younger than 30
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
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: ageAttr},
					query.Variable{Name: datalog.NewSymbol("?age")},
				},
			},
			&query.Comparison{
				Op:    query.OpLT,
				Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?age")},
				Right: query.ConstantTerm{Value: int64(30)},
			},
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Should only find Bob (age 25)
	if result.Size() != 1 {
		t.Errorf("expected 1 result, got %d", result.Size())
	}

	if result.Size() > 0 {
		name := result.Get(0)[0].(string)
		if name != "Bob" {
			t.Errorf("expected Bob, got %s", name)
		}
	}
}

func TestExecutorMultipleFilters(t *testing.T) {
	// Create test data
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	charlie := datalog.NewIdentity("user:charlie")
	david := datalog.NewIdentity("user:david")
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	salaryAttr := datalog.NewKeyword(":user/salary")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: ageAttr, V: int64(30), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: salaryAttr, V: int64(50000), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: nameAttr, V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: ageAttr, V: int64(25), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: salaryAttr, V: int64(45000), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: charlie, A: ageAttr, V: int64(35), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: charlie, A: salaryAttr, V: int64(60000), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: david, A: nameAttr, V: "David", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: david, A: ageAttr, V: int64(28), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: david, A: salaryAttr, V: int64(55000), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher, nil)

	// Query: Find people aged 25-30 with salary > 50000
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: datalog.NewSymbol("?name")},
			query.FindVariable{Symbol: datalog.NewSymbol("?age")},
			query.FindVariable{Symbol: datalog.NewSymbol("?salary")},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: nameAttr},
					query.Variable{Name: datalog.NewSymbol("?name")},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: ageAttr},
					query.Variable{Name: datalog.NewSymbol("?age")},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: salaryAttr},
					query.Variable{Name: datalog.NewSymbol("?salary")},
				},
			},
			&query.Comparison{
				Op:    query.OpGTE,
				Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?age")},
				Right: query.ConstantTerm{Value: int64(25)},
			},
			&query.Comparison{
				Op:    query.OpLTE,
				Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?age")},
				Right: query.ConstantTerm{Value: int64(30)},
			},
			&query.Comparison{
				Op:    query.OpGT,
				Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?salary")},
				Right: query.ConstantTerm{Value: int64(50000)},
			},
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Should only find David (age 28, salary 55000)
	if result.Size() != 1 {
		t.Errorf("expected 1 result, got %d", result.Size())
		for i := 0; i < result.Size(); i++ {
			t.Logf("  %v", result.Get(i))
		}
	}

	if result.Size() > 0 {
		tuple := result.Get(0)
		name := tuple[0].(string)
		age := tuple[1].(int64)
		salary := tuple[2].(int64)

		if name != "David" || age != 28 || salary != 55000 {
			t.Errorf("expected [David 28 55000], got %v", tuple)
		}
	}
}

func TestExecutorEmptyResult(t *testing.T) {
	// Create minimal test data
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher, nil)

	// Query for non-existent attribute
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: datalog.NewSymbol("?email")},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":user/email")},
					query.Variable{Name: datalog.NewSymbol("?email")},
				},
			},
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Should return empty result
	if result.Size() != 0 {
		t.Errorf("expected empty result, got %d tuples", result.Size())
	}
}

func TestResultMethods(t *testing.T) {
	result := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?age")},
		[]Tuple{
			{"Alice", int64(30)},
			{"Bob", int64(25)},
		},
	)

	// Test Size
	if result.Size() != 2 {
		t.Errorf("expected size 2, got %d", result.Size())
	}

	// Test Get
	tuple := result.Get(0)
	if tuple[0] != "Alice" {
		t.Errorf("expected Alice, got %v", tuple[0])
	}

	// Test Get out of bounds
	if result.Get(-1) != nil {
		t.Error("expected nil for negative index")
	}
	if result.Get(2) != nil {
		t.Error("expected nil for out of bounds index")
	}

	// Test ColumnIndex
	if idx := result.SymbolIndex(datalog.NewSymbol("?name")); idx != 0 {
		t.Errorf("expected index 0 for ?name, got %d", idx)
	}
	if idx := result.SymbolIndex(datalog.NewSymbol("?age")); idx != 1 {
		t.Errorf("expected index 1 for ?age, got %d", idx)
	}
	if idx := result.SymbolIndex(datalog.NewSymbol("?missing")); idx != -1 {
		t.Errorf("expected index -1 for missing symbol, got %d", idx)
	}

	// Test GetValue
	if val, ok := result.GetValue(0, datalog.NewSymbol("?name")); !ok || val != "Alice" {
		t.Errorf("expected Alice, got %v", val)
	}
	if val, ok := result.GetValue(1, datalog.NewSymbol("?age")); !ok || val != int64(25) {
		t.Errorf("expected 25, got %v", val)
	}
	if _, ok := result.GetValue(0, datalog.NewSymbol("?missing")); ok {
		t.Error("expected false for missing symbol")
	}
	if _, ok := result.GetValue(2, datalog.NewSymbol("?name")); ok {
		t.Error("expected false for out of bounds tuple")
	}
}
