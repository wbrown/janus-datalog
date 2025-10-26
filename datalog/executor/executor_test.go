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
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: alice, A: ageAttr, V: int64(30), Tx: 1},
		{E: alice, A: friendAttr, V: bob, Tx: 2},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		{E: bob, A: ageAttr, V: int64(25), Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Query: Find all names
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
		},
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Should have 2 names
	if result.Size() != 2 {
		t.Errorf("expected 2 results, got %d", result.Size())
	}

	// Check we got the right names
	names := make(map[string]bool)
	for i := 0; i < result.Size(); i++ {
		name := result.Get(i)[0].(string)
		names[name] = true
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
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: alice, A: ageAttr, V: int64(30), Tx: 1},
		{E: alice, A: friendAttr, V: bob, Tx: 2},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		{E: bob, A: ageAttr, V: int64(25), Tx: 1},
		{E: bob, A: friendAttr, V: charlie, Tx: 2},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Query: Find friends of friends
	// [?p1 :user/friend ?p2]
	// [?p2 :user/friend ?p3]
	// [?p1 :user/name ?name1]
	// [?p3 :user/name ?name3]
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?name1"},
			query.FindVariable{Symbol: "?name3"},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?p1"},
					query.Constant{Value: friendAttr},
					query.Variable{Name: "?p2"},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?p2"},
					query.Constant{Value: friendAttr},
					query.Variable{Name: "?p3"},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?p1"},
					query.Constant{Value: nameAttr},
					query.Variable{Name: "?name1"},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?p3"},
					query.Constant{Value: nameAttr},
					query.Variable{Name: "?name3"},
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
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: alice, A: ageAttr, V: int64(30), Tx: 1},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		{E: bob, A: ageAttr, V: int64(25), Tx: 1},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: 1},
		{E: charlie, A: ageAttr, V: int64(35), Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Query: Find people younger than 30
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
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: ageAttr},
					query.Variable{Name: "?age"},
				},
			},
			&query.Comparison{
				Op:    query.OpLT,
				Left:  query.VariableTerm{Symbol: "?age"},
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
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: alice, A: ageAttr, V: int64(30), Tx: 1},
		{E: alice, A: salaryAttr, V: int64(50000), Tx: 1},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		{E: bob, A: ageAttr, V: int64(25), Tx: 1},
		{E: bob, A: salaryAttr, V: int64(45000), Tx: 1},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: 1},
		{E: charlie, A: ageAttr, V: int64(35), Tx: 1},
		{E: charlie, A: salaryAttr, V: int64(60000), Tx: 1},
		{E: david, A: nameAttr, V: "David", Tx: 1},
		{E: david, A: ageAttr, V: int64(28), Tx: 1},
		{E: david, A: salaryAttr, V: int64(55000), Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Query: Find people aged 25-30 with salary > 50000
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?name"},
			query.FindVariable{Symbol: "?age"},
			query.FindVariable{Symbol: "?salary"},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: nameAttr},
					query.Variable{Name: "?name"},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: ageAttr},
					query.Variable{Name: "?age"},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: salaryAttr},
					query.Variable{Name: "?salary"},
				},
			},
			&query.Comparison{
				Op:    query.OpGTE,
				Left:  query.VariableTerm{Symbol: "?age"},
				Right: query.ConstantTerm{Value: int64(25)},
			},
			&query.Comparison{
				Op:    query.OpLTE,
				Left:  query.VariableTerm{Symbol: "?age"},
				Right: query.ConstantTerm{Value: int64(30)},
			},
			&query.Comparison{
				Op:    query.OpGT,
				Left:  query.VariableTerm{Symbol: "?salary"},
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
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Query for non-existent attribute
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?email"},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: datalog.NewKeyword(":user/email")},
					query.Variable{Name: "?email"},
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
		[]query.Symbol{"?name", "?age"},
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
	if idx := result.ColumnIndex("?name"); idx != 0 {
		t.Errorf("expected index 0 for ?name, got %d", idx)
	}
	if idx := result.ColumnIndex("?age"); idx != 1 {
		t.Errorf("expected index 1 for ?age, got %d", idx)
	}
	if idx := result.ColumnIndex("?missing"); idx != -1 {
		t.Errorf("expected index -1 for missing column, got %d", idx)
	}

	// Test GetValue
	if val, ok := result.GetValue(0, "?name"); !ok || val != "Alice" {
		t.Errorf("expected Alice, got %v", val)
	}
	if val, ok := result.GetValue(1, "?age"); !ok || val != int64(25) {
		t.Errorf("expected 25, got %v", val)
	}
	if _, ok := result.GetValue(0, "?missing"); ok {
		t.Error("expected false for missing column")
	}
	if _, ok := result.GetValue(2, "?name"); ok {
		t.Error("expected false for out of bounds row")
	}
}
