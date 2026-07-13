package executor

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestQueryWithOrderBy(t *testing.T) {
	// Create test data with various types for sorting
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	charlie := datalog.NewIdentity("user:charlie")
	dave := datalog.NewIdentity("user:dave")

	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	scoreAttr := datalog.NewKeyword(":user/score")
	joinedAttr := datalog.NewKeyword(":user/joined")

	// Create dates for testing time sorting
	date1 := time.Date(2020, 1, 15, 0, 0, 0, 0, time.UTC)
	date2 := time.Date(2021, 6, 10, 0, 0, 0, 0, time.UTC)
	date3 := time.Date(2022, 3, 20, 0, 0, 0, 0, time.UTC)
	date4 := time.Date(2023, 9, 5, 0, 0, 0, 0, time.UTC)

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: ageAttr, V: int64(30), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: scoreAttr, V: 85.5, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: joinedAttr, V: date2, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},

		{E: bob, A: nameAttr, V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: ageAttr, V: int64(25), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: scoreAttr, V: 92.0, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: joinedAttr, V: date1, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},

		{E: charlie, A: nameAttr, V: "Charlie", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: charlie, A: ageAttr, V: int64(35), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: charlie, A: scoreAttr, V: 78.5, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: charlie, A: joinedAttr, V: date3, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},

		{E: dave, A: nameAttr, V: "Dave", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: dave, A: ageAttr, V: int64(28), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: dave, A: scoreAttr, V: 88.0, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: dave, A: joinedAttr, V: date4, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher, nil)

	tests := []struct {
		name     string
		query    string
		expected []string // Expected first symbol values in order
	}{
		{
			name: "Sort by name ascending",
			query: `[:find ?name ?age
			         :where [?e :user/name ?name]
			                [?e :user/age ?age]
			         :order-by [[?name :asc]]]`,
			expected: []string{"Alice", "Bob", "Charlie", "Dave"},
		},
		{
			name: "Sort by name descending",
			query: `[:find ?name ?age
			         :where [?e :user/name ?name]
			                [?e :user/age ?age]
			         :order-by [[?name :desc]]]`,
			expected: []string{"Dave", "Charlie", "Bob", "Alice"},
		},
		{
			name: "Sort by age ascending",
			query: `[:find ?name ?age
			         :where [?e :user/name ?name]
			                [?e :user/age ?age]
			         :order-by [[?age :asc]]]`,
			expected: []string{"Bob", "Dave", "Alice", "Charlie"},
		},
		{
			name: "Sort by score descending",
			query: `[:find ?name ?score
			         :where [?e :user/name ?name]
			                [?e :user/score ?score]
			         :order-by [[?score :desc]]]`,
			expected: []string{"Bob", "Dave", "Alice", "Charlie"},
		},
		{
			name: "Sort by date ascending",
			query: `[:find ?name ?joined
			         :where [?e :user/name ?name]
			                [?e :user/joined ?joined]
			         :order-by [[?joined :asc]]]`,
			expected: []string{"Bob", "Alice", "Charlie", "Dave"},
		},
		{
			name: "Multi-symbol sort: age then name",
			query: `[:find ?name ?age ?score
			         :where [?e :user/name ?name]
			                [?e :user/age ?age]
			                [?e :user/score ?score]
			         :order-by [[?age :asc] [?name :asc]]]`,
			expected: []string{"Bob", "Dave", "Alice", "Charlie"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse query
			q, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("failed to parse query: %v", err)
			}

			// Execute query
			result, err := executor.Execute(q)
			if err != nil {
				t.Fatalf("execution failed: %v", err)
			}

			// Check result count
			if result.Size() != 4 {
				t.Errorf("expected 4 results, got %d", result.Size())
			}

			// Check order
			for i := 0; i < result.Size(); i++ {
				tuple := result.Get(i)
				name := tuple[0].(string)
				if name != tt.expected[i] {
					t.Errorf("tuple %d: expected %s, got %s", i, tt.expected[i], name)
				}
			}
		})
	}
}

func TestSortingEdgeCases(t *testing.T) {
	// An order-by variable bound nowhere is a parse error, asserted by
	// TestOrderByUnboundVariableIsError and the parser's
	// TestOrderByValidation.
	tests := []struct {
		name       string
		query      string
		datoms     []datalog.Datom
		expectSize int
	}{
		{
			name: "Sort empty result set",
			query: `[:find ?name ?age
			         :where [?e :user/name ?name]
			                [?e :user/age ?age]
			                [(> ?age 100)]
			         :order-by [[?name :asc]]]`,
			datoms: []datalog.Datom{
				{E: datalog.NewIdentity("user:1"), A: datalog.NewKeyword(":user/name"), V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
				{E: datalog.NewIdentity("user:1"), A: datalog.NewKeyword(":user/age"), V: int64(30), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			},
			expectSize: 0,
		},
		{
			name: "Sort empty result set by non-projected key",
			query: `[:find ?name
			         :where [?e :user/name ?name]
			                [?e :user/age ?age]
			                [(> ?age 100)]
			         :order-by [[?age :asc]]]`,
			datoms: []datalog.Datom{
				{E: datalog.NewIdentity("user:1"), A: datalog.NewKeyword(":user/name"), V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
				{E: datalog.NewIdentity("user:1"), A: datalog.NewKeyword(":user/age"), V: int64(30), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			},
			expectSize: 0,
		},
		{
			name: "Sort single tuple",
			query: `[:find ?name ?age
			         :where [?e :user/name ?name]
			                [?e :user/age ?age]
			         :order-by [[?name :asc]]]`,
			datoms: []datalog.Datom{
				{E: datalog.NewIdentity("user:1"), A: datalog.NewKeyword(":user/name"), V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
				{E: datalog.NewIdentity("user:1"), A: datalog.NewKeyword(":user/age"), V: int64(30), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			},
			expectSize: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher := NewMemoryPatternMatcher(tt.datoms)
			executor := NewExecutor(matcher, nil)

			q, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("failed to parse query: %v", err)
			}

			result, err := executor.Execute(q)
			if err != nil {
				t.Fatalf("execution failed: %v", err)
			}

			if result.Size() != tt.expectSize {
				t.Errorf("expected %d results, got %d", tt.expectSize, result.Size())
			}
		})
	}
}

// Reproduction tests for
// docs/bugs/resolved/BUG_ORDER_BY_NON_PROJECTED_VARIABLE_SILENTLY_IGNORED.md.
// They pin the contract: ordering by any variable bound in :where, whether
// or not it is projected in :find.

// nonProjectedSortDatoms builds six users whose insertion order (alphabetical
// by name) matches neither ascending nor descending age order, so an engine
// that ignores the :order-by cannot pass the ordering assertions by accident.
func nonProjectedSortDatoms() []datalog.Datom {
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	users := []struct {
		id   string
		name string
		age  int64
	}{
		{"user:alice", "Alice", 34},
		{"user:bob", "Bob", 12},
		{"user:carol", "Carol", 51},
		{"user:dave", "Dave", 27},
		{"user:erin", "Erin", 45},
		{"user:frank", "Frank", 8},
	}

	var datoms []datalog.Datom
	for _, u := range users {
		e := datalog.NewIdentity(u.id)
		datoms = append(datoms,
			datalog.Datom{E: e, A: nameAttr, V: u.name, Tx: tx},
			datalog.Datom{E: e, A: ageAttr, V: u.age, Tx: tx},
		)
	}
	return datoms
}

func TestOrderByNonProjectedVariable(t *testing.T) {
	matcher := NewMemoryPatternMatcher(nonProjectedSortDatoms())
	executor := NewExecutor(matcher, nil)

	tests := []struct {
		name     string
		query    string
		expected []string
	}{
		{
			name: "ascending by non-projected age",
			query: `[:find ?name
			         :where [?e :user/name ?name]
			                [?e :user/age ?age]
			         :order-by [[?age :asc]]]`,
			expected: []string{"Frank", "Bob", "Dave", "Alice", "Erin", "Carol"},
		},
		{
			name: "descending by non-projected age",
			query: `[:find ?name
			         :where [?e :user/name ?name]
			                [?e :user/age ?age]
			         :order-by [[?age :desc]]]`,
			expected: []string{"Carol", "Erin", "Alice", "Dave", "Bob", "Frank"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("failed to parse query: %v", err)
			}

			result, err := executor.Execute(q)
			if err != nil {
				t.Fatalf("execution failed: %v", err)
			}

			// The sort variable must not leak into the result shape: the
			// relation contains exactly the :find symbols.
			symbols := result.Symbols()
			if len(symbols) != 1 || symbols[0].String() != "?name" {
				t.Fatalf("expected result symbols [?name], got %v", symbols)
			}

			if result.Size() != len(tt.expected) {
				t.Fatalf("expected %d results, got %d", len(tt.expected), result.Size())
			}

			for i := 0; i < result.Size(); i++ {
				name := result.Get(i)[0].(string)
				if name != tt.expected[i] {
					t.Errorf("tuple %d: expected %s, got %s", i, tt.expected[i], name)
				}
			}
		})
	}
}

func TestOrderByMixedProjectedAndNonProjectedKeys(t *testing.T) {
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	deptAttr := datalog.NewKeyword(":user/dept")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	users := []struct {
		id   string
		name string
		dept string
		age  int64
	}{
		{"user:alice", "Alice", "eng", 34},
		{"user:bob", "Bob", "eng", 12},
		{"user:carol", "Carol", "eng", 51},
		{"user:dave", "Dave", "ops", 27},
		{"user:erin", "Erin", "ops", 45},
		{"user:frank", "Frank", "ops", 8},
	}

	var datoms []datalog.Datom
	for _, u := range users {
		e := datalog.NewIdentity(u.id)
		datoms = append(datoms,
			datalog.Datom{E: e, A: nameAttr, V: u.name, Tx: tx},
			datalog.Datom{E: e, A: deptAttr, V: u.dept, Tx: tx},
			datalog.Datom{E: e, A: ageAttr, V: u.age, Tx: tx},
		)
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher, nil)

	// ?dept is projected, ?age is not. Both keys must be honored in the
	// stated precedence: dept ascending, then age descending within dept.
	q, err := parser.ParseQuery(`[:find ?dept ?name
	                              :where [?e :user/name ?name]
	                                     [?e :user/dept ?dept]
	                                     [?e :user/age ?age]
	                              :order-by [[?dept :asc] [?age :desc]]]`)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	expected := []string{"Carol", "Alice", "Bob", "Erin", "Dave", "Frank"}
	if result.Size() != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), result.Size())
	}

	for i := 0; i < result.Size(); i++ {
		name := result.Get(i)[1].(string)
		if name != expected[i] {
			t.Errorf("tuple %d: expected %s, got %s", i, expected[i], name)
		}
	}
}

func TestOrderByNonProjectedVariableWithLimit(t *testing.T) {
	matcher := NewMemoryPatternMatcher(nonProjectedSortDatoms())
	executor := NewExecutor(matcher, nil)

	// Global top-N: the two youngest users, selected by a sort key that is
	// not in :find. Sorting must happen before the limit is applied.
	q, err := parser.ParseQuery(`[:find ?name
	                              :where [?e :user/name ?name]
	                                     [?e :user/age ?age]
	                              :order-by [[?age :asc]]
	                              :limit 2]`)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	expected := []string{"Frank", "Bob"}
	if result.Size() != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), result.Size())
	}

	for i := 0; i < result.Size(); i++ {
		name := result.Get(i)[0].(string)
		if name != expected[i] {
			t.Errorf("tuple %d: expected %s, got %s", i, expected[i], name)
		}
	}
}

func TestOrderByUnboundVariableIsError(t *testing.T) {
	matcher := NewMemoryPatternMatcher(nonProjectedSortDatoms())
	executor := NewExecutor(matcher, nil)

	// ?bogus is bound nowhere in the query — not in :find, :where, or :in.
	// This must be rejected (at parse or execution time), not silently
	// ignored.
	q, err := parser.ParseQuery(`[:find ?name
	                              :where [?e :user/name ?name]
	                              :order-by [[?bogus :asc]]]`)
	if err != nil {
		return // parse-time rejection satisfies the contract
	}

	_, err = executor.Execute(q)
	if err == nil {
		t.Fatal("expected error for order-by variable bound nowhere in the query, got success")
	}
}

// statusUserDatoms builds users with a status attribute for the scalar-input
// sort-key cases: three "active" users whose ages are shuffled relative to
// name order, plus one "inactive" user the input filters out.
func statusUserDatoms() []datalog.Datom {
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	statusAttr := datalog.NewKeyword(":user/status")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	users := []struct {
		id     string
		name   string
		status string
		age    int64
	}{
		{"user:alice", "Alice", "active", 34},
		{"user:bob", "Bob", "active", 12},
		{"user:carol", "Carol", "active", 51},
		{"user:dave", "Dave", "inactive", 27},
	}

	var datoms []datalog.Datom
	for _, u := range users {
		e := datalog.NewIdentity(u.id)
		datoms = append(datoms,
			datalog.Datom{E: e, A: nameAttr, V: u.name, Tx: tx},
			datalog.Datom{E: e, A: statusAttr, V: u.status, Tx: tx},
			datalog.Datom{E: e, A: ageAttr, V: u.age, Tx: tx},
		)
	}
	return datoms
}

// Case F: a sort key that is a scalar :in constant is a well-defined no-op —
// every row carries the same value, so the query succeeds and returns all
// matching rows (in no particular order).
func TestOrderByScalarInputConstantKeyIsNoOp(t *testing.T) {
	matcher := NewMemoryPatternMatcher(statusUserDatoms())
	executor := NewExecutor(matcher, nil)

	q, err := parser.ParseQuery(`[:find ?name
	                              :in $ ?status
	                              :where [?e :user/status ?status]
	                                     [?e :user/name ?name]
	                              :order-by [[?status :asc]]]`)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	statusRel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?status")}, []Tuple{{"active"}})

	result, err := executor.ExecuteWithRelations(NewContext(nil), q, []Relation{statusRel})
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	if result.Size() != 3 {
		t.Fatalf("expected 3 results, got %d", result.Size())
	}
	seen := map[string]bool{}
	for i := 0; i < result.Size(); i++ {
		seen[result.Get(i)[0].(string)] = true
	}
	for _, name := range []string{"Alice", "Bob", "Carol"} {
		if !seen[name] {
			t.Errorf("expected %s in results, got %v", name, seen)
		}
	}
}

// Case G: a constant scalar-input key composed with a real :where-bound key —
// the constant contributes nothing (identity), the real key must be honored.
func TestOrderByScalarConstantKeyThenRealKey(t *testing.T) {
	matcher := NewMemoryPatternMatcher(statusUserDatoms())
	executor := NewExecutor(matcher, nil)

	q, err := parser.ParseQuery(`[:find ?name
	                              :in $ ?status
	                              :where [?e :user/status ?status]
	                                     [?e :user/name ?name]
	                                     [?e :user/age ?age]
	                              :order-by [[?status :asc] [?age :desc]]]`)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	statusRel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?status")}, []Tuple{{"active"}})

	result, err := executor.ExecuteWithRelations(NewContext(nil), q, []Relation{statusRel})
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	expected := []string{"Carol", "Alice", "Bob"} // ages 51, 34, 12
	if result.Size() != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), result.Size())
	}
	for i := 0; i < result.Size(); i++ {
		name := result.Get(i)[0].(string)
		if name != expected[i] {
			t.Errorf("tuple %d: expected %s, got %s", i, expected[i], name)
		}
	}
}

// Case H: a collection-input variable bound per-row by a :where pattern is a
// genuine relation symbol and sorts normally, projected or not. Names are
// chosen so global name order differs from dept-blocked name order.
func TestOrderByCollectionInputBoundVariable(t *testing.T) {
	nameAttr := datalog.NewKeyword(":user/name")
	deptAttr := datalog.NewKeyword(":user/dept")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	users := []struct {
		id   string
		name string
		dept string
	}{
		{"user:zoe", "Zoe", "eng"},
		{"user:bob", "Bob", "eng"},
		{"user:alice", "Alice", "ops"},
		{"user:dave", "Dave", "ops"},
		{"user:erin", "Erin", "sales"}, // not in the input collection
	}

	var datoms []datalog.Datom
	for _, u := range users {
		e := datalog.NewIdentity(u.id)
		datoms = append(datoms,
			datalog.Datom{E: e, A: nameAttr, V: u.name, Tx: tx},
			datalog.Datom{E: e, A: deptAttr, V: u.dept, Tx: tx},
		)
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher, nil)

	q, err := parser.ParseQuery(`[:find ?name
	                              :in $ [?dept ...]
	                              :where [?e :user/dept ?dept]
	                                     [?e :user/name ?name]
	                              :order-by [[?dept :asc] [?name :asc]]]`)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	deptRel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?dept")}, []Tuple{{"ops"}, {"eng"}})

	result, err := executor.ExecuteWithRelations(NewContext(nil), q, []Relation{deptRel})
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	expected := []string{"Bob", "Zoe", "Alice", "Dave"} // eng block, then ops block
	if result.Size() != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), result.Size())
	}
	for i := 0; i < result.Size(); i++ {
		name := result.Get(i)[0].(string)
		if name != expected[i] {
			t.Errorf("tuple %d: expected %s, got %s", i, expected[i], name)
		}
	}
}

// Case I: a relation-input symbol bound per-row by a :where pattern sorts the
// combined output across all input tuples (the union), not per iteration.
// Inputs are given in reverse key order so iteration order can't pass by
// accident.
func TestOrderByRelationInputSymbolAcrossUnion(t *testing.T) {
	keyAttr := datalog.NewKeyword(":item/key")
	valAttr := datalog.NewKeyword(":item/val")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	var datoms []datalog.Datom
	add := func(seed, key string, val int64) {
		e := datalog.NewIdentity(seed)
		datoms = append(datoms,
			datalog.Datom{E: e, A: keyAttr, V: key, Tx: tx},
			datalog.Datom{E: e, A: valAttr, V: val, Tx: tx},
		)
	}
	add("a1", "A", 1)
	add("a2", "A", 2)
	add("a3", "A", 3)
	add("b1", "B", 4)
	add("b2", "B", 5)
	add("b3", "B", 6)
	add("b4", "B", 7)

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher, nil)

	q, err := parser.ParseQuery(`[:find ?v
	                              :in $ [[?k] ...]
	                              :where [?e :item/key ?k]
	                                     [?e :item/val ?v]
	                              :order-by [[?k :asc] [?v :desc]]]`)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	inputRel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?k")}, []Tuple{{"B"}, {"A"}})

	result, err := executor.ExecuteWithRelations(NewContext(nil), q, []Relation{inputRel})
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	expected := []int64{3, 2, 1, 7, 6, 5, 4} // A block desc, then B block desc
	if result.Size() != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), result.Size())
	}
	for i := 0; i < result.Size(); i++ {
		v := result.Get(i)[0].(int64)
		if v != expected[i] {
			t.Errorf("tuple %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

// Case K: ordering an aggregate query by a group key. The group key is a
// symbol of the post-aggregation relation, so this must sort.
func TestOrderByAggregateGroupKey(t *testing.T) {
	cityAttr := datalog.NewKeyword(":person/city")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("p1"), A: cityAttr, V: "NYC", Tx: tx},
		{E: datalog.NewIdentity("p2"), A: cityAttr, V: "LA", Tx: tx},
		{E: datalog.NewIdentity("p3"), A: cityAttr, V: "SF", Tx: tx},
		{E: datalog.NewIdentity("p4"), A: cityAttr, V: "BOS", Tx: tx},
		{E: datalog.NewIdentity("p5"), A: cityAttr, V: "NYC", Tx: tx},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher, nil)

	q, err := parser.ParseQuery(`[:find ?city (count ?p)
	                              :where [?p :person/city ?city]
	                              :order-by [[?city :asc]]]`)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	expectedCities := []string{"BOS", "LA", "NYC", "SF"}
	expectedCounts := []int64{1, 1, 2, 1}
	if result.Size() != len(expectedCities) {
		t.Fatalf("expected %d groups, got %d", len(expectedCities), result.Size())
	}
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		if city := tuple[0].(string); city != expectedCities[i] {
			t.Errorf("group %d: expected city %s, got %s", i, expectedCities[i], city)
		}
		if count := tuple[1].(int64); count != expectedCounts[i] {
			t.Errorf("group %d: expected count %d, got %d", i, expectedCounts[i], count)
		}
	}
}

// Case L: ordering an aggregate query by a :where variable that is not a
// group key. Aggregation collapses rows — the variable is not an attribute
// of the post-aggregation relation — so this must be rejected, not silently
// ignored.
func TestOrderByAggregateNonFindVariableIsError(t *testing.T) {
	matcher := NewMemoryPatternMatcher(statusUserDatoms())
	executor := NewExecutor(matcher, nil)

	q, err := parser.ParseQuery(`[:find ?status (count ?e)
	                              :where [?e :user/status ?status]
	                                     [?e :user/age ?age]
	                              :order-by [[?age :desc]]]`)
	if err != nil {
		return // parse-time rejection satisfies the contract
	}

	_, err = executor.Execute(q)
	if err == nil {
		t.Fatal("expected error for order-by on a non-group-key variable in an aggregate query, got success")
	}
}

// Case M: janus find results are set semantics (materialization
// deduplicates), so stripping the sort symbol after sorting collapses
// duplicate find values — and the survivor's position is its first
// occurrence in sorted order. Ages 10 (Alice), 20 (Bob), 30 (Alice) sort to
// Alice, Bob, Alice; dedup keeps [Alice, Bob].
func TestOrderByNonProjectedPreservesDuplicates(t *testing.T) {
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("user:alice1"), A: nameAttr, V: "Alice", Tx: tx},
		{E: datalog.NewIdentity("user:alice1"), A: ageAttr, V: int64(30), Tx: tx},
		{E: datalog.NewIdentity("user:alice2"), A: nameAttr, V: "Alice", Tx: tx},
		{E: datalog.NewIdentity("user:alice2"), A: ageAttr, V: int64(10), Tx: tx},
		{E: datalog.NewIdentity("user:bob"), A: nameAttr, V: "Bob", Tx: tx},
		{E: datalog.NewIdentity("user:bob"), A: ageAttr, V: int64(20), Tx: tx},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher, nil)

	q, err := parser.ParseQuery(`[:find ?name
	                              :where [?e :user/name ?name]
	                                     [?e :user/age ?age]
	                              :order-by [[?age :asc]]]`)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	expected := []string{"Alice", "Bob"} // first occurrence in sorted order wins
	if result.Size() != len(expected) {
		t.Fatalf("expected %d results (set semantics), got %d", len(expected), result.Size())
	}
	for i := 0; i < result.Size(); i++ {
		name := result.Get(i)[0].(string)
		if name != expected[i] {
			t.Errorf("tuple %d: expected %s, got %s", i, expected[i], name)
		}
	}
}

// Case P: a (pull ...) find spec combined with a fully *projected* sort
// key. Pulls render at the result boundary after sort/strip/limit, so the
// sort only ever sees Identity in the entity binding; the tied-key variant
// is TestOrderByPullWithTiedSortKeys below.
func TestOrderByProjectedKeyWithPull(t *testing.T) {
	matcher := NewMemoryPatternMatcher(nonProjectedSortDatoms())
	executor := NewExecutor(matcher, nil)

	q, err := parser.ParseQuery(`[:find (pull ?e [:user/name]) ?age
	                              :where [?e :user/age ?age]
	                              :order-by [[?age :asc]]]`)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	expectedAges := []int64{8, 12, 27, 34, 45, 51}
	if result.Size() != len(expectedAges) {
		t.Fatalf("expected %d results, got %d", len(expectedAges), result.Size())
	}
	for i := 0; i < result.Size(); i++ {
		if age := result.Get(i)[1].(int64); age != expectedAges[i] {
			t.Errorf("tuple %d: expected age %d, got %d", i, expectedAges[i], age)
		}
	}
}

// Case N: a (pull ...) find spec combined with a non-projected sort key.
// The retained sort symbol orders the result, the strip projects back to
// the find shape (all value bindings — plain deduplicating projection), and
// the boundary pull renders the surviving rows.
func TestOrderByNonProjectedWithPull(t *testing.T) {
	matcher := NewMemoryPatternMatcher(nonProjectedSortDatoms())
	executor := NewExecutor(matcher, nil)

	q, err := parser.ParseQuery(`[:find (pull ?e [:user/name])
	                              :where [?e :user/age ?age]
	                              :order-by [[?age :asc]]]`)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	expected := []string{"Frank", "Bob", "Dave", "Alice", "Erin", "Carol"}
	if result.Size() != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), result.Size())
	}
	for i := 0; i < result.Size(); i++ {
		pulled, ok := result.Get(i)[0].(map[string]interface{})
		if !ok {
			t.Fatalf("tuple %d: expected pulled map, got %T", i, result.Get(i)[0])
		}
		if name := pulled["user/name"]; name != expected[i] {
			t.Errorf("tuple %d: expected %s, got %v", i, expected[i], name)
		}
	}
}

// Set semantics must hold for a *projected* sort key exactly as it does
// without :order-by: with a projected key there is no post-sort strip, so
// this pins that result membership is identical with and without the sort.
func TestOrderByProjectedKeyDeduplicates(t *testing.T) {
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	// Two distinct entities share a name; projecting to ?name alone creates
	// the duplicate that set semantics must collapse.
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("user:alice1"), A: nameAttr, V: "Alice", Tx: tx},
		{E: datalog.NewIdentity("user:alice1"), A: ageAttr, V: int64(30), Tx: tx},
		{E: datalog.NewIdentity("user:alice2"), A: nameAttr, V: "Alice", Tx: tx},
		{E: datalog.NewIdentity("user:alice2"), A: ageAttr, V: int64(10), Tx: tx},
		{E: datalog.NewIdentity("user:bob"), A: nameAttr, V: "Bob", Tx: tx},
		{E: datalog.NewIdentity("user:bob"), A: ageAttr, V: int64(20), Tx: tx},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher, nil)

	baseline, err := parser.ParseQuery(`[:find ?name
	                                     :where [?e :user/name ?name]
	                                            [?e :user/age ?age]]`)
	if err != nil {
		t.Fatalf("failed to parse baseline query: %v", err)
	}
	baseResult, err := executor.Execute(baseline)
	if err != nil {
		t.Fatalf("baseline execution failed: %v", err)
	}
	baseCount := baseResult.Size()

	sorted, err := parser.ParseQuery(`[:find ?name
	                                   :where [?e :user/name ?name]
	                                          [?e :user/age ?age]
	                                   :order-by [[?name :asc]]]`)
	if err != nil {
		t.Fatalf("failed to parse sorted query: %v", err)
	}
	sortResult, err := executor.Execute(sorted)
	if err != nil {
		t.Fatalf("sorted execution failed: %v", err)
	}

	if sortResult.Size() != baseCount {
		t.Fatalf("order-by changed result membership: %d rows without order-by, %d with", baseCount, sortResult.Size())
	}
	expected := []string{"Alice", "Bob"}
	if sortResult.Size() != len(expected) {
		t.Fatalf("expected %d deduplicated rows, got %d", len(expected), sortResult.Size())
	}
	for i := 0; i < sortResult.Size(); i++ {
		if name := sortResult.Get(i)[0].(string); name != expected[i] {
			t.Errorf("tuple %d: expected %s, got %s", i, expected[i], name)
		}
	}
}

// Unit tests for SortRelation itself (not through Execute): the parser and
// planner guarantee parsed queries never hand SortRelation an unresolvable
// key, so these pin the guards for API-constructed queries and the
// permutation contract.

// SortRelation must surface an unresolvable sort key as a deferred relation
// error — never the silent skip that returned arbitrary order as success.
func TestSortRelationUnresolvableKeyIsDeferredError(t *testing.T) {
	name := datalog.NewSymbol("?name")
	rel := NewMaterializedRelation([]query.Symbol{name}, []Tuple{{"Alice"}, {"Bob"}})

	sorted := SortRelation(rel, []query.OrderByClause{
		{Variable: datalog.NewSymbol("?missing"), Direction: query.OrderAsc},
	})

	it := sorted.Iterator()
	defer it.Close()
	for it.Next() {
	}
	if it.Error() == nil {
		t.Fatal("expected deferred error for unresolvable sort key, got clean iteration")
	}
}

// Pull + order-by with TIED sort keys: rows that tie on every comparable
// symbol must sort and render correctly. Pulls run at the result boundary
// after sort/strip/limit, so relational operations only ever see Identity
// in the entity binding. See docs/bugs/resolved/BUG_PULL_WITH_ORDER_BY_PANICS.md.
func TestOrderByPullWithTiedSortKeys(t *testing.T) {
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	// Two users share age 30: their tuples tie on every comparable result slot.
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("user:alice"), A: nameAttr, V: "Alice", Tx: tx},
		{E: datalog.NewIdentity("user:alice"), A: ageAttr, V: int64(30), Tx: tx},
		{E: datalog.NewIdentity("user:bob"), A: nameAttr, V: "Bob", Tx: tx},
		{E: datalog.NewIdentity("user:bob"), A: ageAttr, V: int64(30), Tx: tx},
		{E: datalog.NewIdentity("user:carol"), A: nameAttr, V: "Carol", Tx: tx},
		{E: datalog.NewIdentity("user:carol"), A: ageAttr, V: int64(10), Tx: tx},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher, nil)

	q, err := parser.ParseQuery(`[:find (pull ?e [:user/name]) ?age
	                              :where [?e :user/age ?age]
	                              :order-by [[?age :asc]]]`)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}
	if result.Size() != 3 {
		t.Fatalf("expected 3 rows, got %d", result.Size())
	}
	if age := result.Get(0)[1].(int64); age != 10 {
		t.Errorf("expected age 10 first, got %d", age)
	}
}

// Maps are not datalog values: pull output is result presentation, rendered
// at the result boundary after sorting (docs/bugs/resolved/BUG_PULL_WITH_ORDER_BY_PANICS.md).
// A relation containing maps is therefore a value-domain violation, and the
// hash layer enforces the domain loudly: sorting such a relation panics
// naming the violation — never hashing by address or comparing wrongly.
func TestSortRelationRejectsNonValueTuples(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected value-domain panic for map values in a relation")
		}
		if msg := fmt.Sprint(r); !strings.Contains(msg, "not a datalog value") {
			t.Fatalf("panic should name the value-domain violation, got: %v", r)
		}
	}()

	e := datalog.NewSymbol("?e")
	age := datalog.NewSymbol("?age")
	rel := NewMaterializedRelationNoDedupe([]query.Symbol{e, age}, []Tuple{
		{map[string]interface{}{"user/name": "Alice"}, int64(34)},
		{map[string]interface{}{"user/name": "Bob"}, int64(34)},
	})

	SortRelation(rel, []query.OrderByClause{
		{Variable: age, Direction: query.OrderAsc},
	})
}

func TestSortingWithNulls(t *testing.T) {
	// Test how sorting handles missing values
	user1 := datalog.NewIdentity("user:1")
	user2 := datalog.NewIdentity("user:2")
	user3 := datalog.NewIdentity("user:3")

	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")

	datoms := []datalog.Datom{
		{E: user1, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: user1, A: ageAttr, V: int64(30), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},

		{E: user2, A: nameAttr, V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		// Bob has no age

		{E: user3, A: nameAttr, V: "Charlie", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: user3, A: ageAttr, V: int64(25), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher, nil)

	// Note: get-else is not implemented, so we test with a simpler query
	// This is more about ensuring no panic than specific null handling

	simpleQuery := `[:find ?name
	                 :where [?e :user/name ?name]
	                 :order-by [[?name :asc]]]`

	q, err := parser.ParseQuery(simpleQuery)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Should have all 3 users
	if result.Size() != 3 {
		t.Errorf("expected 3 results, got %d", result.Size())
	}
}
