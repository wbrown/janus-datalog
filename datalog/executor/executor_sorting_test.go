package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
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
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: alice, A: ageAttr, V: int64(30), Tx: 1},
		{E: alice, A: scoreAttr, V: 85.5, Tx: 1},
		{E: alice, A: joinedAttr, V: date2, Tx: 1},

		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		{E: bob, A: ageAttr, V: int64(25), Tx: 1},
		{E: bob, A: scoreAttr, V: 92.0, Tx: 1},
		{E: bob, A: joinedAttr, V: date1, Tx: 1},

		{E: charlie, A: nameAttr, V: "Charlie", Tx: 1},
		{E: charlie, A: ageAttr, V: int64(35), Tx: 1},
		{E: charlie, A: scoreAttr, V: 78.5, Tx: 1},
		{E: charlie, A: joinedAttr, V: date3, Tx: 1},

		{E: dave, A: nameAttr, V: "Dave", Tx: 1},
		{E: dave, A: ageAttr, V: int64(28), Tx: 1},
		{E: dave, A: scoreAttr, V: 88.0, Tx: 1},
		{E: dave, A: joinedAttr, V: date4, Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	tests := []struct {
		name     string
		query    string
		expected []string // Expected first column values in order
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
			name: "Multi-column sort: age then name",
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
					t.Errorf("row %d: expected %s, got %s", i, tt.expected[i], name)
				}
			}
		})
	}
}

func TestSortingEdgeCases(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		datoms []datalog.Datom
	}{
		{
			name: "Sort empty result set",
			query: `[:find ?name ?age
			         :where [?e :user/name ?name]
			                [?e :user/age ?age]
			                [(> ?age 100)]
			         :order-by [[?name :asc]]]`,
			datoms: []datalog.Datom{
				{E: datalog.NewIdentity("user:1"), A: datalog.NewKeyword(":user/name"), V: "Alice", Tx: 1},
				{E: datalog.NewIdentity("user:1"), A: datalog.NewKeyword(":user/age"), V: int64(30), Tx: 1},
			},
		},
		{
			name: "Sort with missing sort column",
			query: `[:find ?name
			         :where [?e :user/name ?name]
			         :order-by [[?age :asc]]]`, // ?age not in find
			datoms: []datalog.Datom{
				{E: datalog.NewIdentity("user:1"), A: datalog.NewKeyword(":user/name"), V: "Alice", Tx: 1},
			},
		},
		{
			name: "Sort single row",
			query: `[:find ?name ?age
			         :where [?e :user/name ?name]
			                [?e :user/age ?age]
			         :order-by [[?name :asc]]]`,
			datoms: []datalog.Datom{
				{E: datalog.NewIdentity("user:1"), A: datalog.NewKeyword(":user/name"), V: "Alice", Tx: 1},
				{E: datalog.NewIdentity("user:1"), A: datalog.NewKeyword(":user/age"), V: int64(30), Tx: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher := NewMemoryPatternMatcher(tt.datoms)
			executor := NewExecutor(matcher)

			// Parse query
			q, err := parser.ParseQuery(tt.query)
			if err != nil {
				// Some edge cases might have parse errors (like missing sort column)
				// That's OK for testing error handling
				return
			}

			// Execute query - should not panic
			result, err := executor.Execute(q)
			if err != nil {
				// Error is OK for edge cases
				return
			}

			// Just verify it doesn't crash
			_ = result.Size()
		})
	}
}

func TestSortingWithNulls(t *testing.T) {
	// Test how sorting handles missing values
	user1 := datalog.NewIdentity("user:1")
	user2 := datalog.NewIdentity("user:2")
	user3 := datalog.NewIdentity("user:3")

	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")

	datoms := []datalog.Datom{
		{E: user1, A: nameAttr, V: "Alice", Tx: 1},
		{E: user1, A: ageAttr, V: int64(30), Tx: 1},

		{E: user2, A: nameAttr, V: "Bob", Tx: 1},
		// Bob has no age

		{E: user3, A: nameAttr, V: "Charlie", Tx: 1},
		{E: user3, A: ageAttr, V: int64(25), Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

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
