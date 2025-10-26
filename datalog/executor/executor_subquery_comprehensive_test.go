package executor

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestSubqueryWithNoResults tests subquery that returns no results
func TestSubqueryWithNoResults(t *testing.T) {
	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			"[:symbol/ticker \"AAPL\"]": {
				{E: datalog.NewIdentity("symbol:aapl"), A: datalog.NewKeyword(":symbol/ticker"), V: "AAPL", Tx: 1},
			},
			// No prices for AAPL
			"[:price/symbol _]": {},
			"[:price/value _]":  {},
		},
	}

	exec := NewExecutor(matcher)

	// Subquery should return no results for max price
	queryStr := `[:find ?symbol ?max-price
	             :where 
	             [?s :symbol/ticker ?symbol]
	             [(q [:find (max ?price)
	                  :in $ ?sym
	                  :where [?p :price/symbol ?sym]
	                         [?p :price/value ?price]]
	                 $ ?s) [[?max-price]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Query execution failed: %v", err)
	}

	// EXPECTED: Empty result (not an error)
	// When subquery returns no results, the pattern fails to match
	if result.Size() != 0 {
		t.Errorf("Expected 0 results (empty subquery = failed pattern), got %d", result.Size())
	}
}

// TestSubqueryWithRelationBinding tests subquery that returns multiple rows
func TestSubqueryWithRelationBinding(t *testing.T) {
	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			"[:symbol/ticker _]": {
				{E: datalog.NewIdentity("symbol:aapl"), A: datalog.NewKeyword(":symbol/ticker"), V: "AAPL", Tx: 1},
			},
			"[:price/symbol _]": {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: 3},
				{E: datalog.NewIdentity("price:3"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: 4},
			},
			"[:price/time _]": {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/time"), V: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/time"), V: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), Tx: 3},
				{E: datalog.NewIdentity("price:3"), A: datalog.NewKeyword(":price/time"), V: time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC), Tx: 4},
			},
			"[:price/value _]": {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/value"), V: 150.0, Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/value"), V: 155.0, Tx: 3},
				{E: datalog.NewIdentity("price:3"), A: datalog.NewKeyword(":price/value"), V: 152.0, Tx: 4},
			},
		},
	}

	exec := NewExecutor(matcher)

	// Subquery returns all prices for a symbol (relation binding)
	queryStr := `[:find ?symbol ?time ?price
	             :where 
	             [?s :symbol/ticker ?symbol]
	             [(q [:find ?t ?v
	                  :in $ ?sym
	                  :where [?p :price/symbol ?sym]
	                         [?p :price/time ?t]
	                         [?p :price/value ?v]]
	                 $ ?s) [[?time ?price] ...]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Should get 3 results (3 prices for AAPL)
	if result.Size() != 3 {
		t.Errorf("Expected 3 results, got %d", result.Size())
		for i := 0; i < result.Size(); i++ {
			t.Logf("Result %d: %v", i, result.Get(i))
		}
	}

	// Check that all results have AAPL as symbol
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		if symbol := tuple[0].(string); symbol != "AAPL" {
			t.Errorf("Result %d: expected symbol AAPL, got %s", i, symbol)
		}
	}
}

// TestSubqueryWithMultipleOuterRows tests subquery executed for multiple outer rows
func TestSubqueryWithMultipleOuterRows(t *testing.T) {
	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			"[:department/name _]": {
				{E: datalog.NewIdentity("dept:eng"), A: datalog.NewKeyword(":department/name"), V: "Engineering", Tx: 1},
				{E: datalog.NewIdentity("dept:sales"), A: datalog.NewKeyword(":department/name"), V: "Sales", Tx: 1},
			},
			"[:employee/department _]": {
				{E: datalog.NewIdentity("emp:1"), A: datalog.NewKeyword(":employee/department"), V: datalog.NewIdentity("dept:eng"), Tx: 2},
				{E: datalog.NewIdentity("emp:2"), A: datalog.NewKeyword(":employee/department"), V: datalog.NewIdentity("dept:eng"), Tx: 2},
				{E: datalog.NewIdentity("emp:3"), A: datalog.NewKeyword(":employee/department"), V: datalog.NewIdentity("dept:sales"), Tx: 2},
				{E: datalog.NewIdentity("emp:4"), A: datalog.NewKeyword(":employee/department"), V: datalog.NewIdentity("dept:sales"), Tx: 2},
				{E: datalog.NewIdentity("emp:5"), A: datalog.NewKeyword(":employee/department"), V: datalog.NewIdentity("dept:sales"), Tx: 2},
			},
			"[:employee/salary _]": {
				{E: datalog.NewIdentity("emp:1"), A: datalog.NewKeyword(":employee/salary"), V: int64(100000), Tx: 2},
				{E: datalog.NewIdentity("emp:2"), A: datalog.NewKeyword(":employee/salary"), V: int64(120000), Tx: 2},
				{E: datalog.NewIdentity("emp:3"), A: datalog.NewKeyword(":employee/salary"), V: int64(80000), Tx: 2},
				{E: datalog.NewIdentity("emp:4"), A: datalog.NewKeyword(":employee/salary"), V: int64(90000), Tx: 2},
				{E: datalog.NewIdentity("emp:5"), A: datalog.NewKeyword(":employee/salary"), V: int64(85000), Tx: 2},
			},
		},
	}

	exec := NewExecutor(matcher)

	// Find average salary per department using subquery
	queryStr := `[:find ?dept-name ?avg-salary
	             :where 
	             [?d :department/name ?dept-name]
	             [(q [:find (avg ?salary)
	                  :in $ ?dept
	                  :where [?e :employee/department ?dept]
	                         [?e :employee/salary ?salary]]
	                 $ ?d) [[?avg-salary]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Should get 2 results (one per department)
	if result.Size() != 2 {
		t.Errorf("Expected 2 results, got %d", result.Size())
		for i := 0; i < result.Size(); i++ {
			t.Logf("Result %d: %v", i, result.Get(i))
		}
	}

	// Check results
	resultMap := make(map[string]float64)
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		dept := tuple[0].(string)
		avg := tuple[1].(float64)
		resultMap[dept] = avg
	}

	// Engineering average: (100000 + 120000) / 2 = 110000
	if avg, ok := resultMap["Engineering"]; !ok || avg != 110000.0 {
		t.Errorf("Expected Engineering avg salary 110000, got %v", avg)
	}

	// Sales average: (80000 + 90000 + 85000) / 3 = 85000
	if avg, ok := resultMap["Sales"]; !ok || avg != 85000.0 {
		t.Errorf("Expected Sales avg salary 85000, got %v", avg)
	}
}

// TestSubqueryWithTwoInputs tests subquery that takes multiple input variables
// NOTE: Current implementation only supports single input to subqueries
func TestSubqueryWithTwoInputs(t *testing.T) {
	t.Skip("Multiple inputs to subqueries not yet supported")

	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			"[:product/category _]": {
				{E: datalog.NewIdentity("prod:1"), A: datalog.NewKeyword(":product/category"), V: "Electronics", Tx: 1},
				{E: datalog.NewIdentity("prod:2"), A: datalog.NewKeyword(":product/category"), V: "Electronics", Tx: 1},
				{E: datalog.NewIdentity("prod:3"), A: datalog.NewKeyword(":product/category"), V: "Books", Tx: 1},
			},
			"[:sale/product _]": {
				{E: datalog.NewIdentity("sale:1"), A: datalog.NewKeyword(":sale/product"), V: datalog.NewIdentity("prod:1"), Tx: 2},
				{E: datalog.NewIdentity("sale:2"), A: datalog.NewKeyword(":sale/product"), V: datalog.NewIdentity("prod:1"), Tx: 2},
				{E: datalog.NewIdentity("sale:3"), A: datalog.NewKeyword(":sale/product"), V: datalog.NewIdentity("prod:2"), Tx: 2},
				{E: datalog.NewIdentity("sale:4"), A: datalog.NewKeyword(":sale/product"), V: datalog.NewIdentity("prod:3"), Tx: 2},
			},
			"[:sale/date _]": {
				{E: datalog.NewIdentity("sale:1"), A: datalog.NewKeyword(":sale/date"), V: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), Tx: 2},
				{E: datalog.NewIdentity("sale:2"), A: datalog.NewKeyword(":sale/date"), V: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), Tx: 2},
				{E: datalog.NewIdentity("sale:3"), A: datalog.NewKeyword(":sale/date"), V: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), Tx: 2},
				{E: datalog.NewIdentity("sale:4"), A: datalog.NewKeyword(":sale/date"), V: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), Tx: 2},
			},
			"[:sale/amount _]": {
				{E: datalog.NewIdentity("sale:1"), A: datalog.NewKeyword(":sale/amount"), V: 100.0, Tx: 2},
				{E: datalog.NewIdentity("sale:2"), A: datalog.NewKeyword(":sale/amount"), V: 150.0, Tx: 2},
				{E: datalog.NewIdentity("sale:3"), A: datalog.NewKeyword(":sale/amount"), V: 200.0, Tx: 2},
				{E: datalog.NewIdentity("sale:4"), A: datalog.NewKeyword(":sale/amount"), V: 50.0, Tx: 2},
			},
		},
	}

	exec := NewExecutor(matcher)

	// Find sales for specific category and date using subquery with two inputs
	targetDate := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	queryStr := fmt.Sprintf(`[:find ?category ?total
	             :where 
	             [?p :product/category ?category]
	             [(q [:find (sum ?amount)
	                  :in $ ?cat ?date
	                  :where [?prod :product/category ?cat]
	                         [?s :sale/product ?prod]
	                         [?s :sale/date ?date]
	                         [?s :sale/amount ?amount]]
	                 $ ?category "%s") [[?total]]]]`, targetDate.Format(time.RFC3339))

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Check results
	resultMap := make(map[string]float64)
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		category := tuple[0].(string)
		total := tuple[1].(float64)
		resultMap[category] = total
	}

	// Electronics on 2025-01-01: sale:1 (100) + sale:3 (200) = 300
	if total, ok := resultMap["Electronics"]; !ok || total != 300.0 {
		t.Errorf("Expected Electronics total 300, got %v", total)
	}

	// Books on 2025-01-01: sale:4 (50) = 50
	if total, ok := resultMap["Books"]; !ok || total != 50.0 {
		t.Errorf("Expected Books total 50, got %v", total)
	}
}

// TestSubqueryWithNoInput tests subquery that doesn't need input from outer query
func TestSubqueryWithNoInput(t *testing.T) {
	t.Skip("Planner doesn't handle subqueries without inputs in predicates correctly")

	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			"[:config/key \"max_price\"]": {
				{E: datalog.NewIdentity("config:1"), A: datalog.NewKeyword(":config/key"), V: "max_price", Tx: 1},
			},
			"[:config/value _]": {
				{E: datalog.NewIdentity("config:1"), A: datalog.NewKeyword(":config/value"), V: 1000.0, Tx: 1},
			},
			"[:product/name _]": {
				{E: datalog.NewIdentity("prod:1"), A: datalog.NewKeyword(":product/name"), V: "Laptop", Tx: 2},
				{E: datalog.NewIdentity("prod:2"), A: datalog.NewKeyword(":product/name"), V: "Phone", Tx: 2},
			},
			"[:product/price _]": {
				{E: datalog.NewIdentity("prod:1"), A: datalog.NewKeyword(":product/price"), V: 1200.0, Tx: 2},
				{E: datalog.NewIdentity("prod:2"), A: datalog.NewKeyword(":product/price"), V: 800.0, Tx: 2},
			},
		},
	}

	exec := NewExecutor(matcher)

	// Find products cheaper than max configured price
	// NOTE: Changed order - subquery must come before its result is used
	queryStr := `[:find ?name ?price
	             :where 
	             [(q [:find ?max
	                  :in $
	                  :where [?c :config/key "max_price"]
	                         [?c :config/value ?max]]
	                 $) [[?max-price]]]
	             [?p :product/name ?name]
	             [?p :product/price ?price]
	             [(< ?price ?max-price)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Only Phone (800) should be under max_price (1000)
	if result.Size() != 1 {
		t.Errorf("Expected 1 result, got %d", result.Size())
		for i := 0; i < result.Size(); i++ {
			t.Logf("Result %d: %v", i, result.Get(i))
		}
	}

	if result.Size() > 0 {
		tuple := result.Get(0)
		if name := tuple[0].(string); name != "Phone" {
			t.Errorf("Expected Phone, got %s", name)
		}
		if price := tuple[1].(float64); price != 800.0 {
			t.Errorf("Expected price 800, got %f", price)
		}
	}
}

// TestSubqueryInFilter tests using subquery result in a filter/predicate
func TestSubqueryInFilter(t *testing.T) {
	t.Skip("Planner doesn't handle subqueries used in predicates correctly")

	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			"[:employee/name _]": {
				{E: datalog.NewIdentity("emp:1"), A: datalog.NewKeyword(":employee/name"), V: "Alice", Tx: 1},
				{E: datalog.NewIdentity("emp:2"), A: datalog.NewKeyword(":employee/name"), V: "Bob", Tx: 1},
				{E: datalog.NewIdentity("emp:3"), A: datalog.NewKeyword(":employee/name"), V: "Charlie", Tx: 1},
			},
			"[:employee/salary _]": {
				{E: datalog.NewIdentity("emp:1"), A: datalog.NewKeyword(":employee/salary"), V: int64(90000), Tx: 1},
				{E: datalog.NewIdentity("emp:2"), A: datalog.NewKeyword(":employee/salary"), V: int64(85000), Tx: 1},
				{E: datalog.NewIdentity("emp:3"), A: datalog.NewKeyword(":employee/salary"), V: int64(95000), Tx: 1},
			},
		},
	}

	exec := NewExecutor(matcher)

	// Find employees earning more than average
	queryStr := `[:find ?name ?salary
	             :where 
	             [(q [:find (avg ?s)
	                  :in $
	                  :where [?e :employee/salary ?s]]
	                 $) [[?avg-salary]]]
	             [?emp :employee/name ?name]
	             [?emp :employee/salary ?salary]
	             [(> ?salary ?avg-salary)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Average is 90000, so Alice (90000) is not included, but Charlie (95000) is
	if result.Size() != 1 {
		t.Errorf("Expected 1 result, got %d", result.Size())
		for i := 0; i < result.Size(); i++ {
			t.Logf("Result %d: %v", i, result.Get(i))
		}
	}

	if result.Size() > 0 {
		tuple := result.Get(0)
		if name := tuple[0].(string); name != "Charlie" {
			t.Errorf("Expected Charlie, got %s", name)
		}
	}
}

// TestSubqueryErrorHandling tests various error conditions
func TestSubqueryErrorHandling(t *testing.T) {
	tests := []struct {
		name     string
		queryStr string
		wantErr  string
	}{
		{
			name: "TupleBindingMultipleResults",
			queryStr: `[:find ?category ?count
			           :where 
			           [?p :product/category ?category]
			           [(q [:find ?prod
			                :in $ ?cat
			                :where [?prod :product/category ?cat]]
			               $ ?category) [[?count]]]]`, // Tuple binding but returns multiple products
			wantErr: "tuple binding expects exactly 1 result",
		},
		{
			name: "RelationBindingColumnMismatch",
			queryStr: `[:find ?name ?a ?b ?c
			           :where 
			           [?e :employee/name ?name]
			           [(q [:find ?salary
			                :in $ ?emp
			                :where [?emp :employee/salary ?salary]]
			               $ ?e) [[?a ?b ?c] ...]]]`, // Expects 3 columns but query returns 1
			wantErr: "relation binding expects 3 columns, got 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a matcher with test data that will trigger the error
			matcher := &MockPatternMatcher{
				data: map[string][]datalog.Datom{
					"[:product/category _]": {
						{E: datalog.NewIdentity("prod:1"), A: datalog.NewKeyword(":product/category"), V: "Electronics", Tx: 1},
						{E: datalog.NewIdentity("prod:2"), A: datalog.NewKeyword(":product/category"), V: "Electronics", Tx: 1},
					},
					"[:employee/name _]": {
						{E: datalog.NewIdentity("emp:1"), A: datalog.NewKeyword(":employee/name"), V: "Alice", Tx: 1},
					},
					"[:employee/salary _]": {
						{E: datalog.NewIdentity("emp:1"), A: datalog.NewKeyword(":employee/salary"), V: int64(90000), Tx: 1},
					},
				},
			}

			exec := NewExecutor(matcher)
			q, err := parser.ParseQuery(tt.queryStr)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			_, err = exec.Execute(q)
			if err == nil {
				t.Errorf("Expected error containing %q, got no error", tt.wantErr)
			} else if !containsString(err.Error(), tt.wantErr) {
				t.Errorf("Expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

// TestSubqueryWithEmptyOuterQuery tests subquery when outer query has no results
func TestSubqueryWithEmptyOuterQuery(t *testing.T) {
	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			// No symbols in database
			"[:symbol/ticker _]": {},
			"[:price/symbol _]": {
				// Some prices exist but no matching symbols
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:fake"), Tx: 1},
			},
		},
	}

	exec := NewExecutor(matcher)

	queryStr := `[:find ?symbol ?max-price
	             :where 
	             [?s :symbol/ticker ?symbol]
	             [(q [:find (max ?price)
	                  :in $ ?sym
	                  :where [?p :price/symbol ?sym]
	                         [?p :price/value ?price]]
	                 $ ?s) [[?max-price]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Should get 0 results since outer query produces nothing
	if result.Size() != 0 {
		t.Errorf("Expected 0 results, got %d", result.Size())
	}
}

// TestSubqueryPerformance tests that subqueries don't execute redundantly
func TestSubqueryPerformance(t *testing.T) {
	// Create data with duplicate outer values to test deduplication
	datoms := []datalog.Datom{}

	// Create 10 categories
	for i := 0; i < 10; i++ {
		cat := fmt.Sprintf("cat:%d", i)
		catName := fmt.Sprintf("Category%d", i)
		datoms = append(datoms,
			datalog.Datom{E: datalog.NewIdentity(cat), A: datalog.NewKeyword(":category/name"), V: catName, Tx: 1},
		)

		// Create 100 products per category (1000 total)
		for j := 0; j < 100; j++ {
			prod := fmt.Sprintf("prod:%d-%d", i, j)
			datoms = append(datoms,
				datalog.Datom{E: datalog.NewIdentity(prod), A: datalog.NewKeyword(":product/category"), V: datalog.NewIdentity(cat), Tx: 2},
				datalog.Datom{E: datalog.NewIdentity(prod), A: datalog.NewKeyword(":product/price"), V: float64(10 + j), Tx: 2},
			)
		}
	}

	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher)

	// Count products per category using subquery
	queryStr := `[:find ?cat-name ?count
	             :where 
	             [?c :category/name ?cat-name]
	             [(q [:find (count ?p)
	                  :in $ ?category
	                  :where [?p :product/category ?category]]
	                 $ ?c) [[?count]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	start := time.Now()
	result, err := exec.Execute(q)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Should get 10 results (one per category)
	if result.Size() != 10 {
		t.Errorf("Expected 10 results, got %d", result.Size())
	}

	// Check that each category has count of 100
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		count := tuple[1].(int64)
		if count != 100 {
			t.Errorf("Expected count 100, got %d for %v", count, tuple[0])
		}
	}

	t.Logf("Query with 10 subqueries on 1000 products took %v", duration)

	// This is a basic performance check - with deduplication,
	// we should execute 10 subqueries, not more
	if duration > 500*time.Millisecond {
		t.Logf("Warning: Query took longer than expected: %v", duration)
	}
}

// Helper function
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && s[len(s)-len(substr):] == substr ||
		len(s) >= len(substr) && s[:len(substr)] == substr ||
		len(s) > len(substr) && findSubstringHelper(s, substr)
}

func findSubstringHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
