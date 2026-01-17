package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestExpressionQueries(t *testing.T) {
	// Create test data for expression evaluation
	product1 := datalog.NewIdentity("product:1")
	product2 := datalog.NewIdentity("product:2")
	product3 := datalog.NewIdentity("product:3")

	nameAttr := datalog.NewKeyword(":product/name")
	priceAttr := datalog.NewKeyword(":product/price")
	quantityAttr := datalog.NewKeyword(":product/quantity")
	discountAttr := datalog.NewKeyword(":product/discount")

	datoms := []datalog.Datom{
		{E: product1, A: nameAttr, V: "Widget", Tx: 1},
		{E: product1, A: priceAttr, V: 100.0, Tx: 1},
		{E: product1, A: quantityAttr, V: int64(5), Tx: 1},
		{E: product1, A: discountAttr, V: 0.1, Tx: 1},

		{E: product2, A: nameAttr, V: "Gadget", Tx: 1},
		{E: product2, A: priceAttr, V: 50.0, Tx: 1},
		{E: product2, A: quantityAttr, V: int64(10), Tx: 1},
		{E: product2, A: discountAttr, V: 0.2, Tx: 1},

		{E: product3, A: nameAttr, V: "Doohickey", Tx: 1},
		{E: product3, A: priceAttr, V: 75.0, Tx: 1},
		{E: product3, A: quantityAttr, V: int64(3), Tx: 1},
		{E: product3, A: discountAttr, V: 0.0, Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	tests := []struct {
		name          string
		query         string
		expectedCount int
		validate      func(*testing.T, Relation)
	}{
		{
			name: "Arithmetic addition",
			query: `[:find ?name ?price ?qty ?total
			         :where [?p :product/name ?name]
			                [?p :product/price ?price]
			                [?p :product/quantity ?qty]
			                [(* ?price ?qty) ?total]]`,
			expectedCount: 3,
			validate: func(t *testing.T, result Relation) {
				// Find Widget's total (100 * 5 = 500)
				for i := 0; i < result.Size(); i++ {
					tuple := result.Get(i)
					if tuple[0].(string) == "Widget" {
						total := tuple[3].(float64)
						if total != 500.0 {
							t.Errorf("Widget total: expected 500.0, got %f", total)
						}
					}
				}
			},
		},
		{
			name: "Arithmetic with discount",
			query: `[:find ?name ?price ?discount ?final
			         :where [?p :product/name ?name]
			                [?p :product/price ?price]
			                [?p :product/discount ?discount]
			                [(- 1 ?discount) ?multiplier]
			                [(* ?price ?multiplier) ?final]]`,
			expectedCount: 3,
			validate: func(t *testing.T, result Relation) {
				// Find Widget's final price (100 * 0.9 = 90)
				for i := 0; i < result.Size(); i++ {
					tuple := result.Get(i)
					if tuple[0].(string) == "Widget" {
						final := tuple[3].(float64)
						if final != 90.0 {
							t.Errorf("Widget final price: expected 90.0, got %f", final)
						}
					}
				}
			},
		},
		{
			name: "String concatenation",
			query: `[:find ?name ?price ?label
			         :where [?p :product/name ?name]
			                [?p :product/price ?price]
			                [(str ?name " - $" ?price) ?label]]`,
			expectedCount: 3,
			validate: func(t *testing.T, result Relation) {
				// Check string concatenation worked
				for i := 0; i < result.Size(); i++ {
					tuple := result.Get(i)
					label := tuple[2].(string)
					if label == "" {
						t.Error("Expected non-empty label")
					}
				}
			},
		},
		{
			name: "Division expression",
			query: `[:find ?name ?price ?qty ?unit_price
			         :where [?p :product/name ?name]
			                [?p :product/price ?price]
			                [?p :product/quantity ?qty]
			                [(> ?qty 0)]
			                [(/ ?price ?qty) ?unit_price]]`,
			expectedCount: 3,
			validate: func(t *testing.T, result Relation) {
				// Verify division works
				for i := 0; i < result.Size(); i++ {
					tuple := result.Get(i)
					price := tuple[1].(float64)
					qty := float64(tuple[2].(int64))
					unitPrice := tuple[3].(float64)
					expected := price / qty
					if unitPrice != expected {
						t.Errorf("Unit price mismatch: expected %f, got %f", expected, unitPrice)
					}
				}
			},
		},
		{
			name: "Nested expressions",
			query: `[:find ?name ?price ?qty ?total_with_tax
			         :where [?p :product/name ?name]
			                [?p :product/price ?price]
			                [?p :product/quantity ?qty]
			                [(* ?price ?qty) ?subtotal]
			                [(* ?subtotal 1.08) ?total_with_tax]]`,
			expectedCount: 3,
			validate: func(t *testing.T, result Relation) {
				// Check nested calculation
				for i := 0; i < result.Size(); i++ {
					tuple := result.Get(i)
					if tuple[0].(string) == "Widget" {
						total := tuple[3].(float64)
						expected := 100.0 * 5.0 * 1.08
						if total != expected {
							t.Errorf("Widget total with tax: expected %f, got %f", expected, total)
						}
					}
				}
			},
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

			if result.Size() != tt.expectedCount {
				t.Errorf("expected %d results, got %d", tt.expectedCount, result.Size())
			}

			if tt.validate != nil {
				tt.validate(t, result)
			}
		})
	}
}

func TestTimeExtractionQueries(t *testing.T) {
	// Create test data with time values
	event1 := datalog.NewIdentity("event:1")
	event2 := datalog.NewIdentity("event:2")
	event3 := datalog.NewIdentity("event:3")

	nameAttr := datalog.NewKeyword(":event/name")
	timeAttr := datalog.NewKeyword(":event/time")

	time1 := time.Date(2023, 6, 15, 14, 30, 0, 0, time.UTC)
	time2 := time.Date(2023, 6, 20, 9, 15, 0, 0, time.UTC)
	time3 := time.Date(2024, 1, 10, 16, 45, 0, 0, time.UTC)

	datoms := []datalog.Datom{
		{E: event1, A: nameAttr, V: "Meeting", Tx: 1},
		{E: event1, A: timeAttr, V: time1, Tx: 1},

		{E: event2, A: nameAttr, V: "Conference", Tx: 1},
		{E: event2, A: timeAttr, V: time2, Tx: 1},

		{E: event3, A: nameAttr, V: "Workshop", Tx: 1},
		{E: event3, A: timeAttr, V: time3, Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	tests := []struct {
		name          string
		query         string
		expectedCount int
		validate      func(*testing.T, Relation)
	}{
		{
			name: "Extract year",
			query: `[:find ?name ?time ?year
			         :where [?e :event/name ?name]
			                [?e :event/time ?time]
			                [(year ?time) ?year]]`,
			expectedCount: 3,
			validate: func(t *testing.T, result Relation) {
				for i := 0; i < result.Size(); i++ {
					tuple := result.Get(i)
					if tuple[0].(string) == "Workshop" {
						year := tuple[2].(int64)
						if year != 2024 {
							t.Errorf("Workshop year: expected 2024, got %d", year)
						}
					}
				}
			},
		},
		{
			name: "Extract month",
			query: `[:find ?name ?time ?month
			         :where [?e :event/name ?name]
			                [?e :event/time ?time]
			                [(month ?time) ?month]]`,
			expectedCount: 3,
			validate: func(t *testing.T, result Relation) {
				for i := 0; i < result.Size(); i++ {
					tuple := result.Get(i)
					if tuple[0].(string) == "Meeting" {
						month := tuple[2].(int64)
						if month != 6 {
							t.Errorf("Meeting month: expected 6, got %d", month)
						}
					}
				}
			},
		},
		{
			name: "Extract day",
			query: `[:find ?name ?time ?day
			         :where [?e :event/name ?name]
			                [?e :event/time ?time]
			                [(day ?time) ?day]]`,
			expectedCount: 3,
			validate: func(t *testing.T, result Relation) {
				for i := 0; i < result.Size(); i++ {
					tuple := result.Get(i)
					if tuple[0].(string) == "Conference" {
						day := tuple[2].(int64)
						if day != 20 {
							t.Errorf("Conference day: expected 20, got %d", day)
						}
					}
				}
			},
		},
		{
			name: "Extract hour",
			query: `[:find ?name ?time ?hour
			         :where [?e :event/name ?name]
			                [?e :event/time ?time]
			                [(hour ?time) ?hour]]`,
			expectedCount: 3,
			validate: func(t *testing.T, result Relation) {
				for i := 0; i < result.Size(); i++ {
					tuple := result.Get(i)
					if tuple[0].(string) == "Workshop" {
						hour := tuple[2].(int64)
						if hour != 16 {
							t.Errorf("Workshop hour: expected 16, got %d", hour)
						}
					}
				}
			},
		},
		{
			name: "Filter by extracted time component",
			query: `[:find ?name ?time
			         :where [?e :event/name ?name]
			                [?e :event/time ?time]
			                [(month ?time) ?m]
			                [(= ?m 6)]]`,
			expectedCount: 2, // Only June events
			validate: func(t *testing.T, result Relation) {
				// Should only have Meeting and Conference (both in June)
				names := make(map[string]bool)
				for i := 0; i < result.Size(); i++ {
					tuple := result.Get(i)
					names[tuple[0].(string)] = true
				}
				if !names["Meeting"] || !names["Conference"] {
					t.Error("Expected Meeting and Conference in results")
				}
				if names["Workshop"] {
					t.Error("Workshop should not be in results (January)")
				}
			},
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

			if result.Size() != tt.expectedCount {
				t.Errorf("expected %d results, got %d", tt.expectedCount, result.Size())
			}

			if tt.validate != nil {
				tt.validate(t, result)
			}
		})
	}
}

func TestComparisonBindingExpression(t *testing.T) {
	// Create test data
	item1 := datalog.NewIdentity("item:1")
	item2 := datalog.NewIdentity("item:2")
	item3 := datalog.NewIdentity("item:3")
	countAttr := datalog.NewKeyword(":item/count")

	datoms := []datalog.Datom{
		{E: item1, A: countAttr, V: int64(10), Tx: 1}, // > 5, should be true
		{E: item2, A: countAttr, V: int64(3), Tx: 1},  // < 5, should be false
		{E: item3, A: countAttr, V: int64(5), Tx: 1},  // = 5, should be false (not > 5)
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	tests := []struct {
		name     string
		query    string
		validate func(*testing.T, Relation)
	}{
		{
			name: "greater than with binding",
			query: `[:find ?e ?highCount
			         :where [?e :item/count ?count]
			                [(> ?count 5) ?highCount]]`,
			validate: func(t *testing.T, result Relation) {
				// Should have 3 rows, each with entity and boolean
				if result.Size() != 3 {
					t.Errorf("Expected 3 rows, got %d", result.Size())
					return
				}

				// Verify results
				resultMap := make(map[string]bool)
				for i := 0; i < result.Size(); i++ {
					tuple := result.Get(i)
					eid := tuple[0].(datalog.Identity).String()
					flag := tuple[1].(bool)
					resultMap[eid] = flag
				}

				if resultMap["item:1"] != true {
					t.Errorf("item:1 with count 10 should have ?highCount = true, got %v", resultMap["item:1"])
				}
				if resultMap["item:2"] != false {
					t.Errorf("item:2 with count 3 should have ?highCount = false, got %v", resultMap["item:2"])
				}
				if resultMap["item:3"] != false {
					t.Errorf("item:3 with count 5 should have ?highCount = false (5 is not > 5), got %v", resultMap["item:3"])
				}
			},
		},
		{
			name: "equality with binding",
			query: `[:find ?e ?exactFive
			         :where [?e :item/count ?count]
			                [(= ?count 5) ?exactFive]]`,
			validate: func(t *testing.T, result Relation) {
				if result.Size() != 3 {
					t.Errorf("Expected 3 rows, got %d", result.Size())
					return
				}

				resultMap := make(map[string]bool)
				for i := 0; i < result.Size(); i++ {
					tuple := result.Get(i)
					eid := tuple[0].(datalog.Identity).String()
					flag := tuple[1].(bool)
					resultMap[eid] = flag
				}

				if resultMap["item:1"] != false {
					t.Errorf("item:1 with count 10 should have ?exactFive = false")
				}
				if resultMap["item:2"] != false {
					t.Errorf("item:2 with count 3 should have ?exactFive = false")
				}
				if resultMap["item:3"] != true {
					t.Errorf("item:3 with count 5 should have ?exactFive = true")
				}
			},
		},
		{
			name: "not equal with binding",
			query: `[:find ?e ?notFive
			         :where [?e :item/count ?count]
			                [(!= ?count 5) ?notFive]]`,
			validate: func(t *testing.T, result Relation) {
				if result.Size() != 3 {
					t.Errorf("Expected 3 rows, got %d", result.Size())
					return
				}

				resultMap := make(map[string]bool)
				for i := 0; i < result.Size(); i++ {
					tuple := result.Get(i)
					eid := tuple[0].(datalog.Identity).String()
					flag := tuple[1].(bool)
					resultMap[eid] = flag
				}

				if resultMap["item:1"] != true {
					t.Errorf("item:1 with count 10 should have ?notFive = true")
				}
				if resultMap["item:2"] != true {
					t.Errorf("item:2 with count 3 should have ?notFive = true")
				}
				if resultMap["item:3"] != false {
					t.Errorf("item:3 with count 5 should have ?notFive = false")
				}
			},
		},
		{
			name: "less than or equal with binding",
			query: `[:find ?e ?lowOrMid
			         :where [?e :item/count ?count]
			                [(<= ?count 5) ?lowOrMid]]`,
			validate: func(t *testing.T, result Relation) {
				if result.Size() != 3 {
					t.Errorf("Expected 3 rows, got %d", result.Size())
					return
				}

				resultMap := make(map[string]bool)
				for i := 0; i < result.Size(); i++ {
					tuple := result.Get(i)
					eid := tuple[0].(datalog.Identity).String()
					flag := tuple[1].(bool)
					resultMap[eid] = flag
				}

				if resultMap["item:1"] != false {
					t.Errorf("item:1 with count 10 should have ?lowOrMid = false")
				}
				if resultMap["item:2"] != true {
					t.Errorf("item:2 with count 3 should have ?lowOrMid = true")
				}
				if resultMap["item:3"] != true {
					t.Errorf("item:3 with count 5 should have ?lowOrMid = true")
				}
			},
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

			tt.validate(t, result)
		})
	}
}

func TestComparisonPredicateStillFilters(t *testing.T) {
	// Ensure comparison predicates without binding still filter rows
	item1 := datalog.NewIdentity("item:1")
	item2 := datalog.NewIdentity("item:2")
	countAttr := datalog.NewKeyword(":item/count")

	datoms := []datalog.Datom{
		{E: item1, A: countAttr, V: int64(10), Tx: 1},
		{E: item2, A: countAttr, V: int64(3), Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Query without binding - should filter
	queryStr := `[:find ?e ?count
	              :where [?e :item/count ?count]
	                     [(> ?count 5)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	// Should only have item1 (count=10, which is > 5)
	if result.Size() != 1 {
		t.Errorf("Expected 1 row (filtered), got %d", result.Size())
	}

	if result.Size() > 0 {
		tuple := result.Get(0)
		eid := tuple[0].(datalog.Identity).String()
		if eid != "item:1" {
			t.Errorf("Expected item:1 to pass filter, got %s", eid)
		}
	}
}

func TestComparisonBindingWithOrClauseInput(t *testing.T) {
	// Test that comparison binding works when the input variable comes from an OR clause
	// This tests the scenario from the bug report where:
	//   (or [?e :item/count ?count]
	//       [(ground 0) ?count])
	//   [(> ?count 0) ?complete]
	// should correctly bind ?complete

	item1 := datalog.NewIdentity("item:1")
	item2 := datalog.NewIdentity("item:2") // Will have count from pattern
	// item3 will use ground fallback (no :item/count attribute)
	item3 := datalog.NewIdentity("item:3")
	countAttr := datalog.NewKeyword(":item/count")
	nameAttr := datalog.NewKeyword(":item/name")

	datoms := []datalog.Datom{
		{E: item1, A: nameAttr, V: "Widget", Tx: 1},
		{E: item1, A: countAttr, V: int64(10), Tx: 1}, // Has count > 0

		{E: item2, A: nameAttr, V: "Gadget", Tx: 1},
		{E: item2, A: countAttr, V: int64(0), Tx: 1}, // Has count = 0

		{E: item3, A: nameAttr, V: "Thing", Tx: 1},
		// No :item/count - will use ground fallback
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// First, verify OR clause alone works and preserves ?count
	t.Run("OR clause preserves count variable", func(t *testing.T) {
		queryStr := `[:find ?e ?name ?count
		              :where [?e :item/name ?name]
		                     (or [?e :item/count ?count]
		                         [(ground 0) ?count])]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}

		result, err := executor.Execute(q)
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}

		t.Logf("Result columns: %v", result.Columns())
		t.Logf("Result size: %d", result.Size())

		if result.Size() != 3 {
			t.Errorf("Expected 3 rows, got %d", result.Size())
		}

		// Verify ?count is in the columns
		hasCounts := false
		for _, col := range result.Columns() {
			if col == "?count" {
				hasCounts = true
				break
			}
		}
		if !hasCounts {
			t.Errorf("Expected ?count in columns, got %v", result.Columns())
		}
	})

	// Now test with comparison binding
	t.Run("OR clause with comparison binding", func(t *testing.T) {
		queryStr := `[:find ?e ?name ?count ?hasItems
		              :where [?e :item/name ?name]
		                     (or [?e :item/count ?count]
		                         [(ground 0) ?count])
		                     [(> ?count 0) ?hasItems]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}

		result, err := executor.Execute(q)
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}

		t.Logf("Result columns: %v", result.Columns())
		t.Logf("Result size: %d", result.Size())

		// Should have 3 rows
		if result.Size() != 3 {
			t.Errorf("Expected 3 rows, got %d", result.Size())
		}

		// Verify results
		resultMap := make(map[string]struct {
			count    int64
			hasItems bool
		})
		for i := 0; i < result.Size(); i++ {
			tuple := result.Get(i)
			name := tuple[1].(string)
			count := tuple[2].(int64)
			hasItems := tuple[3].(bool)
			resultMap[name] = struct {
				count    int64
				hasItems bool
			}{count, hasItems}
		}

		// Widget: count=10 > 0, hasItems=true
		if data, ok := resultMap["Widget"]; !ok {
			t.Error("Missing Widget in results")
		} else if data.count != 10 || data.hasItems != true {
			t.Errorf("Widget: expected count=10, hasItems=true, got count=%d, hasItems=%v", data.count, data.hasItems)
		}

		// Gadget: count=0, hasItems=false
		if data, ok := resultMap["Gadget"]; !ok {
			t.Error("Missing Gadget in results")
		} else if data.count != 0 || data.hasItems != false {
			t.Errorf("Gadget: expected count=0, hasItems=false, got count=%d, hasItems=%v", data.count, data.hasItems)
		}

		// Thing: count=0 (from ground fallback), hasItems=false
		if data, ok := resultMap["Thing"]; !ok {
			t.Error("Missing Thing in results")
		} else if data.count != 0 || data.hasItems != false {
			t.Errorf("Thing: expected count=0, hasItems=false, got count=%d, hasItems=%v", data.count, data.hasItems)
		}
	})
}

// TestPatternMatchWithBinding tests that pattern matching works correctly
// when a binding relation constrains one of the pattern variables.
func TestPatternMatchWithBinding(t *testing.T) {
	scenario1 := datalog.NewIdentity("scenario:1")
	task1 := datalog.NewIdentity("task:1")
	task2 := datalog.NewIdentity("task:2")

	taskAttr := datalog.NewKeyword(":scenario/task")

	datoms := []datalog.Datom{
		{E: scenario1, A: taskAttr, V: task1, Tx: 1},
		{E: scenario1, A: taskAttr, V: task2, Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	// Create a pattern [?scenario :scenario/task ?t]
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?scenario"},
			query.Constant{Value: taskAttr},
			query.Variable{Name: "?t"},
		},
	}

	// Create a binding relation with ?scenario = scenario:1
	bindingRel := NewMaterializedRelation(
		[]query.Symbol{"?scenario"},
		[]Tuple{{scenario1}},
	)

	// Match with binding
	result, err := matcher.Match(pattern, Relations{bindingRel})
	if err != nil {
		t.Fatalf("Match error: %v", err)
	}

	// Count results by iterating
	var count int
	iter := result.Iterator()
	for iter.Next() {
		t.Logf("  Row %d: %v", count, iter.Tuple())
		count++
	}
	iter.Close()

	t.Logf("Result count: %d (expected 2)", count)
	t.Logf("Result columns: %v", result.Columns())

	if count != 2 {
		t.Errorf("Expected 2 matching datoms, got %d", count)
	}
}

// TestSubqueryCountDirect tests that count aggregation works correctly when
// executed as a correlated subquery (without OR clause wrapping).
func TestSubqueryCountDirect(t *testing.T) {
	scenario1 := datalog.NewIdentity("scenario:1")
	scenario2 := datalog.NewIdentity("scenario:2")
	task1 := datalog.NewIdentity("task:1")
	task2 := datalog.NewIdentity("task:2")
	task3 := datalog.NewIdentity("task:3")

	nameAttr := datalog.NewKeyword(":scenario/name")
	taskAttr := datalog.NewKeyword(":scenario/task")

	datoms := []datalog.Datom{
		// scenario:1 has 2 tasks
		{E: scenario1, A: nameAttr, V: "Scenario One", Tx: 1},
		{E: scenario1, A: taskAttr, V: task1, Tx: 1},
		{E: scenario1, A: taskAttr, V: task2, Tx: 1},

		// scenario:2 has 1 task
		{E: scenario2, A: nameAttr, V: "Scenario Two", Tx: 1},
		{E: scenario2, A: taskAttr, V: task3, Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// First, verify basic count works without subquery
	t.Run("basic count", func(t *testing.T) {
		queryStr := `[:find ?scenario (count ?t)
		              :where [?scenario :scenario/task ?t]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}

		result, err := executor.Execute(q)
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}

		t.Logf("Result size: %d", result.Size())
		for i := 0; i < result.Size(); i++ {
			tuple := result.Get(i)
			t.Logf("Row: scenario=%v, count=%v", tuple[0], tuple[1])
		}

		// Should have 2 rows (one per scenario)
		if result.Size() != 2 {
			t.Errorf("Expected 2 rows, got %d", result.Size())
		}
	})

	// Test correlated subquery with :in parameter
	t.Run("correlated subquery count", func(t *testing.T) {
		queryStr := `[:find ?scenario ?name ?taskCount
		              :where [?scenario :scenario/name ?name]
		                     [(q [:find (count ?t)
		                          :in $ ?scenario
		                          :where [?scenario :scenario/task ?t]]
		                        $ ?scenario) [[?taskCount]]]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}

		result, err := executor.Execute(q)
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}

		t.Logf("Result size: %d", result.Size())
		for i := 0; i < result.Size(); i++ {
			tuple := result.Get(i)
			t.Logf("Row: scenario=%v, name=%v, taskCount=%v", tuple[0], tuple[1], tuple[2])
		}

		// Scenario One should have count=2
		for i := 0; i < result.Size(); i++ {
			tuple := result.Get(i)
			name := tuple[1].(string)
			count := tuple[2].(int64)
			if name == "Scenario One" && count != 2 {
				t.Errorf("Scenario One: expected taskCount=2, got %d", count)
			}
			if name == "Scenario Two" && count != 1 {
				t.Errorf("Scenario Two: expected taskCount=1, got %d", count)
			}
		}
	})
}

func TestComparisonBindingWithOrClauseSubquery(t *testing.T) {
	// Test that comparison binding works when the input variable comes from a SUBQUERY inside an OR clause
	// This tests the scenario:
	//   (or [(q [:find (count ?t) :where [?scenario :scenario/task ?t]] $ ?scenario) [[?taskCount]]]
	//       [(ground 0) ?taskCount])
	//   [(> ?taskCount 0) ?complete]

	scenario1 := datalog.NewIdentity("scenario:1")
	scenario2 := datalog.NewIdentity("scenario:2")
	scenario3 := datalog.NewIdentity("scenario:3")
	task1 := datalog.NewIdentity("task:1")
	task2 := datalog.NewIdentity("task:2")
	task3 := datalog.NewIdentity("task:3")

	nameAttr := datalog.NewKeyword(":scenario/name")
	taskAttr := datalog.NewKeyword(":scenario/task")

	datoms := []datalog.Datom{
		// scenario:1 has 2 tasks
		{E: scenario1, A: nameAttr, V: "Scenario One", Tx: 1},
		{E: scenario1, A: taskAttr, V: task1, Tx: 1},
		{E: scenario1, A: taskAttr, V: task2, Tx: 1},

		// scenario:2 has 1 task
		{E: scenario2, A: nameAttr, V: "Scenario Two", Tx: 1},
		{E: scenario2, A: taskAttr, V: task3, Tx: 1},

		// scenario:3 has no tasks (will use ground fallback)
		{E: scenario3, A: nameAttr, V: "Scenario Three", Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Query with subquery inside OR clause, followed by comparison binding
	queryStr := `[:find ?scenario ?name ?taskCount ?complete
	              :where [?scenario :scenario/name ?name]
	                     (or [(q [:find (count ?t)
	                              :in $ ?scenario
	                              :where [?scenario :scenario/task ?t]]
	                            $ ?scenario) [[?taskCount]]]
	                         [(ground 0) ?taskCount])
	                     [(> ?taskCount 0) ?complete]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	t.Logf("Result columns: %v", result.Columns())
	t.Logf("Result size: %d", result.Size())

	// Should have 3 rows
	if result.Size() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.Size())
	}

	// Verify results
	resultMap := make(map[string]struct {
		taskCount int64
		complete  bool
	})
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		name := tuple[1].(string)
		taskCount := tuple[2].(int64)
		complete := tuple[3].(bool)
		resultMap[name] = struct {
			taskCount int64
			complete  bool
		}{taskCount, complete}
		t.Logf("Row: name=%s, taskCount=%d, complete=%v", name, taskCount, complete)
	}

	// Scenario One: taskCount=2 > 0, complete=true
	if data, ok := resultMap["Scenario One"]; !ok {
		t.Error("Missing Scenario One in results")
	} else if data.taskCount != 2 || data.complete != true {
		t.Errorf("Scenario One: expected taskCount=2, complete=true, got taskCount=%d, complete=%v", data.taskCount, data.complete)
	}

	// Scenario Two: taskCount=1 > 0, complete=true
	if data, ok := resultMap["Scenario Two"]; !ok {
		t.Error("Missing Scenario Two in results")
	} else if data.taskCount != 1 || data.complete != true {
		t.Errorf("Scenario Two: expected taskCount=1, complete=true, got taskCount=%d, complete=%v", data.taskCount, data.complete)
	}

	// Scenario Three: taskCount=0 (from ground fallback), complete=false
	if data, ok := resultMap["Scenario Three"]; !ok {
		t.Error("Missing Scenario Three in results")
	} else if data.taskCount != 0 || data.complete != false {
		t.Errorf("Scenario Three: expected taskCount=0, complete=false, got taskCount=%d, complete=%v", data.taskCount, data.complete)
	}
}

// TestOrClausePlannerDebug verifies the planner correctly assigns OR clauses to phases
// and that the executor properly handles them.
func TestOrClausePlannerDebug(t *testing.T) {
	item1 := datalog.NewIdentity("item:1")
	item2 := datalog.NewIdentity("item:2")
	countAttr := datalog.NewKeyword(":item/count")
	nameAttr := datalog.NewKeyword(":item/name")

	datoms := []datalog.Datom{
		{E: item1, A: nameAttr, V: "Widget", Tx: 1},
		{E: item1, A: countAttr, V: int64(10), Tx: 1},
		{E: item2, A: nameAttr, V: "Gadget", Tx: 1},
		// item2 has no count - will use ground fallback
	}

	matcher := NewMemoryPatternMatcher(datoms)

	queryStr := `[:find ?e ?name ?count
	              :where [?e :item/name ?name]
	                     (or [?e :item/count ?count]
	                         [(ground 0) ?count])]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	// Debug: check what the planner produces
	p := planner.NewPlanner(nil, planner.PlannerOptions{})
	plan, err := p.Plan(q)
	if err != nil {
		t.Fatalf("Plan error: %v", err)
	}

	t.Logf("Plan has %d phases", len(plan.Phases))
	for i, phase := range plan.Phases {
		t.Logf("Phase %d:", i+1)
		t.Logf("  Patterns: %d", len(phase.Patterns))
		for j, pat := range phase.Patterns {
			t.Logf("    Pattern %d: %v", j+1, pat.Pattern)
		}
		t.Logf("  OrClauses: %d", len(phase.OrClauses))
		for j, o := range phase.OrClauses {
			t.Logf("    OrClause %d: %d branches", j+1, len(o.Branches))
		}
		t.Logf("  Expressions: %d", len(phase.Expressions))
		t.Logf("  Provides: %v", phase.Provides)
		t.Logf("  Keep: %v", phase.Keep)
	}

	// Capture annotations for debugging
	var events []annotations.Event
	handler := func(event annotations.Event) {
		events = append(events, event)
		// Log all events for debugging
		t.Logf("Event: %s - %v", event.Name, event.Data)
	}

	// Create executor and context with annotation support
	annotatedMatcher := WrapMatcher(matcher, handler)
	executor := NewExecutor(annotatedMatcher)
	ctx := NewContext(handler) // Context with handler for full annotation support
	result, err := executor.ExecuteWithContext(ctx, q)
	if err != nil {
		t.Logf("Captured %d events before error", len(events))
		t.Fatalf("Execute error: %v", err)
	}

	t.Logf("Result columns: %v", result.Columns())
	t.Logf("Result size: %d", result.Size())

	// Should have 2 rows (item1 and item2)
	if result.Size() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.Size())
	}
}
