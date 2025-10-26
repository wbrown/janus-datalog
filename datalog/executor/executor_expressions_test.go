package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
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
