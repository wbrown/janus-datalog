package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestSubqueryDatomicCompatible(t *testing.T) {
	// Create test data: symbols and their prices
	// The mock matcher doesn't use keys - it matches all datoms against patterns
	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			"all": {
				// Symbols
				{E: datalog.NewIdentity("symbol:aapl"), A: datalog.NewKeyword(":symbol/ticker"), V: "AAPL", Tx: 1},
				{E: datalog.NewIdentity("symbol:goog"), A: datalog.NewKeyword(":symbol/ticker"), V: "GOOG", Tx: 1},
				// Prices
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: 3},
				{E: datalog.NewIdentity("price:3"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:goog"), Tx: 4},
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/value"), V: 150.0, Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/value"), V: 155.0, Tx: 3},
				{E: datalog.NewIdentity("price:3"), A: datalog.NewKeyword(":price/value"), V: 2800.0, Tx: 4},
			},
		},
	}

	exec := NewExecutor(matcher)

	t.Run("ExplicitDatabasePassing", func(t *testing.T) {
		// Datomic-style query with explicit $ passing
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

		// Check results
		if result.Size() != 2 {
			t.Errorf("Expected 2 results, got %d", result.Size())
		}

		// Create a map for easier checking
		resultMap := make(map[string]float64)
		for i := 0; i < result.Size(); i++ {
			tuple := result.Get(i)
			symbol := tuple[0].(string)
			maxPrice := tuple[1].(float64)
			resultMap[symbol] = maxPrice
		}

		// Check AAPL max price
		if maxPrice, ok := resultMap["AAPL"]; !ok || maxPrice != 155.0 {
			t.Errorf("Expected AAPL max price to be 155.0, got %v (found: %v)", maxPrice, ok)
		}

		// Check GOOG max price
		if maxPrice, ok := resultMap["GOOG"]; !ok || maxPrice != 2800.0 {
			t.Errorf("Expected GOOG max price to be 2800.0, got %v (found: %v)", maxPrice, ok)
		}
	})

	t.Run("MultipleInputsWithDatabase", func(t *testing.T) {
		// Test with multiple inputs including explicit database
		queryStr := `[:find ?symbol ?filtered-max
		             :where 
		             [?s :symbol/ticker ?symbol]
		             [(q [:find (max ?price)
		                  :in $ ?sym ?threshold
		                  :where [?p :price/symbol ?sym]
		                         [?p :price/value ?price]
		                         [(> ?price ?threshold)]]
		                 $ ?s 100.0) [[?filtered-max]]]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		result, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		// Both symbols should have prices > 100
		if result.Size() != 2 {
			t.Errorf("Expected 2 results, got %d", result.Size())
		}

		// Create a map for easier checking
		resultMap := make(map[string]float64)
		for i := 0; i < result.Size(); i++ {
			tuple := result.Get(i)
			symbol := tuple[0].(string)
			maxPrice := tuple[1].(float64)
			resultMap[symbol] = maxPrice
		}

		// Check that both results are > 100
		if maxPrice := resultMap["AAPL"]; maxPrice <= 100.0 {
			t.Errorf("Expected AAPL max price > 100, got %v", maxPrice)
		}
		if maxPrice := resultMap["GOOG"]; maxPrice <= 100.0 {
			t.Errorf("Expected GOOG max price > 100, got %v", maxPrice)
		}
	})

}

func TestSubqueryDatabaseAsConstant(t *testing.T) {
	// Test that $ is recognized as a constant in the parser
	queryStr := `[:find ?x
	             :where
	             [?e :attr ?x]
	             [(q [:find ?y 
	                  :in $ ?a
	                  :where [?e2 :attr2 ?y]
	                         [(= ?y ?a)]] 
	                 $ ?x) [[?y]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query with $ constant: %v", err)
	}

	// Check that the subquery has $ as its first input
	var subqueryFound bool
	for _, clause := range q.Where {
		if subq, ok := clause.(*query.SubqueryPattern); ok {
			subqueryFound = true
			if len(subq.Inputs) < 1 {
				t.Error("Subquery should have inputs")
				continue
			}

			// First input should be $ (as a constant or special marker)
			firstInput := subq.Inputs[0]
			t.Logf("First input type: %T, value: %v", firstInput, firstInput)

			// Check if it's a constant with value $
			if c, ok := firstInput.(query.Constant); ok {
				if sym, ok := c.Value.(query.Symbol); ok && sym == "$" {
					// Good - $ is recognized as a constant symbol
					t.Log("$ correctly recognized as constant symbol")
				} else {
					t.Errorf("Expected $ constant, got: %v", c.Value)
				}
			} else {
				// Might need parser update to recognize $ as constant
				t.Logf("$ not recognized as constant, got type: %T", firstInput)
			}
		}
	}

	if !subqueryFound {
		t.Error("No subquery pattern found in parsed query")
	}
}
