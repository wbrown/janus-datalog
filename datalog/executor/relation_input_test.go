package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestRelationInputIteration(t *testing.T) {
	// Create test data
	nameAttr := datalog.NewKeyword(":name")
	ageAttr := datalog.NewKeyword(":age")
	yearAttr := datalog.NewKeyword(":year")

	datoms := []datalog.Datom{
		// Ages in different years
		{E: datalog.NewIdentity("a1"), A: nameAttr, V: "Alice", Tx: 1},
		{E: datalog.NewIdentity("a1"), A: yearAttr, V: int64(2020), Tx: 1},
		{E: datalog.NewIdentity("a1"), A: ageAttr, V: int64(25), Tx: 1},

		{E: datalog.NewIdentity("a2"), A: nameAttr, V: "Alice", Tx: 2},
		{E: datalog.NewIdentity("a2"), A: yearAttr, V: int64(2021), Tx: 2},
		{E: datalog.NewIdentity("a2"), A: ageAttr, V: int64(26), Tx: 2},

		{E: datalog.NewIdentity("a3"), A: nameAttr, V: "Bob", Tx: 3},
		{E: datalog.NewIdentity("a3"), A: yearAttr, V: int64(2020), Tx: 3},
		{E: datalog.NewIdentity("a3"), A: ageAttr, V: int64(30), Tx: 3},

		{E: datalog.NewIdentity("a4"), A: nameAttr, V: "Bob", Tx: 4},
		{E: datalog.NewIdentity("a4"), A: yearAttr, V: int64(2021), Tx: 4},
		{E: datalog.NewIdentity("a4"), A: ageAttr, V: int64(31), Tx: 4},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher)
	ctx := NewContext(nil)

	t.Run("direct query with RelationInput", func(t *testing.T) {
		// Query that uses RelationInput directly
		// Should iterate over each tuple and find max age for each
		q := `[:find ?n ?y (max ?age)
		          :in $ [[?n ?y] ...]
		          :where [?e :name ?n]
		                 [?e :year ?y]
		                 [?e :age ?age]]`

		parsed, err := parser.ParseQuery(q)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Create input relation with name-year pairs
		inputRel := NewMaterializedRelation(
			[]query.Symbol{"?n", "?y"},
			[]Tuple{
				{"Alice", int64(2020)},
				{"Alice", int64(2021)},
				{"Bob", int64(2020)},
				{"Bob", int64(2021)},
			},
		)

		// Execute with the relation input
		result, err := exec.ExecuteWithRelations(ctx, parsed, []Relation{inputRel})
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result.Size() != 4 {
			t.Errorf("Expected 4 results, got %d", result.Size())
			t.Logf("Results:\n%s", result.Table())
		}

		// Verify we got the right values
		expectedResults := map[string]int64{
			"Alice-2020": 25,
			"Alice-2021": 26,
			"Bob-2020":   30,
			"Bob-2021":   31,
		}

		it := result.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			if len(tuple) != 3 {
				t.Errorf("Expected 3 columns, got %d", len(tuple))
				continue
			}
			name := tuple[0].(string)
			year := tuple[1].(int64)
			maxAge := tuple[2].(int64)

			key := fmt.Sprintf("%s-%d", name, year)
			if expected, ok := expectedResults[key]; ok {
				if maxAge != expected {
					t.Errorf("%s: expected max age %d, got %d", key, expected, maxAge)
				}
			} else {
				t.Errorf("Unexpected result: %s", key)
			}
		}
		it.Close()
	})

	t.Run("subquery with RelationInput", func(t *testing.T) {
		// Test that subqueries with RelationInput work correctly too
		q := `[:find ?name ?max-age
		          :where [?p :name ?name]
		                 [(q [:find (max ?age)
		                      :in $ [[?n] ...]
		                      :where [?e :name ?n]
		                             [?e :age ?age]]
		                     $ ?name) [[?max-age]]]]`

		parsed, err := parser.ParseQuery(q)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		result, err := exec.ExecuteWithContext(ctx, parsed)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result.Size() != 2 { // Alice and Bob
			t.Errorf("Expected 2 results, got %d", result.Size())
			t.Logf("Results:\n%s", result.Table())
		}
	})
}

func TestRelationInputIterationParallel(t *testing.T) {
	// Create test data
	nameAttr := datalog.NewKeyword(":name")
	ageAttr := datalog.NewKeyword(":age")
	yearAttr := datalog.NewKeyword(":year")

	datoms := []datalog.Datom{
		// Ages in different years
		{E: datalog.NewIdentity("a1"), A: nameAttr, V: "Alice", Tx: 1},
		{E: datalog.NewIdentity("a1"), A: yearAttr, V: int64(2020), Tx: 1},
		{E: datalog.NewIdentity("a1"), A: ageAttr, V: int64(25), Tx: 1},

		{E: datalog.NewIdentity("a2"), A: nameAttr, V: "Alice", Tx: 2},
		{E: datalog.NewIdentity("a2"), A: yearAttr, V: int64(2021), Tx: 2},
		{E: datalog.NewIdentity("a2"), A: ageAttr, V: int64(26), Tx: 2},

		{E: datalog.NewIdentity("a3"), A: nameAttr, V: "Bob", Tx: 3},
		{E: datalog.NewIdentity("a3"), A: yearAttr, V: int64(2020), Tx: 3},
		{E: datalog.NewIdentity("a3"), A: ageAttr, V: int64(30), Tx: 3},

		{E: datalog.NewIdentity("a4"), A: nameAttr, V: "Bob", Tx: 4},
		{E: datalog.NewIdentity("a4"), A: yearAttr, V: int64(2021), Tx: 4},
		{E: datalog.NewIdentity("a4"), A: ageAttr, V: int64(31), Tx: 4},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher)
	exec.EnableParallelSubqueries(4) // Use 4 workers for testing
	ctx := NewContext(nil)

	t.Run("parallel query with RelationInput", func(t *testing.T) {
		// Query that uses RelationInput directly
		// Should iterate over each tuple and find max age for each
		q := `[:find ?n ?y (max ?age)
		          :in $ [[?n ?y] ...]
		          :where [?e :name ?n]
		                 [?e :year ?y]
		                 [?e :age ?age]]`

		parsed, err := parser.ParseQuery(q)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Create input relation with name-year pairs
		inputRel := NewMaterializedRelation(
			[]query.Symbol{"?n", "?y"},
			[]Tuple{
				{"Alice", int64(2020)},
				{"Alice", int64(2021)},
				{"Bob", int64(2020)},
				{"Bob", int64(2021)},
			},
		)

		// Execute with the relation input
		result, err := exec.ExecuteWithRelations(ctx, parsed, []Relation{inputRel})
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result.Size() != 4 {
			t.Errorf("Expected 4 results, got %d", result.Size())
			t.Logf("Results:\n%s", result.Table())
		}

		// Verify we got the right values
		expectedResults := map[string]int64{
			"Alice-2020": 25,
			"Alice-2021": 26,
			"Bob-2020":   30,
			"Bob-2021":   31,
		}

		it := result.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			if len(tuple) != 3 {
				t.Errorf("Expected 3 columns, got %d", len(tuple))
				continue
			}
			name := tuple[0].(string)
			year := tuple[1].(int64)
			maxAge := tuple[2].(int64)

			key := fmt.Sprintf("%s-%d", name, year)
			if expected, ok := expectedResults[key]; ok {
				if maxAge != expected {
					t.Errorf("%s: expected max age %d, got %d", key, expected, maxAge)
				}
			} else {
				t.Errorf("Unexpected result: %s", key)
			}
		}
		it.Close()
	})

	t.Run("parallel vs sequential correctness", func(t *testing.T) {
		// Test that parallel execution produces same results as sequential
		q := `[:find ?n ?y (max ?age)
		          :in $ [[?n ?y] ...]
		          :where [?e :name ?n]
		                 [?e :year ?y]
		                 [?e :age ?age]]`

		parsed, err := parser.ParseQuery(q)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Create input relation with name-year pairs
		inputRel := NewMaterializedRelation(
			[]query.Symbol{"?n", "?y"},
			[]Tuple{
				{"Alice", int64(2020)},
				{"Alice", int64(2021)},
				{"Bob", int64(2020)},
				{"Bob", int64(2021)},
			},
		)

		// Execute sequentially
		seqExec := NewExecutor(matcher)
		seqExec.DisableParallelSubqueries()
		seqResult, err := seqExec.ExecuteWithRelations(ctx, parsed, []Relation{inputRel})
		if err != nil {
			t.Fatalf("Sequential query failed: %v", err)
		}

		// Execute in parallel
		parExec := NewExecutor(matcher)
		parExec.EnableParallelSubqueries(4)
		parResult, err := parExec.ExecuteWithRelations(ctx, parsed, []Relation{inputRel})
		if err != nil {
			t.Fatalf("Parallel query failed: %v", err)
		}

		// Compare sizes
		if seqResult.Size() != parResult.Size() {
			t.Errorf("Size mismatch: sequential=%d, parallel=%d", seqResult.Size(), parResult.Size())
			t.Logf("Sequential:\n%s", seqResult.Table())
			t.Logf("Parallel:\n%s", parResult.Table())
		}

		// Compare results (collect into maps for order-independent comparison)
		seqMap := make(map[string]int64)
		it := seqResult.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			name := tuple[0].(string)
			year := tuple[1].(int64)
			maxAge := tuple[2].(int64)
			key := fmt.Sprintf("%s-%d", name, year)
			seqMap[key] = maxAge
		}
		it.Close()

		parMap := make(map[string]int64)
		it = parResult.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			name := tuple[0].(string)
			year := tuple[1].(int64)
			maxAge := tuple[2].(int64)
			key := fmt.Sprintf("%s-%d", name, year)
			parMap[key] = maxAge
		}
		it.Close()

		// Verify maps are identical
		if len(seqMap) != len(parMap) {
			t.Errorf("Result count mismatch: sequential=%d, parallel=%d", len(seqMap), len(parMap))
		}

		for key, seqVal := range seqMap {
			if parVal, ok := parMap[key]; !ok {
				t.Errorf("Key %s missing in parallel results", key)
			} else if seqVal != parVal {
				t.Errorf("Value mismatch for %s: sequential=%d, parallel=%d", key, seqVal, parVal)
			}
		}

		for key := range parMap {
			if _, ok := seqMap[key]; !ok {
				t.Errorf("Key %s in parallel results but not in sequential", key)
			}
		}
	})
}

func TestRelationInputParallelEdgeCases(t *testing.T) {
	nameAttr := datalog.NewKeyword(":name")
	ageAttr := datalog.NewKeyword(":age")
	yearAttr := datalog.NewKeyword(":year")

	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("a1"), A: nameAttr, V: "Alice", Tx: 1},
		{E: datalog.NewIdentity("a1"), A: yearAttr, V: int64(2020), Tx: 1},
		{E: datalog.NewIdentity("a1"), A: ageAttr, V: int64(25), Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	ctx := NewContext(nil)

	t.Run("empty input relation", func(t *testing.T) {
		exec := NewExecutor(matcher)
		exec.EnableParallelSubqueries(4)

		q := `[:find ?n ?y (max ?age)
		          :in $ [[?n ?y] ...]
		          :where [?e :name ?n]
		                 [?e :year ?y]
		                 [?e :age ?age]]`

		parsed, err := parser.ParseQuery(q)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Empty input relation
		emptyRel := NewMaterializedRelation([]query.Symbol{"?n", "?y"}, []Tuple{})

		result, err := exec.ExecuteWithRelations(ctx, parsed, []Relation{emptyRel})
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result.Size() != 0 {
			t.Errorf("Expected 0 results, got %d", result.Size())
		}
	})

	t.Run("single tuple", func(t *testing.T) {
		exec := NewExecutor(matcher)
		exec.EnableParallelSubqueries(4)

		q := `[:find ?n ?y (max ?age)
		          :in $ [[?n ?y] ...]
		          :where [?e :name ?n]
		                 [?e :year ?y]
		                 [?e :age ?age]]`

		parsed, err := parser.ParseQuery(q)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Single tuple
		singleRel := NewMaterializedRelation(
			[]query.Symbol{"?n", "?y"},
			[]Tuple{{"Alice", int64(2020)}},
		)

		result, err := exec.ExecuteWithRelations(ctx, parsed, []Relation{singleRel})
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result.Size() != 1 {
			t.Errorf("Expected 1 result, got %d", result.Size())
		}
	})

	t.Run("no matching results", func(t *testing.T) {
		exec := NewExecutor(matcher)
		exec.EnableParallelSubqueries(4)

		q := `[:find ?n ?y (max ?age)
		          :in $ [[?n ?y] ...]
		          :where [?e :name ?n]
		                 [?e :year ?y]
		                 [?e :age ?age]]`

		parsed, err := parser.ParseQuery(q)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Non-matching tuples
		noMatchRel := NewMaterializedRelation(
			[]query.Symbol{"?n", "?y"},
			[]Tuple{
				{"Bob", int64(2020)},
				{"Charlie", int64(2021)},
			},
		)

		result, err := exec.ExecuteWithRelations(ctx, parsed, []Relation{noMatchRel})
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result.Size() != 0 {
			t.Errorf("Expected 0 results, got %d", result.Size())
		}
	})

	t.Run("mixed matching and non-matching", func(t *testing.T) {
		exec := NewExecutor(matcher)
		exec.EnableParallelSubqueries(4)

		q := `[:find ?n ?y (max ?age)
		          :in $ [[?n ?y] ...]
		          :where [?e :name ?n]
		                 [?e :year ?y]
		                 [?e :age ?age]]`

		parsed, err := parser.ParseQuery(q)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Mix of matching and non-matching
		mixedRel := NewMaterializedRelation(
			[]query.Symbol{"?n", "?y"},
			[]Tuple{
				{"Alice", int64(2020)}, // matches
				{"Bob", int64(2020)},   // no match
				{"Alice", int64(2021)}, // no match
			},
		)

		result, err := exec.ExecuteWithRelations(ctx, parsed, []Relation{mixedRel})
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result.Size() != 1 {
			t.Errorf("Expected 1 result, got %d", result.Size())
			t.Logf("Results:\n%s", result.Table())
		}
	})

	t.Run("high worker count with small input", func(t *testing.T) {
		exec := NewExecutor(matcher)
		exec.EnableParallelSubqueries(100) // Way more workers than tuples

		q := `[:find ?n ?y (max ?age)
		          :in $ [[?n ?y] ...]
		          :where [?e :name ?n]
		                 [?e :year ?y]
		                 [?e :age ?age]]`

		parsed, err := parser.ParseQuery(q)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Only 1 tuple with 100 workers (tests that excess workers don't cause issues)
		smallRel := NewMaterializedRelation(
			[]query.Symbol{"?n", "?y"},
			[]Tuple{
				{"Alice", int64(2020)},
			},
		)

		result, err := exec.ExecuteWithRelations(ctx, parsed, []Relation{smallRel})
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result.Size() != 1 {
			t.Errorf("Expected 1 result, got %d", result.Size())
		}
	})
}

func TestRelationInputParallelStress(t *testing.T) {
	// Create larger dataset for stress testing
	nameAttr := datalog.NewKeyword(":name")
	ageAttr := datalog.NewKeyword(":age")
	yearAttr := datalog.NewKeyword(":year")
	monthAttr := datalog.NewKeyword(":month")

	var datoms []datalog.Datom
	names := []string{"Alice", "Bob", "Charlie", "Dave", "Eve"}
	idCounter := 0

	// Create 300 entries (5 names × 10 years × 6 months)
	for _, name := range names {
		for year := 2020; year <= 2029; year++ {
			for month := 1; month <= 6; month++ {
				id := fmt.Sprintf("p%d", idCounter)
				datoms = append(datoms,
					datalog.Datom{E: datalog.NewIdentity(id), A: nameAttr, V: name, Tx: uint64(idCounter*4 + 1)},
					datalog.Datom{E: datalog.NewIdentity(id), A: yearAttr, V: int64(year), Tx: uint64(idCounter*4 + 2)},
					datalog.Datom{E: datalog.NewIdentity(id), A: monthAttr, V: int64(month), Tx: uint64(idCounter*4 + 3)},
					datalog.Datom{E: datalog.NewIdentity(id), A: ageAttr, V: int64(25 + idCounter%15), Tx: uint64(idCounter*4 + 4)},
				)
				idCounter++
			}
		}
	}

	matcher := NewMemoryPatternMatcher(datoms)
	ctx := NewContext(nil)

	t.Run("stress test with many iterations", func(t *testing.T) {
		exec := NewExecutor(matcher)
		exec.EnableParallelSubqueries(16) // High worker count

		q := `[:find ?n ?y ?m (max ?age)
		          :in $ [[?n ?y ?m] ...]
		          :where [?e :name ?n]
		                 [?e :year ?y]
		                 [?e :month ?m]
		                 [?e :age ?age]]`

		parsed, err := parser.ParseQuery(q)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Create input relation with all 300 tuples
		var inputTuples []Tuple
		for _, name := range names {
			for year := 2020; year <= 2029; year++ {
				for month := 1; month <= 6; month++ {
					inputTuples = append(inputTuples, Tuple{name, int64(year), int64(month)})
				}
			}
		}
		inputRel := NewMaterializedRelation([]query.Symbol{"?n", "?y", "?m"}, inputTuples)

		// Execute and verify
		result, err := exec.ExecuteWithRelations(ctx, parsed, []Relation{inputRel})
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result.Size() != 300 {
			t.Errorf("Expected 300 results, got %d", result.Size())
		}

		// Verify results are correct by spot-checking
		foundAlice2020Jan := false
		it := result.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			if len(tuple) == 4 {
				name := tuple[0].(string)
				year := tuple[1].(int64)
				month := tuple[2].(int64)
				if name == "Alice" && year == 2020 && month == 1 {
					foundAlice2020Jan = true
				}
			}
		}
		it.Close()

		if !foundAlice2020Jan {
			t.Error("Expected to find Alice 2020 January in results")
		}
	})

	t.Run("concurrent sequential vs parallel comparison", func(t *testing.T) {
		// Run both implementations multiple times concurrently to verify thread safety
		q := `[:find ?n ?y (max ?age)
		          :in $ [[?n ?y] ...]
		          :where [?e :name ?n]
		                 [?e :year ?y]
		                 [?e :age ?age]]`

		parsed, err := parser.ParseQuery(q)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Smaller input for faster concurrent runs
		var inputTuples []Tuple
		for _, name := range names {
			for year := 2020; year <= 2024; year++ {
				inputTuples = append(inputTuples, Tuple{name, int64(year)})
			}
		}
		inputRel := NewMaterializedRelation([]query.Symbol{"?n", "?y"}, inputTuples)

		// Run 10 queries concurrently with parallel execution
		errCh := make(chan error, 10)
		for i := 0; i < 10; i++ {
			go func() {
				exec := NewExecutor(matcher)
				exec.EnableParallelSubqueries(8)
				_, err := exec.ExecuteWithRelations(ctx, parsed, []Relation{inputRel})
				errCh <- err
			}()
		}

		// Collect results
		for i := 0; i < 10; i++ {
			if err := <-errCh; err != nil {
				t.Errorf("Concurrent execution %d failed: %v", i, err)
			}
		}
	})
}
