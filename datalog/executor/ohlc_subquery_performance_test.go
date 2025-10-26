package executor

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestOHLCSubqueryPerformance reproduces the Gopher Street performance issue
// where multiple subqueries accessing the same data execute independently
func TestOHLCSubqueryPerformance(t *testing.T) {
	// Create test data: a symbol with price bars for multiple days
	datoms := []datalog.Datom{}
	symbolID := datalog.NewIdentity("symbol:TEST")

	// Add the symbol
	datoms = append(datoms, datalog.Datom{
		E:  symbolID,
		A:  datalog.NewKeyword(":symbol/ticker"),
		V:  "TEST",
		Tx: 1,
	})

	// Create price bars for 5 days, 10 bars per day
	// This simulates intraday price data
	barID := 1000
	for day := 1; day <= 5; day++ {
		// Create bars for each day
		dayTime := time.Date(2025, 1, day, 9, 30, 0, 0, time.UTC)

		for minute := 0; minute < 10; minute++ {
			currentBarID := datalog.NewIdentity(fmt.Sprintf("bar:%d", barID))
			barTime := dayTime.Add(time.Duration(minute) * time.Minute)

			// Add bar with symbol reference
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/symbol"),
				V:  symbolID,
				Tx: uint64(barID),
			})

			// Add time
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/time"),
				V:  barTime,
				Tx: uint64(barID),
			})

			// Add minute of day (570 = 9:30 AM)
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/minute-of-day"),
				V:  int64(570 + minute),
				Tx: uint64(barID),
			})

			// Add OHLC data
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/open"),
				V:  100.0 + float64(day) + float64(minute)*0.1,
				Tx: uint64(barID),
			})
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/high"),
				V:  102.0 + float64(day) + float64(minute)*0.2,
				Tx: uint64(barID),
			})
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/low"),
				V:  98.0 + float64(day) - float64(minute)*0.1,
				Tx: uint64(barID),
			})

			barID++
		}
	}

	// Create memory matcher with our test data
	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher)

	// Test 1: Single subquery (just get the open price for each day)
	t.Run("SingleSubquery", func(t *testing.T) {
		queryStr := `
		[:find ?day ?open
		 :where 
		 [?s :symbol/ticker "TEST"]
		 [?morning :price/symbol ?s]
		 [?morning :price/minute-of-day 570]
		 [?morning :price/time ?t]
		 [(day ?t) ?day]
		 [(q [:find (min ?o)
		      :in $ ?sym ?d
		      :where 
		      [?b :price/symbol ?sym]
		      [?b :price/time ?time]
		      [(day ?time) ?bd]
		      [(= ?bd ?d)]
		      [?b :price/open ?o]]
		     $ ?s ?day) [[?open]]]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		start := time.Now()
		result, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}
		duration := time.Since(start)

		t.Logf("Single subquery returned %d rows in %v", result.Size(), duration)
	})

	// Test 2: Multiple subqueries (open, high, low for each day)
	t.Run("MultipleSubqueries", func(t *testing.T) {
		queryStr := `
		[:find ?day ?open ?high ?low
		 :where 
		 [?s :symbol/ticker "TEST"]
		 [?morning :price/symbol ?s]
		 [?morning :price/minute-of-day 570]
		 [?morning :price/time ?t]
		 [(day ?t) ?day]
		 
		 [(q [:find (min ?o)
		      :in $ ?sym ?d
		      :where 
		      [?b :price/symbol ?sym]
		      [?b :price/time ?time]
		      [(day ?time) ?bd]
		      [(= ?bd ?d)]
		      [?b :price/open ?o]]
		     $ ?s ?day) [[?open]]]
		     
		 [(q [:find (max ?h)
		      :in $ ?sym ?d
		      :where 
		      [?b :price/symbol ?sym]
		      [?b :price/time ?time]
		      [(day ?time) ?bd]
		      [(= ?bd ?d)]
		      [?b :price/high ?h]]
		     $ ?s ?day) [[?high]]]
		     
		 [(q [:find (min ?l)
		      :in $ ?sym ?d
		      :where 
		      [?b :price/symbol ?sym]
		      [?b :price/time ?time]
		      [(day ?time) ?bd]
		      [(= ?bd ?d)]
		      [?b :price/low ?l]]
		     $ ?s ?day) [[?low]]]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		start := time.Now()
		result, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}
		duration := time.Since(start)

		t.Logf("Multiple subqueries returned %d rows in %v", result.Size(), duration)
		t.Logf("This demonstrates the O(n×m) pattern: %d days × 3 aggregations", result.Size())
	})
}

// BenchmarkOHLCSubqueries measures the performance difference between
// single vs multiple subqueries accessing the same data
func BenchmarkOHLCSubqueries(b *testing.B) {
	// Create larger dataset for benchmarking
	datoms := []datalog.Datom{}
	symbolID := datalog.NewIdentity("symbol:BENCH")

	// Add symbol
	datoms = append(datoms, datalog.Datom{
		E:  symbolID,
		A:  datalog.NewKeyword(":symbol/ticker"),
		V:  "BENCH",
		Tx: 1,
	})

	// Create 20 days of data, 50 bars per day = 1000 bars
	barID := 1000
	for day := 1; day <= 20; day++ {
		dayTime := time.Date(2025, 1, day, 9, 30, 0, 0, time.UTC)

		for minute := 0; minute < 50; minute++ {
			currentBarID := datalog.NewIdentity(fmt.Sprintf("bar:%d", barID))
			barTime := dayTime.Add(time.Duration(minute) * time.Minute)

			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/symbol"),
				V:  symbolID,
				Tx: uint64(barID),
			})
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/time"),
				V:  barTime,
				Tx: uint64(barID),
			})
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/minute-of-day"),
				V:  int64(570 + minute),
				Tx: uint64(barID),
			})
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/open"),
				V:  100.0 + float64(day) + float64(minute)*0.1,
				Tx: uint64(barID),
			})
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/high"),
				V:  102.0 + float64(day) + float64(minute)*0.2,
				Tx: uint64(barID),
			})
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/low"),
				V:  98.0 + float64(day) - float64(minute)*0.1,
				Tx: uint64(barID),
			})

			barID++
		}
	}

	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher)

	// Benchmark single subquery
	b.Run("SingleAggregation", func(b *testing.B) {
		queryStr := `
		[:find ?day ?open
		 :where 
		 [?s :symbol/ticker "BENCH"]
		 [?morning :price/symbol ?s]
		 [?morning :price/minute-of-day 570]
		 [?morning :price/time ?t]
		 [(day ?t) ?day]
		 [(q [:find (min ?o)
		      :in $ ?sym ?d
		      :where 
		      [?b :price/symbol ?sym]
		      [?b :price/time ?time]
		      [(day ?time) ?bd]
		      [(= ?bd ?d)]
		      [?b :price/open ?o]]
		     $ ?s ?day) [[?open]]]]`

		q, _ := parser.ParseQuery(queryStr)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			exec.Execute(q)
		}
	})

	// Benchmark three subqueries
	b.Run("ThreeAggregations", func(b *testing.B) {
		queryStr := `
		[:find ?day ?open ?high ?low
		 :where 
		 [?s :symbol/ticker "BENCH"]
		 [?morning :price/symbol ?s]
		 [?morning :price/minute-of-day 570]
		 [?morning :price/time ?t]
		 [(day ?t) ?day]
		 
		 [(q [:find (min ?o)
		      :in $ ?sym ?d
		      :where 
		      [?b :price/symbol ?sym]
		      [?b :price/time ?time]
		      [(day ?time) ?bd]
		      [(= ?bd ?d)]
		      [?b :price/open ?o]]
		     $ ?s ?day) [[?open]]]
		     
		 [(q [:find (max ?h)
		      :in $ ?sym ?d
		      :where 
		      [?b :price/symbol ?sym]
		      [?b :price/time ?time]
		      [(day ?time) ?bd]
		      [(= ?bd ?d)]
		      [?b :price/high ?h]]
		     $ ?s ?day) [[?high]]]
		     
		 [(q [:find (min ?l)
		      :in $ ?sym ?d
		      :where 
		      [?b :price/symbol ?sym]
		      [?b :price/time ?time]
		      [(day ?time) ?bd]
		      [(= ?bd ?d)]
		      [?b :price/low ?l]]
		     $ ?s ?day) [[?low]]]]`

		q, _ := parser.ParseQuery(queryStr)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			exec.Execute(q)
		}
	})
}

// BenchmarkRelationInputParallel compares sequential vs parallel execution
// for RelationInput iteration scenarios
func BenchmarkRelationInputParallel(b *testing.B) {
	// Create much larger dataset to properly stress test parallelism
	nameAttr := datalog.NewKeyword(":name")
	ageAttr := datalog.NewKeyword(":age")
	yearAttr := datalog.NewKeyword(":year")
	monthAttr := datalog.NewKeyword(":month")

	var datoms []datalog.Datom
	// 20 names × 10 years × 12 months = 2400 entries
	names := []string{"Alice", "Bob", "Charlie", "Dave", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack",
		"Kate", "Liam", "Mary", "Nick", "Olivia", "Paul", "Quinn", "Rose", "Sam", "Tina"}
	idCounter := 0

	for _, name := range names {
		for year := 2015; year <= 2024; year++ {
			for month := 1; month <= 12; month++ {
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

	// Query with RelationInput iteration
	queryStr := `[:find ?n ?y ?m (max ?age)
	              :in $ [[?n ?y ?m] ...]
	              :where [?e :name ?n]
	                     [?e :year ?y]
	                     [?e :month ?m]
	                     [?e :age ?age]]`

	q, _ := parser.ParseQuery(queryStr)

	// Create input relation (2400 tuples - simulating 40 days × 60 iterations)
	var inputTuples []Tuple
	for _, name := range names {
		for year := 2015; year <= 2024; year++ {
			for month := 1; month <= 12; month++ {
				inputTuples = append(inputTuples, Tuple{name, int64(year), int64(month)})
			}
		}
	}
	inputRel := NewMaterializedRelation([]query.Symbol{"?n", "?y", "?m"}, inputTuples)

	b.Run("Sequential", func(b *testing.B) {
		seqExec := NewExecutor(matcher)
		seqExec.DisableParallelSubqueries()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := seqExec.ExecuteWithRelations(NewContext(nil), q, []Relation{inputRel})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Parallel-2Workers", func(b *testing.B) {
		parExec := NewExecutor(matcher)
		parExec.EnableParallelSubqueries(2)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := parExec.ExecuteWithRelations(NewContext(nil), q, []Relation{inputRel})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Parallel-4Workers", func(b *testing.B) {
		parExec := NewExecutor(matcher)
		parExec.EnableParallelSubqueries(4)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := parExec.ExecuteWithRelations(NewContext(nil), q, []Relation{inputRel})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Parallel-8Workers", func(b *testing.B) {
		parExec := NewExecutor(matcher)
		parExec.EnableParallelSubqueries(8)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := parExec.ExecuteWithRelations(NewContext(nil), q, []Relation{inputRel})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Parallel-16Workers", func(b *testing.B) {
		parExec := NewExecutor(matcher)
		parExec.EnableParallelSubqueries(16)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := parExec.ExecuteWithRelations(NewContext(nil), q, []Relation{inputRel})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Parallel-32Workers", func(b *testing.B) {
		parExec := NewExecutor(matcher)
		parExec.EnableParallelSubqueries(32)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := parExec.ExecuteWithRelations(NewContext(nil), q, []Relation{inputRel})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
