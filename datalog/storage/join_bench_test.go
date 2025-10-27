package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// setupBenchDB creates and populates a test database for benchmarking
func setupBenchDB(b *testing.B, peopleCount int) (*Database, func()) {
	tempDir, err := os.MkdirTemp("", "join-bench-*")
	if err != nil {
		b.Fatal(err)
	}

	db, err := NewDatabase(tempDir)
	if err != nil {
		os.RemoveAll(tempDir)
		b.Fatal(err)
	}

	// Populate in batches
	batchSize := 50
	for batch := 0; batch < peopleCount; batch += batchSize {
		tx := db.NewTransaction()
		end := batch + batchSize
		if end > peopleCount {
			end = peopleCount
		}

		for i := batch; i < end; i++ {
			person := datalog.NewIdentity(fmt.Sprintf("person%d", i))
			tx.Add(person, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
			tx.Add(person, datalog.NewKeyword(":person/email"), fmt.Sprintf("email%d@example.com", i))
		}

		_, err := tx.Commit()
		if err != nil {
			db.Close()
			os.RemoveAll(tempDir)
			b.Fatalf("Batch commit failed: %v", err)
		}
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(tempDir)
	}

	return db, cleanup
}

// BenchmarkStorageBackedJoin benchmarks join performance with real storage
func BenchmarkStorageBackedJoin(b *testing.B) {
	sizes := []int{100, 1000}

	for _, size := range sizes {
		// Setup database ONCE per size
		db, cleanup := setupBenchDB(b, size)

		query := `[:find ?name ?email
		           :where [?p :person/name ?name]
		                  [?p :person/email ?email]]`

		q, err := parser.ParseQuery(query)
		if err != nil {
			cleanup()
			b.Fatalf("Parse failed: %v", err)
		}

		// Benchmark asymmetric
		b.Run(fmt.Sprintf("asymmetric/size_%d", size), func(b *testing.B) {
			b.ReportAllocs()

			// Setup ONCE outside b.N
			opts := executor.ExecutorOptions{
				EnableStreamingJoins:    true,
				EnableSymmetricHashJoin: false,
				DefaultHashTableSize:    256,
			}
			matcher := NewBadgerMatcherWithOptions(db.Store(), opts)
			exec := executor.NewExecutor(matcher)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(q)
				if err != nil {
					b.Fatalf("Execute failed: %v", err)
				}

				// Consume all results
				count := 0
				it := result.Iterator()
				for it.Next() {
					_ = it.Tuple()
					count++
				}
				it.Close()

				if count != size {
					b.Fatalf("Expected %d results, got %d", size, count)
				}
			}
		})

		// Benchmark symmetric
		b.Run(fmt.Sprintf("symmetric/size_%d", size), func(b *testing.B) {
			b.ReportAllocs()

			// Setup ONCE outside b.N
			opts := executor.ExecutorOptions{
				EnableStreamingJoins:    true,
				EnableSymmetricHashJoin: true,
				DefaultHashTableSize:    256,
			}
			matcher := NewBadgerMatcherWithOptions(db.Store(), opts)
			exec := executor.NewExecutor(matcher)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(q)
				if err != nil {
					b.Fatalf("Execute failed: %v", err)
				}

				// Consume all results
				count := 0
				it := result.Iterator()
				for it.Next() {
					_ = it.Tuple()
					count++
				}
				it.Close()

				if count != size {
					b.Fatalf("Expected %d results, got %d", size, count)
				}
			}
		})

		cleanup()
	}
}

// BenchmarkStorageBackedJoinLimit benchmarks early termination with LIMIT
func BenchmarkStorageBackedJoinLimit(b *testing.B) {
	db, cleanup := setupBenchDB(b, 1000)
	defer cleanup()

	query := `[:find ?name ?email
	           :where [?p :person/name ?name]
	                  [?p :person/email ?email]]`

	q, err := parser.ParseQuery(query)
	if err != nil {
		b.Fatalf("Parse failed: %v", err)
	}

	limits := []int{10, 100}

	for _, limit := range limits {
		// Benchmark asymmetric
		b.Run(fmt.Sprintf("asymmetric/limit_%d", limit), func(b *testing.B) {
			b.ReportAllocs()

			// Setup ONCE outside b.N
			opts := executor.ExecutorOptions{
				EnableStreamingJoins:    true,
				EnableSymmetricHashJoin: false,
				DefaultHashTableSize:    256,
			}
			matcher := NewBadgerMatcherWithOptions(db.Store(), opts)
			exec := executor.NewExecutor(matcher)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(q)
				if err != nil {
					b.Fatalf("Execute failed: %v", err)
				}

				// Consume LIMIT results
				count := 0
				it := result.Iterator()
				for count < limit && it.Next() {
					_ = it.Tuple()
					count++
				}
				it.Close()

				if count != limit {
					b.Fatalf("Expected %d results, got %d", limit, count)
				}
			}
		})

		// Benchmark symmetric
		b.Run(fmt.Sprintf("symmetric/limit_%d", limit), func(b *testing.B) {
			b.ReportAllocs()

			// Setup ONCE outside b.N
			opts := executor.ExecutorOptions{
				EnableStreamingJoins:    true,
				EnableSymmetricHashJoin: true,
				DefaultHashTableSize:    256,
			}
			matcher := NewBadgerMatcherWithOptions(db.Store(), opts)
			exec := executor.NewExecutor(matcher)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(q)
				if err != nil {
					b.Fatalf("Execute failed: %v", err)
				}

				// Consume LIMIT results
				count := 0
				it := result.Iterator()
				for count < limit && it.Next() {
					_ = it.Tuple()
					count++
				}
				it.Close()

				if count != limit {
					b.Fatalf("Expected %d results, got %d", limit, count)
				}
			}
		})
	}
}

// BenchmarkStorageBackedJoinWithFilter benchmarks joins with predicates
func BenchmarkStorageBackedJoinWithFilter(b *testing.B) {
	db, cleanup := setupBenchDB(b, 1000)
	defer cleanup()

	query := `[:find ?name
	           :where [?p :person/name ?name]
	                  [?p :person/email ?email]
	                  [(> ?name "Name5")]]`

	q, err := parser.ParseQuery(query)
	if err != nil {
		b.Fatalf("Parse failed: %v", err)
	}

	// Benchmark asymmetric
	b.Run("asymmetric", func(b *testing.B) {
		b.ReportAllocs()

		// Setup ONCE outside b.N
		opts := executor.ExecutorOptions{
			EnableStreamingJoins:    true,
			EnableSymmetricHashJoin: false,
			DefaultHashTableSize:    256,
		}
		matcher := NewBadgerMatcherWithOptions(db.Store(), opts)
		exec := executor.NewExecutor(matcher)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Execute failed: %v", err)
			}

			// Consume all results
			count := 0
			it := result.Iterator()
			for it.Next() {
				_ = it.Tuple()
				count++
			}
			it.Close()
		}
	})

	// Benchmark symmetric
	b.Run("symmetric", func(b *testing.B) {
		b.ReportAllocs()

		// Setup ONCE outside b.N
		opts := executor.ExecutorOptions{
			EnableStreamingJoins:    true,
			EnableSymmetricHashJoin: true,
			DefaultHashTableSize:    256,
		}
		matcher := NewBadgerMatcherWithOptions(db.Store(), opts)
		exec := executor.NewExecutor(matcher)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Execute failed: %v", err)
			}

			// Consume all results
			count := 0
			it := result.Iterator()
			for it.Next() {
				_ = it.Tuple()
				count++
			}
			it.Close()
		}
	})
}
