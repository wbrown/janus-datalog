package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// BenchmarkJoinStrategyThreshold tests the crossover point between
// IndexNestedLoop and HashJoinScan for small binding sizes
func BenchmarkJoinStrategyThreshold(b *testing.B) {
	// Setup test database
	tempDir, err := os.MkdirTemp("", "threshold-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	db, err := NewDatabase(tempDir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Insert 1000 records
	batchSize := 50
	totalRecords := 1000
	entities := make([]datalog.Identity, totalRecords)

	for batch := 0; batch < totalRecords; batch += batchSize {
		tx := db.NewTransaction()
		end := batch + batchSize
		if end > totalRecords {
			end = totalRecords
		}

		for i := batch; i < end; i++ {
			person := datalog.NewIdentity(fmt.Sprintf("person%d", i))
			entities[i] = person
			tx.Add(person, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
			tx.Add(person, datalog.NewKeyword(":person/age"), int64(20+i))
		}

		_, err := tx.Commit()
		if err != nil {
			b.Fatalf("Batch commit failed: %v", err)
		}
	}

	// Test different binding sizes to find crossover
	bindingSizes := []int{1, 2, 3, 4, 5, 7, 10, 15, 20, 30, 50, 100}

	for _, bindingSize := range bindingSizes {
		// Create binding relation with N entities
		bindingTuples := make([]executor.Tuple, bindingSize)
		for i := 0; i < bindingSize; i++ {
			bindingTuples[i] = executor.Tuple{&entities[i]}
		}

		bindingRel := executor.NewMaterializedRelation(
			[]query.Symbol{"?e"},
			bindingTuples,
		)

		// Create pattern: [?e :person/age ?age]
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?e"},
				query.Constant{Value: datalog.NewKeyword(":person/age")},
				query.Variable{Name: "?age"},
			},
		}

		b.Run(fmt.Sprintf("size_%d", bindingSize), func(b *testing.B) {
			// Test IndexNestedLoop
			b.Run("index_nested_loop", func(b *testing.B) {
				opts := executor.ExecutorOptions{
					EnableStreamingJoins:    true,
					EnableSymmetricHashJoin: false,
					DefaultHashTableSize:    256,
				}
				matcher := NewBadgerMatcherWithOptions(db.Store(), opts)

				// Force IndexNestedLoop
				indexNested := IndexNestedLoop
				matcher.ForceJoinStrategy(&indexNested)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := matcher.Match(pattern, executor.Relations{bindingRel})
					if err != nil {
						b.Fatalf("Match failed: %v", err)
					}

					// Consume all results
					count := 0
					it := result.Iterator()
					for it.Next() {
						_ = it.Tuple()
						count++
					}
					it.Close()

					if count != bindingSize {
						b.Fatalf("Expected %d results, got %d", bindingSize, count)
					}
				}
			})

			// Test HashJoinScan
			b.Run("hash_join_scan", func(b *testing.B) {
				opts := executor.ExecutorOptions{
					EnableStreamingJoins:    true,
					EnableSymmetricHashJoin: false,
					DefaultHashTableSize:    256,
				}
				matcher := NewBadgerMatcherWithOptions(db.Store(), opts)

				// Force HashJoinScan
				hashJoin := HashJoinScan
				matcher.ForceJoinStrategy(&hashJoin)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := matcher.Match(pattern, executor.Relations{bindingRel})
					if err != nil {
						b.Fatalf("Match failed: %v", err)
					}

					// Consume all results
					count := 0
					it := result.Iterator()
					for it.Next() {
						_ = it.Tuple()
						count++
					}
					it.Close()

					if count != bindingSize {
						b.Fatalf("Expected %d results, got %d", bindingSize, count)
					}
				}
			})
		})
	}
}
