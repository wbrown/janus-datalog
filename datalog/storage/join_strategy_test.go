package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// =============================================================================
// TESTS - Strategy Verification
// =============================================================================

// TestVerifyStrategyUsed confirms which strategy is actually selected
// and that ForceJoinStrategy() override works correctly
func TestVerifyStrategyUsed(t *testing.T) {
	queryStr := `[:find ?name ?email
	           :where [?p :person/name ?name]
	                  [?p :person/email ?email]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Insert 1000 records
			batchSize := 50
			totalRecords := 1000
			for batch := 0; batch < totalRecords; batch += batchSize {
				tx := db.NewTransaction()
				end := batch + batchSize
				if end > totalRecords {
					end = totalRecords
				}

				for i := batch; i < end; i++ {
					person := datalog.NewIdentity(fmt.Sprintf("person%d", i))
					tx.Add(person, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
					tx.Add(person, datalog.NewKeyword(":person/email"), fmt.Sprintf("email%d@example.com", i))
				}

				_, err := tx.Commit()
				if err != nil {
					t.Fatalf("Batch commit failed: %v", err)
				}
			}

			// Attribute-fetch fusion is off, so the email pattern runs as the
			// join whose strategy each subtest reads.
			planOpts := mode.plannerOptions()
			planOpts.EnableAttributeFetchFusion = false

			t.Run("default_behavior", func(t *testing.T) {
				var strategies []string
				opts := executor.ExecutorOptions{
					EnableStreamingJoins:    true,
					EnableSymmetricHashJoin: false,
					DefaultHashTableSize:    256,
					Handler: func(event annotations.Event) {
						if event.Name == "storage/join-strategy" {
							strategy := fmt.Sprintf("%v", event.Data["join_strategy"])
							strategies = append(strategies, strategy)
						}
					},
				}
				matcher := NewPatternMatcherWithOptions(db.Store(), opts)

				exec := executor.NewExecutorWithOptions(matcher, db, planOpts)

				result, err := exec.Execute(q)
				if err != nil {
					t.Fatalf("Execute failed: %v", err)
				}

				// Consume only 10 results
				count := 0
				it := result.Iterator()
				for count < 10 && it.Next() {
					_ = it.Tuple()
					count++
				}
				it.Close()

				t.Logf("Default strategies used: %v", strategies)
				t.Logf("Got %d results", count)
			})

			t.Run("forced_hash_join", func(t *testing.T) {
				var strategies []string
				opts := executor.ExecutorOptions{
					EnableStreamingJoins:    true,
					EnableSymmetricHashJoin: false,
					DefaultHashTableSize:    256,
					Handler: func(event annotations.Event) {
						if event.Name == "storage/join-strategy" {
							strategy := fmt.Sprintf("%v", event.Data["join_strategy"])
							strategies = append(strategies, strategy)
						}
					},
				}
				matcher := NewPatternMatcherWithOptions(db.Store(), opts)

				// Force HashJoinScan
				hashJoin := HashJoinScan
				matcher.ForceJoinStrategy(&hashJoin)

				exec := executor.NewExecutorWithOptions(matcher, db, planOpts)

				result, err := exec.Execute(q)
				if err != nil {
					t.Fatalf("Execute failed: %v", err)
				}

				// Consume only 10 results
				count := 0
				it := result.Iterator()
				for count < 10 && it.Next() {
					_ = it.Tuple()
					count++
				}
				it.Close()

				t.Logf("Forced strategies used: %v", strategies)
				t.Logf("Got %d results", count)

				// Verify HashJoinScan was used
				if len(strategies) == 0 {
					t.Error("Expected at least one strategy selection event")
				}
				for _, s := range strategies {
					if s != "hash-join-scan" {
						t.Errorf("Expected hash-join-scan, got %s", s)
					}
				}
			})
		})
	}
}

// TestMaterializationDetection adds instrumentation to detect when
// materialization happens in the storage layer
func TestMaterializationDetection(t *testing.T) {
	// Use a 3-way join to trigger multiple strategy selections
	queryText := `[:find ?name ?email ?age
	           :where [?p :person/name ?name]
	                  [?p :person/email ?email]
	                  [?p :person/age ?age]]`

	q, err := parser.ParseQuery(queryText)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Insert 1000 records (to trigger hash join strategy)
			batchSize := 50
			totalRecords := 1000
			for batch := 0; batch < totalRecords; batch += batchSize {
				tx := db.NewTransaction()
				end := batch + batchSize
				if end > totalRecords {
					end = totalRecords
				}

				for i := batch; i < end; i++ {
					person := datalog.NewIdentity(fmt.Sprintf("person%d", i))
					require.NoError(t, tx.Add(person, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i)))
					require.NoError(t, tx.Add(person, datalog.NewKeyword(":person/email"), fmt.Sprintf("email%d@example.com", i)))
				}

				_, err := tx.Commit()
				if err != nil {
					t.Fatalf("Batch commit failed: %v", err)
				}
			}

			// Add age attribute to trigger 3-way join
			for i := 0; i < totalRecords; i++ {
				person := datalog.NewIdentity(fmt.Sprintf("person%d", i))
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(person, datalog.NewKeyword(":person/age"), int64(20+i)))
				_, err := tx.Commit()
				if err != nil {
					t.Fatalf("Failed to add age: %v", err)
				}
			}

			// Create matcher with instrumentation registered on its options
			var events []string
			opts := executor.ExecutorOptions{
				EnableStreamingJoins:    true,
				EnableSymmetricHashJoin: false,
				DefaultHashTableSize:    256,
				Handler: func(event annotations.Event) {
					if event.Name == "storage/join-strategy" {
						joinStrategy := event.Data["join_strategy"]
						bindingSize := event.Data["binding_size"]
						events = append(events, fmt.Sprintf("Join strategy: %v (binding size: %v)", joinStrategy, bindingSize))
					}
					if event.Name == "storage/reuse-strategy" {
						strategyType := event.Data["strategy_type"]
						events = append(events, fmt.Sprintf("Reuse strategy: %v", strategyType))
					}
				},
			}
			matcher := NewPatternMatcherWithOptions(db.Store(), opts)

			exec := executor.NewExecutorWithOptions(matcher, db, mode.plannerOptions())

			t.Log("Executing query...")
			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("Execute failed: %v", err)
			}

			// Log all events captured during query execution
			t.Log("Events during query execution:")
			for _, event := range events {
				t.Log("  ", event)
			}

			symbols := result.Symbols()
			ni := query.SymbolIndex(symbols, datalog.NewSymbol("?name"))
			ei := query.SymbolIndex(symbols, datalog.NewSymbol("?email"))
			ai := query.SymbolIndex(symbols, datalog.NewSymbol("?age"))
			require.True(t, ni >= 0 && ei >= 0 && ai >= 0, "the answer carries ?name, ?email and ?age; got %v", symbols)
			count := 0
			it := result.Iterator()
			for count < 10 && it.Next() {
				tuple := it.Tuple()
				var i int
				_, err := fmt.Sscanf(tuple[ni].(string), "Name%d", &i)
				require.NoError(t, err, "a name of the fixture's form, got %v", tuple[ni])
				assert.Equal(t, fmt.Sprintf("email%d@example.com", i), tuple[ei], "person %d's email", i)
				assert.Equal(t, int64(20+i), tuple[ai], "person %d's age", i)
				count++
			}
			require.NoError(t, it.Error())
			require.NoError(t, it.Close())
			assert.Equal(t, 10, count, "the join answers at least the 10 tuples read")

			t.Log("Check if HashJoinScan strategy was used (indicates materialization)")
		})
	}
}
