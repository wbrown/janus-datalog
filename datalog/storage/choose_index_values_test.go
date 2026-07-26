package storage

import (
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestChooseIndexForValuesAVET verifies that AVET index scan ranges
// are correctly narrowed when attribute and value are bound.
// This was a bug where the AVET case was missing from chooseIndexForValues(),
// causing full index scans instead of targeted lookups.
func TestChooseIndexForValuesAVET(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode)

			// Create test data: tasks belonging to different scenarios
			scenario1 := datalog.NewIdentity("scenario-alpha")
			scenario2 := datalog.NewIdentity("scenario-beta")
			scenario3 := datalog.NewIdentity("scenario-gamma")

			tx := db.NewTransaction()
			// 10 tasks for scenario1
			for i := 0; i < 10; i++ {
				task := datalog.NewIdentity("task-alpha-" + string(rune('A'+i)))
				tx.Add(task, datalog.NewKeyword(":task/scenario"), scenario1)
				tx.Add(task, datalog.NewKeyword(":task/name"), "Alpha Task")
			}
			// 5 tasks for scenario2
			for i := 0; i < 5; i++ {
				task := datalog.NewIdentity("task-beta-" + string(rune('A'+i)))
				tx.Add(task, datalog.NewKeyword(":task/scenario"), scenario2)
				tx.Add(task, datalog.NewKeyword(":task/name"), "Beta Task")
			}
			// 3 tasks for scenario3
			for i := 0; i < 3; i++ {
				task := datalog.NewIdentity("task-gamma-" + string(rune('A'+i)))
				tx.Add(task, datalog.NewKeyword(":task/scenario"), scenario3)
				tx.Add(task, datalog.NewKeyword(":task/name"), "Gamma Task")
			}
			_, err := tx.Commit()
			if err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			matcher := db.Matcher().(*PatternMatcher)

			t.Run("AVET scan range includes value prefix", func(t *testing.T) {
				attr := datalog.NewKeyword(":task/scenario")

				// Get scan range with attribute + value bound
				bound := matcher.scanBoundForValues(AVET, nil, attr, scenario1, 0)
				start, _ := encodeScanBoundForTest(t, matcher, bound)

				// Verify scan range is more than just index + attribute
				// AVET key format: [1 index][32 attr][type + value][20 entity][20 tx]
				// With attribute only: 1 + 32 = 33 bytes
				// With attribute + identity value: 1 + 32 + 1 + 20 = 54 bytes
				expectedMinLen := 54 // index(1) + attr(32) + type(1) + hash(20)
				if len(start) < expectedMinLen {
					t.Errorf("Scan range start too short: got %d bytes, want at least %d", len(start), expectedMinLen)
				}

				// Count datoms in the calculated range
				iter, err := matcher.store.ScanKeysOnly(bound)
				if err != nil {
					t.Fatalf("Scan failed: %v", err)
				}
				count := 0
				for iter.Next() {
					count++
				}
				iter.Close()

				// Should find exactly 10 tasks for scenario1
				if count != 10 {
					t.Errorf("Expected 10 datoms in scan range, got %d", count)
				}
			})

			t.Run("AVET full attribute scan finds all scenarios", func(t *testing.T) {
				attr := datalog.NewKeyword(":task/scenario")

				// Full attribute scan (no value filter)
				iter, err := matcher.store.ScanKeysOnly(ScanBound{
					Index:  AVET,
					Prefix: []datalog.Value{attr},
				})
				if err != nil {
					t.Fatalf("Scan failed: %v", err)
				}
				count := 0
				for iter.Next() {
					count++
				}
				iter.Close()

				// Should find all 18 tasks (10 + 5 + 3)
				if count != 18 {
					t.Errorf("Expected 18 datoms in full scan, got %d", count)
				}
			})

			t.Run("query with input binding uses narrowed scan", func(t *testing.T) {
				queryStr := `[:find ?e :in $ ?scenario :where [?e :task/scenario ?scenario]]`

				// Query for scenario1 - should return 10 results
				result, err := executor.CollectTuples(db.Query(queryStr, scenario1))
				if err != nil {
					t.Fatalf("Query failed: %v", err)
				}
				if len(result) != 10 {
					t.Errorf("Expected 10 results for scenario1, got %d", len(result))
				}

				// Query for scenario2 - should return 5 results
				result, err = executor.CollectTuples(db.Query(queryStr, scenario2))
				if err != nil {
					t.Fatalf("Query failed: %v", err)
				}
				if len(result) != 5 {
					t.Errorf("Expected 5 results for scenario2, got %d", len(result))
				}

				// Query for scenario3 - should return 3 results
				result, err = executor.CollectTuples(db.Query(queryStr, scenario3))
				if err != nil {
					t.Fatalf("Query failed: %v", err)
				}
				if len(result) != 3 {
					t.Errorf("Expected 3 results for scenario3, got %d", len(result))
				}
			})

			t.Run("Match with binding relation returns correct count", func(t *testing.T) {
				pattern := &query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?e")},
						query.Constant{Value: datalog.NewKeyword(":task/scenario")},
						query.Variable{Name: datalog.NewSymbol("?scenario")},
					},
				}

				opts := getDefaultExecutorOptions()
				inputRel := executor.NewMaterializedRelationWithOptions(
					[]query.Symbol{datalog.NewSymbol("?scenario")},
					[]executor.Tuple{{scenario2}},
					opts,
				)

				result, err := matcher.Match(query.PatternQuery(pattern), executor.Relations{inputRel})
				if err != nil {
					t.Fatalf("Match failed: %v", err)
				}

				count := 0
				it := result.Iterator()
				for it.Next() {
					count++
				}
				it.Close()

				if count != 5 {
					t.Errorf("Expected 5 tuples for scenario2, got %d", count)
				}
			})
		})
	}
}

// TestChooseIndexForValuesVAET verifies that VAET index scan ranges
// are correctly narrowed when value is bound.
func TestChooseIndexForValuesVAET(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode)

			// Create test data with references
			parent1 := datalog.NewIdentity("parent-1")
			parent2 := datalog.NewIdentity("parent-2")

			tx := db.NewTransaction()
			tx.Add(parent1, datalog.NewKeyword(":entity/name"), "Parent 1")
			tx.Add(parent2, datalog.NewKeyword(":entity/name"), "Parent 2")

			// 8 children referencing parent1
			for i := 0; i < 8; i++ {
				child := datalog.NewIdentity("child-1-" + string(rune('A'+i)))
				tx.Add(child, datalog.NewKeyword(":child/parent"), parent1)
			}
			// 4 children referencing parent2
			for i := 0; i < 4; i++ {
				child := datalog.NewIdentity("child-2-" + string(rune('A'+i)))
				tx.Add(child, datalog.NewKeyword(":child/parent"), parent2)
			}
			_, err := tx.Commit()
			if err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			matcher := db.Matcher().(*PatternMatcher)

			t.Run("VAET scan range includes value prefix", func(t *testing.T) {
				attr := datalog.NewKeyword(":child/parent")

				// Get scan range with value bound (VAET uses value as first component)
				bound := matcher.scanBoundForValues(VAET, nil, attr, parent1, 0)
				start, _ := encodeScanBoundForTest(t, matcher, bound)

				// VAET key format: [1 index][type + value][32 attr][20 entity][20 tx]
				// With value: 1 + 1 + 20 = 22 bytes minimum
				// With value + attr: 1 + 1 + 20 + 32 = 54 bytes
				expectedMinLen := 22 // index(1) + type(1) + hash(20)
				if len(start) < expectedMinLen {
					t.Errorf("Scan range start too short: got %d bytes, want at least %d", len(start), expectedMinLen)
				}

				// Count datoms in the calculated range
				iter, err := matcher.store.ScanKeysOnly(bound)
				if err != nil {
					t.Fatalf("Scan failed: %v", err)
				}
				count := 0
				for iter.Next() {
					count++
				}
				iter.Close()

				// Should find exactly 8 children for parent1
				if count != 8 {
					t.Errorf("Expected 8 datoms in scan range, got %d", count)
				}
			})

			t.Run("reverse lookup query finds referencing entities", func(t *testing.T) {
				// Find all entities that reference parent1
				queryStr := `[:find ?child :in $ ?parent :where [?child :child/parent ?parent]]`

				result, err := executor.CollectTuples(db.Query(queryStr, parent1))
				if err != nil {
					t.Fatalf("Query failed: %v", err)
				}
				if len(result) != 8 {
					t.Errorf("Expected 8 children for parent1, got %d", len(result))
				}

				result, err = executor.CollectTuples(db.Query(queryStr, parent2))
				if err != nil {
					t.Fatalf("Query failed: %v", err)
				}
				if len(result) != 4 {
					t.Errorf("Expected 4 children for parent2, got %d", len(result))
				}
			})
		})
	}
}

// TestChooseIndexForValuesIdentity verifies that datalog.Identity
// values are handled correctly (Identity is now always a pointer type).
func TestChooseIndexForValuesIdentity(t *testing.T) {
	dir, err := os.MkdirTemp("", "choose-index-identity-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	scenario := datalog.NewIdentity("test-scenario")

	tx := db.NewTransaction()
	for i := 0; i < 5; i++ {
		task := datalog.NewIdentity("ptr-task-" + string(rune('A'+i)))
		tx.Add(task, datalog.NewKeyword(":task/scenario"), scenario)
	}
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	matcher := db.Matcher().(*PatternMatcher)
	attr := datalog.NewKeyword(":task/scenario")

	t.Run("Identity value", func(t *testing.T) {
		iter, _ := matcher.store.ScanKeysOnly(
			matcher.scanBoundForValues(AVET, nil, attr, scenario, 0))
		count := 0
		for iter.Next() {
			count++
		}
		iter.Close()

		if count != 5 {
			t.Errorf("Expected 5 datoms with Identity value, got %d", count)
		}
	})
}

// TestChooseIndexForValuesEAVT verifies EAVT index handling still works correctly.
func TestChooseIndexForValuesEAVT(t *testing.T) {
	dir, err := os.MkdirTemp("", "choose-index-eavt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	entity := datalog.NewIdentity("test-entity")

	tx := db.NewTransaction()
	tx.Add(entity, datalog.NewKeyword(":entity/name"), "Test")
	tx.Add(entity, datalog.NewKeyword(":entity/type"), "example")
	tx.Add(entity, datalog.NewKeyword(":entity/count"), int64(42))
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	matcher := db.Matcher().(*PatternMatcher)

	t.Run("EAVT with entity bound", func(t *testing.T) {
		iter, _ := matcher.store.ScanKeysOnly(
			matcher.scanBoundForValues(EAVT, entity, nil, nil, 0))
		count := 0
		for iter.Next() {
			count++
		}
		iter.Close()

		if count != 3 {
			t.Errorf("Expected 3 datoms for entity, got %d", count)
		}
	})

	t.Run("EAVT with entity and attribute bound", func(t *testing.T) {
		attr := datalog.NewKeyword(":entity/name")
		iter, _ := matcher.store.ScanKeysOnly(
			matcher.scanBoundForValues(EAVT, entity, attr, nil, 0))
		count := 0
		for iter.Next() {
			count++
		}
		iter.Close()

		if count != 1 {
			t.Errorf("Expected 1 datom for entity+attr, got %d", count)
		}
	})
}

// TestChooseIndexForValuesAEVT verifies AEVT index handling still works correctly.
func TestChooseIndexForValuesAEVT(t *testing.T) {
	dir, err := os.MkdirTemp("", "choose-index-aevt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	entity1 := datalog.NewIdentity("entity-1")
	entity2 := datalog.NewIdentity("entity-2")

	tx := db.NewTransaction()
	tx.Add(entity1, datalog.NewKeyword(":entity/name"), "Entity 1")
	tx.Add(entity2, datalog.NewKeyword(":entity/name"), "Entity 2")
	tx.Add(entity1, datalog.NewKeyword(":entity/type"), "alpha")
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	matcher := db.Matcher().(*PatternMatcher)

	t.Run("AEVT with attribute bound", func(t *testing.T) {
		attr := datalog.NewKeyword(":entity/name")
		iter, _ := matcher.store.ScanKeysOnly(
			matcher.scanBoundForValues(AEVT, nil, attr, nil, 0))
		count := 0
		for iter.Next() {
			count++
		}
		iter.Close()

		if count != 2 {
			t.Errorf("Expected 2 datoms for :entity/name, got %d", count)
		}
	})

	t.Run("AEVT with attribute and entity bound", func(t *testing.T) {
		attr := datalog.NewKeyword(":entity/name")
		iter, _ := matcher.store.ScanKeysOnly(
			matcher.scanBoundForValues(AEVT, entity1, attr, nil, 0))
		count := 0
		for iter.Next() {
			count++
		}
		iter.Close()

		if count != 1 {
			t.Errorf("Expected 1 datom for entity1+attr, got %d", count)
		}
	})
}

// TestChooseIndexForValuesAETV verifies that AETV index scan ranges
// are correctly narrowed when attribute and/or entity are bound.
// AETV is the A-primary CRDT-aware index (A → E → Tx↓ → V).
// This test ensures chooseIndexForValues handles AETV properly.
func TestChooseIndexForValuesAETV(t *testing.T) {
	dir, err := os.MkdirTemp("", "choose-index-aetv-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create test data: multiple entities with different attributes
	entity1 := datalog.NewIdentity("person-1")
	entity2 := datalog.NewIdentity("person-2")
	entity3 := datalog.NewIdentity("person-3")

	tx := db.NewTransaction()
	// Each entity has :person/name and :person/age
	tx.Add(entity1, datalog.NewKeyword(":person/name"), "Alice")
	tx.Add(entity1, datalog.NewKeyword(":person/age"), int64(30))
	tx.Add(entity2, datalog.NewKeyword(":person/name"), "Bob")
	tx.Add(entity2, datalog.NewKeyword(":person/age"), int64(25))
	tx.Add(entity3, datalog.NewKeyword(":person/name"), "Charlie")
	tx.Add(entity3, datalog.NewKeyword(":person/age"), int64(35))
	// Add some other attributes to increase total datom count
	tx.Add(entity1, datalog.NewKeyword(":person/city"), "NYC")
	tx.Add(entity2, datalog.NewKeyword(":person/city"), "LA")
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	matcher := db.Matcher().(*PatternMatcher)

	t.Run("AETV with attribute bound only", func(t *testing.T) {
		// Scan AETV for :person/name - should get exactly 3 datoms
		attr := datalog.NewKeyword(":person/name")
		iter, err := matcher.store.ScanKeysOnly(
			matcher.scanBoundForValues(AETV, nil, attr, nil, 0))
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		count := 0
		for iter.Next() {
			count++
		}
		iter.Close()

		// Should find exactly 3 :person/name datoms
		if count != 3 {
			t.Errorf("Expected 3 datoms for :person/name via AETV, got %d", count)
		}
	})

	t.Run("AETV with attribute and entity bound", func(t *testing.T) {
		// Scan AETV for entity1 + :person/name - should get exactly 1 datom
		attr := datalog.NewKeyword(":person/name")
		iter, err := matcher.store.ScanKeysOnly(
			matcher.scanBoundForValues(AETV, entity1, attr, nil, 0))
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		count := 0
		for iter.Next() {
			count++
		}
		iter.Close()

		// Should find exactly 1 datom for entity1 + :person/name
		if count != 1 {
			t.Errorf("Expected 1 datom for entity1+:person/name via AETV, got %d", count)
		}
	})

	t.Run("AETV scan should not exceed attribute datom count", func(t *testing.T) {
		// This test verifies the bug is fixed: without AETV case in chooseIndexForValues,
		// it would scan ALL datoms in the index instead of just the attribute's datoms
		attr := datalog.NewKeyword(":person/age")
		iter, err := matcher.store.ScanKeysOnly(
			matcher.scanBoundForValues(AETV, nil, attr, nil, 0))
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		count := 0
		for iter.Next() {
			count++
		}
		iter.Close()

		// Total datoms in DB is 8, :person/age has 3
		// If AETV case is missing, count would be >= 8 (full scan)
		if count > 3 {
			t.Errorf("AETV scan for :person/age scanned %d datoms, expected 3 (possible full index scan)", count)
		}
		if count != 3 {
			t.Errorf("Expected exactly 3 datoms for :person/age, got %d", count)
		}
	})
}
