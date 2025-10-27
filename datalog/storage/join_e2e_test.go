package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// setupTestDB creates a temporary database for testing
func setupTestDB(t *testing.T) (*Database, func()) {
	tempDir, err := os.MkdirTemp("", "join-e2e-*")
	if err != nil {
		t.Fatal(err)
	}

	db, err := NewDatabase(tempDir)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatal(err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(tempDir)
	}

	return db, cleanup
}

// populatePersonData inserts test people with specified attributes
func populatePersonData(t *testing.T, db *Database, count int, attributes map[string]func(int) interface{}) {
	batchSize := 50

	for batch := 0; batch < count; batch += batchSize {
		tx := db.NewTransaction()
		end := batch + batchSize
		if end > count {
			end = count
		}

		for i := batch; i < end; i++ {
			person := datalog.NewIdentity(fmt.Sprintf("person%d", i))
			for attrName, valueFn := range attributes {
				tx.Add(person, datalog.NewKeyword(attrName), valueFn(i))
			}
		}

		_, err := tx.Commit()
		if err != nil {
			t.Fatalf("Batch commit failed (records %d-%d): %v", batch, end-1, err)
		}
	}
}

// TestStorageBackedJoinE2E tests join correctness using the FULL execution path:
// Parse → Plan → Pattern Match (BadgerDB) → Execute → Iterate
//
// This tests tuple copying in the real pipeline, not isolated primitives
func TestStorageBackedJoinE2E(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Insert 100 people with name and age
	populatePersonData(t, db, 100, map[string]func(int) interface{}{
		":person/name": func(i int) interface{} { return fmt.Sprintf("Name%d", i) },
		":person/age":  func(i int) interface{} { return int64(20 + i) },
	})

	testCases := []struct {
		name             string
		query            string
		symmetric        bool
		expectedCount    int
		checkCorrectness func(t *testing.T, results []executor.Tuple)
	}{
		{
			name: "asymmetric_simple_join",
			query: `[:find ?name ?age
			         :where [?p :person/name ?name]
			                [?p :person/age ?age]]`,
			symmetric:     false,
			expectedCount: 100,
			checkCorrectness: func(t *testing.T, results []executor.Tuple) {
				// Verify join correctness: name and age must match
				for _, tuple := range results {
					if len(tuple) != 2 {
						t.Errorf("Expected 2 columns, got %d", len(tuple))
						continue
					}

					name, ok1 := tuple[0].(string)
					age, ok2 := tuple[1].(int64)
					if !ok1 || !ok2 {
						t.Errorf("Type mismatch: %T, %T", tuple[0], tuple[1])
						continue
					}

					// Extract person ID from name
					var personID int
					fmt.Sscanf(name, "Name%d", &personID)

					// Age should match: 20 + personID
					expectedAge := int64(20 + personID)
					if age != expectedAge {
						t.Errorf("Join corruption: %s has age %d, expected %d",
							name, age, expectedAge)
					}
				}
			},
		},
		{
			name: "symmetric_simple_join",
			query: `[:find ?name ?age
			         :where [?p :person/name ?name]
			                [?p :person/age ?age]]`,
			symmetric:     true,
			expectedCount: 100,
			checkCorrectness: func(t *testing.T, results []executor.Tuple) {
				for _, tuple := range results {
					name, ok1 := tuple[0].(string)
					age, ok2 := tuple[1].(int64)
					if !ok1 || !ok2 {
						t.Errorf("Type mismatch: %T, %T", tuple[0], tuple[1])
						continue
					}

					var personID int
					fmt.Sscanf(name, "Name%d", &personID)
					expectedAge := int64(20 + personID)
					if age != expectedAge {
						t.Errorf("Join corruption: %s has age %d, expected %d",
							name, age, expectedAge)
					}
				}
			},
		},
		{
			name: "asymmetric_with_filter",
			query: `[:find ?name
			         :where [?p :person/name ?name]
			                [?p :person/age ?age]
			                [(> ?age 50)]]`,
			symmetric:     false,
			expectedCount: 69, // ages 51-119 (IDs 31-99)
			checkCorrectness: func(t *testing.T, results []executor.Tuple) {
				for _, tuple := range results {
					name := tuple[0].(string)
					var personID int
					fmt.Sscanf(name, "Name%d", &personID)

					// Should only have people with age > 50
					// age = 20 + personID, so age > 50 means personID > 30
					if personID <= 30 {
						t.Errorf("Filter failed: %s (ID %d) should not appear (age %d <= 50)",
							name, personID, 20+personID)
					}
				}
			},
		},
		{
			name: "symmetric_with_filter",
			query: `[:find ?name
			         :where [?p :person/name ?name]
			                [?p :person/age ?age]
			                [(> ?age 50)]]`,
			symmetric:     true,
			expectedCount: 69,
			checkCorrectness: func(t *testing.T, results []executor.Tuple) {
				for _, tuple := range results {
					name := tuple[0].(string)
					var personID int
					fmt.Sscanf(name, "Name%d", &personID)
					if personID <= 30 {
						t.Errorf("Filter failed: %s should not appear", name)
					}
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse query
			q, err := parser.ParseQuery(tc.query)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			// Create executor with appropriate options
			opts := executor.ExecutorOptions{
				EnableStreamingJoins:    true,
				EnableSymmetricHashJoin: tc.symmetric,
				DefaultHashTableSize:    256,
			}

			matcher := NewBadgerMatcherWithOptions(db.Store(), opts)
			exec := executor.NewExecutor(matcher)

			// Execute through full pipeline
			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("Execute failed: %v", err)
			}

			// Collect results
			var results []executor.Tuple
			it := result.Iterator()
			defer it.Close()

			for it.Next() {
				tuple := it.Tuple()

				// CRITICAL: Copy tuple to detect corruption
				// If iterator reuses buffer, copy will preserve corrupted values
				tupleCopy := make(executor.Tuple, len(tuple))
				copy(tupleCopy, tuple)
				results = append(results, tupleCopy)
			}

			// Check count
			if len(results) != tc.expectedCount {
				t.Errorf("Expected %d results, got %d", tc.expectedCount, len(results))
			}

			// Check no duplicates
			seen := make(map[string]bool)
			for i, tuple := range results {
				key := fmt.Sprintf("%v", tuple)
				if seen[key] {
					t.Errorf("Duplicate tuple at index %d: %v", i, tuple)
				}
				seen[key] = true
			}

			// Check correctness
			if tc.checkCorrectness != nil {
				tc.checkCorrectness(t, results)
			}
		})
	}
}

// TestStorageBackedJoinLimitE2E tests LIMIT queries through full execution path
// This is where symmetric hash join should show massive speedup
func TestStorageBackedJoinLimitE2E(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Insert 1000 people with name and email
	populatePersonData(t, db, 1000, map[string]func(int) interface{}{
		":person/name":  func(i int) interface{} { return fmt.Sprintf("Name%d", i) },
		":person/email": func(i int) interface{} { return fmt.Sprintf("email%d@example.com", i) },
	})

	testCases := []struct {
		name      string
		query     string
		symmetric bool
		limit     int
	}{
		{
			name: "asymmetric_limit_10",
			query: `[:find ?name ?email
			         :where [?p :person/name ?name]
			                [?p :person/email ?email]]`,
			symmetric: false,
			limit:     10,
		},
		{
			name: "symmetric_limit_10",
			query: `[:find ?name ?email
			         :where [?p :person/name ?name]
			                [?p :person/email ?email]]`,
			symmetric: true,
			limit:     10,
		},
		{
			name: "asymmetric_limit_100",
			query: `[:find ?name ?email
			         :where [?p :person/name ?name]
			                [?p :person/email ?email]]`,
			symmetric: false,
			limit:     100,
		},
		{
			name: "symmetric_limit_100",
			query: `[:find ?name ?email
			         :where [?p :person/name ?name]
			                [?p :person/email ?email]]`,
			symmetric: true,
			limit:     100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tc.query)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			opts := executor.ExecutorOptions{
				EnableStreamingJoins:    true,
				EnableSymmetricHashJoin: tc.symmetric,
				DefaultHashTableSize:    256,
			}

			matcher := NewBadgerMatcherWithOptions(db.Store(), opts)
			exec := executor.NewExecutor(matcher)

			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("Execute failed: %v", err)
			}

			// Count results (manually enforce LIMIT by stopping after N results)
			count := 0
			it := result.Iterator()
			defer it.Close()

			for count < tc.limit && it.Next() {
				tuple := it.Tuple()

				// Verify tuple is valid (not corrupted)
				if len(tuple) != 2 {
					t.Errorf("Invalid tuple length: %d", len(tuple))
				}

				name, ok1 := tuple[0].(string)
				email, ok2 := tuple[1].(string)
				if !ok1 || !ok2 {
					t.Errorf("Type corruption: %T, %T", tuple[0], tuple[1])
				}

				// Verify name and email match
				var personID int
				fmt.Sscanf(name, "Name%d", &personID)
				expectedEmail := fmt.Sprintf("email%d@example.com", personID)
				if email != expectedEmail {
					t.Errorf("Join corruption: %s has email %s, expected %s",
						name, email, expectedEmail)
				}

				count++
			}

			// LIMIT should be respected
			if count != tc.limit {
				t.Errorf("Expected exactly %d results, got %d", tc.limit, count)
			}
		})
	}
}
