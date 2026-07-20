package tests

import (
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/qb"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestTupleGroundBasic tests the basic tuple ground syntax:
// [(ground [1 2 3]) [?a ?b ?c]]
func TestTupleGroundBasic(t *testing.T) {
	dir, err := os.MkdirTemp("", "tuple-ground-basic-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Add a simple entity so query has a where clause
	tx := db.NewTransaction()
	e := datalog.NewIdentity("test:1")
	tx.Add(e, datalog.NewKeyword(":test/name"), "dummy")
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Query with tuple ground
	queryStr := `[:find ?a ?b ?c
	              :where [_ :test/name _]
	                     [(ground [1 2 3]) [?a ?b ?c]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := executor.NewExecutorWithOptions(storage.NewBadgerMatcher(db.Store()), nil, mode.plannerOptions())

			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Collect results
			var tuples [][]interface{}
			iter := result.Iterator()
			for iter.Next() {
				tuple := iter.Tuple()
				row := make([]interface{}, len(tuple))
				copy(row, tuple)
				tuples = append(tuples, row)
			}
			iter.Close()

			// Should have 1 row with values [1, 2, 3]
			if len(tuples) != 1 {
				t.Errorf("Expected 1 row, got %d", len(tuples))
			}

			if len(tuples) > 0 {
				row := tuples[0]
				if row[0] != int64(1) || row[1] != int64(2) || row[2] != int64(3) {
					t.Errorf("Expected [1 2 3], got %v", row)
				}
			}
		})
	}
}

// TestTupleGroundOrFallback tests the primary use case - OR fallback with tuple ground:
// (or [(q [...subquery...]) [[?count ?total]]]
//
//	[(ground [0 0]) [?count ?total]])
func TestTupleGroundOrFallback(t *testing.T) {
	dir, err := os.MkdirTemp("", "tuple-ground-or-fallback-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// :scenario/task is cardinality-many (a scenario can have multiple tasks)
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":scenario/task"),
		ValueType:   schema.TypeRef,
		Cardinality: schema.CardinalityMany,
	})
	db.SetSchema(s)

	// Setup: scenarios with tasks (similar to comparison_binding_or_subquery_test.go)
	tx := db.NewTransaction()

	scenario1 := datalog.NewIdentity("scenario:1")
	scenario2 := datalog.NewIdentity("scenario:2")
	scenario3 := datalog.NewIdentity("scenario:3")
	task1 := datalog.NewIdentity("task:1")
	task2 := datalog.NewIdentity("task:2")
	task3 := datalog.NewIdentity("task:3")

	nameAttr := datalog.NewKeyword(":scenario/name")
	taskAttr := datalog.NewKeyword(":scenario/task")
	valueAttr := datalog.NewKeyword(":task/value")

	// scenario:1 has 2 tasks with values 10 and 20
	tx.Add(scenario1, nameAttr, "Scenario One")
	tx.Add(scenario1, taskAttr, task1)
	tx.Add(scenario1, taskAttr, task2)
	tx.Add(task1, valueAttr, int64(10))
	tx.Add(task2, valueAttr, int64(20))

	// scenario:2 has 1 task with value 100
	tx.Add(scenario2, nameAttr, "Scenario Two")
	tx.Add(scenario2, taskAttr, task3)
	tx.Add(task3, valueAttr, int64(100))

	// scenario:3 has no tasks
	tx.Add(scenario3, nameAttr, "Scenario Three")

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Query with OR fallback using tuple ground
	// The key difference from comparison_binding_or_subquery_test.go is that
	// we use tuple ground [(ground [0 0]) [?taskCount ?totalValue]] instead of
	// two separate [(ground 0) ?taskCount] and [(ground 0) ?totalValue]
	queryStr := `[:find ?scenario ?name ?taskCount ?totalValue
	              :where [?scenario :scenario/name ?name]
	                     (or-default [(q [:find (count ?t) (sum ?v)
	                              :in $ ?scenario
	                              :where [?scenario :scenario/task ?t]
	                                     [?t :task/value ?v]]
	                            $ ?scenario) [[?taskCount ?totalValue]]]
	                         [(ground [0 0]) [?taskCount ?totalValue]])]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	t.Logf("Parsed query: %s", q.String())

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			matcher := storage.NewBadgerMatcher(db.Store())
			matcher.SetSchema(s)
			exec := executor.NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			ctx := executor.NewContext(func(event annotations.Event) {
				t.Logf("[ANNOTATION] %s: %v", event.Name, event.Data)
			})

			result, err := exec.ExecuteWithContext(ctx, q)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Collect results
			var tuples [][]interface{}
			iter := result.Iterator()
			for iter.Next() {
				tuple := iter.Tuple()
				row := make([]interface{}, len(tuple))
				copy(row, tuple)
				tuples = append(tuples, row)
			}
			iter.Close()

			t.Logf("Result count: %d", len(tuples))

			// Should have 3 rows
			if len(tuples) != 3 {
				t.Errorf("Expected 3 rows, got %d", len(tuples))
			}

			// Build result map (sum returns float64, count returns int64)
			resultMap := make(map[string]struct {
				taskCount  int64
				totalValue float64
			})

			for _, tuple := range tuples {
				name := tuple[1].(string)
				taskCount := tuple[2].(int64)
				// totalValue is float64 from sum, or int64 from ground fallback
				var totalValue float64
				switch v := tuple[3].(type) {
				case float64:
					totalValue = v
				case int64:
					totalValue = float64(v)
				}
				resultMap[name] = struct {
					taskCount  int64
					totalValue float64
				}{taskCount, totalValue}
				t.Logf("Row: name=%s, taskCount=%d, totalValue=%v", name, taskCount, totalValue)
			}

			// Scenario One: 2 tasks, total value 30 (10 + 20)
			if data, ok := resultMap["Scenario One"]; !ok {
				t.Error("Missing Scenario One in results")
			} else if data.taskCount != 2 || data.totalValue != 30 {
				t.Errorf("Scenario One: expected taskCount=2, totalValue=30, got taskCount=%d, totalValue=%v",
					data.taskCount, data.totalValue)
			}

			// Scenario Two: 1 task, total value 100
			if data, ok := resultMap["Scenario Two"]; !ok {
				t.Error("Missing Scenario Two in results")
			} else if data.taskCount != 1 || data.totalValue != 100 {
				t.Errorf("Scenario Two: expected taskCount=1, totalValue=100, got taskCount=%d, totalValue=%v",
					data.taskCount, data.totalValue)
			}

			// Scenario Three: 0 tasks, 0 total (from tuple ground fallback)
			if data, ok := resultMap["Scenario Three"]; !ok {
				t.Error("Missing Scenario Three in results")
			} else if data.taskCount != 0 || data.totalValue != 0 {
				t.Errorf("Scenario Three: expected taskCount=0, totalValue=0, got taskCount=%d, totalValue=%v",
					data.taskCount, data.totalValue)
			}
		})
	}
}

// TestTupleGroundBackwardCompatibility verifies scalar ground still works
func TestTupleGroundBackwardCompatibility(t *testing.T) {
	dir, err := os.MkdirTemp("", "tuple-ground-compat-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Add a simple entity
	tx := db.NewTransaction()
	e := datalog.NewIdentity("test:1")
	tx.Add(e, datalog.NewKeyword(":test/name"), "dummy")
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Query with scalar ground (original syntax)
	queryStr := `[:find ?x
	              :where [_ :test/name _]
	                     [(ground 42) ?x]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := executor.NewExecutorWithOptions(storage.NewBadgerMatcher(db.Store()), nil, mode.plannerOptions())

			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Collect results
			var tuples [][]interface{}
			iter := result.Iterator()
			for iter.Next() {
				tuple := iter.Tuple()
				row := make([]interface{}, len(tuple))
				copy(row, tuple)
				tuples = append(tuples, row)
			}
			iter.Close()

			// Should have 1 row with value 42
			if len(tuples) != 1 {
				t.Errorf("Expected 1 row, got %d", len(tuples))
			}

			if len(tuples) > 0 && tuples[0][0] != int64(42) {
				t.Errorf("Expected 42, got %v", tuples[0][0])
			}
		})
	}
}

// TestTupleGroundQB tests tuple ground via the query builder API
func TestTupleGroundQB(t *testing.T) {
	dir, err := os.MkdirTemp("", "tuple-ground-qb-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Setup: simple test data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("test:1")
	tx.Add(e, datalog.NewKeyword(":test/name"), "TestEntity")
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Build query using qb with TupleGround
	a, b, c := qb.NewVar("a"), qb.NewVar("b"), qb.NewVar("c")
	TestName := qb.Kw(":test/name")

	q := qb.Query().
		Find(a, b, c).
		Where(
			qb.Pat(qb.NewVar("_e"), TestName, qb.Blank()),
			qb.TupleGround(int64(1), int64(2), int64(3)).As(a, b, c),
		).MustBuild()

	t.Logf("Query built: %s", q.String())

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := executor.NewExecutorWithOptions(storage.NewBadgerMatcher(db.Store()), nil, mode.plannerOptions())

			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Collect results
			var tuples [][]interface{}
			iter := result.Iterator()
			for iter.Next() {
				tuple := iter.Tuple()
				row := make([]interface{}, len(tuple))
				copy(row, tuple)
				tuples = append(tuples, row)
			}
			iter.Close()

			t.Logf("Result count: %d", len(tuples))
			for _, tuple := range tuples {
				t.Logf("Row: %v", tuple)
			}

			// Should have 1 row with [1, 2, 3]
			if len(tuples) != 1 {
				t.Errorf("Expected 1 row, got %d", len(tuples))
			}

			if len(tuples) > 0 {
				row := tuples[0]
				if row[0] != int64(1) || row[1] != int64(2) || row[2] != int64(3) {
					t.Errorf("Expected [1 2 3], got %v", row)
				}
			}
		})
	}
}

// TestTupleGroundQBInOr tests tuple ground in Or clause via qb
// Note: OR clause fallback behavior with pure pattern matching (without subqueries) has
// different semantics than with subqueries. This tests the pattern-only case.
func TestTupleGroundQBInOr(t *testing.T) {
	dir, err := os.MkdirTemp("", "tuple-ground-qb-or-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Setup: scenarios with and without tasks
	tx := db.NewTransaction()

	scenario1 := datalog.NewIdentity("scenario:1")
	scenario2 := datalog.NewIdentity("scenario:2")
	task1 := datalog.NewIdentity("task:1")

	nameAttr := datalog.NewKeyword(":scenario/name")
	taskAttr := datalog.NewKeyword(":scenario/task")
	countAttr := datalog.NewKeyword(":task/count")

	tx.Add(scenario1, nameAttr, "Scenario One")
	tx.Add(scenario1, taskAttr, task1)
	tx.Add(task1, countAttr, int64(5))

	tx.Add(scenario2, nameAttr, "Scenario Two")
	// No tasks for scenario 2

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Build query with OR fallback using TupleGround
	scenario, name := qb.NewVar("scenario"), qb.NewVar("name")
	taskCount := qb.NewVar("taskCount")
	task := qb.NewVar("task") // Shared task variable

	ScenarioName := qb.Kw(":scenario/name")
	ScenarioTask := qb.Kw(":scenario/task")
	TaskCount := qb.Kw(":task/count")

	q := qb.Query().
		Find(scenario, name, taskCount).
		Where(
			qb.Pat(scenario, ScenarioName, name),
			qb.OrDefault().
				Branch(
					// Branch 1: scenario has task with count
					qb.Pat(scenario, ScenarioTask, task),
					qb.Pat(task, TaskCount, taskCount),
				).
				Branch(
					// Branch 2: fallback with tuple ground
					qb.TupleGround(int64(0)).As(taskCount),
				),
		).MustBuild()

	t.Logf("Query built: %s", q.String())

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := executor.NewExecutorWithOptions(storage.NewBadgerMatcher(db.Store()), nil, mode.plannerOptions())

			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Collect results
			var tuples [][]interface{}
			iter := result.Iterator()
			for iter.Next() {
				tuple := iter.Tuple()
				row := make([]interface{}, len(tuple))
				copy(row, tuple)
				tuples = append(tuples, row)
			}
			iter.Close()

			t.Logf("Result count: %d", len(tuples))
			for _, tuple := range tuples {
				t.Logf("Row: %v", tuple)
			}

			// OR fallback correctly triggers for each scenario:
			// - Scenario One: has tasks, pattern branch matches with count 5
			// - Scenario Two: no tasks, fallback branch triggers with count 0
			if len(tuples) != 2 {
				t.Errorf("Expected 2 rows (both scenarios), got %d", len(tuples))
			}

			// Build expected results map (order may vary)
			expected := map[string]int64{
				"Scenario One": 5,
				"Scenario Two": 0,
			}

			for _, tuple := range tuples {
				name := tuple[1].(string)
				taskCount := tuple[2].(int64)
				expectedCount, ok := expected[name]
				if !ok {
					t.Errorf("Unexpected scenario: %s", name)
					continue
				}
				if taskCount != expectedCount {
					t.Errorf("Scenario %s: expected count %d, got %d", name, expectedCount, taskCount)
				}
			}
		})
	}
}
