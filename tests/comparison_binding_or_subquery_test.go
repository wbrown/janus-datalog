package tests

import (
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestComparisonBindingWithOrSubquery_E2E is an end-to-end integration test
// that reproduces the downstream failure:
//
//	column ?complete not found in relation
//
// The pattern is:
//
//	(or-default [(q [...count subquery...] $ ?scenario) [[?taskCount]]]
//	            [(ground 0) ?taskCount])
//	[(> ?taskCount 0) ?complete]
func TestComparisonBindingWithOrSubquery_E2E(t *testing.T) {
	dir, err := os.MkdirTemp("", "comparison-or-subquery-*")
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

	// Insert test data: scenarios with tasks
	tx := db.NewTransaction()

	scenario1 := datalog.NewIdentity("scenario:1")
	scenario2 := datalog.NewIdentity("scenario:2")
	scenario3 := datalog.NewIdentity("scenario:3")
	task1 := datalog.NewIdentity("task:1")
	task2 := datalog.NewIdentity("task:2")
	task3 := datalog.NewIdentity("task:3")

	nameAttr := datalog.NewKeyword(":scenario/name")
	taskAttr := datalog.NewKeyword(":scenario/task")

	// scenario:1 has 2 tasks
	tx.Add(scenario1, nameAttr, "Scenario One")
	tx.Add(scenario1, taskAttr, task1)
	tx.Add(scenario1, taskAttr, task2)

	// scenario:2 has 1 task
	tx.Add(scenario2, nameAttr, "Scenario Two")
	tx.Add(scenario2, taskAttr, task3)

	// scenario:3 has no tasks
	tx.Add(scenario3, nameAttr, "Scenario Three")

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// The problematic query pattern from downstream
	queryStr := `[:find ?scenario ?name ?taskCount ?complete
	              :where [?scenario :scenario/name ?name]
	                     (or-default [(q [:find (count ?t)
	                              :in $ ?scenario
	                              :where [?scenario :scenario/task ?t]]
	                            $ ?scenario) [[?taskCount]]]
	                         [(ground 0) ?taskCount])
	                     [(> ?taskCount 0) ?complete]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	t.Logf("Parsed query: %s", q.String())
	t.Logf("Where clauses: %d", len(q.Where))
	for i, clause := range q.Where {
		t.Logf("  Clause %d: %T - %v", i, clause, clause)
	}

	// Use executor with annotations to trace execution
	opts := storage.DefaultPlannerOptions()
	matcher := storage.NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	exec := executor.NewExecutorWithOptions(matcher, nil, opts)

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

	// Verify we got 3 rows
	if len(tuples) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(tuples))
	}

	// Verify results
	resultMap := make(map[string]struct {
		taskCount int64
		complete  bool
	})

	for _, tuple := range tuples {
		name := tuple[1].(string)
		taskCount := tuple[2].(int64)
		complete := tuple[3].(bool)
		resultMap[name] = struct {
			taskCount int64
			complete  bool
		}{taskCount, complete}
		t.Logf("Row: name=%s, taskCount=%d, complete=%v", name, taskCount, complete)
	}

	// Scenario One: taskCount=2 > 0, complete=true
	if data, ok := resultMap["Scenario One"]; !ok {
		t.Error("Missing Scenario One in results")
	} else if data.taskCount != 2 || data.complete != true {
		t.Errorf("Scenario One: expected taskCount=2, complete=true, got taskCount=%d, complete=%v", data.taskCount, data.complete)
	}

	// Scenario Two: taskCount=1 > 0, complete=true
	if data, ok := resultMap["Scenario Two"]; !ok {
		t.Error("Missing Scenario Two in results")
	} else if data.taskCount != 1 || data.complete != true {
		t.Errorf("Scenario Two: expected taskCount=1, complete=true, got taskCount=%d, complete=%v", data.taskCount, data.complete)
	}

	// Scenario Three: taskCount=0 (from ground fallback), complete=false
	if data, ok := resultMap["Scenario Three"]; !ok {
		t.Error("Missing Scenario Three in results")
	} else if data.taskCount != 0 || data.complete != false {
		t.Errorf("Scenario Three: expected taskCount=0, complete=false, got taskCount=%d, complete=%v", data.taskCount, data.complete)
	}
}
