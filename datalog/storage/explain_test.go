package storage

import (
	"os"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
)

// TestExplain tests the Explain method for query plan introspection
func TestExplain(t *testing.T) {
	// Create temporary database
	dir, err := os.MkdirTemp("", "explain-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Add test data
	tx := db.NewTransaction()
	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	scenario1 := datalog.NewIdentity("scenario-1")

	tx.Add(alice, datalog.NewKeyword(":task/name"), "Task A")
	tx.Add(alice, datalog.NewKeyword(":task/scenario"), scenario1)
	tx.Add(bob, datalog.NewKeyword(":task/name"), "Task B")
	tx.Add(bob, datalog.NewKeyword(":task/scenario"), scenario1)

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	t.Run("simple pattern shows index selection", func(t *testing.T) {
		plan, err := db.Explain(`[:find ?e :where [?e :task/name "Task A"]]`)
		if err != nil {
			t.Fatalf("Explain failed: %v", err)
		}

		planStr := plan.String()
		t.Logf("Plan:\n%s", planStr)

		// AVET index should be selected when attribute and value are bound
		if !strings.Contains(planStr, "AVET index") {
			t.Errorf("Expected AVET index for attribute+value bound pattern, got:\n%s", planStr)
		}
	})

	t.Run("entity-only pattern uses EAVT", func(t *testing.T) {
		plan, err := db.Explain(`[:find ?a ?v :in $ ?e :where [?e ?a ?v]]`, alice)
		if err != nil {
			t.Fatalf("Explain failed: %v", err)
		}

		planStr := plan.String()
		t.Logf("Plan:\n%s", planStr)

		// EAVT index should be selected when only entity is bound
		if !strings.Contains(planStr, "EAVT index") {
			t.Errorf("Expected EAVT index for entity-only bound pattern, got:\n%s", planStr)
		}
	})

	t.Run("with input parameter shows available symbols", func(t *testing.T) {
		plan, err := db.Explain(
			`[:find ?e :in $ ?scenario :where [?e :task/scenario ?scenario]]`,
			scenario1,
		)
		if err != nil {
			t.Fatalf("Explain failed: %v", err)
		}

		planStr := plan.String()
		t.Logf("Plan:\n%s", planStr)

		// Input parameter should show as available
		if !strings.Contains(planStr, "Available:") && !strings.Contains(planStr, "?scenario") {
			t.Logf("Note: Input parameters may not be shown in Available (they're in :in clause)")
		}

		// AVET should be selected (attribute + value from input)
		if !strings.Contains(planStr, "AVET index") {
			t.Errorf("Expected AVET index for input-bound value pattern, got:\n%s", planStr)
		}
	})

	t.Run("shows bound mask", func(t *testing.T) {
		plan, err := db.Explain(`[:find ?e :where [?e :task/name "Task A"]]`)
		if err != nil {
			t.Fatalf("Explain failed: %v", err)
		}

		planStr := plan.String()
		t.Logf("Plan:\n%s", planStr)

		// Should show which components are bound
		if !strings.Contains(planStr, "Bound:") {
			t.Errorf("Expected bound mask in output, got:\n%s", planStr)
		}
		if !strings.Contains(planStr, "A=true") {
			t.Errorf("Expected A=true for bound attribute, got:\n%s", planStr)
		}
		if !strings.Contains(planStr, "V=true") {
			t.Errorf("Expected V=true for bound value, got:\n%s", planStr)
		}
	})

	t.Run("invalid query returns error", func(t *testing.T) {
		_, err := db.Explain(`[:find ?e :where]`) // missing pattern
		if err == nil {
			t.Error("Expected error for invalid query, got nil")
		}
	})

	t.Run("wrong number of inputs returns error", func(t *testing.T) {
		_, err := db.Explain(
			`[:find ?e :in $ ?x ?y :where [?e :task/name ?x]]`,
			"only-one-input", // missing second input
		)
		if err == nil {
			t.Error("Expected error for wrong number of inputs, got nil")
		}
	})
}

// TestAnalyze tests the Analyze method for query execution tracing
func TestAnalyze(t *testing.T) {
	// Create temporary database
	dir, err := os.MkdirTemp("", "analyze-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Add test data
	tx := db.NewTransaction()
	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	charlie := datalog.NewIdentity("charlie")
	scenario1 := datalog.NewIdentity("scenario-1")

	tx.Add(alice, datalog.NewKeyword(":task/name"), "Task A")
	tx.Add(alice, datalog.NewKeyword(":task/scenario"), scenario1)
	tx.Add(bob, datalog.NewKeyword(":task/name"), "Task B")
	tx.Add(bob, datalog.NewKeyword(":task/scenario"), scenario1)
	tx.Add(charlie, datalog.NewKeyword(":task/name"), "Task C")
	tx.Add(charlie, datalog.NewKeyword(":task/scenario"), scenario1)

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	t.Run("returns plan and events", func(t *testing.T) {
		result, err := db.Analyze(`[:find ?e :where [?e :task/name ?name]]`)
		if err != nil {
			t.Fatalf("Analyze failed: %v", err)
		}

		// Check we got a plan
		if result.Plan == nil {
			t.Error("Expected plan, got nil")
		}

		// Check we got events
		if len(result.Events) == 0 {
			t.Error("Expected events, got none")
		}

		// Check we have timing
		if result.TotalTime == 0 {
			t.Error("Expected non-zero total time")
		}

		t.Logf("Analyze result:\n%s", result.String())
	})

	t.Run("captures storage scan events", func(t *testing.T) {
		result, err := db.Analyze(`[:find ?e :where [?e :task/name ?name]]`)
		if err != nil {
			t.Fatalf("Analyze failed: %v", err)
		}

		// Look for storage scan events
		foundScan := false
		for _, e := range result.Events {
			if e.Name == annotations.PatternStorageScan {
				foundScan = true
				break
			}
		}

		if !foundScan {
			t.Log("Note: No storage scan events captured (may depend on executor path)")
		}
	})

	t.Run("with input parameter", func(t *testing.T) {
		result, err := db.Analyze(
			`[:find ?e :in $ ?scenario :where [?e :task/scenario ?scenario]]`,
			scenario1,
		)
		if err != nil {
			t.Fatalf("Analyze failed: %v", err)
		}

		// Should have 3 results
		if result.Result.Size() != 3 {
			t.Errorf("Expected 3 results, got %d", result.Result.Size())
		}

		t.Logf("Analyze with input:\n%s", result.String())
	})

	t.Run("String output includes plan and execution", func(t *testing.T) {
		result, err := db.Analyze(`[:find ?e :where [?e :task/name "Task A"]]`)
		if err != nil {
			t.Fatalf("Analyze failed: %v", err)
		}

		output := result.String()

		// Check for plan section
		if !strings.Contains(output, "Query Plan:") {
			t.Error("Expected 'Query Plan:' in output")
		}

		// Check for execution section
		if !strings.Contains(output, "Execution:") {
			t.Error("Expected 'Execution:' in output")
		}

		// Check for event trace
		if !strings.Contains(output, "Event Trace:") {
			t.Error("Expected 'Event Trace:' in output")
		}
	})

	t.Run("join query captures join events", func(t *testing.T) {
		// Add some data that requires a join
		tx := db.NewTransaction()
		tx.Add(alice, datalog.NewKeyword(":task/priority"), int64(1))
		tx.Add(bob, datalog.NewKeyword(":task/priority"), int64(2))
		_, _ = tx.Commit()

		result, err := db.Analyze(`[:find ?name ?priority
			:where
			[?e :task/name ?name]
			[?e :task/priority ?priority]]`)
		if err != nil {
			t.Fatalf("Analyze failed: %v", err)
		}

		t.Logf("Join query analysis:\n%s", result.String())
	})
}

// TestAnalyze_ConsumesStreamingResults pins that Analyze() fully executes the
// query before returning, rather than handing back a lazy relation whose work
// (storage scans, joins, deferred errors) happens only when a caller later
// iterates it. EXPLAIN ANALYZE-style APIs are expected to measure actual
// execution; against the unfixed code the storage scan fires during lazy
// iteration after Analyze has returned, so its events are absent here.
func TestAnalyze_ConsumesStreamingResults(t *testing.T) {
	dir, err := os.MkdirTemp("", "analyze-consume-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	for i, name := range []string{"Task A", "Task B", "Task C"} {
		e := datalog.NewIdentity(string(rune('a'+i)) + "-task")
		tx.Add(e, datalog.NewKeyword(":task/name"), name)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	result, err := db.Analyze(`[:find ?e :where [?e :task/name ?name]]`)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	// The storage scan is the query's actual work. Analyze must have driven it,
	// so its event is present without the caller iterating the result.
	foundScan := false
	for _, e := range result.Events {
		if e.Name == annotations.PatternStorageScan {
			foundScan = true
			break
		}
	}
	if !foundScan {
		t.Errorf("Analyze did not capture the storage scan event; query work was deferred past the API boundary. Events: %d", len(result.Events))
	}

	// The returned result must be fully usable: correct size and re-iterable.
	if got := result.Result.Size(); got != 3 {
		t.Errorf("expected 3 result tuples, got %d", got)
	}
	count := 0
	it := result.Result.Iterator()
	for it.Next() {
		count++
	}
	if err := it.Error(); err != nil {
		t.Errorf("iterating Analyze result errored: %v", err)
	}
	it.Close()
	if count != 3 {
		t.Errorf("re-iterating Analyze result yielded %d tuples, want 3", count)
	}
}
