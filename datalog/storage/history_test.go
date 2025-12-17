package storage

import (
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestHistoryMode(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "history-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create database with history mode
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:        tmpDir,
		RetractMode: RetractHistory,
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Verify history mode
	if db.RetractMode() != RetractHistory {
		t.Fatal("expected RetractHistory mode")
	}

	// Create entity and add initial value
	entity := datalog.NewIdentity("user:1")
	nameAttr := datalog.NewKeyword(":user/name")

	tx1 := db.NewTransaction()
	if err := tx1.Add(entity, nameAttr, "Alice"); err != nil {
		t.Fatalf("failed to add: %v", err)
	}
	txID1, err := tx1.Commit()
	if err != nil {
		t.Fatalf("failed to commit tx1: %v", err)
	}
	t.Logf("Committed assertion at tx %d", txID1)

	// Retract the value
	tx2 := db.NewTransaction()
	if err := tx2.Retract(entity, nameAttr, "Alice"); err != nil {
		t.Fatalf("failed to retract: %v", err)
	}
	txID2, err := tx2.Commit()
	if err != nil {
		t.Fatalf("failed to commit tx2: %v", err)
	}
	t.Logf("Committed retraction at tx %d", txID2)

	// Query current state using Query API - should be empty (value was retracted)
	currentResults, err := db.ExecuteQuery(`[:find ?v :where [_ :user/name ?v]]`)
	if err != nil {
		t.Fatalf("current state query failed: %v", err)
	}
	if len(currentResults) != 0 {
		t.Errorf("expected empty current state, got %v", currentResults)
	}
	t.Logf("Current state: %d results (expected 0)", len(currentResults))

	// Query history using Query API - should see both assertion and retraction
	// 5-element pattern: [?e ?a ?v ?tx ?op]
	historyResults, err := db.ExecuteHistoryQuery(`[:find ?v ?tx ?op :where [_ :user/name ?v ?tx ?op]]`)
	if err != nil {
		t.Fatalf("history query failed: %v", err)
	}

	t.Logf("History results: %d entries", len(historyResults))
	for _, row := range historyResults {
		t.Logf("  value=%q tx=%v op=%v", row[0], row[1], row[2])
	}

	// Should have 2 history entries
	if len(historyResults) != 2 {
		t.Errorf("expected 2 history entries, got %d", len(historyResults))
	}

	// Verify we have both assertion (op=true) and retraction (op=false)
	hasAssert := false
	hasRetract := false
	for _, row := range historyResults {
		if op, ok := row[2].(bool); ok {
			if op {
				hasAssert = true
			} else {
				hasRetract = true
			}
		}
	}

	if !hasAssert {
		t.Error("missing assertion in history")
	}
	if !hasRetract {
		t.Error("missing retraction in history")
	}
}

func TestDefaultRetractMode(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "default-retract-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create default database
	db, err := NewDatabase(tmpDir)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Default mode should be RetractDelete
	if db.RetractMode() != RetractDelete {
		t.Errorf("expected RetractDelete mode, got %v", db.RetractMode())
	}

	// History() should return nil for non-history database
	if db.History() != nil {
		t.Error("History() should return nil for RetractDelete database")
	}
}
