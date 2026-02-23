package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
)

// =============================================================================
// OrFallbackRelation Copy Tests (Integration)
//
// These tests verify that OR clause execution properly handles tuple copying
// when the underlying storage returns StreamingRelation (RequiresCopy = true).
// =============================================================================

func createOrTestDB(t *testing.T) (*Database, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "or_fallback_copy_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create database: %v", err)
	}

	return db, func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}
}

// TestOrClauseTupleStability verifies that tuples from OR clause execution
// are stable (not corrupted by workspace reuse).
// Uses ExecuteQueryRelation to get streaming results without materialization.
func TestOrClauseTupleStability(t *testing.T) {
	db, cleanup := createOrTestDB(t)
	defer cleanup()

	// Add test data - entities with status "active" or "pending"
	tx := db.NewTransaction()
	for i := 0; i < 10; i++ {
		e := datalog.NewIdentity("entity:" + string(rune('a'+i)))
		tx.Add(e, datalog.NewKeyword(":entity/name"), "name"+string(rune('a'+i)))
		if i%2 == 0 {
			tx.Add(e, datalog.NewKeyword(":entity/status"), "active")
		} else {
			tx.Add(e, datalog.NewKeyword(":entity/status"), "pending")
		}
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Enable annotations to see what's happening
	db.SetAnnotationHandler(func(e annotations.Event) {
		t.Logf("[TRACE] %s: %v", e.Name, e.Data)
	})

	// Query with OR clause using DATA PATTERNS (not expression predicates)
	// This exercises the storage-backed StreamingRelation path through OrFallbackRelation
	queryStr := `[:find ?name
                 :where [?e :entity/name ?name]
                        (or [?e :entity/status "active"]
                            [?e :entity/status "pending"])]`

	// Use Query to get streaming results WITHOUT materialization
	rel, err := db.Query(queryStr)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	t.Logf("Result relation type: %T", rel)

	// Iterate and store tuple references WITHOUT copying
	// If the iterator reuses workspace, stored tuples will be corrupted
	var storedTuples [][]interface{}
	it := rel.Iterator()
	for it.Next() {
		storedTuples = append(storedTuples, it.Tuple())
	}
	it.Close()

	// Should have 10 results
	if len(storedTuples) != 10 {
		t.Errorf("expected 10 tuples, got %d", len(storedTuples))
	}

	// Verify we got all unique names (not corrupted duplicates)
	names := make(map[string]bool)
	for i, tuple := range storedTuples {
		if len(tuple) >= 1 {
			if name, ok := tuple[0].(string); ok {
				names[name] = true
			} else {
				t.Errorf("tuple %d has wrong type: %T", i, tuple[0])
			}
		}
	}

	if len(names) != 10 {
		t.Errorf("expected 10 unique names, got %d (possible tuple corruption)", len(names))
		t.Logf("names: %v", names)
	}
}

// TestOrClauseMultipleBranches verifies tuple stability with multiple OR branches.
// Uses ExecuteQueryRelation for streaming without materialization.
func TestOrClauseMultipleBranches(t *testing.T) {
	db, cleanup := createOrTestDB(t)
	defer cleanup()

	// Add test data with different categories
	tx := db.NewTransaction()
	categories := []string{"electronics", "books", "clothing"}
	for i := 0; i < 9; i++ {
		e := datalog.NewIdentity("product:" + string(rune('a'+i)))
		tx.Add(e, datalog.NewKeyword(":product/name"), "product"+string(rune('a'+i)))
		tx.Add(e, datalog.NewKeyword(":product/category"), categories[i%3])
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Query with multiple OR branches using DATA PATTERNS
	// This exercises storage-backed StreamingRelation path
	queryStr := `[:find ?name
                 :where [?e :product/name ?name]
                        (or [?e :product/category "electronics"]
                            [?e :product/category "books"]
                            [?e :product/category "clothing"])]`

	// Use Query for streaming WITHOUT materialization
	rel, err := db.Query(queryStr)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// Iterate and store tuple references WITHOUT copying
	var storedTuples [][]interface{}
	it := rel.Iterator()
	for it.Next() {
		storedTuples = append(storedTuples, it.Tuple())
	}
	it.Close()

	// Should have 9 results
	if len(storedTuples) != 9 {
		t.Errorf("expected 9 tuples, got %d", len(storedTuples))
	}

	// Verify unique products (corruption would cause duplicates)
	products := make(map[string]bool)
	for _, tuple := range storedTuples {
		if len(tuple) >= 1 {
			if name, ok := tuple[0].(string); ok {
				products[name] = true
			}
		}
	}

	if len(products) != 9 {
		t.Errorf("expected 9 unique products, got %d (possible tuple corruption)", len(products))
		t.Logf("products: %v", products)
	}
}
