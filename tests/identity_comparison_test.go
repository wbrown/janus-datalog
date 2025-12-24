package tests

import (
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestIdentityComparisonBestPractices demonstrates the correct way to compare
// Identity values in query results.
//
// IMPORTANT: Use .Equal() to compare Identities, NOT == or !=
//
// Identity structs contain multiple fields:
// - value: [20]byte - the actual SHA1 hash (the true identity)
// - l85: string - lazily computed L85 encoding
// - str: string - original string (if known)
// - l85Computed: bool
//
// Identities loaded from storage have different auxiliary fields than the
// original Identities created in code. Struct equality compares ALL fields,
// so it will fail even when the hashes match.
func TestIdentityComparisonBestPractices(t *testing.T) {
	dir, err := os.MkdirTemp("", "identity-comparison-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	refAttr := datalog.NewKeyword(":test/ref")
	codeAttr := datalog.NewKeyword(":test/code")

	// Create entities
	parent := datalog.NewIdentity(generateUUID())
	child := datalog.NewIdentity(generateUUID())

	tx := db.NewTransaction()
	tx.Add(child, refAttr, parent)
	tx.Add(child, codeAttr, "child1")
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Query to find the child
	tuples, err := db.ExecuteQueryWithInputs(
		`[:find ?c :in $ ?parent :where [?c :test/ref ?parent]]`,
		parent,
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(tuples) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(tuples))
	}

	// Get the returned entity
	foundID, ok := tuples[0][0].(datalog.Identity)
	if !ok {
		t.Fatalf("Result is not an Identity: %T", tuples[0][0])
	}

	// WRONG: struct equality (compares all fields including str, l85)
	// This will FAIL even when the identities represent the same entity!
	if foundID == child {
		t.Log("Struct equality (==) matched - this is luck, not guaranteed!")
	} else {
		// This is the EXPECTED case - struct fields differ
		t.Log("Struct equality (==) did NOT match - expected behavior")
		t.Logf("  original child.str: %q", child.String())
		t.Logf("  returned foundID:   %q (from storage, no original string)", foundID.String())
	}

	// CORRECT: use .Equal() which compares only the hash
	if foundID.Equal(child) {
		t.Log("Hash equality (.Equal()) matched - CORRECT!")
	} else {
		t.Errorf("Hash equality (.Equal()) failed - this indicates a real bug!")
	}

	// Also correct: compare hashes directly
	if foundID.Hash() == child.Hash() {
		t.Log("Direct hash comparison matched - also correct")
	} else {
		t.Errorf("Direct hash comparison failed - this indicates a real bug!")
	}
}
