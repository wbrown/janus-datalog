package tests

import (
	"crypto/rand"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/codec"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// generateUUID generates an L85-encoded binary UUID.
func generateUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return codec.EncodeL85(b)
}

// TestIdentityInputMatchingBug demonstrates a bug where Identity values passed as
// query input parameters don't correctly filter results.
//
// Bug: When querying [?e :attr ?inputIdentity] where ?inputIdentity is bound to
// a specific Identity value, the query returns entities whose :attr value is
// NOT equal to the input Identity.
//
// See: docs/bugs/active/IDENTITY_INPUT_MATCHING_BUG.md
func TestIdentityInputMatchingBug(t *testing.T) {
	// Create temporary directory for test database
	dir, err := os.MkdirTemp("", "identity-input-bug-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create database
	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Define attributes
	refAttr := datalog.NewKeyword(":test/ref")
	codeAttr := datalog.NewKeyword(":test/code")

	// Create two different "parent" entities
	parent1 := datalog.NewIdentity(generateUUID())
	parent2 := datalog.NewIdentity(generateUUID())

	t.Logf("parent1: %v", parent1)
	t.Logf("parent2: %v", parent2)

	// Create child1 referencing parent1
	child1 := datalog.NewIdentity(generateUUID())
	tx1 := db.NewTransaction()
	tx1.Add(child1, refAttr, parent1)
	tx1.Add(child1, codeAttr, "child1")
	if _, err := tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit child1: %v", err)
	}

	// Create child2 referencing parent2
	child2 := datalog.NewIdentity(generateUUID())
	tx2 := db.NewTransaction()
	tx2.Add(child2, refAttr, parent2)
	tx2.Add(child2, codeAttr, "child2")
	if _, err := tx2.Commit(); err != nil {
		t.Fatalf("Failed to commit child2: %v", err)
	}

	t.Logf("child1: %v (refs parent1)", child1)
	t.Logf("child2: %v (refs parent2)", child2)

	// Query: find children of parent1
	tuples, err := executor.CollectTuples(db.Query(
		`[:find ?c ?code :in $ ?parent :where [?c :test/ref ?parent] [?c :test/code ?code]]`,
		parent1,
	))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	t.Logf("Query for parent1's children returned %d results:", len(tuples))
	for i, tuple := range tuples {
		t.Logf("  Result %d: ID=%v Code=%v", i, tuple[0], tuple[1])
	}

	// We should find exactly 1 child: child1 with code "child1"
	if len(tuples) != 1 {
		t.Errorf("Expected 1 result, got %d", len(tuples))
		if len(tuples) == 2 {
			t.Error("Identity input not filtering correctly - matching both children")
		}
		if len(tuples) == 0 {
			t.Error("Identity input not matching at all")
		}
		return
	}

	// Verify we got the RIGHT child
	foundID, ok := tuples[0][0].(datalog.Identity)
	if !ok {
		t.Fatalf("Result is not an Identity: %T", tuples[0][0])
	}

	// IMPORTANT: Use .Equal() to compare Identities by hash, not struct equality!
	// Identity structs from storage have different l85/str fields than the originals
	if !foundID.Equal(child1) {
		t.Errorf("Wrong child returned:\n  expected hash: %x\n  got hash:      %x\n  expected L85: %v\n  got L85:      %v",
			child1.Hash(), foundID.Hash(), child1.L85(), foundID.L85())

		// Debug: what is the actual ref of the found entity?
		refs, _ := executor.CollectTuples(db.Query(
			`[:find ?ref :in $ ?e :where [?e :test/ref ?ref]]`,
			foundID,
		))
		if len(refs) > 0 {
			t.Logf("Found entity's actual ref (L85): %v", refs[0][0])
			t.Logf("Our parent1 (L85):               %v", parent1.L85())
			if refID, ok := refs[0][0].(datalog.Identity); ok && refID != nil {
				t.Logf("Refs equal (by hash): %v", refID.Equal(parent1))
				t.Logf("Refs hash match: expected=%x got=%x", parent1.Hash(), refID.Hash())
			}
		}
	}

	code, ok := tuples[0][1].(string)
	if !ok || code != "child1" {
		t.Errorf("Wrong code: expected 'child1', got %v", tuples[0][1])
	}
}

// TestIdentityInputMatchingWithMultipleConstraints tests Identity matching with
// additional constraints (similar to real-world entity queries).
func TestIdentityInputMatchingWithMultipleConstraints(t *testing.T) {
	// Create temporary directory for test database
	dir, err := os.MkdirTemp("", "identity-input-multi-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create database
	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Define attributes (simulating a real entity schema)
	mapAttr := datalog.NewKeyword(":entity/map")
	codeAttr := datalog.NewKeyword(":entity/code")
	typeAttr := datalog.NewKeyword(":entity/type")
	dungeonType := datalog.NewKeyword(":entity.type/dungeon")

	// Create a map entity
	mapID := datalog.NewIdentity(generateUUID())
	tx := db.NewTransaction()
	tx.Add(mapID, datalog.NewKeyword(":map/id"), mapID.L85())
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Create a dungeon entity belonging to this map
	dungeonID := datalog.NewIdentity(generateUUID())
	tx2 := db.NewTransaction()
	tx2.Add(dungeonID, typeAttr, dungeonType)
	tx2.Add(dungeonID, codeAttr, "POI A")
	tx2.Add(dungeonID, mapAttr, mapID)
	if _, err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}

	t.Logf("Created map: %v", mapID)
	t.Logf("Created dungeon: %v with map=%v", dungeonID, mapID)

	// Query: find dungeon by map, code, and type
	tuples, err := executor.CollectTuples(db.Query(
		`[:find ?e :in $ ?map ?code :where [?e :entity/map ?map] [?e :entity/code ?code] [?e :entity/type :entity.type/dungeon]]`,
		mapID, "POI A",
	))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	t.Logf("Query returned %d results", len(tuples))

	if len(tuples) != 1 {
		t.Errorf("Expected 1 result, got %d", len(tuples))
		return
	}

	foundID, ok := tuples[0][0].(datalog.Identity)
	if !ok {
		t.Fatalf("Result is not an Identity: %T", tuples[0][0])
	}

	t.Logf("Found ID (L85): %v", foundID.L85())

	// IMPORTANT: Use .Equal() to compare Identities by hash, not struct equality!
	if !foundID.Equal(dungeonID) {
		t.Errorf("Wrong entity returned:\n  expected hash: %x\n  got hash:      %x", dungeonID.Hash(), foundID.Hash())

		// Debug: what are the actual attributes of the found entity?
		attrs, _ := executor.CollectTuples(db.Query(
			`[:find ?a ?v :in $ ?e :where [?e ?a ?v]]`,
			foundID,
		))
		t.Logf("Found entity attributes:")
		for _, attr := range attrs {
			t.Logf("  %v = %v", attr[0], attr[1])
		}
	}
}
