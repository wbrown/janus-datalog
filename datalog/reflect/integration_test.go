package reflect_test

import (
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	dlreflect "github.com/wbrown/janus-datalog/datalog/reflect"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// Test types for integration testing
type Person struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name string           `datalog:"name"`
	Age  int64            `datalog:"age"`
}

type PersonWithFriends struct {
	ID      datalog.Identity     `datalog:"-,id"`
	Name    string               `datalog:"name"`
	Age     int64                `datalog:"age"`
	Friends []*PersonWithFriends `datalog:"friends"`
}

type PersonWithTags struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name string           `datalog:"name"`
	Tags []string         `datalog:"tags"`
}

// EntityWithKeyword tests that Keyword fields (pointer type alias) are handled correctly
// This is a regression test for the bug where *keyword was incorrectly dereferenced
// and treated as a nested struct requiring an ID field.
type EntityWithKeyword struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name string           `datalog:"name"`
	Type datalog.Keyword  `datalog:"type"`
}

func TestSchemaFromStruct(t *testing.T) {
	schema, err := dlreflect.SchemaFromStruct(Person{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check that attributes are defined
	nameAttr := schema.GetAttribute(datalog.NewKeyword(":person/name"))
	if nameAttr == nil {
		t.Fatal("expected :person/name attribute")
	}
	if nameAttr.ValueType != "db.type/string" {
		t.Errorf("expected name to be string, got %s", nameAttr.ValueType)
	}

	ageAttr := schema.GetAttribute(datalog.NewKeyword(":person/age"))
	if ageAttr == nil {
		t.Fatal("expected :person/age attribute")
	}
	if ageAttr.ValueType != "db.type/long" {
		t.Errorf("expected age to be long, got %s", ageAttr.ValueType)
	}
}

// TestSaveStruct_KeywordField is a regression test for a bug where Keyword fields
// (which are pointer type aliases *keyword) were incorrectly dereferenced and
// treated as nested entity structs requiring an ID field.
func TestSaveStruct_KeywordField(t *testing.T) {
	// Create temp database
	tmpDir, err := os.MkdirTemp("", "reflect-keyword-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Generate schema from struct
	sch, err := dlreflect.SchemaFromStruct(EntityWithKeyword{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	// Verify the type attribute is correctly typed as keyword
	typeAttr := sch.GetAttribute(datalog.NewKeyword(":entity-with-keyword/type"))
	if typeAttr == nil {
		t.Fatal("expected :entity-with-keyword/type attribute")
	}
	if typeAttr.ValueType != schema.TypeKeyword {
		t.Errorf("expected type to be keyword, got %s", typeAttr.ValueType)
	}

	// Create database with schema
	db, err := storage.NewDatabaseWithSchema(tmpDir, sch)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create and write an entity with a Keyword field
	entity := EntityWithKeyword{
		Name: "TestEntity",
		Type: datalog.NewKeyword(":crawl/page"),
	}
	tx := db.NewTransaction()
	entityID, err := tx.SaveStruct(&entity)
	if err != nil {
		// This was the bug: "field Type: nested struct keyword has no ID field"
		t.Fatalf("failed to save struct with Keyword field: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Read back using PullInto
	var loaded EntityWithKeyword
	if err := db.PullInto(entityID, &loaded); err != nil {
		t.Fatalf("failed to pull: %v", err)
	}

	// Verify the keyword was stored and retrieved correctly
	if loaded.Name != "TestEntity" {
		t.Errorf("expected name 'TestEntity', got %q", loaded.Name)
	}
	if loaded.Type == nil {
		t.Error("expected Type to be non-nil")
	} else if loaded.Type.String() != ":crawl/page" {
		t.Errorf("expected Type ':crawl/page', got %q", loaded.Type.String())
	}

	// Verify it's the same interned keyword
	if loaded.Type != datalog.NewKeyword(":crawl/page") {
		t.Error("expected Type to be the same interned keyword pointer")
	}
}

func TestSaveStructAndPullInto_Simple(t *testing.T) {
	// Create temp database
	tmpDir, err := os.MkdirTemp("", "reflect-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Generate schema from struct
	schema, err := dlreflect.SchemaFromStruct(Person{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	// Create database with schema
	db, err := storage.NewDatabaseWithSchema(tmpDir, schema)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create and write a person
	alice := Person{Name: "Alice", Age: 30}
	tx := db.NewTransaction()
	aliceID, err := tx.SaveStruct(&alice)
	if err != nil {
		t.Fatalf("failed to add struct: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify ID was set on struct
	var zeroHash [20]byte
	if alice.ID.Hash() == zeroHash {
		t.Error("expected alice.ID to be set")
	}

	// Read back using PullInto
	var loaded Person
	if err := db.PullInto(aliceID, &loaded); err != nil {
		t.Fatalf("failed to pull: %v", err)
	}

	// Verify values
	if loaded.Name != "Alice" {
		t.Errorf("expected Name=Alice, got %s", loaded.Name)
	}
	if loaded.Age != 30 {
		t.Errorf("expected Age=30, got %d", loaded.Age)
	}
}

func TestSaveStructAndPullInto_CardinalityMany(t *testing.T) {
	// Create temp database
	tmpDir, err := os.MkdirTemp("", "reflect-test-many")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Generate schema from struct
	schema, err := dlreflect.SchemaFromStruct(PersonWithTags{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	// Create database with schema
	db, err := storage.NewDatabaseWithSchema(tmpDir, schema)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create and write a person with tags
	alice := PersonWithTags{
		Name: "Alice",
		Tags: []string{"developer", "team-lead", "mentor"},
	}
	tx := db.NewTransaction()
	aliceID, err := tx.SaveStruct(&alice)
	if err != nil {
		t.Fatalf("failed to add struct: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Read back using PullInto
	var loaded PersonWithTags
	if err := db.PullInto(aliceID, &loaded); err != nil {
		t.Fatalf("failed to pull: %v", err)
	}

	// Verify values
	if loaded.Name != "Alice" {
		t.Errorf("expected Name=Alice, got %s", loaded.Name)
	}
	if len(loaded.Tags) != 3 {
		t.Errorf("expected 3 tags, got %d", len(loaded.Tags))
	}
}

func TestGeneratePullPattern(t *testing.T) {
	pattern := dlreflect.GeneratePullPattern(Person{}, nil)
	if pattern == "" {
		t.Error("expected non-empty pattern")
	}

	// Should contain the attributes
	expected := "[:person/name :person/age]"
	if pattern != expected {
		t.Errorf("expected pattern=%s, got %s", expected, pattern)
	}
}

func TestGeneratePullPattern_Nested(t *testing.T) {
	pattern := dlreflect.GeneratePullPattern(PersonWithFriends{}, nil)
	if pattern == "" {
		t.Error("expected non-empty pattern")
	}

	// Should contain nested pattern for friends
	// Due to recursion handling, it should include the nested pattern
	t.Logf("Generated pattern: %s", pattern)
}

func TestSaveStruct_WithExistingID(t *testing.T) {
	// Create temp database
	tmpDir, err := os.MkdirTemp("", "reflect-test-existing-id")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := storage.NewDatabase(tmpDir)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create person with explicit ID
	explicitID := datalog.NewIdentity("alice-unique-id")
	alice := Person{
		ID:   explicitID,
		Name: "Alice",
		Age:  30,
	}

	tx := db.NewTransaction()
	returnedID, err := tx.SaveStruct(&alice)
	if err != nil {
		t.Fatalf("failed to add struct: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Should use the existing ID
	if returnedID.Hash() != explicitID.Hash() {
		t.Error("expected returned ID to match explicit ID")
	}
}

func TestPullIntoMany(t *testing.T) {
	// Create temp database
	tmpDir, err := os.MkdirTemp("", "reflect-test-many-entities")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	schema, err := dlreflect.SchemaFromStruct(Person{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	db, err := storage.NewDatabaseWithSchema(tmpDir, schema)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create multiple people
	people := []Person{
		{Name: "Alice", Age: 30},
		{Name: "Bob", Age: 25},
		{Name: "Carol", Age: 35},
	}

	var ids []datalog.Identity
	tx := db.NewTransaction()
	for i := range people {
		id, err := tx.SaveStruct(&people[i])
		if err != nil {
			t.Fatalf("failed to add struct: %v", err)
		}
		ids = append(ids, id)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Read back using PullIntoMany
	var loaded []Person
	if err := db.PullIntoMany(ids, &loaded); err != nil {
		t.Fatalf("failed to pull many: %v", err)
	}

	// Verify all people loaded
	if len(loaded) != 3 {
		t.Fatalf("expected 3 people, got %d", len(loaded))
	}

	// Verify names
	names := make(map[string]bool)
	for i, p := range loaded {
		if p.Name == "" {
			t.Logf("person %d has empty name, Age=%d", i, p.Age)
		}
		names[p.Name] = true
	}
	if !names["Alice"] || !names["Bob"] || !names["Carol"] {
		t.Errorf("expected to find all names, found: %v", names)
		// Debug: check original data
		for i, p := range people {
			t.Logf("original person %d: Name=%q, Age=%d, ID=%s", i, p.Name, p.Age, p.ID.String()[:20])
		}
	}
}

func TestSaveStructCardinalityOne(t *testing.T) {
	// Create temp database
	tmpDir, err := os.MkdirTemp("", "reflect-test-update")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	schema, err := dlreflect.SchemaFromStruct(Person{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	db, err := storage.NewDatabaseWithSchema(tmpDir, schema)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create initial person
	alice := Person{Name: "Alice", Age: 30}

	tx := db.NewTransaction()
	id, err := tx.SaveStruct(&alice)
	if err != nil {
		t.Fatalf("failed to add struct: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	t.Logf("Created person with ID: %s", id.L85())

	// Verify initial state
	var loaded Person
	if err := db.PullInto(id, &loaded); err != nil {
		t.Fatalf("failed to pull: %v", err)
	}
	if loaded.Name != "Alice" || loaded.Age != 30 {
		t.Fatalf("initial load failed: got Name=%q Age=%d", loaded.Name, loaded.Age)
	}

	// Update the person
	alice.Age = 31
	alice.Name = "Alice Smith"

	tx2 := db.NewTransaction()
	if _, err := tx2.SaveStruct(&alice); err != nil {
		t.Fatalf("failed to save struct: %v", err)
	}
	if _, err := tx2.Commit(); err != nil {
		t.Fatalf("failed to commit update: %v", err)
	}

	// Verify updated state
	var updated Person
	if err := db.PullInto(id, &updated); err != nil {
		t.Fatalf("failed to pull updated: %v", err)
	}

	if updated.Name != "Alice Smith" {
		t.Errorf("expected Name='Alice Smith', got %q", updated.Name)
	}
	if updated.Age != 31 {
		t.Errorf("expected Age=31, got %d", updated.Age)
	}

	// Verify there's only ONE age value (not both 30 and 31)
	// Query for all age values of this entity
	results, err := executor.CollectTuples(db.Query(
		`[:find ?age :in $ ?e :where [?e :person/age ?age]]`,
		id,
	))
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 age value, got %d: %v", len(results), results)
	}
}

func TestSaveStructCardinalityMany(t *testing.T) {
	// Create temp database
	tmpDir, err := os.MkdirTemp("", "reflect-test-update-many")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	schema, err := dlreflect.SchemaFromStruct(PersonWithTags{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	db, err := storage.NewDatabaseWithSchema(tmpDir, schema)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create initial person with tags
	alice := PersonWithTags{Name: "Alice", Tags: []string{"developer", "golang"}}

	tx := db.NewTransaction()
	id, err := tx.SaveStruct(&alice)
	if err != nil {
		t.Fatalf("failed to add struct: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify initial tags
	results, err := executor.CollectTuples(db.Query(
		`[:find ?tag :in $ ?e :where [?e :person-with-tags/tags ?tag]]`,
		id,
	))
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 tags initially, got %d", len(results))
	}

	// Update - should replace all tags (diff-based)
	alice.Tags = []string{"architect", "rust"}

	tx2 := db.NewTransaction()
	if _, err := tx2.SaveStruct(&alice); err != nil {
		t.Fatalf("failed to save struct: %v", err)
	}
	if _, err := tx2.Commit(); err != nil {
		t.Fatalf("failed to commit update: %v", err)
	}

	// Verify only new tags exist
	results, err = executor.CollectTuples(db.Query(
		`[:find ?tag :in $ ?e :where [?e :person-with-tags/tags ?tag]]`,
		id,
	))
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 tags after replace, got %d", len(results))
	}

	// Check we have the right tags
	tags := make(map[string]bool)
	for _, r := range results {
		if s, ok := r[0].(string); ok {
			tags[s] = true
		}
	}
	if !tags["architect"] || !tags["rust"] {
		t.Errorf("expected architect and rust tags, got: %v", tags)
	}
	if tags["developer"] || tags["golang"] {
		t.Errorf("old tags should have been removed, got: %v", tags)
	}
}

// TestNilVsEmptySliceSemantics verifies that nil slices are skipped (leave existing)
// while empty slices clear existing values.
func TestNilVsEmptySliceSemantics(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "reflect-test-nil-empty")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	schema, err := dlreflect.SchemaFromStruct(PersonWithTags{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	db, err := storage.NewDatabaseWithSchema(tmpDir, schema)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create person with tags
	alice := PersonWithTags{Name: "Alice", Tags: []string{"developer", "golang"}}
	tx := db.NewTransaction()
	id, err := tx.SaveStruct(&alice)
	if err != nil {
		t.Fatalf("SaveStruct failed: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Test 1: Nil slice should leave existing tags alone
	partialUpdate := PersonWithTags{ID: id, Name: "Alice Smith", Tags: nil}
	tx2 := db.NewTransaction()
	if _, err := tx2.SaveStruct(&partialUpdate); err != nil {
		t.Fatalf("SaveStruct with nil tags failed: %v", err)
	}
	if _, err := tx2.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Verify tags unchanged
	results, err := executor.CollectTuples(db.Query(
		`[:find ?tag :in $ ?e :where [?e :person-with-tags/tags ?tag]]`,
		id,
	))
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("nil slice should leave tags unchanged, expected 2, got %d: %v", len(results), results)
	}

	// Test 2: Empty slice (not nil) should clear all tags
	emptyTags := make([]string, 0) // Empty but not nil
	clearUpdate := PersonWithTags{ID: id, Name: "Alice Smith", Tags: emptyTags}
	tx3 := db.NewTransaction()
	if _, err := tx3.SaveStruct(&clearUpdate); err != nil {
		t.Fatalf("SaveStruct with empty tags failed: %v", err)
	}
	if _, err := tx3.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Verify tags cleared
	results, err = executor.CollectTuples(db.Query(
		`[:find ?tag :in $ ?e :where [?e :person-with-tags/tags ?tag]]`,
		id,
	))
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("empty slice should clear tags, expected 0, got %d: %v", len(results), results)
	}
}

// TestSaveStructUpsertSemantics verifies that SaveStruct uses upsert semantics
// so calling it twice on the same entity properly updates values instead of duplicating.
func TestSaveStructUpsertSemantics(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "reflect-test-upsert")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	schema, err := dlreflect.SchemaFromStruct(Person{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	db, err := storage.NewDatabaseWithSchema(tmpDir, schema)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// First add
	alice := Person{Name: "Alice", Age: 30}
	tx := db.NewTransaction()
	id, err := tx.SaveStruct(&alice)
	if err != nil {
		t.Fatalf("first SaveStruct failed: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("first commit failed: %v", err)
	}

	// Modify and add again (using the same ID)
	alice.Name = "Alice Smith"
	alice.Age = 31
	// alice.ID should already be set from first SaveStruct

	tx2 := db.NewTransaction()
	id2, err := tx2.SaveStruct(&alice)
	if err != nil {
		t.Fatalf("second SaveStruct failed: %v", err)
	}
	if _, err := tx2.Commit(); err != nil {
		t.Fatalf("second commit failed: %v", err)
	}

	// Verify same ID was used
	if !id.Equal(id2) {
		t.Errorf("expected same ID, got different: %s vs %s", id.L85(), id2.L85())
	}

	// Verify updated values
	var loaded Person
	if err := db.PullInto(id, &loaded); err != nil {
		t.Fatalf("PullInto failed: %v", err)
	}

	if loaded.Name != "Alice Smith" {
		t.Errorf("expected Name='Alice Smith', got %q", loaded.Name)
	}
	if loaded.Age != 31 {
		t.Errorf("expected Age=31, got %d", loaded.Age)
	}

	// Verify only one value for each attribute (no duplicates)
	results, err := executor.CollectTuples(db.Query(
		`[:find ?age :in $ ?e :where [?e :person/age ?age]]`,
		id,
	))
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 age value (upsert), got %d: %v", len(results), results)
	}
}

// PersonWithFriendRefs tests cardinality-many ref slices ([]datalog.Identity)
type PersonWithFriendRefs struct {
	ID      datalog.Identity   `datalog:"-,id"`
	Name    string             `datalog:"name"`
	Friends []datalog.Identity `datalog:"friends"`
}

// TestPullInto_CardinalityManyRefs verifies that PullInto correctly loads
// all elements of cardinality-many ref slices ([]datalog.Identity).
// This is a regression test for a bug where only the first ref was loaded.
func TestPullInto_CardinalityManyRefs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "reflect-test-many-refs")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	schema, err := dlreflect.SchemaFromStruct(PersonWithFriendRefs{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	db, err := storage.NewDatabaseWithSchema(tmpDir, schema)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create 5 friend entities
	friends := make([]datalog.Identity, 5)
	friendNames := []string{"Bob", "Carol", "Dave", "Eve", "Frank"}
	for i, name := range friendNames {
		friend := &PersonWithFriendRefs{Name: name}
		tx := db.NewTransaction()
		id, err := tx.SaveStruct(friend)
		if err != nil {
			t.Fatalf("failed to create friend %s: %v", name, err)
		}
		if _, err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit friend %s: %v", name, err)
		}
		friends[i] = id
		t.Logf("Created friend %s with ID: %s", name, id.L85()[:20])
	}

	// Create Alice with all 5 friends
	alice := &PersonWithFriendRefs{
		Name:    "Alice",
		Friends: friends,
	}
	tx := db.NewTransaction()
	aliceID, err := tx.SaveStruct(alice)
	if err != nil {
		t.Fatalf("failed to create Alice: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit Alice: %v", err)
	}
	t.Logf("Created Alice with ID: %s", aliceID.L85()[:20])

	// Verify all 5 friends are stored via direct query
	tuples, err := executor.CollectTuples(db.Query(
		`[:find ?f :in $ ?alice :where [?alice :person-with-friend-refs/friends ?f]]`,
		aliceID,
	))
	if err != nil {
		t.Fatalf("direct query failed: %v", err)
	}
	t.Logf("Direct query found %d friends", len(tuples))
	if len(tuples) != 5 {
		t.Errorf("Direct query: expected 5 friends, got %d", len(tuples))
	}

	// Now test PullInto - this is where the bug manifests
	var loaded PersonWithFriendRefs
	if err := db.PullInto(aliceID, &loaded); err != nil {
		t.Fatalf("PullInto failed: %v", err)
	}

	t.Logf("PullInto loaded %d friends", len(loaded.Friends))
	if len(loaded.Friends) != 5 {
		t.Errorf("PullInto: expected 5 friends, got %d (BUG: only first ref loaded)", len(loaded.Friends))
		// Show which friends were loaded
		for i, f := range loaded.Friends {
			t.Logf("  loaded.Friends[%d] = %s", i, f.L85()[:20])
		}
	}

	// Verify Name was loaded correctly
	if loaded.Name != "Alice" {
		t.Errorf("expected Name='Alice', got %q", loaded.Name)
	}
}

// TestPullInto_CardinalityManyRefs_ManualSchema tests the same scenario
// but with a manually-defined schema (like real-world usage) instead of SchemaFromStruct.
// This reproduces a bug seen in narrative-generators where PullInto only returns the first ref.
func TestPullInto_CardinalityManyRefs_ManualSchema(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "reflect-test-manual-schema")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Manually define schema (like real-world usage)
	builder := schema.NewBuilder()
	builder.Attribute(":person-with-friend-refs/name").Type(schema.TypeString).Add()
	builder.Attribute(":person-with-friend-refs/friends").Type(schema.TypeRef).Many().Add()
	manualSchema, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build schema: %v", err)
	}

	db, err := storage.NewDatabaseWithSchema(tmpDir, manualSchema)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create 5 friend entities
	friends := make([]datalog.Identity, 5)
	friendNames := []string{"Bob", "Carol", "Dave", "Eve", "Frank"}
	for i, name := range friendNames {
		friend := &PersonWithFriendRefs{Name: name}
		tx := db.NewTransaction()
		id, err := tx.SaveStruct(friend)
		if err != nil {
			t.Fatalf("failed to create friend %s: %v", name, err)
		}
		if _, err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit friend %s: %v", name, err)
		}
		friends[i] = id
		t.Logf("Created friend %s with ID: %s", name, id.L85()[:20])
	}

	// Create Alice with all 5 friends
	alice := &PersonWithFriendRefs{
		Name:    "Alice",
		Friends: friends,
	}
	tx := db.NewTransaction()
	aliceID, err := tx.SaveStruct(alice)
	if err != nil {
		t.Fatalf("failed to create Alice: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit Alice: %v", err)
	}
	t.Logf("Created Alice with ID: %s", aliceID.L85()[:20])

	// Verify all 5 friends are stored via direct query
	tuples, err := executor.CollectTuples(db.Query(
		`[:find ?f :in $ ?alice :where [?alice :person-with-friend-refs/friends ?f]]`,
		aliceID,
	))
	if err != nil {
		t.Fatalf("direct query failed: %v", err)
	}
	t.Logf("Direct query found %d friends", len(tuples))
	if len(tuples) != 5 {
		t.Errorf("Direct query: expected 5 friends, got %d", len(tuples))
	}

	// Now test PullInto - this is where the bug manifests with manual schema
	var loaded PersonWithFriendRefs
	if err := db.PullInto(aliceID, &loaded); err != nil {
		t.Fatalf("PullInto failed: %v", err)
	}

	t.Logf("PullInto loaded %d friends", len(loaded.Friends))
	if len(loaded.Friends) != 5 {
		t.Errorf("PullInto with manual schema: expected 5 friends, got %d (BUG: only first ref loaded)", len(loaded.Friends))
		for i, f := range loaded.Friends {
			t.Logf("  loaded.Friends[%d] = %s", i, f.L85()[:20])
		}
	}
}

// TestLookupAttributeWithStructAPI verifies that LookupAttribute works with
// entities and attributes created via the struct reflection API.
// This test validates the integration between SaveStruct and LookupAttribute.
func TestLookupAttributeWithStructAPI(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "reflect-test-lookup")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	schema, err := dlreflect.SchemaFromStruct(Person{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	db, err := storage.NewDatabaseWithSchema(tmpDir, schema)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create person via struct API
	alice := Person{Name: "Alice", Age: 30}
	tx := db.NewTransaction()
	id, err := tx.SaveStruct(&alice)
	if err != nil {
		t.Fatalf("failed to add struct: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	t.Logf("Created entity with ID: %s", id.L85())

	// Test LookupAttribute
	matcher := storage.NewBadgerMatcher(db.Store())

	// Lookup name
	nameAttr := datalog.NewKeyword(":person/name")
	val, found := matcher.LookupAttribute(id, nameAttr)
	if !found {
		t.Errorf("LookupAttribute failed to find :person/name for entity %s", id.L85())
	} else {
		t.Logf("Found name: %v (type: %T)", val, val)
		if val != "Alice" {
			t.Errorf("expected name='Alice', got %v", val)
		}
	}

	// Lookup age
	ageAttr := datalog.NewKeyword(":person/age")
	val, found = matcher.LookupAttribute(id, ageAttr)
	if !found {
		t.Errorf("LookupAttribute failed to find :person/age for entity %s", id.L85())
	} else {
		t.Logf("Found age: %v (type: %T)", val, val)
		if val != int64(30) {
			t.Errorf("expected age=30, got %v", val)
		}
	}

	// Verify query also works (sanity check)
	results, err := executor.CollectTuples(db.Query(
		`[:find ?name ?age :in $ ?e :where [?e :person/name ?name] [?e :person/age ?age]]`,
		id,
	))
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 query result, got %d", len(results))
	} else {
		t.Logf("Query result: name=%v age=%v", results[0][0], results[0][1])
	}
}

// ============================================================================
// Annotation tests for ReflectContext
// ============================================================================

// TestStructReader_AnnotationsEmitted verifies that StructReader emits annotation events.
func TestStructReader_AnnotationsEmitted(t *testing.T) {
	// Create test data to read
	pullResult := map[string]interface{}{
		"person/name": "Alice",
		"person/age":  int64(30),
	}

	// Create struct reader with handler
	var events []annotations.Event
	handler := func(e annotations.Event) {
		events = append(events, e)
	}

	reader, err := dlreflect.NewStructReaderWithHandler(&Person{}, nil, handler)
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}

	// Read into struct
	var loaded Person
	if err := reader.Read(pullResult, &loaded); err != nil {
		t.Fatalf("read failed: %v", err)
	}

	// Verify struct was loaded correctly
	if loaded.Name != "Alice" {
		t.Errorf("expected Name=Alice, got %s", loaded.Name)
	}
	if loaded.Age != 30 {
		t.Errorf("expected Age=30, got %d", loaded.Age)
	}

	// Verify events were emitted
	if len(events) == 0 {
		t.Fatal("expected annotation events to be emitted, got none")
	}

	// Check for read begin/complete events
	var foundBegin, foundComplete bool
	for _, e := range events {
		if e.Name == annotations.ReflectReadBegin {
			foundBegin = true
			if e.Data["struct_type"] != "Person" {
				t.Errorf("expected struct_type=Person, got %v", e.Data["struct_type"])
			}
			if e.Data["input_keys"] != 2 {
				t.Errorf("expected input_keys=2, got %v", e.Data["input_keys"])
			}
		}
		if e.Name == annotations.ReflectReadComplete {
			foundComplete = true
			if e.Data["success"] != true {
				t.Errorf("expected success=true, got %v", e.Data["success"])
			}
		}
	}

	if !foundBegin {
		t.Error("expected reflect/read.begin event")
	}
	if !foundComplete {
		t.Error("expected reflect/read.complete event")
	}
}

// TestStructReader_NoEventsWhenHandlerNil verifies zero-overhead when no handler.
func TestStructReader_NoEventsWhenHandlerNil(t *testing.T) {
	pullResult := map[string]interface{}{
		"person/name": "Alice",
		"person/age":  int64(30),
	}

	// Create reader without handler
	reader, err := dlreflect.NewStructReader(&Person{}, nil)
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}

	// Read into struct - should work without panic
	var loaded Person
	if err := reader.Read(pullResult, &loaded); err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if loaded.Name != "Alice" {
		t.Errorf("expected Name=Alice, got %s", loaded.Name)
	}
}

// TestStructWriter_AnnotationsEmitted verifies that StructWriter emits annotation events.
func TestStructWriter_AnnotationsEmitted(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "reflect-writer-annotation-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	schema, err := dlreflect.SchemaFromStruct(Person{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	db, err := storage.NewDatabaseWithSchema(tmpDir, schema)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create writer with handler
	var events []annotations.Event
	handler := func(e annotations.Event) {
		events = append(events, e)
	}

	alice := Person{Name: "Alice", Age: 30}
	writer, err := dlreflect.NewStructWriterWithHandler(&alice, schema, handler)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Write struct
	tx := db.NewTransaction()
	entityID := datalog.NewIdentity("test:alice")
	if err := writer.Write(tx, entityID, &alice); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Verify events were emitted
	if len(events) == 0 {
		t.Fatal("expected annotation events to be emitted, got none")
	}

	// Check for write begin/complete events
	var foundBegin, foundComplete bool
	for _, e := range events {
		if e.Name == annotations.ReflectWriteBegin {
			foundBegin = true
			if e.Data["struct_type"] != "Person" {
				t.Errorf("expected struct_type=Person, got %v", e.Data["struct_type"])
			}
		}
		if e.Name == annotations.ReflectWriteComplete {
			foundComplete = true
			if e.Data["success"] != true {
				t.Errorf("expected success=true, got %v", e.Data["success"])
			}
		}
	}

	if !foundBegin {
		t.Error("expected reflect/write.begin event")
	}
	if !foundComplete {
		t.Error("expected reflect/write.complete event")
	}
}

// WorldEntity is a reproduction test struct for a downstream bug where
// SaveStruct/PullInto failed with structs containing Keyword and Identity fields.
// Mimics the structure from narrative-generators/pkg/schema/entities.go:353
type WorldEntity struct {
	ID            datalog.Identity   `datalog:"-,id"`
	Type          datalog.Keyword    `datalog:"entity/type"`
	Map           datalog.Identity   `datalog:"entity/map"`
	Name          string             `datalog:"entity/name"`
	IdeaGenre     string             `datalog:"idea/genre"`
	IdeaSubgenre  string             `datalog:"idea/subgenre"`
	IdeaPower     string             `datalog:"idea/power"`
	IdeaIntensity string             `datalog:"idea/intensity"`
	IdeaScope     string             `datalog:"idea/scope"`
	IdeaCulture   string             `datalog:"idea/culture"`
	IdeaElement   string             `datalog:"idea/element"`
	IdeaTags      []string           `datalog:"idea/tag"`
	Nations       []datalog.Identity `datalog:"entity/nations"`
}

// TestSaveStructAndPullInto_WorldEntity is a reproduction test for a downstream bug
// where SaveStruct/PullInto failed with structs containing Keyword and Identity fields.
// The bug manifested as: entities saved with SaveStruct came back empty on PullInto.
func TestSaveStructAndPullInto_WorldEntity(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "reflect-world-entity-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Generate schema from struct
	sch, err := dlreflect.SchemaFromStruct(WorldEntity{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	// Create database with schema
	db, err := storage.NewDatabaseWithSchema(tmpDir, sch)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create a map entity first (for the Map reference)
	mapID := datalog.NewIdentity("map:test-map-1")

	// Create the world entity with all field types
	worldType := datalog.NewKeyword(":entity.type/world")
	world := WorldEntity{
		Type:          worldType,
		Map:           mapID,
		Name:          "Test World",
		IdeaGenre:     "fantasy",
		IdeaSubgenre:  "high fantasy",
		IdeaPower:     "magic",
		IdeaIntensity: "dark",
		IdeaScope:     "continental",
		IdeaCulture:   "Nordic",
		IdeaElement:   "dark lord",
		IdeaTags:      []string{"dragons", "undead", "magic", "medieval"},
	}

	// Save the world entity
	tx := db.NewTransaction()
	worldID, err := tx.SaveStruct(&world)
	if err != nil {
		t.Fatalf("SaveStruct failed: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	t.Logf("Saved world entity with ID: %s", worldID.L85())

	// Pull it back
	var loaded WorldEntity
	if err := db.PullInto(worldID, &loaded); err != nil {
		t.Fatalf("PullInto failed: %v", err)
	}

	// Verify all fields came back correctly
	t.Run("ID", func(t *testing.T) {
		if loaded.ID == nil {
			t.Error("expected ID to be set")
		} else if !loaded.ID.Equal(worldID) {
			t.Errorf("expected ID %s, got %s", worldID.L85(), loaded.ID.L85())
		}
	})

	t.Run("Type_Keyword", func(t *testing.T) {
		if loaded.Type == nil {
			t.Error("expected Type to be non-nil")
		} else if loaded.Type.String() != ":entity.type/world" {
			t.Errorf("expected Type ':entity.type/world', got %q", loaded.Type.String())
		}
		// Verify interning
		if loaded.Type != worldType {
			t.Error("expected Type to be same interned keyword pointer")
		}
	})

	t.Run("Map_Identity", func(t *testing.T) {
		if loaded.Map == nil {
			t.Error("expected Map to be non-nil")
		} else if !loaded.Map.Equal(mapID) {
			t.Errorf("expected Map %s, got %s", mapID.L85(), loaded.Map.L85())
		}
	})

	t.Run("Name_String", func(t *testing.T) {
		if loaded.Name != "Test World" {
			t.Errorf("expected Name 'Test World', got %q", loaded.Name)
		}
	})

	t.Run("IdeaGenre", func(t *testing.T) {
		if loaded.IdeaGenre != "fantasy" {
			t.Errorf("expected IdeaGenre 'fantasy', got %q", loaded.IdeaGenre)
		}
	})

	t.Run("IdeaSubgenre", func(t *testing.T) {
		if loaded.IdeaSubgenre != "high fantasy" {
			t.Errorf("expected IdeaSubgenre 'high fantasy', got %q", loaded.IdeaSubgenre)
		}
	})

	t.Run("IdeaPower", func(t *testing.T) {
		if loaded.IdeaPower != "magic" {
			t.Errorf("expected IdeaPower 'magic', got %q", loaded.IdeaPower)
		}
	})

	t.Run("IdeaIntensity", func(t *testing.T) {
		if loaded.IdeaIntensity != "dark" {
			t.Errorf("expected IdeaIntensity 'dark', got %q", loaded.IdeaIntensity)
		}
	})

	t.Run("IdeaScope", func(t *testing.T) {
		if loaded.IdeaScope != "continental" {
			t.Errorf("expected IdeaScope 'continental', got %q", loaded.IdeaScope)
		}
	})

	t.Run("IdeaCulture", func(t *testing.T) {
		if loaded.IdeaCulture != "Nordic" {
			t.Errorf("expected IdeaCulture 'Nordic', got %q", loaded.IdeaCulture)
		}
	})

	t.Run("IdeaElement", func(t *testing.T) {
		if loaded.IdeaElement != "dark lord" {
			t.Errorf("expected IdeaElement 'dark lord', got %q", loaded.IdeaElement)
		}
	})

	t.Run("IdeaTags_CardinalityMany", func(t *testing.T) {
		if len(loaded.IdeaTags) != 4 {
			t.Errorf("expected 4 tags, got %d: %v", len(loaded.IdeaTags), loaded.IdeaTags)
		}
		expectedTags := map[string]bool{"dragons": true, "undead": true, "magic": true, "medieval": true}
		for _, tag := range loaded.IdeaTags {
			if !expectedTags[tag] {
				t.Errorf("unexpected tag: %q", tag)
			}
		}
	})

	// Test the exact pattern from downstream: Query to find entity, then PullInto
	// This mimics store_world.go findWorldEntity() + GetWorldTags()
	t.Run("QueryThenPullInto", func(t *testing.T) {
		// First, let's see what's actually stored for this entity
		debugTuples, err := executor.CollectTuples(db.Query(
			`[:find ?a ?v :in $ ?e :where [?e ?a ?v]]`,
			worldID,
		))
		if err != nil {
			t.Fatalf("debug query failed: %v", err)
		}
		t.Logf("Debug: Entity %s has %d attribute/value pairs:", worldID.L85(), len(debugTuples))
		for _, tuple := range debugTuples {
			t.Logf("  %v = %v (type: %T)", tuple[0], tuple[1], tuple[1])
		}

		// Check if the type attribute is stored correctly
		// Note: struct tag "entity/type" contains "/" so it's a full path :entity/type
		typeTuples, err := executor.CollectTuples(db.Query(
			`[:find ?type :in $ ?e :where [?e :entity/type ?type]]`,
			worldID,
		))
		if err != nil {
			t.Fatalf("type query failed: %v", err)
		}
		t.Logf("Type query returned %d tuples", len(typeTuples))
		for _, tuple := range typeTuples {
			t.Logf("  Type value: %v (type: %T)", tuple[0], tuple[0])
		}

		// Query to find the world entity by type and map (like findWorldEntity)
		// Note: struct tags with "/" are treated as full paths, not prefixed with struct name
		tuples, err := executor.CollectTuples(db.Query(
			`[:find ?e :in $ ?map :where
			  [?e :entity/type :entity.type/world]
			  [?e :entity/map ?map]]`,
			mapID,
		))
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(tuples) == 0 {
			t.Fatal("query returned no results - entity not found")
		}

		t.Logf("Query returned %d tuples, first: %T = %v", len(tuples), tuples[0][0], tuples[0][0])

		// Extract the entity ID from query result
		var foundID datalog.Identity
		switch v := tuples[0][0].(type) {
		case datalog.Identity:
			foundID = v
		case *datalog.Identity:
			foundID = *v
		default:
			t.Fatalf("unexpected type from query: %T", tuples[0][0])
		}

		if foundID == nil {
			t.Fatal("foundID is nil")
		}

		t.Logf("Found entity ID: %s", foundID.L85())

		// PullInto using the queried ID
		var pulled WorldEntity
		if err := db.PullInto(foundID, &pulled); err != nil {
			t.Fatalf("PullInto failed: %v", err)
		}

		// Verify the pulled data
		if pulled.Name != "Test World" {
			t.Errorf("expected Name 'Test World', got %q", pulled.Name)
		}
		if pulled.IdeaGenre != "fantasy" {
			t.Errorf("expected IdeaGenre 'fantasy', got %q", pulled.IdeaGenre)
		}
		if pulled.Type == nil || pulled.Type.String() != ":entity.type/world" {
			t.Errorf("expected Type ':entity.type/world', got %v", pulled.Type)
		}
	})
}

// TestStructWriter_NoEventsWhenHandlerNil verifies zero-overhead when no handler.
func TestStructWriter_NoEventsWhenHandlerNil(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "reflect-writer-no-annotation-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	schema, err := dlreflect.SchemaFromStruct(Person{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	db, err := storage.NewDatabaseWithSchema(tmpDir, schema)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create writer without handler
	alice := Person{Name: "Alice", Age: 30}
	writer, err := dlreflect.NewStructWriter(&alice, schema)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Write struct - should work without panic
	tx := db.NewTransaction()
	entityID := datalog.NewIdentity("test:alice")
	if err := writer.Write(tx, entityID, &alice); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Verify data was written
	var loaded Person
	if err := db.PullInto(entityID, &loaded); err != nil {
		t.Fatalf("pull failed: %v", err)
	}
	if loaded.Name != "Alice" {
		t.Errorf("expected Name=Alice, got %s", loaded.Name)
	}
}

// DungeonEntity mimics the downstream testDungeonEntity struct
type DungeonEntity struct {
	ID    datalog.Identity `datalog:"-,id"`
	Type  datalog.Keyword  `datalog:"entity/type"`
	Map   datalog.Identity `datalog:"entity/map"`
	Code  string           `datalog:"entity/code"`
	Hooks []string         `datalog:"dungeon/hooks"`
}

// TestPullInto_CardinalityManyStrings_MultipleValues is a regression test for the
// downstream bug where PullInto only returns the first value for []string fields.
// This reproduces the scenario from narrative-generators where:
// - Raw datoms show all values: [[tag1] [tag2] [tag3]]
// - But PullInto only returns: ["tag1"]
func TestPullInto_CardinalityManyStrings_MultipleValues(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "reflect-test-many-strings")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Generate schema from struct (like downstream does)
	sch, err := dlreflect.SchemaFromStruct(DungeonEntity{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	// Verify the schema correctly marks :dungeon/hooks as Many
	// Note: When tag contains "/" like `datalog:"dungeon/hooks"`, it's the full attr name
	hooksAttr := sch.GetAttribute(datalog.NewKeyword(":dungeon/hooks"))
	if hooksAttr == nil {
		t.Fatal("expected :dungeon/hooks attribute in schema")
	}
	t.Logf("Schema :dungeon/hooks cardinality: %s", hooksAttr.Cardinality)
	if hooksAttr.Cardinality != schema.CardinalityMany {
		t.Errorf("expected :dungeon-entity/hooks to have CardinalityMany, got %s", hooksAttr.Cardinality)
	}

	db, err := storage.NewDatabaseWithSchema(tmpDir, sch)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create a map entity ID (just for the reference)
	mapID := datalog.NewIdentity("map:test-map")

	// Create dungeon with multiple hooks
	dungeon := DungeonEntity{
		Type:  datalog.NewKeyword(":entity.type/dungeon"),
		Map:   mapID,
		Code:  "TEST",
		Hooks: []string{"hook1", "hook2", "hook3"},
	}

	tx := db.NewTransaction()
	dungeonID, err := tx.SaveStruct(&dungeon)
	if err != nil {
		t.Fatalf("SaveStruct failed: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	t.Logf("Created dungeon with ID: %s", dungeonID.L85())

	// Query raw datoms to verify all hooks are stored
	tuples, err := executor.CollectTuples(db.Query(
		`[:find ?v :in $ ?e :where [?e :dungeon/hooks ?v]]`,
		dungeonID,
	))
	if err != nil {
		t.Fatalf("direct query failed: %v", err)
	}
	t.Logf("Raw datoms for :dungeon/hooks: %v (count=%d)", tuples, len(tuples))
	if len(tuples) != 3 {
		t.Errorf("Direct query: expected 3 hooks stored, got %d", len(tuples))
	}

	// Now test PullInto - this is where the bug manifests
	var loaded DungeonEntity
	if err := db.PullInto(dungeonID, &loaded); err != nil {
		t.Fatalf("PullInto failed: %v", err)
	}

	t.Logf("Saved Hooks: %q", dungeon.Hooks)
	t.Logf("Loaded Hooks: %q", loaded.Hooks)

	if len(loaded.Hooks) != 3 {
		t.Errorf("PullInto: expected 3 hooks, got %d (BUG: only first value returned)", len(loaded.Hooks))
		t.Errorf("Expected: %v", dungeon.Hooks)
		t.Errorf("Got: %v", loaded.Hooks)
	}
}
