package reflect_test

import (
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	dlreflect "github.com/wbrown/janus-datalog/datalog/reflect"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// Test types for integration testing
type Person struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name string           `datalog:"name"`
	Age  int64            `datalog:"age"`
}

type PersonWithFriends struct {
	ID      datalog.Identity    `datalog:"-,id"`
	Name    string              `datalog:"name"`
	Age     int64               `datalog:"age"`
	Friends []*PersonWithFriends `datalog:"friends"`
}

type PersonWithTags struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name string           `datalog:"name"`
	Tags []string         `datalog:"tags"`
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

func TestAddStructAndPullInto_Simple(t *testing.T) {
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
	aliceID, err := tx.AddStructAuto(&alice)
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

func TestAddStructAndPullInto_CardinalityMany(t *testing.T) {
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
	aliceID, err := tx.AddStructAuto(&alice)
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

func TestAddStruct_WithExistingID(t *testing.T) {
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
	returnedID, err := tx.AddStructAuto(&alice)
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
		id, err := tx.AddStructAuto(&people[i])
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
