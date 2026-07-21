package reflect_test

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	dlreflect "github.com/wbrown/janus-datalog/datalog/reflect"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// CharacterWithSkills tests cardinality-vector fields
// Skills should be an ordered list where order matters and duplicates are allowed
type CharacterWithSkills struct {
	ID     datalog.Identity `datalog:"-,id"`
	Name   string           `datalog:"name"`
	Skills []string         `datalog:"skills"`
}

// TestReflect_VectorOrderPreserved verifies that vector order is preserved through SaveStruct/PullInto.
func TestReflect_VectorOrderPreserved(t *testing.T) {
	// Manually define schema with CardinalityVector (SchemaFromStruct doesn't support .Vector())
	builder := schema.NewBuilder()
	builder.Attribute(":character-with-skills/name").Type(schema.TypeString).Add()
	builder.Attribute(":character-with-skills/skills").Type(schema.TypeString).Vector().Add()
	sch, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build schema: %v", err)
	}

	// Verify schema has vector cardinality
	skillsAttr := sch.GetAttribute(datalog.NewKeyword(":character-with-skills/skills"))
	if skillsAttr == nil {
		t.Fatal("expected :character-with-skills/skills attribute")
	}
	if skillsAttr.Cardinality != schema.CardinalityVector {
		t.Fatalf("expected CardinalityVector, got %s", skillsAttr.Cardinality)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
				Path:           t.TempDir(),
				Schema:         sch,
				PlannerOptions: &popts,
			})
			if err != nil {
				t.Fatalf("failed to create database: %v", err)
			}
			defer db.Close()

			// Create character with ordered skills - order matters!
			character := CharacterWithSkills{
				Name:   "Alice",
				Skills: []string{"stealth", "archery", "lockpicking"}, // Specific order
			}

			tx := db.NewTransaction()
			charID, err := tx.SaveStruct(&character)
			if err != nil {
				t.Fatalf("SaveStruct failed: %v", err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Pull back and verify order is preserved
			var loaded CharacterWithSkills
			if err := db.PullInto(charID, &loaded); err != nil {
				t.Fatalf("PullInto failed: %v", err)
			}

			t.Logf("Original skills: %v", character.Skills)
			t.Logf("Loaded skills:   %v", loaded.Skills)

			if len(loaded.Skills) != 3 {
				t.Errorf("expected 3 skills, got %d", len(loaded.Skills))
			}

			// Order must be preserved for vectors
			for i, expected := range character.Skills {
				if i >= len(loaded.Skills) {
					t.Errorf("missing skill at position %d: expected %q", i, expected)
					continue
				}
				if loaded.Skills[i] != expected {
					t.Errorf("skill order wrong at position %d: expected %q, got %q", i, expected, loaded.Skills[i])
				}
			}
		})
	}
}

// TestReflect_VectorDuplicatesAllowed verifies that vectors allow duplicate values.
func TestReflect_VectorDuplicatesAllowed(t *testing.T) {
	// Schema with vector cardinality
	builder := schema.NewBuilder()
	builder.Attribute(":character-with-skills/name").Type(schema.TypeString).Add()
	builder.Attribute(":character-with-skills/skills").Type(schema.TypeString).Vector().Add()
	sch, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build schema: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
				Path:           t.TempDir(),
				Schema:         sch,
				PlannerOptions: &popts,
			})
			if err != nil {
				t.Fatalf("failed to create database: %v", err)
			}
			defer db.Close()

			// Create character with duplicate skills - duplicates should be allowed!
			character := CharacterWithSkills{
				Name:   "Bob",
				Skills: []string{"stealth", "archery", "stealth"}, // "stealth" appears twice
			}

			tx := db.NewTransaction()
			charID, err := tx.SaveStruct(&character)
			if err != nil {
				t.Fatalf("SaveStruct failed: %v", err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Pull back and verify duplicates are preserved
			var loaded CharacterWithSkills
			if err := db.PullInto(charID, &loaded); err != nil {
				t.Fatalf("PullInto failed: %v", err)
			}

			t.Logf("Original skills: %v", character.Skills)
			t.Logf("Loaded skills:   %v", loaded.Skills)

			// Vectors allow duplicates - should have 3 elements
			if len(loaded.Skills) != 3 {
				t.Errorf("expected 3 skills (with duplicate), got %d - duplicates were incorrectly deduplicated", len(loaded.Skills))
			}

			// Verify exact content including duplicates
			if len(loaded.Skills) == 3 {
				if loaded.Skills[0] != "stealth" || loaded.Skills[1] != "archery" || loaded.Skills[2] != "stealth" {
					t.Errorf("expected [stealth, archery, stealth], got %v", loaded.Skills)
				}
			}
		})
	}
}

// TestReflect_VectorUpdatePreservesUnchangedPrefix verifies that updating a vector
// uses the prefix-diff optimization (only changes what's different).
func TestReflect_VectorUpdatePreservesUnchangedPrefix(t *testing.T) {
	// Schema with vector cardinality
	builder := schema.NewBuilder()
	builder.Attribute(":character-with-skills/name").Type(schema.TypeString).Add()
	builder.Attribute(":character-with-skills/skills").Type(schema.TypeString).Vector().Add()
	sch, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build schema: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
				Path:           t.TempDir(),
				Schema:         sch,
				PlannerOptions: &popts,
			})
			if err != nil {
				t.Fatalf("failed to create database: %v", err)
			}
			defer db.Close()

			// Create character with initial skills
			character := CharacterWithSkills{
				Name:   "Carol",
				Skills: []string{"stealth", "archery", "lockpicking"},
			}

			tx := db.NewTransaction()
			charID, err := tx.SaveStruct(&character)
			if err != nil {
				t.Fatalf("SaveStruct failed: %v", err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Update: append a new skill (common case)
			character.Skills = []string{"stealth", "archery", "lockpicking", "magic"}

			tx2 := db.NewTransaction()
			if _, err := tx2.SaveStruct(&character); err != nil {
				t.Fatalf("SaveStruct update failed: %v", err)
			}
			if _, err := tx2.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Pull back and verify
			var loaded CharacterWithSkills
			if err := db.PullInto(charID, &loaded); err != nil {
				t.Fatalf("PullInto failed: %v", err)
			}

			t.Logf("Updated skills: %v", character.Skills)
			t.Logf("Loaded skills:  %v", loaded.Skills)

			// Should have all 4 skills in order
			expected := []string{"stealth", "archery", "lockpicking", "magic"}
			if len(loaded.Skills) != len(expected) {
				t.Errorf("expected %d skills, got %d", len(expected), len(loaded.Skills))
			}

			for i, exp := range expected {
				if i >= len(loaded.Skills) {
					t.Errorf("missing skill at position %d: expected %q", i, exp)
					continue
				}
				if loaded.Skills[i] != exp {
					t.Errorf("skill wrong at position %d: expected %q, got %q", i, exp, loaded.Skills[i])
				}
			}
		})
	}
}

// TestReflect_VectorReplaceMiddleElement verifies that replacing an element in the middle works.
func TestReflect_VectorReplaceMiddleElement(t *testing.T) {
	// Schema with vector cardinality
	builder := schema.NewBuilder()
	builder.Attribute(":character-with-skills/name").Type(schema.TypeString).Add()
	builder.Attribute(":character-with-skills/skills").Type(schema.TypeString).Vector().Add()
	sch, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build schema: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
				Path:           t.TempDir(),
				Schema:         sch,
				PlannerOptions: &popts,
			})
			if err != nil {
				t.Fatalf("failed to create database: %v", err)
			}
			defer db.Close()

			// Create character with initial skills
			character := CharacterWithSkills{
				Name:   "Dave",
				Skills: []string{"stealth", "archery", "lockpicking"},
			}

			tx := db.NewTransaction()
			charID, err := tx.SaveStruct(&character)
			if err != nil {
				t.Fatalf("SaveStruct failed: %v", err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Update: replace middle element
			character.Skills = []string{"stealth", "MAGIC", "lockpicking"}

			tx2 := db.NewTransaction()
			if _, err := tx2.SaveStruct(&character); err != nil {
				t.Fatalf("SaveStruct update failed: %v", err)
			}
			if _, err := tx2.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Pull back and verify
			var loaded CharacterWithSkills
			if err := db.PullInto(charID, &loaded); err != nil {
				t.Fatalf("PullInto failed: %v", err)
			}

			t.Logf("Updated skills: %v", character.Skills)
			t.Logf("Loaded skills:  %v", loaded.Skills)

			// Should have exactly [stealth, MAGIC, lockpicking]
			expected := []string{"stealth", "MAGIC", "lockpicking"}
			if len(loaded.Skills) != len(expected) {
				t.Errorf("expected %d skills, got %d: %v", len(expected), len(loaded.Skills), loaded.Skills)
			}

			for i, exp := range expected {
				if i >= len(loaded.Skills) {
					t.Errorf("missing skill at position %d: expected %q", i, exp)
					continue
				}
				if loaded.Skills[i] != exp {
					t.Errorf("skill wrong at position %d: expected %q, got %q", i, exp, loaded.Skills[i])
				}
			}
		})
	}
}

// TestReflect_VectorRemoveFromMiddle verifies that removing an element from the middle
// doesn't orphan subsequent elements.
func TestReflect_VectorRemoveFromMiddle(t *testing.T) {
	builder := schema.NewBuilder()
	builder.Attribute(":character-with-skills/name").Type(schema.TypeString).Add()
	builder.Attribute(":character-with-skills/skills").Type(schema.TypeString).Vector().Add()
	sch, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build schema: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
				Path:           t.TempDir(),
				Schema:         sch,
				PlannerOptions: &popts,
			})
			if err != nil {
				t.Fatalf("failed to create database: %v", err)
			}
			defer db.Close()

			// Create character with skills
			character := CharacterWithSkills{
				Name:   "Eve",
				Skills: []string{"a", "b", "c", "d", "e"},
			}

			tx := db.NewTransaction()
			charID, err := tx.SaveStruct(&character)
			if err != nil {
				t.Fatalf("SaveStruct failed: %v", err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Remove middle element "c" - should result in [a, b, d, e]
			character.Skills = []string{"a", "b", "d", "e"}

			tx2 := db.NewTransaction()
			if _, err := tx2.SaveStruct(&character); err != nil {
				t.Fatalf("SaveStruct update failed: %v", err)
			}
			if _, err := tx2.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			var loaded CharacterWithSkills
			if err := db.PullInto(charID, &loaded); err != nil {
				t.Fatalf("PullInto failed: %v", err)
			}

			t.Logf("Expected: %v", character.Skills)
			t.Logf("Got:      %v", loaded.Skills)

			expected := []string{"a", "b", "d", "e"}
			if len(loaded.Skills) != len(expected) {
				t.Errorf("expected %d skills, got %d: %v", len(expected), len(loaded.Skills), loaded.Skills)
			}

			for i, exp := range expected {
				if i >= len(loaded.Skills) {
					t.Errorf("missing skill at position %d: expected %q", i, exp)
					continue
				}
				if loaded.Skills[i] != exp {
					t.Errorf("skill wrong at position %d: expected %q, got %q", i, exp, loaded.Skills[i])
				}
			}
		})
	}
}

// TestReflect_VectorReorder verifies that reordering elements works.
func TestReflect_VectorReorder(t *testing.T) {
	builder := schema.NewBuilder()
	builder.Attribute(":character-with-skills/name").Type(schema.TypeString).Add()
	builder.Attribute(":character-with-skills/skills").Type(schema.TypeString).Vector().Add()
	sch, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build schema: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
				Path:           t.TempDir(),
				Schema:         sch,
				PlannerOptions: &popts,
			})
			if err != nil {
				t.Fatalf("failed to create database: %v", err)
			}
			defer db.Close()

			// Create character with skills in specific order
			character := CharacterWithSkills{
				Name:   "Frank",
				Skills: []string{"a", "b", "c"},
			}

			tx := db.NewTransaction()
			charID, err := tx.SaveStruct(&character)
			if err != nil {
				t.Fatalf("SaveStruct failed: %v", err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Reorder: [a, b, c] -> [c, b, a]
			character.Skills = []string{"c", "b", "a"}

			tx2 := db.NewTransaction()
			if _, err := tx2.SaveStruct(&character); err != nil {
				t.Fatalf("SaveStruct update failed: %v", err)
			}
			if _, err := tx2.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			var loaded CharacterWithSkills
			if err := db.PullInto(charID, &loaded); err != nil {
				t.Fatalf("PullInto failed: %v", err)
			}

			t.Logf("Expected: %v", character.Skills)
			t.Logf("Got:      %v", loaded.Skills)

			expected := []string{"c", "b", "a"}
			if len(loaded.Skills) != len(expected) {
				t.Errorf("expected %d skills, got %d: %v", len(expected), len(loaded.Skills), loaded.Skills)
			}

			for i, exp := range expected {
				if i >= len(loaded.Skills) {
					t.Errorf("missing skill at position %d: expected %q", i, exp)
					continue
				}
				if loaded.Skills[i] != exp {
					t.Errorf("skill wrong at position %d: expected %q, got %q", i, exp, loaded.Skills[i])
				}
			}
		})
	}
}

// TestReflect_VectorPrepend verifies that prepending an element works.
func TestReflect_VectorPrepend(t *testing.T) {
	builder := schema.NewBuilder()
	builder.Attribute(":character-with-skills/name").Type(schema.TypeString).Add()
	builder.Attribute(":character-with-skills/skills").Type(schema.TypeString).Vector().Add()
	sch, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build schema: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
				Path:           t.TempDir(),
				Schema:         sch,
				PlannerOptions: &popts,
			})
			if err != nil {
				t.Fatalf("failed to create database: %v", err)
			}
			defer db.Close()

			// Create character with skills
			character := CharacterWithSkills{
				Name:   "Grace",
				Skills: []string{"b", "c"},
			}

			tx := db.NewTransaction()
			charID, err := tx.SaveStruct(&character)
			if err != nil {
				t.Fatalf("SaveStruct failed: %v", err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Prepend "a": [b, c] -> [a, b, c]
			character.Skills = []string{"a", "b", "c"}

			tx2 := db.NewTransaction()
			if _, err := tx2.SaveStruct(&character); err != nil {
				t.Fatalf("SaveStruct update failed: %v", err)
			}
			if _, err := tx2.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			var loaded CharacterWithSkills
			if err := db.PullInto(charID, &loaded); err != nil {
				t.Fatalf("PullInto failed: %v", err)
			}

			t.Logf("Expected: %v", character.Skills)
			t.Logf("Got:      %v", loaded.Skills)

			expected := []string{"a", "b", "c"}
			if len(loaded.Skills) != len(expected) {
				t.Errorf("expected %d skills, got %d: %v", len(expected), len(loaded.Skills), loaded.Skills)
			}

			for i, exp := range expected {
				if i >= len(loaded.Skills) {
					t.Errorf("missing skill at position %d: expected %q", i, exp)
					continue
				}
				if loaded.Skills[i] != exp {
					t.Errorf("skill wrong at position %d: expected %q, got %q", i, exp, loaded.Skills[i])
				}
			}
		})
	}
}

// TestReflect_VectorInsertInMiddle verifies that inserting an element in the middle works.
func TestReflect_VectorInsertInMiddle(t *testing.T) {
	builder := schema.NewBuilder()
	builder.Attribute(":character-with-skills/name").Type(schema.TypeString).Add()
	builder.Attribute(":character-with-skills/skills").Type(schema.TypeString).Vector().Add()
	sch, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build schema: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
				Path:           t.TempDir(),
				Schema:         sch,
				PlannerOptions: &popts,
			})
			if err != nil {
				t.Fatalf("failed to create database: %v", err)
			}
			defer db.Close()

			// Create character with skills
			character := CharacterWithSkills{
				Name:   "Henry",
				Skills: []string{"a", "c"},
			}

			tx := db.NewTransaction()
			charID, err := tx.SaveStruct(&character)
			if err != nil {
				t.Fatalf("SaveStruct failed: %v", err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Insert "b" in middle: [a, c] -> [a, b, c]
			character.Skills = []string{"a", "b", "c"}

			tx2 := db.NewTransaction()
			if _, err := tx2.SaveStruct(&character); err != nil {
				t.Fatalf("SaveStruct update failed: %v", err)
			}
			if _, err := tx2.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			var loaded CharacterWithSkills
			if err := db.PullInto(charID, &loaded); err != nil {
				t.Fatalf("PullInto failed: %v", err)
			}

			t.Logf("Expected: %v", character.Skills)
			t.Logf("Got:      %v", loaded.Skills)

			expected := []string{"a", "b", "c"}
			if len(loaded.Skills) != len(expected) {
				t.Errorf("expected %d skills, got %d: %v", len(expected), len(loaded.Skills), loaded.Skills)
			}

			for i, exp := range expected {
				if i >= len(loaded.Skills) {
					t.Errorf("missing skill at position %d: expected %q", i, exp)
					continue
				}
				if loaded.Skills[i] != exp {
					t.Errorf("skill wrong at position %d: expected %q, got %q", i, exp, loaded.Skills[i])
				}
			}
		})
	}
}

// TestReflect_VectorClearAndRepopulate verifies that clearing and repopulating a vector works.
func TestReflect_VectorClearAndRepopulate(t *testing.T) {
	builder := schema.NewBuilder()
	builder.Attribute(":character-with-skills/name").Type(schema.TypeString).Add()
	builder.Attribute(":character-with-skills/skills").Type(schema.TypeString).Vector().Add()
	sch, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build schema: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
				Path:           t.TempDir(),
				Schema:         sch,
				PlannerOptions: &popts,
			})
			if err != nil {
				t.Fatalf("failed to create database: %v", err)
			}
			defer db.Close()

			// Create character with skills
			character := CharacterWithSkills{
				Name:   "Ivy",
				Skills: []string{"old1", "old2", "old3"},
			}

			tx := db.NewTransaction()
			charID, err := tx.SaveStruct(&character)
			if err != nil {
				t.Fatalf("SaveStruct failed: %v", err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Replace with completely different values
			character.Skills = []string{"new1", "new2"}

			tx2 := db.NewTransaction()
			if _, err := tx2.SaveStruct(&character); err != nil {
				t.Fatalf("SaveStruct update failed: %v", err)
			}
			if _, err := tx2.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			var loaded CharacterWithSkills
			if err := db.PullInto(charID, &loaded); err != nil {
				t.Fatalf("PullInto failed: %v", err)
			}

			t.Logf("Expected: %v", character.Skills)
			t.Logf("Got:      %v", loaded.Skills)

			expected := []string{"new1", "new2"}
			if len(loaded.Skills) != len(expected) {
				t.Errorf("expected %d skills, got %d: %v", len(expected), len(loaded.Skills), loaded.Skills)
			}

			for i, exp := range expected {
				if i >= len(loaded.Skills) {
					t.Errorf("missing skill at position %d: expected %q", i, exp)
					continue
				}
				if loaded.Skills[i] != exp {
					t.Errorf("skill wrong at position %d: expected %q, got %q", i, exp, loaded.Skills[i])
				}
			}
		})
	}
}

// TestReflect_SchemaFromStructDoesNotSupportVector verifies that SchemaFromStruct
// does not have a way to mark fields as cardinality-vector.
// This documents the current limitation.
func TestReflect_SchemaFromStructDoesNotSupportVector(t *testing.T) {
	// SchemaFromStruct infers cardinality from Go types:
	// - Scalar types → CardinalityOne
	// - Slice types → CardinalityMany
	// There's no way to indicate CardinalityVector via struct tags

	sch, err := dlreflect.SchemaFromStruct(CharacterWithSkills{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	skillsAttr := sch.GetAttribute(datalog.NewKeyword(":character-with-skills/skills"))
	if skillsAttr == nil {
		t.Fatal("expected :character-with-skills/skills attribute")
	}

	t.Logf("SchemaFromStruct inferred cardinality: %s", skillsAttr.Cardinality)

	// Currently infers CardinalityMany for slices - no way to get CardinalityVector
	if skillsAttr.Cardinality == schema.CardinalityVector {
		t.Error("SchemaFromStruct unexpectedly supports CardinalityVector - this test needs updating")
	}

	if skillsAttr.Cardinality != schema.CardinalityMany {
		t.Errorf("expected SchemaFromStruct to infer CardinalityMany for slice, got %s", skillsAttr.Cardinality)
	}

	// Document: To use vectors, you must manually build the schema with .Vector()
	t.Log("NOTE: To use CardinalityVector, manually build schema with .Vector() - SchemaFromStruct cannot infer it")
}
