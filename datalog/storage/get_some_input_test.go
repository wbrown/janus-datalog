package storage

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// TestGetSome_WithScalarInput reproduces a bug where a query using get-some
// with a scalar input that only appears in the expression (not in any data
// pattern) causes a nil pointer panic in relationToSlice.
//
// The query:
//
//	[:find ?id :in $ ?e :where [(get-some $ ?e :entity/code :entity/name) ?id]]
//
// The scalar input ?e only appears in the get-some expression, not in any
// data pattern. This should work: get-some looks up attributes on the input
// entity and binds the first found value to ?id.
func TestGetSome_WithScalarInput(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:           t.TempDir(),
				PlannerOptions: &popts,
			})
			if err != nil {
				t.Fatalf("Failed to create database: %v", err)
			}
			defer db.Close()

			// Create an entity with :entity/code attribute
			entity := datalog.NewIdentity("test:entity1")
			codeAttr := datalog.NewKeyword(":entity/code")
			nameAttr := datalog.NewKeyword(":entity/name")

			tx := db.NewTransaction()
			tx.Add(entity, codeAttr, "E1-CODE")
			_, err = tx.Commit()
			if err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			// Query: get-some should return :entity/code value since entity has it
			results, err := executor.CollectTuples(db.Query(
				`[:find ?id :in $ ?e :where [(get-some $ ?e :entity/code :entity/name) ?id]]`,
				entity,
			))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(results) != 1 {
				t.Fatalf("Expected 1 result, got %d", len(results))
			}

			if results[0][0] != "E1-CODE" {
				t.Errorf("Expected 'E1-CODE', got %v", results[0][0])
			}

			// Also test with :entity/name (second fallback)
			entity2 := datalog.NewIdentity("test:entity2")
			tx2 := db.NewTransaction()
			tx2.Add(entity2, nameAttr, "Entity Two Name")
			_, err = tx2.Commit()
			if err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			results2, err := executor.CollectTuples(db.Query(
				`[:find ?id :in $ ?e :where [(get-some $ ?e :entity/code :entity/name) ?id]]`,
				entity2,
			))
			if err != nil {
				t.Fatalf("Query failed for entity2: %v", err)
			}

			if len(results2) != 1 {
				t.Fatalf("Expected 1 result for entity2, got %d", len(results2))
			}

			if results2[0][0] != "Entity Two Name" {
				t.Errorf("Expected 'Entity Two Name', got %v", results2[0][0])
			}
		})
	}
}

// TestGetSome_WithScalarInput_NoMatch tests get-some when the entity has
// none of the requested attributes. Should return empty results, not panic.
func TestGetSome_WithScalarInput_NoMatch(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:           t.TempDir(),
				PlannerOptions: &popts,
			})
			if err != nil {
				t.Fatalf("Failed to create database: %v", err)
			}
			defer db.Close()

			// Create an entity with a different attribute (not :entity/code or :entity/name)
			entity := datalog.NewIdentity("test:entity1")
			otherAttr := datalog.NewKeyword(":entity/other")

			tx := db.NewTransaction()
			tx.Add(entity, otherAttr, "some value")
			_, err = tx.Commit()
			if err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			// Query: get-some should return empty (entity has neither requested attr)
			results, err := executor.CollectTuples(db.Query(
				`[:find ?id :in $ ?e :where [(get-some $ ?e :entity/code :entity/name) ?id]]`,
				entity,
			))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Should be empty, not panic
			if len(results) != 0 {
				t.Errorf("Expected 0 results (no matching attrs), got %d: %v", len(results), results)
			}
		})
	}
}
