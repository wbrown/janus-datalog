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

// getSomeGroupsFixture writes two entities carrying :entity/kind "thing":
// one that also carries :entity/name (a get-some hit), one that carries
// neither :entity/code nor :entity/name (a get-some miss).
func getSomeGroupsFixture(t *testing.T, db *Database) (named, bare datalog.Identity) {
	t.Helper()
	named = datalog.NewIdentity("entity:named")
	bare = datalog.NewIdentity("entity:bare")

	tx := db.NewTransaction()
	for _, e := range []datalog.Identity{named, bare} {
		if err := tx.Add(e, datalog.NewKeyword(":entity/kind"), "thing"); err != nil {
			t.Fatalf("Failed to add kind: %v", err)
		}
	}
	if err := tx.Add(named, datalog.NewKeyword(":entity/name"), "Widget"); err != nil {
		t.Fatalf("Failed to add name: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	return named, bare
}

// TestGetSome_InBoundEntityWithGroups_AllMissing pins the all-constants
// expression arm that evaluates against the environment while data-pattern
// groups are present: a data pattern in :where routes the query through the
// with-groups arm of executeExpression, unlike TestGetSome_WithScalarInput_NoMatch,
// whose pattern-free :where routes through the no-groups arm. With every
// listed attribute missing, the rows drop — the get-some no-match sentinel
// must be consumed by the expression, never escape into tuples
// (BUG_GETSOME_ALL_ATTRS_MISSING_PANICS_TUPLE_KEY_HASH).
func TestGetSome_InBoundEntityWithGroups_AllMissing(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode)
			_, bare := getSomeGroupsFixture(t, db)

			results, err := executor.CollectTuples(db.Query(
				`[:find ?k ?v :in $ ?e :where [?e :entity/kind ?k] [(get-some $ ?e :entity/code :entity/name) ?v]]`,
				bare,
			))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(results) != 0 {
				t.Errorf("Expected 0 results (no matching attrs), got %d: %v", len(results), results)
			}
		})
	}
}

// TestGetSome_InBoundEntityWithGroups_AttrPresent pins the found leg of the
// same arm: the sentinel carries the value and must be unwrapped before the
// binding enters tuples (BUG_GETSOME_ALL_ATTRS_MISSING_PANICS_TUPLE_KEY_HASH).
func TestGetSome_InBoundEntityWithGroups_AttrPresent(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode)
			named, _ := getSomeGroupsFixture(t, db)

			results, err := executor.CollectTuples(db.Query(
				`[:find ?k ?v :in $ ?e :where [?e :entity/kind ?k] [(get-some $ ?e :entity/code :entity/name) ?v]]`,
				named,
			))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(results) != 1 {
				t.Fatalf("Expected 1 result, got %d: %v", len(results), results)
			}
			if results[0][0] != "thing" || results[0][1] != "Widget" {
				t.Errorf("Expected [thing Widget], got %v", results[0])
			}
		})
	}
}

// TestGetSome_RelationInputEntities_MixedFound pins the per-Run environment
// path: a relation input iterates the query once per entity, and each Run
// rebuilds the environment the expression evaluates against. One Run finds
// :entity/name, the other finds nothing — the miss must drop only its own
// Run's rows (BUG_GETSOME_ALL_ATTRS_MISSING_PANICS_TUPLE_KEY_HASH).
func TestGetSome_RelationInputEntities_MixedFound(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode)
			named, bare := getSomeGroupsFixture(t, db)

			results, err := executor.CollectTuples(db.Query(
				`[:find ?e ?v :in $ [[?e] ...] :where [?e :entity/kind "thing"] [(get-some $ ?e :entity/code :entity/name) ?v]]`,
				[][]interface{}{{named}, {bare}},
			))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(results) != 1 {
				t.Fatalf("Expected 1 result (only the named entity), got %d: %v", len(results), results)
			}
			id, ok := results[0][0].(datalog.Identity)
			if !ok {
				t.Fatalf("?e must be an Identity, got %T", results[0][0])
			}
			if id.String() != named.String() || results[0][1] != "Widget" {
				t.Errorf("Expected [%s Widget], got %v", named, results[0])
			}
		})
	}
}

// TestGetSome_LiteralEntity pins the zero-required-symbols expression arm:
// a get-some whose entity is a tagged literal has no required symbols and no
// groups, routing through the ground-expression arm of executeExpression —
// a third consumer of the get-some sentinel, distinct from both the
// with-groups and no-groups environment arms
// (BUG_GETSOME_ALL_ATTRS_MISSING_PANICS_TUPLE_KEY_HASH).
func TestGetSome_LiteralEntity(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode)
			getSomeGroupsFixture(t, db)

			t.Run("attr-present", func(t *testing.T) {
				results, err := executor.CollectTuples(db.Query(
					`[:find ?v :where [(get-some $ #id "entity:named" :entity/code :entity/name) ?v]]`,
				))
				if err != nil {
					t.Fatalf("Query failed: %v", err)
				}
				if len(results) != 1 || results[0][0] != "Widget" {
					t.Fatalf("Expected [[Widget]], got %v", results)
				}
			})

			t.Run("all-missing", func(t *testing.T) {
				results, err := executor.CollectTuples(db.Query(
					`[:find ?v :where [(get-some $ #id "entity:bare" :entity/code :entity/name) ?v]]`,
				))
				if err != nil {
					t.Fatalf("Query failed: %v", err)
				}
				if len(results) != 0 {
					t.Fatalf("Expected 0 results (no matching attrs), got %v", results)
				}
			})
		})
	}
}

// TestGetSome_CollectionInputEntities_MixedFound is the stay-green contrast
// to the relation-input leg: a collection input binds as a data relation, so
// ?e is a group symbol and the expression evaluates per tuple — the path
// that always consumed the sentinel correctly.
func TestGetSome_CollectionInputEntities_MixedFound(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode)
			named, bare := getSomeGroupsFixture(t, db)

			results, err := executor.CollectTuples(db.Query(
				`[:find ?e ?v :in $ [?e ...] :where [?e :entity/kind "thing"] [(get-some $ ?e :entity/code :entity/name) ?v]]`,
				[]interface{}{named, bare},
			))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(results) != 1 {
				t.Fatalf("Expected 1 result (only the named entity), got %d: %v", len(results), results)
			}
			id, ok := results[0][0].(datalog.Identity)
			if !ok {
				t.Fatalf("?e must be an Identity, got %T", results[0][0])
			}
			if id.String() != named.String() || results[0][1] != "Widget" {
				t.Errorf("Expected [%s Widget], got %v", named, results[0])
			}
		})
	}
}
