package storage

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// TestGetElseWithConstantEntity reproduces a bug where get-else fails when
// the entity is a constant (input parameter or literal) rather than a symbol
// bound from a pattern.
//
// When RequiredSymbols() returns empty (because the entity is constant-bound),
// the executor takes a code path that calls expr.Function.Eval() directly
// instead of checking for DatabaseFunction and calling EvalWithLookup().
//
// Error: "get-else requires database access; use EvalWithLookup instead"
func TestGetElseWithConstantEntity(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Create test data
			tx := db.NewTransaction()
			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")

			tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice")
			tx.Add(alice, datalog.NewKeyword(":person/nickname"), "Ali")

			tx.Add(bob, datalog.NewKeyword(":person/name"), "Bob")
			// Bob has no nickname

			// Add some unrelated data for pattern matching
			tx.Add(datalog.NewIdentity("item1"), datalog.NewKeyword(":item/name"), "Widget")
			tx.Add(datalog.NewIdentity("item2"), datalog.NewKeyword(":item/name"), "Gadget")

			_, err := tx.Commit()
			if err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			t.Run("entity from input parameter", func(t *testing.T) {
				// Pass entity as input parameter - get-else should still work
				// The entity is constant-bound via :in clause, not from a pattern
				results, err := executor.CollectTuples(db.Query(
					`[:find ?nick
			  :in $ ?entity
			  :where
			  [(get-else $ ?entity :person/nickname "No Nickname") ?nick]]`,
					alice,
				))
				if err != nil {
					t.Fatalf("Query failed: %v", err)
				}

				if len(results) != 1 {
					t.Fatalf("Expected 1 result, got %d", len(results))
				}

				nick := results[0][0].(string)
				if nick != "Ali" {
					t.Errorf("Expected 'Ali', got %q", nick)
				}
			})

			t.Run("entity from input with default value", func(t *testing.T) {
				charlie := datalog.NewIdentity("charlie")
				// Charlie doesn't exist - should return default
				results, err := executor.CollectTuples(db.Query(
					`[:find ?nick
			  :in $ ?entity
			  :where
			  [(get-else $ ?entity :person/nickname "Unknown") ?nick]]`,
					charlie,
				))
				if err != nil {
					t.Fatalf("Query failed: %v", err)
				}

				if len(results) != 1 {
					t.Fatalf("Expected 1 result, got %d", len(results))
				}

				nick := results[0][0].(string)
				if nick != "Unknown" {
					t.Errorf("Expected 'Unknown', got %q", nick)
				}
			})

			t.Run("missing? with constant entity", func(t *testing.T) {
				// Similar bug for missing? with constant entity
				results, err := executor.CollectTuples(db.Query(
					`[:find ?is-missing
			  :in $ ?entity
			  :where
			  [(missing? $ ?entity :person/email) ?is-missing]]`,
					alice,
				))
				if err != nil {
					t.Fatalf("Query failed: %v", err)
				}

				if len(results) != 1 {
					t.Fatalf("Expected 1 result, got %d", len(results))
				}

				isMissing := results[0][0].(bool)
				if !isMissing {
					t.Errorf("Expected true (email is missing), got %v", isMissing)
				}
			})

			t.Run("get-else with no patterns at all", func(t *testing.T) {
				// Edge case: query with ONLY get-else, no patterns
				results, err := executor.CollectTuples(db.Query(
					`[:find ?name
			  :in $ ?entity
			  :where
			  [(get-else $ ?entity :person/name "Anon") ?name]]`,
					alice,
				))
				if err != nil {
					t.Fatalf("Query failed: %v", err)
				}

				if len(results) != 1 {
					t.Fatalf("Expected 1 result, got %d", len(results))
				}

				name := results[0][0].(string)
				if name != "Alice" {
					t.Errorf("Expected 'Alice', got %q", name)
				}
			})

			// THIS IS THE BUG CASE: patterns exist but don't bind the get-else entity
			// The query has groups from the pattern, but the entity is constant-bound
			// triggering the code path at line 423 that doesn't check for DatabaseFunction
			t.Run("get-else with unrelated pattern - BUG CASE", func(t *testing.T) {
				// Query has a pattern [?i :item/name ?item-name] which creates groups
				// But get-else uses ?entity which is constant-bound from input
				// This triggers: len(groups) > 0 && len(unresolvedExprSyms) == 0
				results, err := executor.CollectTuples(db.Query(
					`[:find ?item-name ?nick
			  :in $ ?entity
			  :where
			  [?i :item/name ?item-name]
			  [(get-else $ ?entity :person/nickname "No Nickname") ?nick]]`,
					alice,
				))
				if err != nil {
					t.Fatalf("Query failed: %v", err)
				}

				// Should return 2 tuples (one per item), each with Alice's nickname
				if len(results) != 2 {
					t.Fatalf("Expected 2 results, got %d", len(results))
				}

				for _, tuple := range results {
					nick := tuple[1].(string)
					if nick != "Ali" {
						t.Errorf("Expected 'Ali', got %q", nick)
					}
				}
			})

			t.Run("missing? with unrelated pattern - BUG CASE", func(t *testing.T) {
				results, err := executor.CollectTuples(db.Query(
					`[:find ?item-name ?is-missing
			  :in $ ?entity
			  :where
			  [?i :item/name ?item-name]
			  [(missing? $ ?entity :person/email) ?is-missing]]`,
					alice,
				))
				if err != nil {
					t.Fatalf("Query failed: %v", err)
				}

				if len(results) != 2 {
					t.Fatalf("Expected 2 results, got %d", len(results))
				}

				for _, tuple := range results {
					isMissing := tuple[1].(bool)
					if !isMissing {
						t.Errorf("Expected true, got %v", isMissing)
					}
				}
			})
		})
	}
}
