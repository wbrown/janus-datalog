package tests

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// TestIdentityComparisonBestPractices demonstrates how Identity values in
// query results compare.
//
// Identity is an interned pointer (*identity): every constructor and the
// storage decode path intern by hash, so two Identities with the same hash
// are the same pointer. == is therefore hash equality — including between
// an Identity created in code and one decoded from storage. .Equal()
// compares the hashes directly and states the same fact without relying on
// the interning invariant.
func TestIdentityComparisonBestPractices(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode)

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
			tuples, err := executor.CollectTuples(db.Query(
				`[:find ?c :in $ ?parent :where [?c :test/ref ?parent]]`,
				parent,
			))
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

			// Interning: identities with the same hash are the same pointer, so ==
			// matches even though foundID was decoded from storage.
			if foundID != child {
				t.Errorf("== did not match: interning guarantees one pointer per hash")
			}

			// .Equal() compares hashes — the semantic comparison, independent of
			// the interning invariant.
			if !foundID.Equal(child) {
				t.Errorf("Hash equality (.Equal()) failed - this indicates a real bug!")
			}

			// Hash() comparison states the same fact.
			if foundID.Hash() != child.Hash() {
				t.Errorf("Direct hash comparison failed - this indicates a real bug!")
			}
		})
	}
}
