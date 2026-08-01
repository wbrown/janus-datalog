package tests

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestIDLiteralMatchesEntity pins the #id literal end to end: #id "seed" in
// query text constructs the same Identity as NewIdentity("seed"), so a
// pattern using it matches the entity written under that seed.
func TestIDLiteralMatchesEntity(t *testing.T) {
	eachBackendAndMode(t, func(t *testing.T, db *storage.Database) {
		alice := datalog.NewIdentity("user:alice")
		bob := datalog.NewIdentity("user:bob")
		nameAttr := datalog.NewKeyword(":user/name")
		tx := db.NewTransaction()
		tx.Add(alice, nameAttr, "Alice")
		tx.Add(bob, nameAttr, "Bob")
		if _, err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		tuples, err := executor.CollectTuples(db.Query(
			`[:find ?n :where [#id "user:alice" :user/name ?n]]`,
		))
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(tuples) != 1 {
			t.Fatalf("expected 1 result, got %d", len(tuples))
		}
		if name, ok := tuples[0][0].(string); !ok || name != "Alice" {
			t.Errorf("expected \"Alice\", got %v", tuples[0][0])
		}
	})
}
