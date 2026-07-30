package storage

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// TestJoinCopyAnnotationE2E verifies that copy tracking annotations are emitted
// during actual query execution with joins.
func TestJoinCopyAnnotationE2E(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			// The recording flag scopes collection to the query below; the
			// handler is registered at open because everything the database
			// builds is constructed with it.
			var events []annotations.Event
			recording := false
			db := createOptimizerModeDB(t, mode, func(e annotations.Event) {
				if recording {
					events = append(events, e)
				}
			})

			// Add some entities with relationships that will require joins
			tx := db.NewTransaction()

			// Add people
			tx.Add(datalog.NewIdentity("person-1"), datalog.NewKeyword(":person/name"), "Alice")
			tx.Add(datalog.NewIdentity("person-1"), datalog.NewKeyword(":person/dept"), "Engineering")
			tx.Add(datalog.NewIdentity("person-2"), datalog.NewKeyword(":person/name"), "Bob")
			tx.Add(datalog.NewIdentity("person-2"), datalog.NewKeyword(":person/dept"), "Sales")
			tx.Add(datalog.NewIdentity("person-3"), datalog.NewKeyword(":person/name"), "Charlie")
			tx.Add(datalog.NewIdentity("person-3"), datalog.NewKeyword(":person/dept"), "Engineering")

			if _, err := tx.Commit(); err != nil {
				t.Fatalf("failed to commit: %v", err)
			}

			recording = true

			// Execute a query that requires a join
			queryStr := `[:find ?name ?dept
                  :where [?p :person/name ?name]
                         [?p :person/dept ?dept]]`

			results, err := executor.CollectTuples(db.Query(queryStr))
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}

			// Verify we got results
			if len(results) != 3 {
				t.Errorf("expected 3 results, got %d", len(results))
			}

			// Check for JoinBuildCopy annotation
			var foundCopyAnnotation bool
			var copyCount, skipCount int
			var requiresCopy bool

			for _, e := range events {
				if e.Name == annotations.JoinBuildCopy {
					foundCopyAnnotation = true
					if c, ok := e.Data["copied"].(int); ok {
						copyCount = c
					}
					if s, ok := e.Data["passthru"].(int); ok {
						skipCount = s
					}
					if rc, ok := e.Data["requires_copy"].(bool); ok {
						requiresCopy = rc
					}
					t.Logf("JoinBuildCopy: copied=%d, passthru=%d, requires_copy=%v", copyCount, skipCount, requiresCopy)
				}
			}

			if !foundCopyAnnotation {
				// List all events we did receive
				var eventNames []string
				for _, e := range events {
					eventNames = append(eventNames, e.Name)
				}
				t.Errorf("expected JoinBuildCopy annotation, but got events: %v", strings.Join(eventNames, ", "))
			}

			// StreamingRelation from storage requires copy, so we expect copyCount > 0
			// MaterializedRelation doesn't require copy, so we'd expect skipCount > 0
			// The actual values depend on which side is the build relation
			if copyCount+skipCount == 0 {
				t.Error("expected either copies or skips, but both are 0")
			}
		})
	}
}

// TestJoinCopyAnnotationMaterialized verifies that MaterializedRelation skips copies
func TestJoinCopyAnnotationMaterialized(t *testing.T) {
	var copyEvent *annotations.Event
	opts := executor.ExecutorOptions{
		Handler: func(e annotations.Event) {
			if e.Name == annotations.JoinBuildCopy {
				copyEvent = &e
			}
		},
	}

	// Create two materialized relations (RequiresCopy = false)
	leftSymbols := []datalog.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	leftTuples := []executor.Tuple{
		{1, "a"}, {2, "b"}, {3, "c"},
	}
	left := executor.NewMaterializedRelationWithOptions(leftSymbols, leftTuples, opts)

	rightSymbols := []datalog.Symbol{datalog.NewSymbol("?y"), datalog.NewSymbol("?z")}
	rightTuples := []executor.Tuple{
		{"a", 100}, {"b", 200},
	}
	right := executor.NewMaterializedRelationWithOptions(rightSymbols, rightTuples, opts)

	// Join
	joined := executor.HashJoinWithOptions(left, right, []datalog.Symbol{datalog.NewSymbol("?y")}, opts)

	// Materialize to execute the join
	it := joined.Iterator()
	for it.Next() {
		_ = it.Tuple()
	}
	it.Close()

	if copyEvent == nil {
		t.Fatal("expected JoinBuildCopy annotation")
	}
	copyCount, _ := copyEvent.Data["copied"].(int)
	skipCount, _ := copyEvent.Data["passthru"].(int)

	// MaterializedRelation doesn't require copying
	if copyCount != 0 {
		t.Errorf("expected 0 copies for MaterializedRelation, got %d", copyCount)
	}
	if skipCount == 0 {
		t.Errorf("expected passthru > 0 for MaterializedRelation, got %d", skipCount)
	}

	t.Logf("MaterializedRelation join: copied=%d, passthru=%d", copyCount, skipCount)
}
