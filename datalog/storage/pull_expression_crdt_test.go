package storage

import (
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// PullExprTestEntity is a minimal entity with a status field that gets updated multiple times.
type PullExprTestEntity struct {
	ID     datalog.Identity `datalog:"db/id"`
	Name   string           `datalog:"task/name"`
	Status datalog.Keyword  `datalog:"task/status"` // CardinalityOne
}

func TestPullInto_CardinalityOne_MultipleWrites(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			// Create schema with CardinalityOne status attribute
			s, err := schema.NewBuilder().
				Attribute(":task/name").Type(schema.TypeString).Add().
				Attribute(":task/status").Type(schema.TypeKeyword).Add(). // CardinalityOne (no .Many())
				Build()
			if err != nil {
				t.Fatalf("Failed to build schema: %v", err)
			}

			// Create temp database
			dir, err := os.MkdirTemp("", "pullinto-test-*")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)

			popts := mode.plannerOptions()
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:           dir,
				Schema:         s,
				PlannerOptions: &popts,
			})
			if err != nil {
				t.Fatalf("Failed to create database: %v", err)
			}
			defer db.Close()

			// Create entity with initial status
			task := &PullExprTestEntity{
				Name:   "test-task",
				Status: datalog.NewKeyword(":status/pending"),
			}
			tx := db.NewTransaction()
			_, err = tx.SaveStruct(task)
			if err != nil {
				t.Fatalf("Failed to save initial task: %v", err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			// Update status to running via SaveStruct (2nd write to same attribute)
			task.Status = datalog.NewKeyword(":status/running")
			tx2 := db.NewTransaction()
			if _, err := tx2.SaveStruct(task); err != nil {
				t.Fatalf("Failed to save running status: %v", err)
			}
			if _, err := tx2.Commit(); err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			// Update status to complete via SaveStruct (3rd write to same attribute)
			task.Status = datalog.NewKeyword(":status/complete")
			tx3 := db.NewTransaction()
			if _, err := tx3.SaveStruct(task); err != nil {
				t.Fatalf("Failed to save complete status: %v", err)
			}
			if _, err := tx3.Commit(); err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			// PullInto should return the current (latest) value, not all historical values
			var loaded PullExprTestEntity
			if err := db.PullInto(task.ID, &loaded); err != nil {
				t.Fatalf("PullInto failed: %v", err)
			}

			// Expected: Status == :status/complete (the latest value per LWW semantics)
			// Actual: Error "expected Keyword, got []interface {}" because PullInto
			// returns all historical values [pending, running, complete] instead of
			// resolving to the current value via CRDT LWW semantics.
			expected := datalog.NewKeyword(":status/complete")
			if loaded.Status != expected {
				t.Errorf("Expected status %v, got %v", expected, loaded.Status)
			}
		})
	}
}

func TestPullExpression_CardinalityOne_MultipleWrites(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			// Create schema with CardinalityOne status attribute
			s, err := schema.NewBuilder().
				Attribute(":task/name").Type(schema.TypeString).Add().
				Attribute(":task/status").Type(schema.TypeKeyword).Add(). // CardinalityOne (no .Many())
				Build()
			if err != nil {
				t.Fatalf("Failed to build schema: %v", err)
			}

			// Create temp database
			dir, err := os.MkdirTemp("", "pullexpr-test-*")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)

			popts := mode.plannerOptions()
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:           dir,
				Schema:         s,
				PlannerOptions: &popts,
			})
			if err != nil {
				t.Fatalf("Failed to create database: %v", err)
			}
			defer db.Close()

			// Create entity with initial status
			task := &PullExprTestEntity{
				Name:   "test-task",
				Status: datalog.NewKeyword(":status/pending"),
			}
			tx := db.NewTransaction()
			_, err = tx.SaveStruct(task)
			if err != nil {
				t.Fatalf("Failed to save initial task: %v", err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			// Update status to running via SaveStruct (2nd write to same attribute)
			task.Status = datalog.NewKeyword(":status/running")
			tx2 := db.NewTransaction()
			if _, err := tx2.SaveStruct(task); err != nil {
				t.Fatalf("Failed to save running status: %v", err)
			}
			if _, err := tx2.Commit(); err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			// Update status to complete via SaveStruct (3rd write to same attribute)
			task.Status = datalog.NewKeyword(":status/complete")
			tx3 := db.NewTransaction()
			if _, err := tx3.SaveStruct(task); err != nil {
				t.Fatalf("Failed to save complete status: %v", err)
			}
			if _, err := tx3.Commit(); err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			// Query using pull expression (pull ?e [*]) - this is where the bug manifests
			// The pull expression should return CRDT-resolved values, not raw historical data
			var loaded PullExprTestEntity
			found, err := db.QueryOneInto(&loaded,
				`[:find (pull ?e [*]) :where [?e :task/name "test-task"]]`)
			if err != nil {
				t.Fatalf("QueryOneInto with pull expression failed: %v", err)
			}
			if !found {
				t.Fatal("Entity not found")
			}

			// Expected: Status == :status/complete (the latest value per LWW semantics)
			// Actual bug: Error "expected Keyword, got []interface {}" because pull expression
			// returns all historical values [pending, running, complete] instead of
			// resolving to the current value via CRDT LWW semantics.
			expected := datalog.NewKeyword(":status/complete")
			if loaded.Status != expected {
				t.Errorf("Expected status %v, got %v", expected, loaded.Status)
			}
		})
	}
}
