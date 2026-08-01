package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TaskEntity matches the exact bug scenario from PULLINTO_CRDT_RESOLUTION.md
type TaskEntity struct {
	ID     datalog.Identity `datalog:"-,id"`
	Status datalog.Keyword  `datalog:"task/status"`
}

// TestBugVerification_PullInto_CardinalityOne reproduces the exact bug from the doc:
// "Error: field Status (attr task/status): expected Keyword, got []interface {}"
func TestBugVerification_PullInto_CardinalityOne(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Create schema with CardinalityOne (exactly as in bug doc)
			s, err := schema.NewBuilder().
				Attribute(":task/status").Type(schema.TypeKeyword).One().Add().
				Build()
			require.NoError(t, err)
			db.SetSchema(s)

			// Create entity
			taskID := datalog.NewIdentity("task-1")

			// Write status TWICE - this is the bug scenario
			// First: pending
			tx1 := db.NewTransaction()
			err = tx1.Set(taskID, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/pending"))
			require.NoError(t, err)
			_, err = tx1.Commit()
			require.NoError(t, err)

			// Second: complete
			tx2 := db.NewTransaction()
			err = tx2.Set(taskID, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete"))
			require.NoError(t, err)
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Clear cache to force storage resolution
			db.Cache().Clear()

			// THE ACTUAL BUG: PullInto fails with "expected Keyword, got []interface{}"
			var task TaskEntity
			err = db.PullInto(taskID, &task)

			// If bug exists, this fails with: "field Status (attr task/status): expected Keyword, got []interface {}"
			require.NoError(t, err, "PullInto should not fail - if it does, the bug still exists")

			// Verify we got the LWW-resolved value (latest wins)
			assert.Equal(t, datalog.NewKeyword(":status/complete"), task.Status,
				"should return LWW-resolved status")
		})
	}
}
