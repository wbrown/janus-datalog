package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TestDatabaseClockRestoration pins the open-path clock restoration: a new
// database session over existing stored state must restore the Lamport clock
// from the store's max ElementID, so new writes sort after every prior
// session's writes. Runs over the reopen backend matrix — the mechanism under
// test is the open logic reading the store, not the store's durability.
func TestDatabaseClockRestoration(t *testing.T) {
	for _, c := range reopenBackendCases() {
		t.Run(c.name, func(t *testing.T) {
			// Phase 1: first session writes multiple transactions to
			// advance the clock.
			db := c.open(t, DatabaseOptions{ReplicaID: 1})
			for i := 0; i < 10; i++ {
				tx := db.NewTransaction()
				entity := datalog.NewIdentity(fmt.Sprintf("test:entity%d", i))
				attr := datalog.NewKeyword(":test/value")
				tx.Add(entity, attr, fmt.Sprintf("value%d", i))
				_, err := tx.Commit()
				require.NoError(t, err)
			}

			maxAfterFirstSession, err := db.store.MaxElementID()
			require.NoError(t, err)
			t.Logf("Max ElementID after first session: %v", maxAfterFirstSession)

			// Phase 2: reopen over the same state and verify restoration.
			db2 := c.open(t, DatabaseOptions{ReplicaID: 1})

			currentClock := db2.clock.Current()
			t.Logf("Clock after reopen: %d", currentClock)
			assert.GreaterOrEqual(t, currentClock, maxAfterFirstSession.Lamport,
				"Clock should be restored to at least the max from previous session")

			// New writes should have higher ElementIDs.
			tx := db2.NewTransaction()
			entity := datalog.NewIdentity("test:new-entity")
			attr := datalog.NewKeyword(":test/value")
			tx.Add(entity, attr, "new-value")
			_, err = tx.Commit()
			require.NoError(t, err)

			newMax, err := db2.store.MaxElementID()
			require.NoError(t, err)
			t.Logf("Max ElementID after new write: %v", newMax)

			assert.True(t, maxAfterFirstSession.Less(newMax),
				"New writes after reopen should have higher ElementIDs than previous session")
		})
	}
}
