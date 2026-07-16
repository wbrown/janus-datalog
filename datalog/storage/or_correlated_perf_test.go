//go:build !(js && wasm)

package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// TestOrCorrelatedUnionPartialOuterRelation reproduces a performance bug where
// correlated union only injects one input group as the outer relation, leaving
// collection-bound variables (?fwd) unbound. This causes branch patterns to
// match ALL attributes instead of just the collection keywords, turning a
// microsecond query into a multi-second full-DB scan.
//
// See docs/bugs/BUG-CORRELATED-UNION-PARTIAL-OUTER-RELATION.md
func TestOrCorrelatedUnionPartialOuterRelation(t *testing.T) {
	dir, err := os.MkdirTemp("", "or-partial-outer-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })

	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	// Build a database with ~600 entities across three types.
	// Enough to expose the O(entities × attributes) blowup when ?fwd is unbound.
	group1 := datalog.NewIdentity("group:alpha")
	group2 := datalog.NewIdentity("group:beta")

	tx := db.NewTransaction()

	// 500 items with :item/group refs + several other attributes
	items := make([]datalog.Identity, 500)
	for i := range items {
		items[i] = datalog.NewIdentity(fmt.Sprintf("item:%d", i))
		tx.Add(items[i], datalog.NewKeyword(":item/label"), fmt.Sprintf("Item %d", i))
		tx.Add(items[i], datalog.NewKeyword(":item/kind"), datalog.NewKeyword(":kind/widget"))
		tx.Add(items[i], datalog.NewKeyword(":item/sku"), fmt.Sprintf("SKU%03d", i))
		if i < 250 {
			tx.Add(items[i], datalog.NewKeyword(":item/group"), group1)
		} else {
			tx.Add(items[i], datalog.NewKeyword(":item/group"), group2)
		}
	}

	// 50 agents with :agent/container refs
	agents := make([]datalog.Identity, 50)
	for i := range agents {
		agents[i] = datalog.NewIdentity(fmt.Sprintf("agent:%d", i))
		tx.Add(agents[i], datalog.NewKeyword(":item/label"), fmt.Sprintf("Agent %d", i))
		tx.Add(agents[i], datalog.NewKeyword(":item/kind"), datalog.NewKeyword(":kind/agent"))
		tx.Add(agents[i], datalog.NewKeyword(":agent/container"), items[i%500])
	}

	// 50 events — have :item/label and :item/sku but NO :agent/container
	// This is the bug trigger: ?fwd = [:agent/container] but entity has no such attr
	events := make([]datalog.Identity, 50)
	for i := range events {
		events[i] = datalog.NewIdentity(fmt.Sprintf("event:%d", i))
		tx.Add(events[i], datalog.NewKeyword(":item/label"), fmt.Sprintf("Event %d", i))
		tx.Add(events[i], datalog.NewKeyword(":item/kind"), datalog.NewKeyword(":kind/event"))
		tx.Add(events[i], datalog.NewKeyword(":item/sku"), fmt.Sprintf("EV%03d", i))
	}

	tx.Add(group1, datalog.NewKeyword(":item/label"), "Alpha Group")
	tx.Add(group2, datalog.NewKeyword(":item/label"), "Beta Group")

	_, err = tx.Commit()
	require.NoError(t, err)

	// Bug query: ?self is an event (no :agent/container), ?fwd = [:agent/container].
	// Branch 1 should fail immediately (event has no :agent/container).
	// Branches 2-3 check :index/mentions which has 0 datoms.
	// Correct: 0 results, microseconds.
	//
	// With the bug: ?fwd is unbound, [?self ?fwd ?target] matches ALL attributes
	// of ?self, and for each the inner OR scans the full DB.
	self := events[0]

	start := time.Now()
	results, err := executor.CollectTuples(db.Query(`
		[:find ?related :in $ ?self [?fwd ...]
		 :where
		 (or (and [?self ?fwd ?target]
		          (or [?related ?fwd ?target]
		              [(identity ?target) ?related]))
		     (and [?self :item/label ?label]
		          [?related :index/mentions ?label])
		     (and [?self :item/sku ?sku]
		          [?related :index/mentions ?sku]))]`,
		self,
		[]datalog.Keyword{datalog.NewKeyword(":agent/container")},
	))
	elapsed := time.Since(start)
	require.NoError(t, err)

	t.Logf("Results: %d, Time: %v", len(results), elapsed)

	// Should return 0 results (event has no :agent/container)
	assert.Empty(t, results, "entity without the collection attribute should return 0 results")

	// Should complete in well under 1 second — the bug makes it take 10+ seconds
	assert.Less(t, elapsed, 1*time.Second,
		"query took %v — ?fwd is likely unbound inside OR (partial outer relation bug)", elapsed)
}
