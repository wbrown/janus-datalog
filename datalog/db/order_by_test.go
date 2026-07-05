package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TestOrderByNonProjectedThroughStorage exercises the non-projected sort-key
// fix end-to-end against real BadgerDB storage (planner retention through
// the BadgerMatcher path, executor sort-strip finalization), not a synthetic
// in-memory matcher. See
// docs/bugs/resolved/BUG_ORDER_BY_NON_PROJECTED_VARIABLE_SILENTLY_IGNORED.md.
func TestOrderByNonProjectedThroughStorage(t *testing.T) {
	d := tempDB(t)

	name := datalog.NewKeyword(":task/name")
	priority := datalog.NewKeyword(":task/priority")

	tasks := []struct {
		id       string
		name     string
		priority int64
	}{
		{"task:a", "alpha", 30},
		{"task:b", "beta", 10},
		{"task:c", "gamma", 50},
		{"task:d", "delta", 20},
	}

	tx := d.NewTransaction()
	for _, task := range tasks {
		e := datalog.NewIdentity(task.id)
		require.NoError(t, tx.Add(e, name, task.name))
		require.NoError(t, tx.Add(e, priority, task.priority))
	}
	_, err := tx.Commit()
	require.NoError(t, err)

	t.Run("sorts by non-projected priority", func(t *testing.T) {
		rel, err := d.Query(`[:find ?name
		                      :where [?t :task/name ?name]
		                             [?t :task/priority ?p]
		                      :order-by [[?p :desc]]]`)
		require.NoError(t, err)
		got := collectFirstCol(t, rel)
		require.Equal(t, []interface{}{"gamma", "alpha", "delta", "beta"}, got)
		// The retained sort column is stripped from the result shape.
		require.Len(t, rel.Symbols(), 1)
	})

	t.Run("highest-priority row via non-projected key and limit 1", func(t *testing.T) {
		rel, err := d.Query(`[:find ?name
		                      :where [?t :task/name ?name]
		                             [?t :task/priority ?p]
		                      :order-by [[?p :desc]]
		                      :limit 1]`)
		require.NoError(t, err)
		got := collectFirstCol(t, rel)
		require.Equal(t, []interface{}{"gamma"}, got)
	})
}
