package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

func TestGroupedRowIndexKeysUnique(t *testing.T) {
	e1 := datalog.NewIdentity("gri:e1")
	e2 := datalog.NewIdentity("gri:e2")

	t.Run("unique_keys", func(t *testing.T) {
		g := groupRows([]Tuple{
			{e1, int64(10)},
			{e2, int64(20)},
		}, []int{0}, []int{0})
		assert.True(t, g.keysUnique())
	})

	t.Run("fanout_key", func(t *testing.T) {
		g := groupRows([]Tuple{
			{e1, int64(10)},
			{e1, int64(11)},
			{e2, int64(20)},
		}, []int{0}, []int{0})
		assert.False(t, g.keysUnique())
	})

	t.Run("empty", func(t *testing.T) {
		g := groupRows(nil, []int{0}, []int{0})
		assert.True(t, g.keysUnique())
		assert.Nil(t, g.probe(Tuple{e1}))
	})

	t.Run("mixed_span_distinct_keys_are_unique", func(t *testing.T) {
		// Two distinct keys sharing one hash: hand-placed collision state.
		// Each key groups one row, so uniqueness holds despite the shared span.
		rows := []Tuple{{e1, int64(10)}, {e2, int64(20)}}
		g := &groupedRowIndex{
			rows: rows,
			spans: map[uint64]rowSpan{
				hashTuplePositions(Tuple{e1}, []int{0}): {start: 0, end: 2},
			},
			probePos: []int{0},
			rowPos:   []int{0},
		}
		g.regroupCollidingSpans()
		require.True(t, g.spans[hashTuplePositions(Tuple{e1}, []int{0})].mixed)
		assert.True(t, g.keysUnique())
	})

	t.Run("mixed_span_with_fanout_group_is_not_unique", func(t *testing.T) {
		rows := []Tuple{{e1, int64(10)}, {e2, int64(20)}, {e1, int64(11)}}
		g := &groupedRowIndex{
			rows: rows,
			spans: map[uint64]rowSpan{
				hashTuplePositions(Tuple{e1}, []int{0}): {start: 0, end: 3},
			},
			probePos: []int{0},
			rowPos:   []int{0},
		}
		g.regroupCollidingSpans()
		assert.False(t, g.keysUnique())
	})
}
