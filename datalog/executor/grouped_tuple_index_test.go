package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

func TestGroupedTupleIndexKeysUnique(t *testing.T) {
	e1 := datalog.NewIdentity("gti:e1")
	e2 := datalog.NewIdentity("gti:e2")

	t.Run("unique_keys", func(t *testing.T) {
		g := groupTuples([]Tuple{
			{e1, int64(10)},
			{e2, int64(20)},
		}, []int{0}, []int{0})
		assert.True(t, g.keysUnique())
	})

	t.Run("fanout_key", func(t *testing.T) {
		g := groupTuples([]Tuple{
			{e1, int64(10)},
			{e1, int64(11)},
			{e2, int64(20)},
		}, []int{0}, []int{0})
		assert.False(t, g.keysUnique())
	})

	t.Run("empty", func(t *testing.T) {
		g := groupTuples(nil, []int{0}, []int{0})
		assert.True(t, g.keysUnique())
		assert.Nil(t, g.probe(Tuple{e1}))
	})

	t.Run("mixed_span_distinct_keys_are_unique", func(t *testing.T) {
		// Two distinct keys sharing one hash: hand-placed collision state.
		// Each key groups one tuple, so uniqueness holds despite the shared
		// span.
		tuples := []Tuple{{e1, int64(10)}, {e2, int64(20)}}
		g := &groupedTupleIndex{
			tuples: tuples,
			spans: map[uint64]tupleSpan{
				hashTuplePositions(Tuple{e1}, []int{0}): {start: 0, end: 2},
			},
			probePos:  []int{0},
			storedPos: []int{0},
		}
		g.regroupCollidingSpans()
		require.True(t, g.spans[hashTuplePositions(Tuple{e1}, []int{0})].mixed)
		assert.True(t, g.keysUnique())
	})

	t.Run("mixed_span_with_fanout_group_is_not_unique", func(t *testing.T) {
		tuples := []Tuple{{e1, int64(10)}, {e2, int64(20)}, {e1, int64(11)}}
		g := &groupedTupleIndex{
			tuples: tuples,
			spans: map[uint64]tupleSpan{
				hashTuplePositions(Tuple{e1}, []int{0}): {start: 0, end: 3},
			},
			probePos:  []int{0},
			storedPos: []int{0},
		}
		g.regroupCollidingSpans()
		assert.False(t, g.keysUnique())
	})
}
