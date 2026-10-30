package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A set sized from a relation's size answer starts empty when that answer
// is -1, a stream's size, and admits keys as any set does
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, L8). SemiJoin and AntiJoin
// size their right side's key set this way.
func TestTupleSet_WithAnUnknownCapacityStartsEmpty(t *testing.T) {
	positions := allPositions(2)
	keys := NewTupleSet(positions, positions, -1)
	key := Tuple{int64(1), "a"}
	_, hash, held := keys.Find(key)
	require.False(t, held, "a new set holds no key")
	require.Zero(t, keys.Len())
	keys.Add(key, hash)
	require.True(t, keys.Probe(key), "an added key is held")
	require.Equal(t, 1, keys.Len())
}
