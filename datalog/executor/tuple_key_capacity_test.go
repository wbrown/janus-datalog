package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A key map sized from a relation's size answer starts empty when that answer
// is -1, a stream's size, and admits keys as any map does
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, L8). SemiJoin and AntiJoin
// size their right side's key map this way.
func TestTupleKeyMapWithAnUnknownCapacityStartsEmpty(t *testing.T) {
	keys := NewTupleKeyMapWithCapacity(-1)
	key := NewTupleKeyFull(Tuple{int64(1), "a"})
	require.False(t, keys.Exists(key), "a new map holds no key")
	require.False(t, keys.PutIfAbsent(key, true), "the first put of a key reports it absent")
	require.True(t, keys.Exists(key), "a put key is held")
}
