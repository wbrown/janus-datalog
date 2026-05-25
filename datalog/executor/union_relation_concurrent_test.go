package executor

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestUnionRelation_ConcurrentIteratorWhileBuilding reproduces Failure Mode 1 of
// BUG_UNION_RELATION_CONCURRENT_ITERATION_AND_ERROR_DROP.
//
// UnionRelation is supposed to be a reusable Relation: every Iterator() call must
// see the complete union. But Iterator() only holds the mutex while constructing
// the iterator, not while the cache is being built. Two Iterator() calls that
// race before cacheBuilt both receive a UnionIterator over the SAME one-shot
// channel and the SAME cache slice. They therefore:
//   - split the channel's items (neither sees the complete union), and
//   - append to the shared cache concurrently (a data race under -race).
//
// Each concurrent Iterator() must instead see all n tuples.
func TestUnionRelation_ConcurrentIteratorWhileBuilding(t *testing.T) {
	const n = 500
	syms := []query.Symbol{datalog.NewSymbol("?x")}

	ch := make(chan relationItem, n)
	for i := 0; i < n; i++ {
		ch <- relationItem{relation: NewMaterializedRelation(syms, []Tuple{{int64(i)}})}
	}
	close(ch)

	ur := NewUnionRelation(ch, syms, ExecutorOptions{})

	collect := func() int {
		it := ur.Iterator()
		defer it.Close()
		count := 0
		for it.Next() {
			count++
		}
		return count
	}

	var wg sync.WaitGroup
	var c1, c2 int
	wg.Add(2)
	go func() { defer wg.Done(); c1 = collect() }()
	go func() { defer wg.Done(); c2 = collect() }()
	wg.Wait()

	require.Equal(t, n, c1, "first concurrent Iterator() must see the complete union, not a split of the channel")
	require.Equal(t, n, c2, "second concurrent Iterator() must see the complete union, not a split of the channel")
}

// countingRelation records how many times Iterator() is called on it.
type countingRelation struct {
	Relation
	calls *int64
}

func (c countingRelation) Iterator() Iterator {
	atomic.AddInt64(c.calls, 1)
	return c.Relation.Iterator()
}

// TestUnionRelation_OnlyOneChannelConsumer verifies the one-shot channel is
// consumed exactly once (by the sole builder), and that a later Iterator() call
// replays from the cache without re-iterating the source relations.
func TestUnionRelation_OnlyOneChannelConsumer(t *testing.T) {
	const n = 50
	syms := []query.Symbol{datalog.NewSymbol("?x")}

	var calls int64
	ch := make(chan relationItem, n)
	for i := 0; i < n; i++ {
		base := NewMaterializedRelation(syms, []Tuple{{int64(i)}})
		ch <- relationItem{relation: countingRelation{Relation: base, calls: &calls}}
	}
	close(ch)

	ur := NewUnionRelation(ch, syms, ExecutorOptions{})

	drain := func() int {
		it := ur.Iterator()
		defer it.Close()
		c := 0
		for it.Next() {
			c++
		}
		return c
	}

	// First Iterator(): the sole builder consumes every source relation once.
	require.Equal(t, n, drain(), "builder must see all tuples")
	require.Equal(t, int64(n), atomic.LoadInt64(&calls), "builder must iterate each source relation exactly once")

	// Second Iterator(): replays from cache; it must not re-iterate the sources.
	require.Equal(t, n, drain(), "replay must see all tuples")
	require.Equal(t, int64(n), atomic.LoadInt64(&calls), "replay must not re-consume source relations")
}
