package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TestDatomSlabsPutCopiesTheWorkspace pins the slot contract: put returns a
// fresh address holding the value at put time, immune to later workspace reuse,
// across chunk boundaries.
func TestDatomSlabsPutCopiesTheWorkspace(t *testing.T) {
	var slabs datomSlabs
	attr := datalog.NewKeyword(":slab/value")

	const n = 1000
	var workspace datalog.Datom
	stored := make([]*datalog.Datom, n)
	for i := 0; i < n; i++ {
		workspace = datalog.Datom{
			E:  datalog.NewIdentity(fmt.Sprintf("slab:entity:%d", i)),
			A:  attr,
			V:  int64(i),
			Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 1},
		}
		stored[i] = slabs.put(&workspace)
		require.NotSame(t, &workspace, stored[i])
	}

	// Every slot still holds its own value: a chunk that reallocated under a
	// later put would have stranded earlier pointers on stale bytes.
	for i, d := range stored {
		require.Equal(t, int64(i), d.V, "slot %d", i)
		require.Equal(t, uint64(i+1), d.Tx.Lamport, "slot %d", i)
	}
}

// TestDatomSlabsChunkGrowth pins the object-count shape: chunks double from
// slabFirstChunk toward slabMaxChunk, so n datoms cost O(log n) chunk objects.
func TestDatomSlabsChunkGrowth(t *testing.T) {
	workspace := datalog.Datom{
		E:  datalog.NewIdentity("slab:growth"),
		A:  datalog.NewKeyword(":slab/value"),
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
	}

	put := func(slabs *datomSlabs, n int) {
		for i := 0; i < n; i++ {
			slabs.put(&workspace)
		}
	}

	var exactlyFirst datomSlabs
	put(&exactlyFirst, slabFirstChunk)
	require.Len(t, exactlyFirst.chunks, 1)

	var overflowsFirst datomSlabs
	put(&overflowsFirst, slabFirstChunk+1)
	require.Len(t, overflowsFirst.chunks, 2)

	// 64+128+256+512+1024 = 1984 is the first doubling prefix covering 1000.
	var thousand datomSlabs
	put(&thousand, 1000)
	require.Len(t, thousand.chunks, 5)
}
