//go:build !(js && wasm)

package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLamportClockNext(t *testing.T) {
	clock := NewLamportClock(42)

	// First call returns 1
	id1 := clock.Next()
	assert.Equal(t, uint64(1), id1.Lamport, "first Next() should return Lamport=1")
	assert.Equal(t, uint64(42), id1.ReplicaID, "ReplicaID should match constructor")

	// Second call returns 2
	id2 := clock.Next()
	assert.Equal(t, uint64(2), id2.Lamport, "second Next() should return Lamport=2")

	// Verify monotonicity
	for i := 0; i < 100; i++ {
		prev := clock.Current()
		next := clock.Next()
		assert.Equal(t, prev+1, next.Lamport, "Next() should be monotonically increasing")
	}
}

func TestLamportClockNextMonotonicity(t *testing.T) {
	clock := NewLamportClock(1)

	var prev ElementID
	for i := 0; i < 1000; i++ {
		curr := clock.Next()
		if i > 0 {
			assert.True(t, prev.Less(curr), "Next() must produce strictly increasing ElementIDs")
		}
		prev = curr
	}
}

func TestLamportClockReceive(t *testing.T) {
	clock := NewLamportClock(1)

	// Start with some local events
	clock.Next() // L=1
	clock.Next() // L=2
	assert.Equal(t, uint64(2), clock.Current())

	// Receive from remote with higher Lamport
	clock.Receive(ElementID{Lamport: 10, ReplicaID: 2})
	// Rule 3: L = max(L, L_remote) + 1 = max(2, 10) + 1 = 11
	assert.Equal(t, uint64(11), clock.Current())

	// Next local event should be 12
	next := clock.Next()
	assert.Equal(t, uint64(12), next.Lamport)
}

func TestLamportClockReceiveLowerValue(t *testing.T) {
	clock := NewLamportClock(1)

	// Advance to L=100
	for i := 0; i < 100; i++ {
		clock.Next()
	}
	assert.Equal(t, uint64(100), clock.Current())

	// Receive from remote with LOWER Lamport
	clock.Receive(ElementID{Lamport: 50, ReplicaID: 2})
	// Rule 3: L = max(L, L_remote) + 1 = max(100, 50) + 1 = 101
	assert.Equal(t, uint64(101), clock.Current())
}

func TestLamportClockReceiveCausality(t *testing.T) {
	// Key property: after Receive(remote), subsequent Next() must be > remote
	clock := NewLamportClock(1)

	remote := ElementID{Lamport: 500, ReplicaID: 99}
	clock.Receive(remote)

	next := clock.Next()
	assert.True(t, remote.Less(next),
		"after Receive(remote), Next() must be greater than remote: %v not > %v", next, remote)
}

func TestLamportClockRestore(t *testing.T) {
	clock := NewLamportClock(1)

	// Advance clock
	clock.Next() // L=1
	clock.Next() // L=2

	// Restore to higher value
	clock.Restore(ElementID{Lamport: 100, ReplicaID: 99})
	assert.Equal(t, uint64(100), clock.Current())

	// Next should be 101
	next := clock.Next()
	assert.Equal(t, uint64(101), next.Lamport)
}

func TestLamportClockRestoreLowerValue(t *testing.T) {
	clock := NewLamportClock(1)

	// Advance clock to L=50
	for i := 0; i < 50; i++ {
		clock.Next()
	}
	assert.Equal(t, uint64(50), clock.Current())

	// Restore with LOWER value should be ignored
	clock.Restore(ElementID{Lamport: 10, ReplicaID: 99})
	assert.Equal(t, uint64(50), clock.Current(), "Restore with lower value should be ignored")
}

func TestLamportClockRestoreVsReceive(t *testing.T) {
	// Demonstrate the difference between Restore and Receive

	// Restore: sets to exact value (for database recovery)
	clockRestore := NewLamportClock(1)
	clockRestore.Restore(ElementID{Lamport: 100, ReplicaID: 99})
	assert.Equal(t, uint64(100), clockRestore.Current(), "Restore sets exact value")

	// Receive: sets to max+1 (for causal message receipt)
	clockReceive := NewLamportClock(1)
	clockReceive.Receive(ElementID{Lamport: 100, ReplicaID: 99})
	assert.Equal(t, uint64(101), clockReceive.Current(), "Receive sets to max+1")
}

func TestLamportClockReplicaID(t *testing.T) {
	clock := NewLamportClock(12345)
	assert.Equal(t, uint64(12345), clock.ReplicaID())

	// All generated ElementIDs should have same ReplicaID
	for i := 0; i < 10; i++ {
		id := clock.Next()
		assert.Equal(t, uint64(12345), id.ReplicaID)
	}
}

func TestLamportClockPeek(t *testing.T) {
	clock := NewLamportClock(42)

	peek1 := clock.Peek()
	assert.Equal(t, uint64(1), peek1.Lamport, "Peek should return next value")
	assert.Equal(t, uint64(42), peek1.ReplicaID)

	// Peek doesn't consume
	peek2 := clock.Peek()
	assert.Equal(t, peek1, peek2, "Peek should return same value without consuming")

	// Next consumes
	next := clock.Next()
	assert.Equal(t, peek1, next, "Next should match previous Peek")

	// Now Peek returns higher value
	peek3 := clock.Peek()
	assert.Equal(t, uint64(2), peek3.Lamport)
}

func TestLamportClockConcurrency(t *testing.T) {
	clock := NewLamportClock(1)
	numGoroutines := 100
	numOpsPerGoroutine := 1000

	var wg sync.WaitGroup
	results := make(chan ElementID, numGoroutines*numOpsPerGoroutine)

	// Launch goroutines that all call Next()
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < numOpsPerGoroutine; i++ {
				id := clock.Next()
				results <- id
			}
		}()
	}

	wg.Wait()
	close(results)

	// Collect all results
	seen := make(map[uint64]bool)
	for id := range results {
		// Each Lamport value should be unique
		require.False(t, seen[id.Lamport],
			"Lamport value %d seen twice - clock is not thread-safe!", id.Lamport)
		seen[id.Lamport] = true

		// All should have same ReplicaID
		assert.Equal(t, uint64(1), id.ReplicaID)
	}

	// Should have exactly numGoroutines * numOpsPerGoroutine unique values
	expected := numGoroutines * numOpsPerGoroutine
	assert.Equal(t, expected, len(seen), "should have %d unique Lamport values", expected)

	// Final counter should match
	assert.Equal(t, uint64(expected), clock.Current())
}

func TestLamportClockConcurrentReceive(t *testing.T) {
	clock := NewLamportClock(1)
	numGoroutines := 50

	var wg sync.WaitGroup

	// Launch goroutines that call Receive with various values
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(val uint64) {
			defer wg.Done()
			clock.Receive(ElementID{Lamport: val, ReplicaID: val})
		}(uint64(g * 10))
	}

	wg.Wait()

	// Final value should be max of all receives + 1 (for each receive)
	// But since receives happen concurrently, the final value depends on ordering
	// We can only assert it's >= the max input + 1
	maxInput := uint64((numGoroutines - 1) * 10) // 490
	assert.GreaterOrEqual(t, clock.Current(), maxInput+1,
		"clock should be at least max(inputs)+1")
}

func TestLamportClockConcurrentNextAndReceive(t *testing.T) {
	clock := NewLamportClock(1)
	numOps := 1000

	var wg sync.WaitGroup

	// Half do Next(), half do Receive()
	for i := 0; i < numOps; i++ {
		wg.Add(1)
		if i%2 == 0 {
			go func() {
				defer wg.Done()
				clock.Next()
			}()
		} else {
			go func(val uint64) {
				defer wg.Done()
				clock.Receive(ElementID{Lamport: val, ReplicaID: 99})
			}(uint64(i))
		}
	}

	wg.Wait()

	// Clock should have advanced significantly
	// Exact value depends on execution order, but should be substantial
	assert.Greater(t, clock.Current(), uint64(numOps/2),
		"clock should have advanced from concurrent operations")
}

func TestLamportClockZeroReplicaID(t *testing.T) {
	// ReplicaID 0 is valid (though unusual)
	clock := NewLamportClock(0)
	assert.Equal(t, uint64(0), clock.ReplicaID())

	id := clock.Next()
	assert.Equal(t, uint64(1), id.Lamport)
	assert.Equal(t, uint64(0), id.ReplicaID)
}

func TestLamportClockMaxValues(t *testing.T) {
	// Test behavior near max uint64
	clock := NewLamportClock(^uint64(0)) // Max ReplicaID

	clock.Restore(ElementID{Lamport: ^uint64(0) - 10, ReplicaID: 0})

	// Should be able to advance a few more times
	for i := 0; i < 5; i++ {
		id := clock.Next()
		assert.Equal(t, ^uint64(0), id.ReplicaID)
	}
}

// Benchmark tests

func BenchmarkLamportClockNext(b *testing.B) {
	clock := NewLamportClock(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clock.Next()
	}
}

func BenchmarkLamportClockNextParallel(b *testing.B) {
	clock := NewLamportClock(1)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			clock.Next()
		}
	})
}

func BenchmarkLamportClockReceive(b *testing.B) {
	clock := NewLamportClock(1)
	remote := ElementID{Lamport: 1000000, ReplicaID: 2}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clock.Receive(remote)
	}
}
