package storage

import "sync/atomic"

// LamportClock implements Lamport's logical clock algorithm.
//
// IMPORTANT: One clock per Database instance, shared across all attributes.
// This provides global temporal ordering for point-in-time queries, causal
// debugging, and as-of functionality.
//
// Rules (from Lamport 1978):
//  1. Before each local event: L = L + 1
//  2. When sending: include L in message
//  3. When receiving message with L_remote: L = max(L, L_remote) + 1
//
// This ensures: if A causally precedes B, then L(A) < L(B)
//
// Thread Safety: All methods are safe for concurrent use. The counter
// is updated using atomic operations, ensuring correctness under high
// write contention from multiple goroutines.
type LamportClock struct {
	counter   uint64
	replicaID uint64
}

// NewLamportClock creates a clock for the given replica.
// The clock starts at 0; the first call to Next() returns Lamport=1.
func NewLamportClock(replicaID uint64) *LamportClock {
	return &LamportClock{replicaID: replicaID}
}

// Next generates the next ElementID for a local event.
// Implements rule 1: L = L + 1 before each event.
//
// This is the primary method for generating timestamps during writes.
// Each call returns a unique, monotonically increasing ElementID.
//
// Thread-safe: uses atomic increment.
func (c *LamportClock) Next() ElementID {
	next := atomic.AddUint64(&c.counter, 1)
	return ElementID{Lamport: next, ReplicaID: c.replicaID}
}

// Receive updates the clock when receiving data from another node.
// Implements rule 3: L = max(L, L_remote) + 1
//
// This is the critical Lamport property: receiving a message is an event,
// and the receiver's clock must be greater than both its previous value
// AND the sender's timestamp.
//
// Use this when merging data from another replica. After Receive(),
// subsequent Next() calls will return ElementIDs greater than the remote.
//
// Thread-safe: uses compare-and-swap loop.
func (c *LamportClock) Receive(remote ElementID) {
	for {
		current := atomic.LoadUint64(&c.counter)
		// L = max(L, L_remote) + 1
		newVal := maxUint64(current, remote.Lamport) + 1
		if atomic.CompareAndSwapUint64(&c.counter, current, newVal) {
			return
		}
		// CAS failed due to concurrent modification, retry
	}
}

// Restore sets the clock to a known value without incrementing.
// Used ONLY for restoring state on database reopen, NOT for receiving messages.
//
// Unlike Receive(), Restore() does not add 1. This is intentional:
// - Receive() models a causal event (message receipt) → must advance
// - Restore() models state recovery (not a causal event) → just set value
//
// After Restore(maxSeen), the next Next() call returns maxSeen.Lamport + 1.
//
// Thread-safe: uses compare-and-swap loop.
func (c *LamportClock) Restore(maxSeen ElementID) {
	for {
		current := atomic.LoadUint64(&c.counter)
		if maxSeen.Lamport <= current {
			return // Already at or past this value
		}
		if atomic.CompareAndSwapUint64(&c.counter, current, maxSeen.Lamport) {
			return
		}
		// CAS failed, retry
	}
}

// Current returns the current clock value without incrementing.
// This is the Lamport timestamp that would be returned by the PREVIOUS Next() call.
// The NEXT Next() call will return Current() + 1.
//
// Thread-safe: uses atomic load.
func (c *LamportClock) Current() uint64 {
	return atomic.LoadUint64(&c.counter)
}

// ReplicaID returns this clock's replica identifier.
// This is set at construction time and never changes.
func (c *LamportClock) ReplicaID() uint64 {
	return c.replicaID
}

// Peek returns what the next ElementID would be without consuming it.
// Equivalent to ElementID{Lamport: Current() + 1, ReplicaID: ReplicaID()}.
//
// Note: In a concurrent environment, the actual next ElementID may differ
// if another goroutine calls Next() between Peek() and your Next() call.
func (c *LamportClock) Peek() ElementID {
	return ElementID{
		Lamport:   atomic.LoadUint64(&c.counter) + 1,
		ReplicaID: c.replicaID,
	}
}

// maxUint64 returns the larger of two uint64 values.
func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
