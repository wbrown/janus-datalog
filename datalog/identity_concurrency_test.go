package datalog

import (
	"sync"
	"testing"
)

// TestIdentityL85ConcurrentAccess is a regression test for
// BUG_IDENTITY_L85_LAZY_RACE. Identity values are globally interned and shared
// across goroutines, so L85()/String() must not mutate the shared identity.
//
// Run under -race: a lazy write to l85/l85Computed on the shared interned
// object would race across concurrent L85() calls. The identity is immutable
// — l85 is computed at construction — so concurrent reads are safe.
func TestIdentityL85ConcurrentAccess(t *testing.T) {
	id := NewIdentity("race-target")

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				_ = id.L85()
				_ = id.String()
			}
		}()
	}
	wg.Wait()
}
