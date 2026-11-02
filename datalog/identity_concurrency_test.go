package datalog

import (
	"crypto/sha1"
	"sync"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/codec"
)

// TestIdentityL85ConcurrentAccess is a regression test for
// BUG_IDENTITY_L85_LAZY_RACE. Identity values are globally interned and shared
// across goroutines, so L85()/String() must not mutate the shared identity.
//
// Run under -race: a lazy write to l85/l85Computed on the shared interned
// object would race across concurrent L85() calls. The identity is immutable
// and L85 is computed from its hash on each call, so concurrent reads are
// safe. Every
// concurrent read answers the L85 of the seed's SHA1, and the hash is the
// seed's SHA1 after them.
func TestIdentityL85ConcurrentAccess(t *testing.T) {
	hash := sha1.Sum([]byte("race-target"))
	want := codec.EncodeL85(hash[:])
	id := NewIdentity("race-target")

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				if got := id.L85(); got != want {
					t.Errorf("L85() = %q under concurrent reads, want %q", got, want)
					return
				}
				if got := id.String(); got != want {
					t.Errorf("String() = %q under concurrent reads, want %q", got, want)
					return
				}
			}
		}()
	}
	wg.Wait()
	if id.Hash() != hash {
		t.Fatalf("the shared identity's hash is %x after concurrent reads, want %x", id.Hash(), hash)
	}
}
