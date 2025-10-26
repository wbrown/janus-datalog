package datalog

import (
	"fmt"
	"testing"
)

// BenchmarkInternKeyword measures current keyword interning performance
func BenchmarkInternKeyword(b *testing.B) {
	// Clear cache for clean benchmark
	ClearInterns()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Simulate realistic attribute names (100 unique)
			key := fmt.Sprintf(":attr/%d", i%100)
			InternKeyword(key)
			i++
		}
	})
}

// BenchmarkInternKeywordHighContention measures with high contention (10 unique keys)
func BenchmarkInternKeywordHighContention(b *testing.B) {
	ClearInterns()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Only 10 unique keys - high contention
			key := fmt.Sprintf(":attr/%d", i%10)
			InternKeyword(key)
			i++
		}
	})
}

// BenchmarkInternIdentity measures identity interning performance
func BenchmarkInternIdentity(b *testing.B) {
	ClearInterns()

	// Pre-create identities
	ids := make([]Identity, 100)
	for i := 0; i < 100; i++ {
		ids[i] = NewIdentity(fmt.Sprintf("entity:%d", i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			InternIdentity(ids[i%100])
			i++
		}
	})
}

// BenchmarkInternIdentityFromHash measures hash-based identity interning
func BenchmarkInternIdentityFromHash(b *testing.B) {
	ClearInterns()

	// Pre-create hashes
	hashes := make([][20]byte, 100)
	for i := 0; i < 100; i++ {
		id := NewIdentity(fmt.Sprintf("entity:%d", i))
		hashes[i] = id.Hash()
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			InternIdentityFromHash(hashes[i%100])
			i++
		}
	})
}

// BenchmarkInternMixed simulates realistic workload (both keywords and identities)
func BenchmarkInternMixed(b *testing.B) {
	ClearInterns()

	// Pre-create test data
	ids := make([]Identity, 50)
	for i := 0; i < 50; i++ {
		ids[i] = NewIdentity(fmt.Sprintf("entity:%d", i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Alternate between keywords and identities
			if i%2 == 0 {
				key := fmt.Sprintf(":attr/%d", i%50)
				InternKeyword(key)
			} else {
				InternIdentity(ids[i%50])
			}
			i++
		}
	})
}
