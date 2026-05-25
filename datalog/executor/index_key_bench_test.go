package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// BenchmarkInMemoryIndexKeys compares the in-memory matcher's (E,A) index using
// the old string key (E.L85()+"|"+A.String()) against the new interned-pointer
// key (eaIndexKey{E, A}). It models the build + lookup the matcher does per datom.
//
// The string variant uses PRECOMPUTED L85 strings, which is the fair model of the
// old design: L85 was cached on the identity, so the per-datom cost was the "|"
// concatenation + a ~50-char map hash, not the Base85 encode. The pointer variant
// builds a 16-byte two-pointer key with no allocation and a 16-byte hash.
func BenchmarkInMemoryIndexKeys(b *testing.B) {
	const numEntities, numAttrs = 1000, 8

	entities := make([]datalog.Identity, numEntities)
	eL85 := make([]string, numEntities)
	for i := range entities {
		entities[i] = datalog.NewIdentity(fmt.Sprintf("entity:%d", i))
		eL85[i] = entities[i].L85()
	}
	attrs := make([]datalog.Keyword, numAttrs)
	aStr := make([]string, numAttrs)
	for i := range attrs {
		attrs[i] = datalog.NewKeyword(fmt.Sprintf(":attr/%d", i))
		aStr[i] = attrs[i].String()
	}

	b.Run("string_key", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			idx := make(map[string][]int, numEntities*numAttrs)
			for i := 0; i < numEntities; i++ {
				for j := 0; j < numAttrs; j++ {
					key := eL85[i] + "|" + aStr[j]
					idx[key] = append(idx[key], i*numAttrs+j)
				}
			}
			sink := 0
			for i := 0; i < numEntities; i++ {
				for j := 0; j < numAttrs; j++ {
					sink += len(idx[eL85[i]+"|"+aStr[j]])
				}
			}
			if sink != numEntities*numAttrs {
				b.Fatalf("sink=%d", sink)
			}
		}
	})

	b.Run("pointer_key", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			idx := make(map[eaIndexKey][]int, numEntities*numAttrs)
			for i := 0; i < numEntities; i++ {
				for j := 0; j < numAttrs; j++ {
					key := eaIndexKey{e: entities[i], a: attrs[j]}
					idx[key] = append(idx[key], i*numAttrs+j)
				}
			}
			sink := 0
			for i := 0; i < numEntities; i++ {
				for j := 0; j < numAttrs; j++ {
					sink += len(idx[eaIndexKey{e: entities[i], a: attrs[j]}])
				}
			}
			if sink != numEntities*numAttrs {
				b.Fatalf("sink=%d", sink)
			}
		}
	})
}
