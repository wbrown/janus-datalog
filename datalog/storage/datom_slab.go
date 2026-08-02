package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
)

const (
	// slabFirstChunk keeps a small batch's slab proportionate: a handful of
	// datoms costs one 64-slot chunk, not a bulk-sized block.
	slabFirstChunk = 64
	// slabMaxChunk bounds the doubling. At ~100 bytes per datom a full chunk is
	// ~6.5 MB, and a bulk import of millions of datoms stays under ~100 chunks.
	slabMaxChunk = 65536
)

// datomSlabs hands out stable *Datom slots from chunked backing arrays, so a
// batch of n datoms costs O(log n + n/slabMaxChunk) heap objects instead of n.
// The zero value is ready to use. Chunks double from slabFirstChunk to
// slabMaxChunk; a chunk's capacity is fixed at creation and a full chunk is
// never appended to, so a handed-out pointer is never invalidated.
//
// A chunk stays reachable while any datom in it is reachable. Slabs therefore
// belong to owners whose datoms share one lifetime — a version builder's batch,
// published or abandoned whole.
type datomSlabs struct {
	chunks [][]datalog.Datom
}

// put copies d into the slab and returns the copy's address. The argument is
// the caller's workspace, unread after put returns.
func (s *datomSlabs) put(d *datalog.Datom) *datalog.Datom {
	if len(s.chunks) == 0 {
		s.chunks = append(s.chunks, make([]datalog.Datom, 0, slabFirstChunk))
	} else if last := s.chunks[len(s.chunks)-1]; len(last) == cap(last) {
		next := 2 * cap(last)
		if next > slabMaxChunk {
			next = slabMaxChunk
		}
		s.chunks = append(s.chunks, make([]datalog.Datom, 0, next))
	}

	c := &s.chunks[len(s.chunks)-1]
	*c = append(*c, *d)
	return &(*c)[len(*c)-1]
}
