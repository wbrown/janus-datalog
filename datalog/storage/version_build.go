package storage

import (
	"slices"

	"github.com/wbrown/janus-datalog/datalog"
)

// versionFromDatoms builds a version from scratch, deriving each index's order
// from one already built instead of sorting all eight independently. The datoms
// must be duplicate-free — the dump format's own property, trusted rather than
// swept for.
//
// The derivations, in sequence over the caller's slice w and one scratch s:
//
//	EAVT  full sort of w — a one-worker JDZL import arrives in this order,
//	      so the sort is pdqsort's presorted pass
//	AEVT  stable bucket of w by A into s: within an A, EAVT's relative order
//	      is (E,V,Tx,tail) — AEVT's suffix — so no datom compares at all
//	EATV  w's (E,A) groups resorted by (Tx,V,tail); the groups are attribute
//	      histories, mostly a single datom
//	AETV  stable bucket of w by A into s, as AEVT was
//	AVET  s's per-A runs resorted by (V,E,Tx,tail)
//	VAET  k-way merge of AVET's V-sorted runs into w, k = distinct attributes
//	ATEV  s's per-A runs resorted by (Tx,E,V,tail)
//	TAEV  full sort of w — Tx leads, so comparisons rarely pass one component
func versionFromDatoms(datoms []*datalog.Datom) *storeVersion {
	next := &storeVersion{}
	build := func(index IndexType, ordered []*datalog.Datom) {
		t := newDatomTree(index)
		t.buildFromSorted(ordered)
		next.trees[index] = t
	}

	if len(datoms) == 0 {
		for _, index := range Indices {
			build(index, nil)
		}
		return next
	}

	w := datoms
	s := make([]*datalog.Datom, len(datoms))

	eavt := newDatomTree(EAVT)
	slices.SortFunc(w, func(a, b *datalog.Datom) int {
		return compareDatomsInOrder(eavt.order, a, b)
	})
	eavt.buildFromSorted(w)
	next.trees[EAVT] = eavt

	runs := collectAttributeRuns(w)
	runs.place(w, s)
	build(AEVT, s)

	resortEAGroups(w)
	build(EATV, w)

	runs.place(w, s)
	build(AETV, s)

	runs.resort(s, compareVETxTail)
	build(AVET, s)

	runs.mergeByValue(s, w)
	build(VAET, w)

	runs.resort(s, compareTxEVTail)
	build(ATEV, s)

	taev := newDatomTree(TAEV)
	slices.SortFunc(w, func(a, b *datalog.Datom) int {
		return compareDatomsInOrder(taev.order, a, b)
	})
	taev.buildFromSorted(w)
	next.trees[TAEV] = taev

	return next
}

// attributeRuns is the partition of a batch by attribute: run r holds the
// datoms of attrs[r], occupying [starts[r], starts[r+1]) in any A-major slice
// the runs are placed or resorted in. Attributes are in compare order, so
// concatenated runs are A-major order.
type attributeRuns struct {
	attrs  []datalog.Keyword
	starts []int
	run    map[datalog.Keyword]int
}

func collectAttributeRuns(datoms []*datalog.Datom) attributeRuns {
	counts := make(map[datalog.Keyword]int)
	for _, d := range datoms {
		counts[d.A]++
	}
	attrs := make([]datalog.Keyword, 0, len(counts))
	for a := range counts {
		attrs = append(attrs, a)
	}
	slices.SortFunc(attrs, func(x, y datalog.Keyword) int { return x.Compare(y) })

	starts := make([]int, len(attrs)+1)
	run := make(map[datalog.Keyword]int, len(attrs))
	for r, a := range attrs {
		starts[r+1] = starts[r] + counts[a]
		run[a] = r
	}
	return attributeRuns{attrs: attrs, starts: starts, run: run}
}

// place buckets src into dst by attribute, stably: within each run, datoms keep
// src's relative order. A source sorted with A as a trailing component therefore
// arrives with each run holding that order's suffix, already sorted.
func (ar attributeRuns) place(src, dst []*datalog.Datom) {
	cursors := append([]int(nil), ar.starts[:len(ar.attrs)]...)
	for _, d := range src {
		r := ar.run[d.A]
		dst[cursors[r]] = d
		cursors[r]++
	}
}

// resort sorts each run in place by cmp, which orders the components after A —
// A is constant within a run and never compared.
func (ar attributeRuns) resort(s []*datalog.Datom, cmp func(a, b *datalog.Datom) int) {
	for r := 0; r < len(ar.attrs); r++ {
		run := s[ar.starts[r]:ar.starts[r+1]]
		if len(run) > 1 {
			slices.SortFunc(run, cmp)
		}
	}
}

// mergeByValue merges per-A runs that are each (V,E,Tx,tail)-sorted into one
// (V,A,E,Tx,tail)-sorted slice. Ties on V break by run index, which is A's
// compare order; within a run the arrival order supplies the rest, since a
// run's datoms enter the heap one at a time.
func (ar attributeRuns) mergeByValue(src, dst []*datalog.Datom) {
	if len(ar.attrs) == 1 {
		copy(dst, src)
		return
	}

	type head struct{ pos, run, end int }
	heap := make([]head, 0, len(ar.attrs))
	less := func(x, y head) bool {
		if c := compareComponent(componentV, src[x.pos], src[y.pos]); c != 0 {
			return c < 0
		}
		return x.run < y.run
	}
	siftUp := func(i int) {
		for i > 0 {
			parent := (i - 1) / 2
			if !less(heap[i], heap[parent]) {
				break
			}
			heap[i], heap[parent] = heap[parent], heap[i]
			i = parent
		}
	}
	siftDown := func(i int) {
		for {
			smallest := i
			if l := 2*i + 1; l < len(heap) && less(heap[l], heap[smallest]) {
				smallest = l
			}
			if r := 2*i + 2; r < len(heap) && less(heap[r], heap[smallest]) {
				smallest = r
			}
			if smallest == i {
				return
			}
			heap[i], heap[smallest] = heap[smallest], heap[i]
			i = smallest
		}
	}

	for r := 0; r < len(ar.attrs); r++ {
		if ar.starts[r] < ar.starts[r+1] {
			heap = append(heap, head{pos: ar.starts[r], run: r, end: ar.starts[r+1]})
			siftUp(len(heap) - 1)
		}
	}
	for w := 0; len(heap) > 0; w++ {
		top := &heap[0]
		dst[w] = src[top.pos]
		top.pos++
		if top.pos == top.end {
			heap[0] = heap[len(heap)-1]
			heap = heap[:len(heap)-1]
		}
		siftDown(0)
	}
}

// resortEAGroups converts an EAVT-sorted slice to EATV order in place: the two
// share the (E,A) prefix, so only each (E,A) group resorts, by the suffix.
func resortEAGroups(w []*datalog.Datom) {
	for i := 0; i < len(w); {
		j := i + 1
		for j < len(w) && sameEA(w[i], w[j]) {
			j++
		}
		if j-i > 1 {
			slices.SortFunc(w[i:j], compareTxVTail)
		}
		i = j
	}
}

func sameEA(a, b *datalog.Datom) bool {
	return compareComponent(componentE, a, b) == 0 &&
		compareComponent(componentA, a, b) == 0
}

func compareTxVTail(a, b *datalog.Datom) int {
	if c := compareComponent(componentTx, a, b); c != 0 {
		return c
	}
	if c := compareComponent(componentV, a, b); c != 0 {
		return c
	}
	return compareKeyTail(a, b)
}

func compareVETxTail(a, b *datalog.Datom) int {
	if c := compareComponent(componentV, a, b); c != 0 {
		return c
	}
	if c := compareComponent(componentE, a, b); c != 0 {
		return c
	}
	if c := compareComponent(componentTx, a, b); c != 0 {
		return c
	}
	return compareKeyTail(a, b)
}

func compareTxEVTail(a, b *datalog.Datom) int {
	if c := compareComponent(componentTx, a, b); c != 0 {
		return c
	}
	if c := compareComponent(componentE, a, b); c != 0 {
		return c
	}
	if c := compareComponent(componentV, a, b); c != 0 {
		return c
	}
	return compareKeyTail(a, b)
}
