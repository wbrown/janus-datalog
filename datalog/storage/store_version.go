package storage

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/wbrown/janus-datalog/datalog"
)

// indexCount is the number of index orders a version carries, which is every
// entry in Indices. TestStoreVersionCoversEveryIndex holds the two together.
const indexCount = 8

// storeVersion is one state of the memory store: every index tree, published
// together as a single value.
//
// They are one value rather than eight pointers because eight independent swaps
// are not one transition. A session opening mid-commit would see some orders
// updated and others not, and since index ordering is the CRDT resolution, a
// torn cross-index state resolves the same datom set two different ways.
//
// There is no blob tier here. Blobs exist so a large value need not sit inside
// a fixed-width key; a tree holds whole datoms and has no key to keep small.
type storeVersion struct {
	trees [indexCount]*datomTree
}

// emptyStoreVersion is the state of a store with nothing in it.
func emptyStoreVersion() *storeVersion {
	v := &storeVersion{}
	for _, index := range Indices {
		v.trees[index] = newDatomTree(index)
	}
	return v
}

// tree returns the version's tree for one index order.
func (v *storeVersion) tree(index IndexType) *datomTree {
	if int(index) >= len(v.trees) || v.trees[index] == nil {
		panic(fmt.Sprintf("storeVersion: no tree for index %v", index))
	}
	return v.trees[index]
}

// datomCount is the number of datoms in the version. Every datom is in every
// tree, so any tree answers; EAVT is arbitrary among equals.
func (v *storeVersion) datomCount() int { return v.trees[EAVT].Len() }

// versionBuilder accumulates one batch — a commit, or an import chunk — across
// all eight trees, and produces the single version that publishes them
// together.
type versionBuilder struct {
	base  *storeVersion
	trees [indexCount]*treeBuilder
	done  bool
}

func (v *storeVersion) transient() *versionBuilder {
	b := &versionBuilder{base: v}
	for _, index := range Indices {
		b.trees[index] = v.tree(index).transient()
	}
	return b
}

// addDatom inserts into every index and reports whether the datom was new.
//
// The trees must agree. Equality under an index's comparator is all components
// equal, which is the same relation whatever order the index visits them in, so
// a disagreement means the trees hold different sets — the corruption this
// whole design exists to prevent, and worth failing on rather than resolving.
func (b *versionBuilder) addDatom(d *datalog.Datom) bool {
	b.mustBeOpen()

	added := b.trees[Indices[0]].insert(d)
	for _, index := range Indices[1:] {
		if b.trees[index].insert(d) != added {
			panic(fmt.Sprintf(
				"versionBuilder: index %v disagreed with %v on whether a datom was new; "+
					"the trees hold different sets", index, Indices[0]))
		}
	}
	return added
}

// removeDatom deletes from every index and reports whether the datom was
// there. As with addDatom, the trees must agree: they hold one set, visited in
// eight orders, so a disagreement means they no longer do.
func (b *versionBuilder) removeDatom(d *datalog.Datom) bool {
	b.mustBeOpen()

	removed := b.trees[Indices[0]].remove(d)
	for _, index := range Indices[1:] {
		if b.trees[index].remove(d) != removed {
			panic(fmt.Sprintf(
				"versionBuilder: index %v disagreed with %v on whether a datom was present; "+
					"the trees hold different sets", index, Indices[0]))
		}
	}
	return removed
}

// commit publishes the batch as one version and closes the builder.
func (b *versionBuilder) commit() *storeVersion {
	b.mustBeOpen()
	b.done = true

	next := &storeVersion{}
	for _, index := range Indices {
		next.trees[index] = b.trees[index].commit()
	}
	return next
}

func (b *versionBuilder) mustBeOpen() {
	if b.done {
		panic("versionBuilder: used after commit")
	}
}

// versionHolder owns the published version. Readers take the current one and
// keep it; writers are serialized and swap it when their batch completes.
//
// A reader never blocks a writer and a writer never blocks a reader, which is
// what the snapshot contract needs and what a lock over mutable trees cannot
// give without either holding a reader lock for a query's whole lifetime or
// copying the store at session open.
type versionHolder struct {
	current atomic.Pointer[storeVersion]
	writeMu sync.Mutex
}

func newVersionHolder() *versionHolder {
	h := &versionHolder{}
	h.current.Store(emptyStoreVersion())
	return h
}

// read returns the current version. Holding it keeps that whole state walkable
// no matter what commits afterward — a read session is a retained version, not
// a lock.
func (h *versionHolder) read() *storeVersion { return h.current.Load() }

// begin opens a write batch, excluding other writers until it is published or
// abandoned.
func (h *versionHolder) begin() *versionBuilder {
	h.writeMu.Lock()
	return h.current.Load().transient()
}

// publish swaps in the batch's version and releases the write lock.
func (h *versionHolder) publish(b *versionBuilder) *storeVersion {
	next := b.commit()
	h.current.Store(next)
	h.writeMu.Unlock()
	return next
}

// abandon discards the batch. Rollback is dropping a root: nothing the builder
// touched was ever reachable, so there is no undo to apply.
func (h *versionHolder) abandon(b *versionBuilder) {
	b.done = true
	h.writeMu.Unlock()
}
