package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// nodeGrowthStep is how much a node's slices grow when they fill. Go's append
// grows by roughly doubling, which at branchingFactor 256 overshoots badly —
// a node one entry past 128 would jump to 256 slots. Growth is explicit and
// small instead, and never exceeds one entry past the branching factor, which
// is the transient overflow a split immediately resolves.
const nodeGrowthStep = 8

// ownerToken identifies the builder that created a node. A node carrying the
// active builder's token is unreachable by anyone else and can be mutated in
// place; any other node must be copied before it is changed.
//
// The field is load-bearing and must not be removed. Go returns the same
// address for every zero-sized allocation, so a struct{} token would make every
// builder's &ownerToken{} identical — every node would look self-owned, and
// builders would mutate published nodes in place while readers walked them.
// TestOwnerTokensAreDistinct pins it.
type ownerToken struct {
	_ byte
}

// treeBuilder applies a batch of inserts to a tree, producing a new root that
// shares everything the batch did not touch. This is the Clojure transient
// pattern: within one batch a node is copied at most once, so a commit
// path-copies once per *touched node* rather than once per datom.
//
// The builder never writes through to the tree it came from. Its token is
// dropped at commit, after which every node it created is immutable, because
// no future builder can hold the same token.
type treeBuilder struct {
	index IndexType
	order [componentsPerIndex]keyComponent
	root  *node
	count int
	token *ownerToken
}

// transient opens a builder over the tree. The tree itself is not modified;
// readers holding it keep seeing exactly what they saw.
func (t *datomTree) transient() *treeBuilder {
	return &treeBuilder{
		index: t.index,
		order: t.order,
		root:  t.root,
		count: t.count,
		token: &ownerToken{},
	}
}

// commit publishes the batch as a new tree and closes the builder. Using a
// builder after commit would mutate nodes the returned tree has published, so
// the token is cleared and further inserts panic rather than corrupt it.
func (b *treeBuilder) commit() *datomTree {
	if b.token == nil {
		panic("treeBuilder: commit called twice")
	}
	b.token = nil
	return &datomTree{index: b.index, order: b.order, root: b.root, count: b.count}
}

func (b *treeBuilder) compare(x, y *datalog.Datom) int {
	return compareDatomsInOrder(b.order, x, y)
}

// insert adds d and reports whether it was new. A datom already present under
// this index's order leaves the tree untouched: the trees are sets.
func (b *treeBuilder) insert(d *datalog.Datom) bool {
	if b.token == nil {
		panic("treeBuilder: insert after commit")
	}

	if b.root == nil {
		b.root = &node{level: 0, owner: b.token, keys: []*datalog.Datom{d}}
		b.count = 1
		return true
	}

	updated, split, added := b.insertInto(b.root, d)
	if !added {
		return false
	}
	if split != nil {
		b.root = &node{
			level:    updated.level + 1,
			owner:    b.token,
			keys:     []*datalog.Datom{updated.maxDatom(), split.maxDatom()},
			children: []*node{updated, split},
		}
	} else {
		b.root = updated
	}
	b.count++
	return true
}

// remove deletes d and reports whether it was there.
//
// Nodes are allowed to fall below half full: retraction is a CRDT Remove datom
// rather than a deletion, so this path serves truncate and rebuild, not user
// writes. Skipping rebalancing removes the most intricate part of a B-tree, and
// a store that deletes enough to care can rebuild its trees from what remains.
// Empty nodes are still dropped, and a root left with one child collapses, so
// repeated deletion cannot leave a spine of useless levels behind.
func (b *treeBuilder) remove(d *datalog.Datom) bool {
	if b.token == nil {
		panic("treeBuilder: remove after commit")
	}
	if b.root == nil {
		return false
	}

	updated, removed := b.removeFrom(b.root, d)
	if !removed {
		return false
	}
	for updated != nil && !updated.isLeaf() && len(updated.children) == 1 {
		updated = updated.children[0]
	}
	b.root = updated
	b.count--
	return true
}

// removeFrom deletes d from the subtree at n, returning the node to use in n's
// place — nil when the subtree is now empty — and whether d was found.
func (b *treeBuilder) removeFrom(n *node, d *datalog.Datom) (updated *node, removed bool) {
	if n.isLeaf() {
		idx := b.searchNode(n, d)
		if idx == len(n.keys) || b.compare(n.keys[idx], d) != 0 {
			return n, false
		}
		editable := b.editable(n)
		editable.keys = removeDatomAt(editable.keys, idx)
		if len(editable.keys) == 0 {
			return nil, true
		}
		return editable, true
	}

	idx := b.searchNode(n, d)
	if idx == len(n.keys) {
		// Past every separator, so d is beyond this subtree's maximum.
		return n, false
	}

	childUpdated, removed := b.removeFrom(n.children[idx], d)
	if !removed {
		return n, false
	}

	editable := b.editable(n)
	if childUpdated == nil {
		editable.keys = removeDatomAt(editable.keys, idx)
		editable.children = removeNodeAt(editable.children, idx)
		if len(editable.children) == 0 {
			return nil, true
		}
		return editable, true
	}

	editable.children[idx] = childUpdated
	editable.keys[idx] = childUpdated.maxDatom()
	return editable, true
}

// insertInto places d beneath n, returning the node to use in n's place, a
// right sibling when n split, and whether d was new.
func (b *treeBuilder) insertInto(n *node, d *datalog.Datom) (updated, split *node, added bool) {
	if n.isLeaf() {
		return b.insertIntoLeaf(n, d)
	}
	return b.insertIntoBranch(n, d)
}

func (b *treeBuilder) insertIntoLeaf(n *node, d *datalog.Datom) (updated, split *node, added bool) {
	idx := b.searchNode(n, d)
	if idx < len(n.keys) && b.compare(n.keys[idx], d) == 0 {
		return n, nil, false
	}

	editable := b.editable(n)
	editable.keys = insertDatomAt(editable.keys, idx, d)
	left, right := b.splitIfFull(editable)
	return left, right, true
}

func (b *treeBuilder) insertIntoBranch(n *node, d *datalog.Datom) (updated, split *node, added bool) {
	idx := b.searchNode(n, d)
	if idx == len(n.keys) {
		// Past every separator, so d extends the last child and that child's
		// maximum — and this node's separator for it — grows.
		idx = len(n.keys) - 1
	}

	childUpdated, childSplit, added := b.insertInto(n.children[idx], d)
	if !added {
		return n, nil, false
	}

	editable := b.editable(n)
	editable.children[idx] = childUpdated
	editable.keys[idx] = childUpdated.maxDatom()
	if childSplit != nil {
		editable.keys = insertDatomAt(editable.keys, idx+1, childSplit.maxDatom())
		editable.children = insertNodeAt(editable.children, idx+1, childSplit)
	}

	left, right := b.splitIfFull(editable)
	return left, right, true
}

// editable returns n when this builder created it, and a copy otherwise. The
// copy's slices are fresh: mutating a slice shared with a published node would
// be visible to readers holding that node.
func (b *treeBuilder) editable(n *node) *node {
	if n.owner == b.token {
		return n
	}
	copied := &node{level: n.level, owner: b.token}
	copied.keys = make([]*datalog.Datom, len(n.keys), growthCapacity(len(n.keys)))
	copy(copied.keys, n.keys)
	if n.children != nil {
		copied.children = make([]*node, len(n.children), growthCapacity(len(n.children)))
		copy(copied.children, n.children)
	}
	return copied
}

// splitIfFull divides an over-full node in half, returning it and its new right
// sibling. A node is allowed to reach one entry past the branching factor
// during an insert; this is what resolves that.
func (b *treeBuilder) splitIfFull(n *node) (left, right *node) {
	if len(n.keys) <= branchingFactor {
		return n, nil
	}

	mid := len(n.keys) / 2
	right = &node{level: n.level, owner: b.token}
	right.keys = make([]*datalog.Datom, len(n.keys)-mid, growthCapacity(len(n.keys)-mid))
	copy(right.keys, n.keys[mid:])
	if n.children != nil {
		right.children = make([]*node, len(n.children)-mid, growthCapacity(len(n.children)-mid))
		copy(right.children, n.children[mid:])
	}

	// The vacated slots still hold pointers, and the collector scans a backing
	// array to its capacity — leaving them set would keep dead datoms and whole
	// dead subtrees reachable.
	clearDatomSlots(n.keys, mid)
	n.keys = n.keys[:mid]
	if n.children != nil {
		clearNodeSlots(n.children, mid)
		n.children = n.children[:mid]
	}
	return n, right
}

func (b *treeBuilder) searchNode(n *node, target *datalog.Datom) int {
	lo, hi := 0, len(n.keys)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if b.compare(n.keys[mid], target) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// growthCapacity is the allocation for a slice holding n entries: a fixed step
// beyond it, never past the one-entry overflow a split resolves.
func growthCapacity(n int) int {
	capacity := n + nodeGrowthStep
	if capacity > branchingFactor+1 {
		capacity = branchingFactor + 1
	}
	if capacity < n {
		capacity = n
	}
	return capacity
}

func insertDatomAt(s []*datalog.Datom, idx int, d *datalog.Datom) []*datalog.Datom {
	if len(s) == cap(s) {
		grown := make([]*datalog.Datom, len(s), growthCapacity(len(s)+1))
		copy(grown, s)
		s = grown
	}
	s = s[:len(s)+1]
	copy(s[idx+1:], s[idx:])
	s[idx] = d
	return s
}

func insertNodeAt(s []*node, idx int, n *node) []*node {
	if len(s) == cap(s) {
		grown := make([]*node, len(s), growthCapacity(len(s)+1))
		copy(grown, s)
		s = grown
	}
	s = s[:len(s)+1]
	copy(s[idx+1:], s[idx:])
	s[idx] = n
	return s
}

// removeDatomAt drops one entry, shifting the rest down. Only the slot the
// shift vacated needs clearing: everything above len is already nil, because
// growth extends into nil slots and splitIfFull clears what it truncates.
func removeDatomAt(s []*datalog.Datom, idx int) []*datalog.Datom {
	copy(s[idx:], s[idx+1:])
	s[len(s)-1] = nil
	return s[:len(s)-1]
}

func removeNodeAt(s []*node, idx int) []*node {
	copy(s[idx:], s[idx+1:])
	s[len(s)-1] = nil
	return s[:len(s)-1]
}

func clearDatomSlots(s []*datalog.Datom, from int) {
	full := s[:cap(s)]
	for i := from; i < len(full); i++ {
		full[i] = nil
	}
}

func clearNodeSlots(s []*node, from int) {
	full := s[:cap(s)]
	for i := from; i < len(full); i++ {
		full[i] = nil
	}
}
