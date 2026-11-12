package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// branchingFactor is the maximum number of entries in a node of a store made
// with no factor of its own (MemoryTreeStoreOptions.BranchingFactor). It is an
// empirical starting point, not a derivation: at full packing, three million
// datoms reach depth three (256³ = 16.7M), which 512 also reaches while
// doubling the path copy per level. A commit copies each node on its path
// before changing it, in every index, so the factor is what a commit of few
// datoms costs; a store that commits a datom at a time is made with a smaller
// one.
const branchingFactor = 256

// MinBranchingFactor is the smallest factor a store is made with: a split
// leaves each half two entries or more, so no branch is left with one child.
const MinBranchingFactor = 4

// MaxBranchingFactor is the largest factor a store is made with: a tree holds
// its factor in sixteen bits (datomTree.branching).
const MaxBranchingFactor = 1<<16 - 1

// bulkPackingFraction is how full a sorted bulk build packs a leaf, as a
// fraction of the tree's branching factor. Packing is a build property rather
// than a structural constant, and it sets the store's resting memory: the
// sizing model in MEMORY_DATOM_INDEXES.md assumes a near-full sorted build.
// This is the other empirical parameter.
const bulkPackingFraction = 1.0

// maxTreeDepth is the length of the array inline in a cursor, which is its
// path over a tree no deeper. A node split from a full one holds at least half
// the tree's branching factor, so at the default factor this admits 128⁶
// datoms beneath a root, and a cursor over such a tree allocates once. A tree
// of a small factor is deeper, and its cursor's path is a slice of the tree's
// depth (treeCursor.path). Seven levels and the path's slice are the memory a
// fixed path of eight levels took.
const maxTreeDepth = 7

// node is one tree node. Level zero is a leaf; above that, keys[i] is the
// maximum datom in children[i], so a search descends into the first child whose
// maximum is not less than the target.
//
// One node type with a level discriminant rather than an interface or two
// types: interface dispatch on descent sits inside the comparison loop and
// blocks inlining, while a level test is a predictable branch.
//
// Nodes are immutable once published. Go's memory model gives no safe-
// publication guarantee for an ordinary store, so a node reaches a reader only
// through the atomic root swap that publishes the version holding it.
type node struct {
	level int8
	// owner is the builder that created this node, or nil once published. A
	// builder mutates a node in place only when the token matches its own,
	// which means no reader can reach it yet.
	owner    *ownerToken
	keys     []*datalog.Datom
	children []*node
}

func (n *node) isLeaf() bool { return n.level == 0 }

// maxDatom is the node's largest entry, which is what its parent stores as the
// separator for this subtree.
func (n *node) maxDatom() *datalog.Datom { return n.keys[len(n.keys)-1] }

// datomTree is a persistent sorted set of datoms in one index's order. A tree
// value is its root; sharing a root shares the whole structure.
//
// branching is the maximum number of entries in a node of the tree. Every tree
// built from this one carries it, so a store's trees keep the factor the store
// was made with through every commit, bulk build and fork. It is sixteen bits
// wide and declared before the root, where it lies in the bytes the index and
// the order leave unused: a commit makes a tree for every index, and the
// factor adds nothing to what each takes.
type datomTree struct {
	index     IndexType
	order     [componentsPerIndex]keyComponent
	branching uint16
	root      *node
	count     int
}

// newDatomTree returns an empty tree ordered for index, whose nodes hold at
// most branching entries, a factor no larger than MaxBranchingFactor.
func newDatomTree(index IndexType, branching int) *datomTree {
	order, err := componentOrder(index)
	if err != nil {
		panic(fmt.Sprintf("newDatomTree: %v", err))
	}
	return &datomTree{index: index, order: order, branching: uint16(branching)}
}

func (t *datomTree) compare(a, b *datalog.Datom) int {
	return compareDatomsInOrder(t.order, a, b)
}

func (t *datomTree) Len() int { return t.count }

// buildFromSorted constructs a tree bottom-up from datoms sorted and
// duplicate-free under this index's order — the JDZL hydration path. Leaves are
// filled to the packing fraction and each branch level is built in one pass
// over the level below: no path copying, no comparisons. The input's order is
// its caller's doing — versionFromDatoms sorts with this tree's own comparator
// immediately before calling — and the dump format is duplicate-free by
// definition, trusted rather than swept for.
func (t *datomTree) buildFromSorted(datoms []*datalog.Datom) {
	t.count = len(datoms)
	if len(datoms) == 0 {
		t.root = nil
		return
	}

	level := buildLeafLevel(datoms, int(t.branching))
	for len(level) > 1 {
		level = buildBranchLevel(level, int(t.branching))
	}
	t.root = level[0]
}

// buildLeafLevel packs the sorted datoms into leaves of at most branching
// entries. The final leaf takes whatever remains rather than being balanced
// against its predecessor: a bulk build is followed by reads, not by inserts at
// the tail.
func buildLeafLevel(datoms []*datalog.Datom, branching int) []*node {
	perLeaf := int(float64(branching) * bulkPackingFraction)
	if perLeaf < 1 {
		perLeaf = 1
	}

	leaves := make([]*node, 0, (len(datoms)+perLeaf-1)/perLeaf)
	for start := 0; start < len(datoms); start += perLeaf {
		end := start + perLeaf
		if end > len(datoms) {
			end = len(datoms)
		}
		keys := make([]*datalog.Datom, end-start)
		copy(keys, datoms[start:end])
		leaves = append(leaves, &node{level: 0, keys: keys})
	}
	return leaves
}

// buildBranchLevel groups the level below into parents of at most branching
// children, each keyed by its children's maxima.
func buildBranchLevel(below []*node, branching int) []*node {
	parents := make([]*node, 0, (len(below)+branching-1)/branching)
	for start := 0; start < len(below); start += branching {
		end := start + branching
		if end > len(below) {
			end = len(below)
		}
		group := below[start:end]

		keys := make([]*datalog.Datom, len(group))
		children := make([]*node, len(group))
		for i, child := range group {
			keys[i] = child.maxDatom()
			children[i] = child
		}
		parents = append(parents, &node{
			level:    group[0].level + 1,
			keys:     keys,
			children: children,
		})
	}
	return parents
}

// fullDatomCompare asks searchNode for the whole order, tail included, rather
// than a leading run of components.
const fullDatomCompare = -1

// searchNode returns the index of the first entry not less than target. On a
// branch that is the child to descend into; on a leaf it is where target is or
// would be inserted. It returns len(keys) when every entry is less than target.
//
// components is how many leading components to compare, or fullDatomCompare for
// all of them plus the tail. A prefix search is what a ScanBound needs: the
// probe carries only the components the bound names, and the rest must not be
// read. It descends correctly because a coarser order agrees with the full one
// on which child holds a target — a node's maximum under the full order is also
// its maximum under any prefix of that order.
func (t *datomTree) searchNode(n *node, target *datalog.Datom, components int) int {
	lo, hi := 0, len(n.keys)
	if components == fullDatomCompare {
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if t.compare(n.keys[mid], target) < 0 {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		return lo
	}
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if comparePrefixInOrder(t.order, components, n.keys[mid], target) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}
