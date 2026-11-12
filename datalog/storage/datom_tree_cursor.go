package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// cursorLevel is one step of a cursor's descent: the node and which of its
// entries the cursor is on.
type cursorLevel struct {
	n   *node
	idx int
}

// treeCursor walks a datomTree in index order. The first maxTreeDepth levels of
// the path are a fixed-size array inline in the struct, so a cursor over a tree
// no deeper than that allocates once and neither seeking nor stepping allocates
// again. The levels of a deeper tree, one of a small branching factor, are in
// deep, which grows to the tree's depth on the first descent and is kept.
//
// A cursor holds nodes from one root. Because nodes are immutable once
// published, that root stays walkable no matter what commits afterward — which
// is what makes a read session a retained root rather than a lock.
type treeCursor struct {
	tree  *datomTree
	path  [maxTreeDepth]cursorLevel
	deep  []cursorLevel
	depth int
	valid bool
}

// level is the cursor's step at depth i of its path, counted from the root.
func (c *treeCursor) level(i int) *cursorLevel {
	if i < len(c.path) {
		return &c.path[i]
	}
	return &c.deep[i-len(c.path)]
}

func (t *datomTree) cursor() *treeCursor {
	return &treeCursor{tree: t}
}

// seek positions the cursor at the first datom not less than target and reports
// whether one exists, comparing the leading `components` of the index's order —
// or all of them plus the tail, for fullDatomCompare.
//
// A ScanBound seeks with its own component count: the probe carries only the
// positions the bound names, so the rest must never be read.
func (c *treeCursor) seek(target *datalog.Datom, components int) bool {
	c.depth = 0
	c.valid = false
	if c.tree.root == nil {
		return false
	}

	n := c.tree.root
	for {
		idx := c.tree.searchNode(n, target, components)
		if idx == len(n.keys) {
			// Every entry here is below target. Only the root can reach this:
			// a child is descended into only when its maximum is not less than
			// target, so within a subtree a position always exists.
			c.depth = 0
			return false
		}
		c.push(n, idx)
		if n.isLeaf() {
			c.valid = true
			return true
		}
		n = n.children[idx]
	}
}

// seekFirst positions the cursor at the tree's first datom.
func (c *treeCursor) seekFirst() bool {
	c.depth = 0
	c.valid = false
	if c.tree.root == nil {
		return false
	}
	c.descendLeftmost(c.tree.root)
	c.valid = true
	return true
}

// next advances to the following datom and reports whether one exists.
func (c *treeCursor) next() bool {
	if !c.valid {
		return false
	}

	// Advance within the leaf when it has more entries.
	leaf := c.level(c.depth - 1)
	if leaf.idx+1 < len(leaf.n.keys) {
		leaf.idx++
		return true
	}

	// Otherwise climb to the nearest ancestor with an unvisited child, then
	// take that subtree's first datom.
	for c.depth--; c.depth > 0; c.depth-- {
		level := c.level(c.depth - 1)
		if level.idx+1 < len(level.n.keys) {
			level.idx++
			c.descendLeftmost(level.n.children[level.idx])
			return true
		}
	}

	c.valid = false
	return false
}

// datom returns the entry the cursor is on, or nil when it is exhausted.
func (c *treeCursor) datom() *datalog.Datom {
	if !c.valid {
		return nil
	}
	level := c.level(c.depth - 1)
	return level.n.keys[level.idx]
}

// descendLeftmost walks from n to its first leaf, recording the path. The
// caller has already placed everything above n.
func (c *treeCursor) descendLeftmost(n *node) {
	for {
		c.push(n, 0)
		if n.isLeaf() {
			return
		}
		n = n.children[0]
	}
}

// push records the next step of a descent. A step past the inline path goes
// into deep, appended where the cursor has not been that deep before.
func (c *treeCursor) push(n *node, idx int) {
	step := cursorLevel{n: n, idx: idx}
	switch past := c.depth - len(c.path); {
	case past < 0:
		c.path[c.depth] = step
	case past < len(c.deep):
		c.deep[past] = step
	default:
		c.deep = append(c.deep, step)
	}
	c.depth++
}
