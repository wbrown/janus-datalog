package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// scan returns an iterator over the run the bound names, walking the version's
// tree for that index.
//
// The iterator retains the version, so the state it walks stays whole no matter
// what commits afterward. That is the snapshot contract, satisfied by holding a
// pointer rather than by a lock or a copy.
func (v *storeVersion) scan(bound ScanBound) (Iterator, error) {
	tree := v.tree(bound.Index)
	probe, err := boundProbeDatom(tree.order, bound)
	if err != nil {
		return nil, err
	}
	return &memoryTreeIterator{
		version: v,
		tree:    tree,
		cursor:  tree.cursor(),
		probe:   probe,
		boundN:  len(bound.Prefix),
	}, nil
}

// boundProbeDatom places a bound's values into the datom positions the index's
// order assigns them. Only those positions are set; a prefix comparison never
// reads the rest.
//
// Each position admits exactly one kind of value, so anything else is a caller
// defect rather than something to coerce — the same rule the encoder's bound
// path applies, stated once per backend because each reads the values in its
// own form.
func boundProbeDatom(order [componentsPerIndex]keyComponent, bound ScanBound) (*datalog.Datom, error) {
	if len(bound.Prefix) > len(order) {
		return nil, fmt.Errorf("scan bound on %v: binds %d components, index orders %d",
			bound.Index, len(bound.Prefix), len(order))
	}

	probe := &datalog.Datom{}
	for i, value := range bound.Prefix {
		switch order[i] {
		case componentE:
			id, ok := value.(datalog.Identity)
			if !ok || id == nil {
				return nil, fmt.Errorf("scan bound on %v: E must be a non-nil Identity, got %T",
					bound.Index, value)
			}
			probe.E = id

		case componentA:
			kw, ok := value.(datalog.Keyword)
			if !ok || kw == nil {
				return nil, fmt.Errorf("scan bound on %v: A must be a non-nil Keyword, got %T",
					bound.Index, value)
			}
			probe.A = kw

		case componentV:
			probe.V = value

		case componentTx:
			eid, ok := value.(datalog.ElementID)
			if !ok {
				return nil, fmt.Errorf("scan bound on %v: Tx must be an ElementID, got %T",
					bound.Index, value)
			}
			probe.Tx = eid

		default:
			return nil, fmt.Errorf("scan bound on %v: unknown component %v at position %d",
				bound.Index, order[i], i)
		}
	}
	return probe, nil
}

// memoryTreeIterator walks the run a ScanBound names in one index tree.
//
// It needs no membership test. The bound is equality on leading components, and
// under the typed comparator the datoms satisfying it are contiguous, so the run
// is "seek, then walk while the leading components still match". The byte-key
// backend cannot do this — an encoded value carries no length, so keys for a
// longer value interleave with those for a prefix of it, which is what
// EncodedRun's membership rule exists to filter. That difference is the point of
// the representation, not an accident of it.
//
// Datom returns the tree's own pointer, valid for as long as the caller keeps
// it: datoms are immutable and the iterator retains the version holding them.
type memoryTreeIterator struct {
	version *storeVersion
	tree    *datomTree
	cursor  *treeCursor
	probe   *datalog.Datom
	boundN  int

	current *datalog.Datom

	started bool
	done    bool
	valid   bool
	scanned int
	err     error
}

func (it *memoryTreeIterator) Next() bool {
	if it.err != nil || it.done {
		it.valid = false
		return false
	}

	if !it.started {
		it.started = true
		if !it.position() {
			return it.finish()
		}
	} else if !it.cursor.next() {
		return it.finish()
	}

	current := it.cursor.datom()
	if current == nil {
		return it.finish()
	}

	// Arriving at the run's end is not intake, so the count follows the check.
	if it.boundN > 0 && comparePrefixInOrder(it.tree.order, it.boundN, it.probe, current) != 0 {
		return it.finish()
	}
	it.scanned++

	it.current = current
	it.valid = true
	return true
}

// position places the cursor at the run's first candidate.
func (it *memoryTreeIterator) position() bool {
	if it.boundN == 0 {
		return it.cursor.seekFirst()
	}
	return it.cursor.seek(it.probe, it.boundN)
}

func (it *memoryTreeIterator) finish() bool {
	it.done = true
	it.valid = false
	return false
}

func (it *memoryTreeIterator) Datom() (*datalog.Datom, error) {
	if it.err != nil {
		return nil, it.err
	}
	if !it.valid {
		return nil, fmt.Errorf("no current datom")
	}
	return it.current, nil
}

func (it *memoryTreeIterator) ElementID() datalog.ElementID {
	if !it.valid {
		return datalog.ElementID{}
	}
	return it.current.Tx
}

// Seek replaces the bound, so the run it names supplies both where iteration
// resumes and where it ends. Adopting only the start would walk past this run
// into whatever the tree still holds.
func (it *memoryTreeIterator) Seek(bound ScanBound) {
	if it.err != nil {
		return
	}
	if bound.Index != it.tree.index {
		it.err = fmt.Errorf("scan bound on %v seeks %v: an iterator walks one index",
			it.tree.index, bound.Index)
		return
	}

	probe, err := boundProbeDatom(it.tree.order, bound)
	if err != nil {
		it.err = err
		return
	}

	it.probe = probe
	it.boundN = len(bound.Prefix)
	it.started = false
	it.done = false
	it.valid = false
}

func (it *memoryTreeIterator) Close() error {
	it.done = true
	it.valid = false
	return nil
}

func (it *memoryTreeIterator) Error() error { return it.err }

func (it *memoryTreeIterator) Scanned() int { return it.scanned }
