package executor

import (
	"github.com/wbrown/janus-datalog/datalog/internal/errjoin"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// registryBoundRelation ties a query's scan registry to the query's result:
// the registry's shared scans still open close when the result's iterator is
// exhausted or closed, whichever comes first, so a scan no reader read to its
// end closes at the query's end. An access path that bypasses Iterator()
// leaves the scans to their finalizers.
type registryBoundRelation struct {
	Relation
	scans *ScanRegistry
}

func newRegistryBoundRelation(inner Relation, scans *ScanRegistry) Relation {
	return &registryBoundRelation{Relation: inner, scans: scans}
}

func (r *registryBoundRelation) Iterator() Iterator {
	return &registryBoundIterator{Iterator: r.Relation.Iterator(), scans: r.scans}
}

// Rename is the inner relation's rename bound to the same registry, so the
// shared scans close when the renamed result's iterator is exhausted or
// closed.
func (r *registryBoundRelation) Rename(subst query.Substitution) (Relation, error) {
	renamed, err := r.Relation.Rename(subst)
	if err != nil {
		return nil, err
	}
	return newRegistryBoundRelation(renamed, r.scans), nil
}

// registryBoundIterator closes the registry's open scans at exhaustion or
// Close. Error answers the inner iterator's failure, and Close answers the
// registry's close failure beside the inner iterator's, so each reaches the
// reader once.
type registryBoundIterator struct {
	Iterator
	scans      *ScanRegistry
	released   bool
	releaseErr error
}

// release closes the registry's open scans once and keeps the failure for
// Close to answer.
func (it *registryBoundIterator) release() {
	if it.released {
		return
	}
	it.released = true
	it.releaseErr = it.scans.Close()
}

func (it *registryBoundIterator) Next() bool {
	if it.Iterator.Next() {
		return true
	}
	it.release()
	return false
}

func (it *registryBoundIterator) Close() error {
	err := it.Iterator.Close()
	it.release()
	return errjoin.Join(err, it.releaseErr)
}

func (it *registryBoundIterator) Error() error {
	return it.Iterator.Error()
}

func (it *registryBoundIterator) RequiresCopy() bool { return it.Iterator.RequiresCopy() }
