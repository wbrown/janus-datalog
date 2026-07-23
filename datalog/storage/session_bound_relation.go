package storage

import (
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// sessionBoundRelation ties a query's ReadSession to its result: the session
// closes when the result's iterator is exhausted or closed, whichever comes
// first. Access paths that bypass Iterator() — derived relations, formatted
// dumps — leave release to the session's finalizer backstop.
type sessionBoundRelation struct {
	executor.Relation
	session ReadSession
}

func newSessionBoundRelation(inner executor.Relation, session ReadSession) executor.Relation {
	return &sessionBoundRelation{Relation: inner, session: session}
}

func (r *sessionBoundRelation) Iterator() executor.Iterator {
	return &sessionBoundIterator{Iterator: r.Relation.Iterator(), session: r.session}
}

// sessionBoundIterator releases the session at exhaustion or Close. A close
// failure surfaces through Close's return or Error(), never silently.
type sessionBoundIterator struct {
	executor.Iterator
	session    ReadSession
	released   bool
	releaseErr error
}

func (it *sessionBoundIterator) releaseSession() {
	if it.released {
		return
	}
	it.released = true
	it.releaseErr = it.session.Close()
}

func (it *sessionBoundIterator) Next() bool {
	if it.Iterator.Next() {
		return true
	}
	it.releaseSession()
	return false
}

func (it *sessionBoundIterator) Close() error {
	err := it.Iterator.Close()
	it.releaseSession()
	if err == nil {
		err = it.releaseErr
	}
	return err
}

func (it *sessionBoundIterator) Error() error {
	if err := it.Iterator.Error(); err != nil {
		return err
	}
	return it.releaseErr
}
