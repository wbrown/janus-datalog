package storage

import (
	"time"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/internal/errjoin"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// queryCompletion is a query's end as its handler sees it: the handler, the
// moment the query began, and whether the completion has fired. One is
// shared by the query's result and every rename of it, so the query
// completes once, from whichever iterator ends it.
type queryCompletion struct {
	handler annotations.Handler
	start   time.Time
	fired   bool
}

// fire reports the query complete once, with the tuples drained from its one
// result relation by the time it ended and, when the drain ended in an
// error, that error. A drain that ran to its end or was closed before it
// completes with success.
func (c *queryCompletion) fire(drained int, err error) {
	if c.fired {
		return
	}
	c.fired = true
	data := map[string]interface{}{
		annotations.KeyRelationsCount: 1,
		annotations.KeyTuplesCount:    drained,
		annotations.KeySuccess:        err == nil,
	}
	if err != nil {
		data[annotations.KeyError] = err.Error()
	}
	c.handler(annotations.TimedEvent(annotations.QueryComplete, c.start, data))
}

// sessionBoundRelation ties a query's ReadSession, the scan registry its
// context holds and its completion to its result: the registry's shared
// scans close, the session closes, and the query is reported complete, when
// the result's iterator is exhausted or closed, whichever comes first. Access
// paths that bypass Iterator() — derived relations, formatted dumps — leave
// release to the session's finalizer backstop and report no completion.
type sessionBoundRelation struct {
	executor.Relation
	session ReadSession
	// scans is the query's scan registry. A shared scan holds a storage
	// iterator of session until the query ends, whether or not a reader read
	// it to its end.
	scans *executor.ScanRegistry
	// completion is nil when the query runs with no handler registered.
	completion *queryCompletion
}

func newSessionBoundRelation(inner executor.Relation, session ReadSession, scans *executor.ScanRegistry, completion *queryCompletion) executor.Relation {
	return &sessionBoundRelation{Relation: inner, session: session, scans: scans, completion: completion}
}

func (r *sessionBoundRelation) Iterator() executor.Iterator {
	return &sessionBoundIterator{Iterator: r.Relation.Iterator(), session: r.session, scans: r.scans, completion: r.completion}
}

// Rename is the inner relation's rename bound to the same session, the same
// registry and the same completion, so the shared scans and the session
// close and the query completes when the renamed result's iterator is
// exhausted or closed.
func (r *sessionBoundRelation) Rename(subst query.Substitution) (executor.Relation, error) {
	renamed, err := r.Relation.Rename(subst)
	if err != nil {
		return nil, err
	}
	return newSessionBoundRelation(renamed, r.session, r.scans, r.completion), nil
}

// sessionBoundIterator reports the query complete, closes the shared scans
// and releases the session at exhaustion or Close, counting the tuples it
// yields on the way. Error answers the inner iterator's failure, and Close
// answers the registry's and the session's close failures beside the inner
// iterator's, so each reaches the reader once.
type sessionBoundIterator struct {
	executor.Iterator
	session    ReadSession
	scans      *executor.ScanRegistry
	completion *queryCompletion
	drained    int
	released   bool
	releaseErr error
}

// release ends the query once: the completion fires with the tuples drained
// and the error the drain ended in, the registry closes every shared scan's
// source, then the session closes.
func (it *sessionBoundIterator) release(err error) {
	if it.released {
		return
	}
	it.released = true
	if it.completion != nil {
		it.completion.fire(it.drained, err)
	}
	it.releaseErr = errjoin.Join(it.scans.Close(), it.session.Close())
}

func (it *sessionBoundIterator) Next() bool {
	if it.Iterator.Next() {
		it.drained++
		return true
	}
	it.release(it.Iterator.Error())
	return false
}

func (it *sessionBoundIterator) Close() error {
	err := it.Iterator.Close()
	it.release(err)
	return errjoin.Join(err, it.releaseErr)
}

func (it *sessionBoundIterator) Error() error {
	return it.Iterator.Error()
}

func (it *sessionBoundIterator) RequiresCopy() bool { return it.Iterator.RequiresCopy() }
