package storage

import (
	"time"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
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

// sessionBoundRelation ties a query's ReadSession and its completion to its
// result: the session closes, and the query is reported complete, when the
// result's iterator is exhausted or closed, whichever comes first. Access
// paths that bypass Iterator() — derived relations, formatted dumps — leave
// release to the session's finalizer backstop and report no completion.
type sessionBoundRelation struct {
	executor.Relation
	session ReadSession
	// completion is nil when the query runs with no handler registered.
	completion *queryCompletion
}

func newSessionBoundRelation(inner executor.Relation, session ReadSession, completion *queryCompletion) executor.Relation {
	return &sessionBoundRelation{Relation: inner, session: session, completion: completion}
}

func (r *sessionBoundRelation) Iterator() executor.Iterator {
	return &sessionBoundIterator{Iterator: r.Relation.Iterator(), session: r.session, completion: r.completion}
}

// Rename is the inner relation's rename bound to the same session and the
// same completion, so the session closes and the query completes when the
// renamed result's iterator is exhausted or closed.
func (r *sessionBoundRelation) Rename(subst query.Substitution) (executor.Relation, error) {
	renamed, err := r.Relation.Rename(subst)
	if err != nil {
		return nil, err
	}
	return newSessionBoundRelation(renamed, r.session, r.completion), nil
}

// sessionBoundIterator reports the query complete and releases the session
// at exhaustion or Close, counting the tuples it yields on the way. A close
// failure surfaces through Close's return or Error(), never silently.
type sessionBoundIterator struct {
	executor.Iterator
	session    ReadSession
	completion *queryCompletion
	drained    int
	released   bool
	releaseErr error
}

// release ends the query once: the completion fires with the tuples drained
// and the error the drain ended in, then the session closes.
func (it *sessionBoundIterator) release(err error) {
	if it.released {
		return
	}
	it.released = true
	if it.completion != nil {
		it.completion.fire(it.drained, err)
	}
	it.releaseErr = it.session.Close()
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

func (it *sessionBoundIterator) RequiresCopy() bool { return it.Iterator.RequiresCopy() }
