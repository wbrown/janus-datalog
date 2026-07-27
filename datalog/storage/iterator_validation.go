package storage

import (
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Iterator validation and statistics functions shared across iterator implementations.
//
// PERFORMANCE NOTE: These functions introduce a small overhead (~6.4% or 0.2ns per datom)
// compared to inlined code, as measured by BenchmarkIteratorLoop. This tradeoff was
// accepted to eliminate ~120 lines of validation and statistics code that would
// otherwise be copied into each iterator type. The absolute overhead is minimal:
//   - 1M datoms:  +200 microseconds
//   - 10M datoms: +2 milliseconds
//   - 100M datoms: +20 milliseconds
//
// See iterator_refactoring_bench_test.go for benchmark details.

// validateDatomWithConstraints checks if a datom passes transaction and constraint validation.
// This consolidates logic that was duplicated across nonReusingIterator and
// unboundIterator.
//
// Returns true if the datom:
//  1. Passes transaction time filtering (txID check)
//  2. Satisfies all storage constraints
//
// This function is called in the hot path (once per datom scanned) and introduces
// ~0.2ns overhead per call due to function call costs and inability to inline
// (the constraint loop makes it too complex for the Go compiler's inliner).
func validateDatomWithConstraints(
	datom *datalog.Datom,
	txID *datalog.ElementID,
	constraints []executor.StorageConstraint,
) bool {
	// Check transaction validity (skip datoms from after the as-of target)
	if txID != nil && *txID != (datalog.ElementID{}) && txID.Less(datom.Tx) {
		return false
	}

	// Apply storage constraints
	for _, constraint := range constraints {
		if !constraint.Evaluate(datom) {
			return false
		}
	}

	return true
}

// emitIteratorStatistics emits annotation events for iterator performance tracking.
// This consolidates the Close() logic that was duplicated across iterator types.
//
// opened is when the scan was opened, so the event's Latency is the iterator's
// lifetime through Close. That span includes time the consumer spent between
// Next calls, so it measures how long the scan was held open rather than CPU
// spent scanning. Latency is the field every other timed event carries and the
// one the output formatter renders as the line's prefix.
//
// The three counts are the query's funnel through this scan, narrowest last:
//
//   - datomsScanned — intake: keys taken from the index, including any the
//     bound's membership rule then rejected. Comes from Iterator.Scanned.
//   - datomsResolved — what the iterator's source produced. On the normal read
//     path that source is CRDT resolution, so this sits below intake by the
//     history depth: resolution emits one datom per (E, A) for a
//     cardinality-one attribute however many times it was written. In history
//     mode there is no resolution and it nearly meets intake.
//   - datomsMatched — what survived the pattern and its constraints.
//
// scanned/resolved is what the index charged; resolved/matched is what the
// pattern rejected. Reporting only one of them makes a wide scan and a narrow
// one look alike.
//
// Called only once per iterator (in Close()), so performance overhead is negligible.
//
// The caller guards on the handler. That guard belongs to the caller because it
// gates the caller's own argument preparation — the extraData map above all —
// as well as the map built here, and because at the call site it marks the
// block as observability rather than part of closing the iterator. A nil
// handler reaching here is a caller that skipped its guard, and panics rather
// than silently doing nothing.
func emitIteratorStatistics(
	handler func(annotations.Event),
	eventName string,
	pattern *query.DataPattern,
	index IndexType,
	opened time.Time,
	datomsScanned int,
	datomsResolved int,
	datomsMatched int,
	extraData map[string]interface{},
) {
	data := map[string]interface{}{
		"pattern":         pattern.String(),
		"index":           index.String(),
		"datoms.scanned":  datomsScanned,
		"datoms.resolved": datomsResolved,
		"datoms.matched":  datomsMatched,
	}

	// Merge in extra data
	for k, v := range extraData {
		data[k] = v
	}

	handler(annotations.Event{
		Name:    eventName,
		Start:   opened,
		Latency: time.Since(opened),
		Data:    data,
	})
}
