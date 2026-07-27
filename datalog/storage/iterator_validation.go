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

// scanFunnel is what a scan cost the query, narrowest last. The three counts
// travel together because each is only readable against the others: scanned
// over resolved is what the index charged, resolved over matched is what the
// pattern rejected, and either number alone makes a wide scan and a narrow one
// look alike.
//
// A struct rather than three int parameters. The counts are three ints in a
// fixed order, which is the shape where a transposed pair compiles and then
// reports a scan that read less than it returned.
type scanFunnel struct {
	// scanned is intake: keys taken from the index, including any the bound's
	// membership rule then rejected. Comes from Iterator.Scanned.
	scanned int
	// resolved is what the scan's source produced. On the normal read path that
	// source is CRDT resolution, so this sits below intake by the history depth:
	// resolution emits one datom per (E, A) for a cardinality-one attribute
	// however many times it was written. In history mode there is no resolution
	// and it nearly meets intake.
	resolved int
	// matched is what survived the pattern and its constraints.
	matched int
}

// emitScanCompletion emits the event a scan owes its reader when it finishes:
// what it was scanning for, and what it cost. Every scan-completion event in
// the engine goes through here — the unbound and cardinality-dispatch scans,
// the three binding-driven strategies, and cache resolution — so the funnel
// keys and the event envelope have one definition rather than one per producer.
//
// opened is when the scan was opened, so the event's Latency is the scan's
// lifetime through Close. That span includes time the consumer spent between
// Next calls, so it measures how long the scan was held open rather than CPU
// spent scanning. Latency is the field every other timed event carries: the
// output formatter renders it as the line's prefix and Database.Analyze sums it
// per event name, so a scan that omitted it would report as 0 ms in both.
//
// The index is not a parameter. Two of the producers address no single run:
// cache resolution picks an index by cardinality inside resolution and reads
// none at all on a hit, and the per-binding path runs chooseIndex once per
// binding tuple. A producer that walked one index names it in extraData under
// annotations.KeyIndex; the rest say nothing rather than naming a run that did
// not happen.
//
// Values go in typed — the pattern, and whatever the producer adds. Rendering
// belongs to the formatter, and flattening here would spend an allocation per
// emit to hand the consumer a string to parse.
//
// Called once per scan, so the map construction is not on any hot path.
//
// The caller guards on the handler. That guard belongs to the caller because it
// gates the caller's own argument preparation — the extraData map above all —
// as well as the map built here, and because at the call site it marks the
// block as observability rather than part of finishing the scan. A nil handler
// reaching here is a caller that skipped its guard, and panics rather than
// silently doing nothing.
func emitScanCompletion(
	handler func(annotations.Event),
	eventName string,
	pattern *query.DataPattern,
	opened time.Time,
	funnel scanFunnel,
	extraData map[string]interface{},
) {
	data := map[string]interface{}{
		annotations.KeyPattern:        pattern,
		annotations.KeyDatomsScanned:  funnel.scanned,
		annotations.KeyDatomsResolved: funnel.resolved,
		annotations.KeyDatomsMatched:  funnel.matched,
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
