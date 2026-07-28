package storage

import (
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
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
// what it was scanning for, and what it cost. Every event reporting a scan's
// funnel goes through here — the unbound and cardinality-dispatch scans, the
// three binding-driven strategies, the unique lookup — so the funnel keys and
// the event envelope have one definition rather than one per producer.
//
// The funnel is a required parameter, not an optional field, and that is what
// keeps this function's subject singular. A producer with no funnel to report
// does not pass an empty one: cache resolution reads an entry rather than an
// index, so it emits its own event directly (see matchFromCache) rather than
// borrowing an envelope whose payload would then have to be made optional for
// everyone.
//
// opened is when the scan was opened, so the event's Latency is the scan's
// lifetime through Close. That span includes time the consumer spent between
// Next calls, so it measures how long the scan was held open rather than CPU
// spent scanning. Latency is the field every other timed event carries: the
// output formatter renders it as the line's prefix and Database.Analyze sums it
// per event name, so a scan that omitted it would report as 0 ms in both.
//
// Neither the run nor the cause is a parameter, for the same reason: not every
// scan has one, and a parameter would force the ones that don't to supply
// something.
//
// Two producers address no single run — the per-binding path runs chooseIndex
// once per binding tuple, and the unique lookup walks AVET for the claimant and
// then the claimant's own history. A producer that walked one run reports it
// into extraData with addBoundFields; the rest say nothing rather than naming a
// run that did not happen. Half a run — an index without the components bound
// under it — is not one of the options.
//
// The cause is what asked for the scan, and it is not always a data pattern.
// LookupAttribute, LookupAllAttributes, the attribute-fetch fusion path,
// pull_batch and PrefetchEntities read on behalf of get-else, missing?,
// get-some and pull. They are scans a query paid for and they belong in this
// event family — a trace that omits them under-reports the query — but they
// have no *query.DataPattern, and a required pattern parameter is exactly what
// kept them out of the envelope and reading as arms that forgot to emit. A
// pattern-bearing producer writes annotations.KeyPattern into extraData; the
// rest name their own cause. The key is a constant, so searching it finds every
// producer that claims a pattern — the same audit a sole-writer function would
// give, without a function whose body is one assignment.
//
// Note the asymmetry with the funnel, which stays required. An absent funnel
// would have to be passed as an empty one and would render "0 scanned, 0
// resolved, 0 matched" — an assertion that a scan ran and found nothing, which
// is false. An absent cause renders as a line that names no cause. Omitting a
// key that has no value is not the same act as supplying a zero-valued
// structure that reads as a measurement.
//
// One event family, not one name per kind of cause: Database.Analyze sums per
// event name, so anything asking what a query spent on index reads would
// otherwise have to enumerate the names and would silently miss the next one
// added.
//
// Values go in typed — whatever the producer adds. Rendering belongs to the
// formatter, and flattening here would spend an allocation per emit to hand the
// consumer a string to parse.
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
	opened time.Time,
	funnel scanFunnel,
	extraData map[string]interface{},
) {
	data := map[string]interface{}{
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
