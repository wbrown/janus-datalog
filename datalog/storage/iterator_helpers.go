package storage

import (
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Iterator helper functions to reduce code duplication across iterator implementations.
//
// PERFORMANCE NOTE: These helpers introduce a small overhead (~6.4% or 0.2ns per datom)
// compared to inlined code, as measured by BenchmarkIteratorLoop. This tradeoff was
// accepted to eliminate ~120 lines of duplicated validation and statistics code across
// three iterator types. The absolute overhead is minimal:
//   - 1M datoms:  +200 microseconds
//   - 10M datoms: +2 milliseconds
//   - 100M datoms: +20 milliseconds
//
// See iterator_refactoring_bench_test.go for benchmark details.

// validateDatomWithConstraints checks if a datom passes transaction and constraint validation.
// This consolidates logic that was duplicated across reusingIterator, nonReusingIterator,
// and unboundIterator.
//
// Returns true if the datom:
//   1. Passes transaction time filtering (txID check)
//   2. Satisfies all storage constraints
//
// This function is called in the hot path (once per datom scanned) and introduces
// ~0.2ns overhead per call due to function call costs and inability to inline
// (the constraint loop makes it too complex for the Go compiler's inliner).
func validateDatomWithConstraints(
	datom *datalog.Datom,
	txID uint64,
	constraints []executor.StorageConstraint,
) bool {
	// Check transaction validity
	if txID > 0 && datom.Tx > txID {
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
// Called only once per iterator (in Close()), so performance overhead is negligible.
func emitIteratorStatistics(
	handler func(annotations.Event),
	eventName string,
	pattern *query.DataPattern,
	index IndexType,
	datomsScanned int,
	datomsMatched int,
	extraData map[string]interface{},
) {
	if handler == nil {
		return
	}

	data := map[string]interface{}{
		"pattern":        pattern.String(),
		"index":          indexName(index),
		"datoms.scanned": datomsScanned,
		"datoms.matched": datomsMatched,
	}

	// Merge in extra data
	for k, v := range extraData {
		data[k] = v
	}

	handler(annotations.Event{
		Name:  eventName,
		Start: time.Now(),
		Data:  data,
	})
}
