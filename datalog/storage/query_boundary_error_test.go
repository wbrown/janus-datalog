package storage

import (
	"crypto/rand"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Integration reproduction for docs/bugs/BUG_ITERATOR_ERRORS_DROPPED_AT_PUBLIC_BOUNDARIES.md.
//
// A Tier-3 value stores its compressed bytes in the blob store with a content
// hash in the index key. If the blob is missing, decoding the value fails — and
// the storage iterator surfaces that only through Error() after Next() returns
// false. This drives a REAL deferred-iterator failure through the public
// boundaries (CollectTuples / QueryInto / QueryOneInto), which must not report
// it as an empty or "not found" success.

// writeTier3ValueThenCorruptBlob writes one datom whose value lands in the Tier-3
// blob store, then deletes every blob key so the value can no longer be decoded.
func writeTier3ValueThenCorruptBlob(t *testing.T) (*Database, datalog.Identity, datalog.Keyword) {
	t.Helper()
	dir := t.TempDir()

	// DisableCache so reads hit storage (and thus the blob) rather than a warm
	// resolved value. Small compression threshold so the value is compressed;
	// large incompressible payload so the compressed form exceeds the in-key
	// size limit and routes to Tier 3.
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:                 dir,
		DisableCache:         true,
		CompressionThreshold: 64,
	})
	require.NoError(t, err)

	e := datalog.NewIdentity("doc-1")
	a := datalog.NewKeyword(":doc/blob")

	// Tier 3 requires the *compressed* size to exceed maxKeyValueSize (60000).
	// Use a large incompressible (random) region so it can't shrink below that,
	// plus a compressible (zero) region so Compress() reports a net benefit and
	// returns non-nil (pure-random returns nil and would store raw, inline).
	payload := make([]byte, 80*1024)
	_, err = rand.Read(payload)
	require.NoError(t, err)
	payload = append(payload, make([]byte, 80*1024)...)

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, payload))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Delete all blob keys (prefix 0xFF) so the value cannot be decoded.
	deleted := 0
	err = db.store.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte{0xFF}
		var keys [][]byte
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keys = append(keys, it.Item().KeyCopy(nil))
		}
		for _, k := range keys {
			if derr := txn.Delete(k); derr != nil {
				return derr
			}
			deleted++
		}
		return nil
	})
	require.NoError(t, err)
	require.Greater(t, deleted, 0, "expected a Tier-3 blob to corrupt; value may not have routed to the blob store")

	return db, e, a
}

func TestCollectTuples_SurfacesBlobDecodeError(t *testing.T) {
	db, e, _ := writeTier3ValueThenCorruptBlob(t)
	defer db.Close()

	_, err := executor.CollectTuples(db.Query(`[:find ?v :in $ ?e :where [?e :doc/blob ?v]]`, e))
	require.ErrorContains(t, err, "blob", "a missing blob must not be reported as an empty result")
}

// Analyze fully executes the query (EXPLAIN ANALYZE-style), so a deferred
// blob-decode failure must surface as an error from Analyze itself — not be
// deferred past the API boundary into a lazy result the caller iterates later.
func TestAnalyze_SurfacesBlobDecodeError(t *testing.T) {
	db, e, _ := writeTier3ValueThenCorruptBlob(t)
	defer db.Close()

	_, err := db.Analyze(`[:find ?v :in $ ?e :where [?e :doc/blob ?v]]`, e)
	require.ErrorContains(t, err, "blob", "Analyze must surface a deferred decode error, not return a clean lazy result")
}

func TestQueryInto_SurfacesBlobDecodeError(t *testing.T) {
	db, e, _ := writeTier3ValueThenCorruptBlob(t)
	defer db.Close()

	var out [][]byte
	err := db.QueryInto(&out, `[:find ?v :in $ ?e :where [?e :doc/blob ?v]]`, e)
	require.ErrorContains(t, err, "blob", "a missing blob must not be reported as an empty slice")
}

func TestQueryOneInto_SurfacesBlobDecodeError(t *testing.T) {
	db, e, _ := writeTier3ValueThenCorruptBlob(t)
	defer db.Close()

	var out []byte
	found, err := db.QueryOneInto(&out, `[:find ?v :in $ ?e :where [?e :doc/blob ?v]]`, e)
	require.ErrorContains(t, err, "blob", "a missing blob must not be reported as found=false,nil")
	require.False(t, found)
}

// TestQueryOrderBy_SurfacesBlobDecodeError: order-by materializes (Sort) the
// failing scan; the error must survive that transform to the boundary.
func TestQueryOrderBy_SurfacesBlobDecodeError(t *testing.T) {
	db, e, _ := writeTier3ValueThenCorruptBlob(t)
	defer db.Close()

	_, err := executor.CollectTuples(db.Query(`[:find ?v :in $ ?e :where [?e :doc/blob ?v] :order-by [?v]]`, e))
	require.ErrorContains(t, err, "blob", "order-by over a missing blob must surface the error")
}

// TestQueryAggregate_SurfacesBlobDecodeError: aggregation consumes the failing
// scan; the error must survive that transform to the boundary.
func TestQueryAggregate_SurfacesBlobDecodeError(t *testing.T) {
	db, e, _ := writeTier3ValueThenCorruptBlob(t)
	defer db.Close()

	_, err := executor.CollectTuples(db.Query(`[:find (count ?v) :in $ ?e :where [?e :doc/blob ?v]]`, e))
	require.ErrorContains(t, err, "blob", "aggregate over a missing blob must surface the error")
}

// TestQueryNot_SurfacesBlobDecodeError: the (not [?e :doc/blob ?v]) inner scan
// decodes the value; when that fails, the inner relation looks empty and NOT
// would wrongly include the entity. The error must surface instead of producing
// a silently-wrong result.
func TestQueryNot_SurfacesBlobDecodeError(t *testing.T) {
	db, e, _ := writeTier3ValueThenCorruptBlob(t)
	defer db.Close()

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, datalog.NewKeyword(":doc/name"), "doc-one"))
	_, err := tx.Commit()
	require.NoError(t, err)

	_, err = executor.CollectTuples(db.Query(`[:find ?n :in $ ?e :where [?e :doc/name ?n] (not [?e :doc/blob ?v])]`, e))
	require.ErrorContains(t, err, "blob", "a failed inner scan must surface, not silently un-exclude the entity")
}

// TestIndexNestedLoop_SurfacesBlobDecodeError: the index-nested-loop strategy
// (reusingIterator) is not reachable via db.Query (the matcher hardcodes
// HashJoinScan), so this drives it directly with IndexNestedLoopThreshold high.
// The reusingIterator must surface the inner scan's deferred error on exhaustion.
func TestIndexNestedLoop_SurfacesBlobDecodeError(t *testing.T) {
	db, e, _ := writeTier3ValueThenCorruptBlob(t)
	defer db.Close()

	q, err := parser.ParseQuery(`[:find ?v :in $ ?e :where [?e :doc/blob ?v]]`)
	require.NoError(t, err)
	pattern := q.Where[0].(*query.DataPattern)

	matcher := NewBadgerMatcherWithOptions(db.store, executor.ExecutorOptions{IndexNestedLoopThreshold: 999999})
	bindingRel := executor.NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?e")}, []executor.Tuple{{e}})

	result, err := matcher.Match(query.PatternQuery(pattern), executor.Relations{bindingRel})
	require.NoError(t, err)
	it := result.Iterator()
	for it.Next() {
	}
	require.ErrorContains(t, it.Error(), "blob", "index-nested-loop scan must surface the blob decode error")
	it.Close()
}

// TestQueryGroupedAggregate_SurfacesBlobDecodeError: a grouped aggregation
// (group-by var in :find) routes through the grouped path; the failing scan's
// error must survive.
func TestQueryGroupedAggregate_SurfacesBlobDecodeError(t *testing.T) {
	db, e, _ := writeTier3ValueThenCorruptBlob(t)
	defer db.Close()

	_, err := executor.CollectTuples(db.Query(`[:find ?e (count ?v) :in $ ?e :where [?e :doc/blob ?v]]`, e))
	require.ErrorContains(t, err, "blob", "grouped aggregate over a missing blob must surface the error")
}

// TestQueryRelationInput_SurfacesBlobDecodeError: a RelationInput query
// (:in $ [[?e] ...]) iterates per input tuple and collects the per-tuple results;
// that collection must propagate a failing scan's error, not drop it.
func TestQueryRelationInput_SurfacesBlobDecodeError(t *testing.T) {
	db, e, _ := writeTier3ValueThenCorruptBlob(t)
	defer db.Close()

	_, err := executor.CollectTuples(db.Query(
		`[:find ?v :in $ [[?e] ...] :where [?e :doc/blob ?v]]`,
		[][]any{{e}}))
	require.ErrorContains(t, err, "blob", "relation-input iteration over a missing blob must surface the error")
}

// TestQuerySubquery_SurfacesBlobDecodeError: a subquery whose inner scan decodes
// the corrupted blob must surface the error through subquery result combination.
func TestQuerySubquery_SurfacesBlobDecodeError(t *testing.T) {
	db, e, _ := writeTier3ValueThenCorruptBlob(t)
	defer db.Close()

	_, err := executor.CollectTuples(db.Query(
		`[:find ?v :in $ ?e
		  :where [(q [:find ?bv :in $ ?e2 :where [?e2 :doc/blob ?bv]] $ ?e) [[?v]]]]`,
		e))
	require.ErrorContains(t, err, "blob", "subquery over a missing blob must surface the error")
}

// TestQueryOr_SurfacesBlobDecodeError: an (or ...) branch that scans the
// corrupted blob must surface the error through union of branch results.
func TestQueryOr_SurfacesBlobDecodeError(t *testing.T) {
	db, e, _ := writeTier3ValueThenCorruptBlob(t)
	defer db.Close()

	_, err := executor.CollectTuples(db.Query(
		`[:find ?v :in $ ?e :where (or [?e :doc/blob ?v] [?e :doc/missing ?v])]`,
		e))
	require.ErrorContains(t, err, "blob", "OR branch over a missing blob must surface the error")
}

// TestQueryMultiPhase_SurfacesBlobDecodeError: a two-pattern join over the
// corrupted value must surface the error rather than return empty. NOTE: this
// propagates via the collapsed/streaming join path; it does NOT force the failing
// scan into a non-last phase's Keep projection in executor.go — that laundering
// site (Site 1) is not yet triggered and remains open.
func TestQueryMultiPhase_SurfacesBlobDecodeError(t *testing.T) {
	db, e, _ := writeTier3ValueThenCorruptBlob(t)
	defer db.Close()

	_, err := executor.CollectTuples(db.Query(
		`[:find ?e2 :in $ ?e :where [?e :doc/blob ?v] [?e2 :doc/blob ?v]]`,
		e))
	require.ErrorContains(t, err, "blob", "multi-phase join over a missing blob must surface the error")
}

// writeValidThenCorruptBlob writes two :doc/blob datoms so an unbound scan yields
// a VALID row first and a FAILING row second. The attr-bound, E/V-unbound scan is
// E-primary (AETV for cardinality-one, AEVT for cardinality-many), and L85 is
// sort-order-preserving and IS the E key component, so the entity with the lower
// L85 is scanned first. That entity gets a small inline value (always decodes);
// the other gets a large Tier-3 value whose blob is then deleted. Deleting every
// blob key corrupts only the Tier-3 entity, so the failure lands on the SECOND
// Next() — exercising the truncation / second-Next() boundary paths.
func writeValidThenCorruptBlob(t *testing.T) *Database {
	t.Helper()
	dir := t.TempDir()

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:                 dir,
		DisableCache:         true,
		CompressionThreshold: 64,
	})
	require.NoError(t, err)

	a := datalog.NewKeyword(":doc/blob")
	low, high := datalog.NewIdentity("doc-1"), datalog.NewIdentity("doc-2")
	if high.L85() < low.L85() {
		low, high = high, low // ensure `low` sorts first in the E-primary scan
	}

	// Tier 3 requires the compressed size to exceed maxKeyValueSize (60000): a
	// large incompressible region plus a compressible region so Compress() helps.
	payload := make([]byte, 80*1024)
	_, err = rand.Read(payload)
	require.NoError(t, err)
	payload = append(payload, make([]byte, 80*1024)...)

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(low, a, []byte("ok"))) // small → inline, always decodes
	require.NoError(t, tx.Set(high, a, payload))     // large → Tier 3 blob
	_, err = tx.Commit()
	require.NoError(t, err)

	// Delete all blob keys (prefix 0xFF). Only `high` is Tier-3, so only its value
	// becomes undecodable; `low` is inline and still resolves.
	deleted := 0
	err = db.store.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte{0xFF}
		var keys [][]byte
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keys = append(keys, it.Item().KeyCopy(nil))
		}
		for _, k := range keys {
			if derr := txn.Delete(k); derr != nil {
				return derr
			}
			deleted++
		}
		return nil
	})
	require.NoError(t, err)
	require.Greater(t, deleted, 0, "expected a Tier-3 blob to corrupt")

	return db
}

// TestQueryInto_SurfacesBlobDecodeErrorAfterPartialResults: QueryInto consumes a
// scan that yields one valid row then fails (Failure Mode 3, truncation). The
// error must surface AND the destination must not be populated with the partial
// prefix. If the ForEach error check were missing, out would hold the valid "ok"
// row and err would be nil.
func TestQueryInto_SurfacesBlobDecodeErrorAfterPartialResults(t *testing.T) {
	db := writeValidThenCorruptBlob(t)
	defer db.Close()

	var out [][]byte
	err := db.QueryInto(&out, `[:find ?v :where [?e :doc/blob ?v]]`)
	require.ErrorContains(t, err, "blob", "a mid-iteration failure must surface, not yield a truncated success")
	require.Nil(t, out, "destination must not be populated with the partial prefix on error")
}

// TestQueryOneInto_SurfacesBlobDecodeErrorOnSecondNext: QueryOneInto reads a
// valid first row, then the second Next() fails (Verification Plan #4). The error
// must surface as found=false. If the second-Next() Error() check were missing,
// the first row would be mapped and the call would return found=true, nil.
func TestQueryOneInto_SurfacesBlobDecodeErrorOnSecondNext(t *testing.T) {
	db := writeValidThenCorruptBlob(t)
	defer db.Close()

	var out []byte
	found, err := db.QueryOneInto(&out, `[:find ?v :where [?e :doc/blob ?v]]`)
	require.ErrorContains(t, err, "blob", "a failure on the second Next() must surface, not look like exactly one result")
	require.False(t, found, "a failed second Next() must not be reported as a found single result")
}

// TestScan_YieldsValidRowThenFails asserts the precondition the two tests above
// rely on: the scan yields exactly one valid row and THEN fails. This is what
// makes them exercise the truncation / second-Next() branches rather than the
// already-covered first-Next() path. If the scan order ever changes, this fails
// and flags the coverage regression directly.
func TestScan_YieldsValidRowThenFails(t *testing.T) {
	db := writeValidThenCorruptBlob(t)
	defer db.Close()

	rel, err := db.Query(`[:find ?v :where [?e :doc/blob ?v]]`)
	require.NoError(t, err)
	it := rel.Iterator()
	defer it.Close()

	yielded := 0
	for it.Next() {
		yielded++
	}
	require.ErrorContains(t, it.Error(), "blob")
	require.Equal(t, 1, yielded, "exactly one valid row must precede the failing row (truncation precondition)")
}
