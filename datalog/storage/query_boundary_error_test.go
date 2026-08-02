package storage

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// Integration reproduction for BUG_ITERATOR_ERRORS_DROPPED_AT_PUBLIC_BOUNDARIES.md.
//
// A Tier-3 value stores its compressed bytes in the blob store with a content
// hash in the index key. If the blob is missing, decoding the value fails — and
// the storage iterator surfaces that only through Error() after Next() returns
// false. This drives a REAL deferred-iterator failure through the public
// boundaries (CollectTuples / QueryInto / QueryOneInto), which must not report
// it as an empty or "not found" success.

// appendBlobFaultCase adds the reproduction of the original defect: a
// real Tier-3 blob deleted out from under a real value. It runs on the stores
// that keep blobs — the ones whose fixed-width keys force a large value
// out of line — so the injected case carries these assertions on the rest.
func appendBlobFaultCase(t *testing.T, cases []queryBoundaryFaultCase) []queryBoundaryFaultCase {
	return append(cases, queryBoundaryFaultCase{
		name:                 "tier3-blob",
		modes:                byteKeyBackends(t),
		openFailing:          writeTier3ValueThenCorruptBlob,
		openValidThenFailing: writeValidThenCorruptBlob,
		errText:              "blob",
	})
}

// writeTier3ValueThenCorruptBlob writes one datom whose value lands in the Tier-3
// blob store, then deletes every blob key so the value can no longer be decoded.
func writeTier3ValueThenCorruptBlob(t *testing.T, mode optimizerMode) (*Database, datalog.Identity, datalog.Keyword) {
	t.Helper()

	// DisableCache so reads hit storage (and thus the blob) rather than a warm
	// resolved value. Small compression threshold so the value is compressed;
	// large incompressible payload so the compressed form exceeds the in-key
	// size limit and routes to Tier 3.
	db := createOptimizerModeDB(t, mode, DatabaseOptions{
		DisableCache:         true,
		CompressionThreshold: 64,
	})

	e := datalog.NewIdentity("doc-1")
	a := datalog.NewKeyword(":doc/blob")

	// Tier 3 requires the *compressed* size to exceed maxKeyValueSize (60000).
	// Use a large incompressible (random) region so it can't shrink below that,
	// plus a compressible (zero) region so Compress() reports a net benefit and
	// returns non-nil (pure-random returns nil and would store raw, inline).
	payload := make([]byte, 80*1024)
	_, err := rand.Read(payload)
	require.NoError(t, err)
	payload = append(payload, make([]byte, 80*1024)...)

	// :doc/name is the decodable attribute the NOT boundary needs: the outer
	// pattern must match so the failing inner scan is what the test observes.
	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, payload))
	require.NoError(t, tx.Set(e, datalog.NewKeyword(":doc/name"), "doc-one"))
	_, err = tx.Commit()
	require.NoError(t, err)

	deleted, _ := deleteStoreBlobs(t, db.Store())
	require.Greater(t, deleted, 0, "expected a Tier-3 blob to corrupt; value may not have routed to the blob store")

	return db, e, a
}

func TestKeyOnlyIteratorRetainsBlobErrorAfterRepeatedNext(t *testing.T) {
	for _, mode := range byteKeyBackends(t) {
		t.Run(mode.name, func(t *testing.T) {
			db, entity, attr := writeTier3ValueThenCorruptBlob(t, mode)

			iter, err := db.store.ScanKeysOnly(ScanBound{
				Index:  EATV,
				Prefix: []datalog.Value{entity, attr},
			})
			require.NoError(t, err)
			defer iter.Close()

			require.True(t, iter.Next())
			_, err = iter.Datom()
			require.ErrorContains(t, err, "blob")
			firstErr := iter.Error()
			require.ErrorIs(t, firstErr, err)
			require.False(t, iter.Next())
			require.ErrorIs(t, iter.Error(), firstErr)
		})
	}
}

// After Next() returns false at an exclusive end bound, Badger may still sit
// on the successor key. Datom()/Key() must report no current position — not
// decode the out-of-range neighbor.
func TestKeyOnlyIterator_DatomRejectsEndBoundSuccessor(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			first := datalog.NewIdentity("bound-a")
			second := datalog.NewIdentity("bound-b")
			if bytes.Compare(first.Bytes(), second.Bytes()) > 0 {
				first, second = second, first
			}
			attr := datalog.NewKeyword(":bound/v")
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(first, attr, "one"))
			require.NoError(t, tx.Add(second, attr, "two"))
			_, err := tx.Commit()
			require.NoError(t, err)

			firstBytes := first.Bytes()
			iter, err := db.store.ScanKeysOnly(ScanBound{Index: EAVT, Prefix: []datalog.Value{first}})
			require.NoError(t, err)
			defer iter.Close()

			require.True(t, iter.Next())
			d, err := iter.Datom()
			require.NoError(t, err)
			require.True(t, bytes.Equal(d.E.Bytes(), firstBytes[:]))

			require.False(t, iter.Next(), "scan of first entity must stop before successor")
			_, err = iter.Datom()
			require.ErrorContains(t, err, "no current datom")
		})
	}
}

// writeValidThenCorruptBlob writes two :doc/blob datoms so an unbound scan yields
// a VALID datom first and a FAILING one second. The attr-bound, E/V-unbound scan is
// E-primary (AETV for cardinality-one, AEVT for cardinality-many), and the raw
// identity hash is the E key component, so the entity with the lower hash is
// scanned first. That entity gets a small inline value (always decodes);
// the other gets a large Tier-3 value whose blob is then deleted. Deleting every
// blob key corrupts only the Tier-3 entity, so the failure lands on the SECOND
// Next() — exercising the truncation / second-Next() boundary paths.
func writeValidThenCorruptBlob(t *testing.T, mode optimizerMode) *Database {
	t.Helper()

	db := createOptimizerModeDB(t, mode, DatabaseOptions{
		DisableCache:         true,
		CompressionThreshold: 64,
	})

	a := datalog.NewKeyword(":doc/blob")
	low, high := datalog.NewIdentity("doc-1"), datalog.NewIdentity("doc-2")
	if bytes.Compare(high.Bytes(), low.Bytes()) < 0 {
		low, high = high, low // ensure `low` sorts first in the E-primary scan
	}

	// Tier 3 requires the compressed size to exceed maxKeyValueSize (60000): a
	// large incompressible region plus a compressible region so Compress() helps.
	payload := make([]byte, 80*1024)
	_, err := rand.Read(payload)
	require.NoError(t, err)
	payload = append(payload, make([]byte, 80*1024)...)

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(low, a, []byte("ok"))) // small → inline, always decodes
	require.NoError(t, tx.Set(high, a, payload))     // large → Tier 3 blob
	_, err = tx.Commit()
	require.NoError(t, err)

	// Only `high` is Tier-3, so only its value becomes undecodable; `low` is
	// inline and still resolves.
	deleted, _ := deleteStoreBlobs(t, db.Store())
	require.Greater(t, deleted, 0, "expected a Tier-3 blob to corrupt")

	return db
}

// TestQueryInto_SurfacesBlobDecodeErrorAfterPartialResults: QueryInto consumes a
// scan that yields one valid datom then fails (truncation). The
// error must surface AND the destination must not be populated with the partial
// prefix. If the ForEach error check were missing, out would hold the valid "ok"
// entry and err would be nil.
func TestQueryInto_SurfacesBlobDecodeErrorAfterPartialResults(t *testing.T) {
	for _, mode := range byteKeyBackends(t) {
		t.Run(mode.name, func(t *testing.T) {
			db := writeValidThenCorruptBlob(t, mode)

			var out [][]byte
			err := db.QueryInto(&out, `[:find ?v :where [?e :doc/blob ?v]]`)
			require.ErrorContains(t, err, "blob", "a mid-iteration failure must surface, not yield a truncated success")
			require.Nil(t, out, "destination must not be populated with the partial prefix on error")
		})
	}
}

// TestQueryOneInto_SurfacesBlobDecodeErrorOnSecondNext: QueryOneInto reads a
// valid first tuple, then the second Next() fails. The error
// must surface as found=false. If the second-Next() Error() check were missing,
// the first tuple would be mapped and the call would return found=true, nil.
func TestQueryOneInto_SurfacesBlobDecodeErrorOnSecondNext(t *testing.T) {
	for _, mode := range byteKeyBackends(t) {
		t.Run(mode.name, func(t *testing.T) {
			db := writeValidThenCorruptBlob(t, mode)

			var out []byte
			found, err := db.QueryOneInto(&out, `[:find ?v :where [?e :doc/blob ?v]]`)
			require.ErrorContains(t, err, "blob", "a failure on the second Next() must surface, not look like exactly one result")
			require.False(t, found, "a failed second Next() must not be reported as a found single result")
		})
	}
}

// TestScan_YieldsValidDatomThenFails asserts the precondition the two tests above
// rely on: the scan yields exactly one valid datom and THEN fails. This is what
// makes them exercise the truncation / second-Next() branches rather than the
// already-covered first-Next() path. If the scan order ever changes, this fails
// and flags the coverage regression directly.
func TestScan_YieldsValidDatomThenFails(t *testing.T) {
	for _, mode := range byteKeyBackends(t) {
		t.Run(mode.name, func(t *testing.T) {
			db := writeValidThenCorruptBlob(t, mode)

			rel, err := db.Query(`[:find ?v :where [?e :doc/blob ?v]]`)
			require.NoError(t, err)
			it := rel.Iterator()
			defer it.Close()

			yielded := 0
			for it.Next() {
				yielded++
			}
			require.ErrorContains(t, it.Error(), "blob")
			require.Equal(t, 1, yielded, "exactly one valid datom must precede the failing one (truncation precondition)")
		})
	}
}
