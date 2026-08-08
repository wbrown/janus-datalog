package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// =============================================================================
// Tier 3 blob reclamation — see BUG_BLOBS_ARE_NEVER_RECLAIMED
// =============================================================================
//
// Nothing in the tree deletes a blob. putBlob writes, ReadBlob reads, and there
// is no third operation, so every physically removed datom whose value went out
// of line leaks its blob permanently. Two production paths remove index keys:
// Transaction.Retract (via retractDatom, on the ordinary commit path) and
// Database.TruncateTo (via Store.DeleteDatoms).
//
// The first test below is the defect. The two after it pass today and are pins
// on the fix rather than reproducers: blobs are content-addressed and shared, so
// a reference check that reclaims too eagerly turns a space leak into a dangling
// read. They are what tells a fix from a regression, and must not be relaxed to
// make a reclamation change go green.
// =============================================================================

// tier3ValueAt returns the resolved value at (e, a), or nil when the attribute
// is absent. It reads through the pattern matcher rather than the query engine
// so a failure is attributable to storage.
func tier3ValueAt(t *testing.T, db *Database, e datalog.Identity, a datalog.Keyword) interface{} {
	t.Helper()
	matcher := NewPatternMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: e},
			query.Constant{Value: a},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)
	iter := results.Iterator()
	if !iter.Next() {
		return nil
	}
	return iter.Tuple()[0]
}

// TestBlobReclaimedWhenLastReferenceDeleted pins that removing the only datom
// referencing an out-of-line value reclaims the blob it referenced.
//
// Both physical-removal paths are covered because both leak, and they are
// gated differently: TruncateTo drains writers before calling DeleteDatoms,
// while Transaction.Retract runs on the ordinary commit path.
func TestBlobReclaimedWhenLastReferenceDeleted(t *testing.T) {
	for _, mode := range byteKeyBackends(t) {
		t.Run(mode.name, func(t *testing.T) {
			t.Run("commit-path retract", func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{CompressionThreshold: 256})
				entity := datalog.NewIdentity("blob-reclaim:sole")
				attr := datalog.NewKeyword(":blob/payload")
				value := tier3Bytes(t, 200000)

				tx := db.NewTransaction()
				require.NoError(t, tx.Add(entity, attr, value))
				_, err := tx.Commit()
				require.NoError(t, err)
				requireBlobCount(t, db, 1, "the value went out of line")

				tx = db.NewTransaction()
				require.NoError(t, tx.Retract(entity, attr, value))
				_, err = tx.Commit()
				require.NoError(t, err)

				require.Nil(t, tier3ValueAt(t, db, entity, attr),
					"the datom's index keys are gone")
				requireBlobCount(t, db, 0,
					"the last datom referencing this blob was removed")
			})

			t.Run("truncate path", func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{CompressionThreshold: 256})
				entity := datalog.NewIdentity("blob-reclaim:sole")
				attr := datalog.NewKeyword(":blob/payload")
				value := tier3Bytes(t, 200000)

				tx := db.NewTransaction()
				require.NoError(t, tx.Add(entity, attr, value))
				_, err := tx.Commit()
				require.NoError(t, err)
				requireBlobCount(t, db, 1, "the value went out of line")

				stored, err := db.Store().DatomsAfter(datalog.ElementID{})
				require.NoError(t, err)
				require.NotEmpty(t, stored)
				_, err = db.Store().DeleteDatoms(stored)
				require.NoError(t, err)

				require.Nil(t, tier3ValueAt(t, db, entity, attr),
					"the datom's index keys are gone")
				requireBlobCount(t, db, 0,
					"the last datom referencing this blob was removed")
			})
		})
	}
}

// TestBlobReferenceCountedWithinOneDeleteCall pins that the reference count is
// taken after the call's own deletes have landed, against what survives them.
//
// Three datoms share one blob and two go in a single DeleteDatoms call. The
// count that decides the blob's fate is therefore 1 — a count taken before the
// deletes would read 3 and never reclaim anything, and one derived by subtracting
// the call's own datoms from a prior count is bookkeeping that drifts the moment
// the same content appears twice in one call. Only the surviving index answers.
func TestBlobReferenceCountedWithinOneDeleteCall(t *testing.T) {
	for _, mode := range byteKeyBackends(t) {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{CompressionThreshold: 256})
			attr := datalog.NewKeyword(":blob/payload")
			value := tier3Bytes(t, 200000)

			for _, name := range []string{"a", "b", "c"} {
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(datalog.NewIdentity("blob-count:"+name), attr, value))
				_, err := tx.Commit()
				require.NoError(t, err)
			}
			requireBlobCount(t, db, 1, "three datoms, one content-addressed blob")

			stored, err := db.Store().DatomsAfter(datalog.ElementID{})
			require.NoError(t, err)

			// Every Commit also writes a :db/txInstant datom, so the store holds
			// more than the three under test. Select the ones carrying the blob.
			var payload []datalog.Datom
			for _, datom := range stored {
				if datom.A == attr {
					payload = append(payload, datom)
				}
			}
			require.Len(t, payload, 3)

			_, err = db.Store().DeleteDatoms(payload[:2])
			require.NoError(t, err)
			requireBlobCount(t, db, 1, "one surviving datom still refers to the blob")

			survivor := payload[2]
			assert.Equal(t, value, tier3ValueAt(t, db, survivor.E, attr),
				"the survivor must still read its value out of the blob")

			_, err = db.Store().DeleteDatoms(payload[2:])
			require.NoError(t, err)
			requireBlobCount(t, db, 0, "nothing refers to the blob now")
		})
	}
}

// TestBlobSurvivesWhileAnotherEntityReferencesIt pins the shared-blob half.
//
// Blobs are content-addressed, so one blob serves every datom with that content.
// A reference check that answers "delete" from the removed datom alone drops a
// blob the surviving entity still needs, and its read fails with
// "blob not found for hash". Passes today because nothing reclaims at all.
func TestBlobSurvivesWhileAnotherEntityReferencesIt(t *testing.T) {
	for _, mode := range byteKeyBackends(t) {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{CompressionThreshold: 256})
			attr := datalog.NewKeyword(":blob/payload")
			value := tier3Bytes(t, 200000)
			doomed := datalog.NewIdentity("blob-shared:doomed")
			survivor := datalog.NewIdentity("blob-shared:survivor")

			for _, entity := range []datalog.Identity{doomed, survivor} {
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(entity, attr, value))
				_, err := tx.Commit()
				require.NoError(t, err)
			}
			requireBlobCount(t, db, 1, "identical content is one blob")

			tx := db.NewTransaction()
			require.NoError(t, tx.Retract(doomed, attr, value))
			_, err := tx.Commit()
			require.NoError(t, err)

			require.Nil(t, tier3ValueAt(t, db, doomed, attr))
			assert.Equal(t, value, tier3ValueAt(t, db, survivor, attr),
				"the surviving entity still references this blob")
			requireBlobCount(t, db, 1, "a shared blob outlives any one of its referents")
		})
	}
}

// TestBlobSharedAcrossStringAndBytesEncodings pins the trap in reference
// counting by value search.
//
// putBlob keys on sha1(compressed) alone, but the index key carries a type tag,
// and compressAndRoute picks TypeHashedString for a string and TypeHashedBytes
// for a []byte. Compression is deterministic, so the same bytes stored both ways
// share one blob under two distinct value encodings — and therefore two distinct
// VAET prefixes. A reference check that scans only the deleted datom's own tag
// reports zero survivors and deletes a live blob.
func TestBlobSharedAcrossStringAndBytesEncodings(t *testing.T) {
	for _, mode := range byteKeyBackends(t) {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{CompressionThreshold: 256})
			attr := datalog.NewKeyword(":blob/payload")
			asBytes := tier3Bytes(t, 200000)
			asString := string(asBytes)

			bytesType, _, bytesBlob := datalog.EncodeValue(asBytes, 256)
			stringType, _, stringBlob := datalog.EncodeValue(asString, 256)
			require.Equal(t, datalog.TypeHashedBytes, bytesType)
			require.Equal(t, datalog.TypeHashedString, stringType)
			require.NotNil(t, bytesBlob)
			require.NotNil(t, stringBlob)
			require.Equal(t, bytesBlob.Hash, stringBlob.Hash,
				"one blob, reached under two type tags — this is what the reference count must union")

			bytesEntity := datalog.NewIdentity("blob-tags:bytes")
			stringEntity := datalog.NewIdentity("blob-tags:string")
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(bytesEntity, attr, asBytes))
			require.NoError(t, tx.Add(stringEntity, attr, asString))
			_, err := tx.Commit()
			require.NoError(t, err)
			requireBlobCount(t, db, 1, "both encodings hash to one blob")

			tx = db.NewTransaction()
			require.NoError(t, tx.Retract(bytesEntity, attr, asBytes))
			_, err = tx.Commit()
			require.NoError(t, err)

			require.Nil(t, tier3ValueAt(t, db, bytesEntity, attr))
			assert.Equal(t, asString, tier3ValueAt(t, db, stringEntity, attr),
				"the string datom still references the blob the []byte datom shared")
			requireBlobCount(t, db, 1, "a blob reached under the other type tag is still referenced")
		})
	}
}

// tier3Bytes returns a payload large enough that its compressed form exceeds
// maxKeyValueSize and routes out of line. The routing is asserted rather than
// assumed: a payload that stayed in its key would make every blob assertion
// below vacuously true.
func tier3Bytes(t *testing.T, size int) []byte {
	t.Helper()
	value := makeTier3Data(size)
	vType, _, blobData := datalog.EncodeValue(value, 256)
	require.Equal(t, datalog.TypeHashedBytes, vType,
		"payload must reach tier 3 for a blob assertion to mean anything")
	require.NotNil(t, blobData)
	return value
}

// requireBlobCount asserts how many out-of-line values the store holds.
func requireBlobCount(t *testing.T, db *Database, want int, why string) {
	t.Helper()
	keys, hasBlobTier := blobKeys(t, db.Store())
	require.True(t, hasBlobTier,
		"byteKeyBackends selects the stores whose fixed-width keys force a large value out of line")
	require.Equal(t, want, len(keys), why)
}
