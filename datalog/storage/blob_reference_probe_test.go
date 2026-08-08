package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/wbrown/janus-datalog/datalog"
)

// Blob reference probing — see BUG_BLOBS_ARE_NEVER_RECLAIMED.
//
// Reclamation asks whether any index key names a blob's hash. VAET leads with
// the value payload and a tier-3 payload is the fixed-width [type][hash], so the
// question is a prefix seek. These tests pin the two properties that makes rest
// on: the probe addresses the key VAET actually writes, and one blob needs two
// probes because it is reachable under either hashed type tag.

// TestVAETHashPrefixAddressesTheWrittenKey ties the probe to the key layout.
// Reordering VAET's components, or widening the type tag, silently turns every
// reference probe into a miss — which reads as "unreferenced" and deletes live
// blobs. This is the test that refuses that.
func TestVAETHashPrefixAddressesTheWrittenKey(t *testing.T) {
	encoder := &BinaryKeyEncoder{CompressionThreshold: 256}
	payload := makeTier3Data(200000)

	vType, _, blobData := datalog.EncodeValue(payload, 256)
	require.Equal(t, datalog.TypeHashedBytes, vType,
		"payload must reach tier 3 or this pins nothing")
	require.NotNil(t, blobData)

	datom := datalog.Datom{
		E:  datalog.NewIdentity("probe:entity"),
		A:  datalog.NewKeyword(":probe/payload"),
		V:  payload,
		Tx: datalog.ElementID{Lamport: 7, ReplicaID: 3},
	}
	key := encoder.EncodeKey(VAET, &datom)
	prefix := encoder.VAETHashPrefix(vType, blobData.Hash)

	require.Len(t, prefix, 22, "index byte + type byte + 20-byte hash")
	require.True(t, bytes.HasPrefix(key, prefix),
		"the probe must address the key VAET writes for this datom")
}

// TestVAETHashPrefixSeparatesTheHashedTags pins why datalog.HashedValueTypes
// exists. The same bytes stored as a string and as a []byte compress
// identically, so they share one blob — but their index keys carry different
// type tags, so a probe under one tag cannot see a reference held under the
// other. A reference check that probes a single tag deletes live blobs.
func TestVAETHashPrefixSeparatesTheHashedTags(t *testing.T) {
	encoder := &BinaryKeyEncoder{CompressionThreshold: 256}
	asBytes := makeTier3Data(200000)
	asString := string(asBytes)

	bytesType, _, bytesBlob := datalog.EncodeValue(asBytes, 256)
	stringType, _, stringBlob := datalog.EncodeValue(asString, 256)
	require.Equal(t, datalog.TypeHashedBytes, bytesType)
	require.Equal(t, datalog.TypeHashedString, stringType)
	require.Equal(t, bytesBlob.Hash, stringBlob.Hash,
		"identical content is one blob whichever Go type carried it")

	bytesPrefix := encoder.VAETHashPrefix(bytesType, bytesBlob.Hash)
	stringPrefix := encoder.VAETHashPrefix(stringType, stringBlob.Hash)
	require.NotEqual(t, bytesPrefix, stringPrefix,
		"one blob, two prefixes")

	attr := datalog.NewKeyword(":probe/payload")
	bytesDatom := datalog.Datom{
		E: datalog.NewIdentity("probe:bytes"), A: attr, V: asBytes,
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
	}
	stringDatom := datalog.Datom{
		E: datalog.NewIdentity("probe:string"), A: attr, V: asString,
		Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1},
	}
	bytesKey := encoder.EncodeKey(VAET, &bytesDatom)
	stringKey := encoder.EncodeKey(VAET, &stringDatom)

	require.True(t, bytes.HasPrefix(bytesKey, bytesPrefix))
	require.True(t, bytes.HasPrefix(stringKey, stringPrefix))
	require.False(t, bytes.HasPrefix(stringKey, bytesPrefix),
		"probing the []byte tag alone misses the string datom's reference")
	require.False(t, bytes.HasPrefix(bytesKey, stringPrefix),
		"and the converse")
}

// TestBlobIsReferencedKeepsABlobHeldUnderAnyTag pins that a reference under one
// tag keeps the blob even when the other tag has none. Concluding absence from a
// single tag's probe is the shape that deletes live blobs.
//
// The tags come from the encoder's own key for a datom carrying each of the two
// domain types that can reach a blob, not from a list this test also owns —
// datalog.TestBlobReferenceTaxonomyMatchesTheMinter holds the set itself to what
// EncodeValue mints.
func TestBlobIsReferencedKeepsABlobHeldUnderAnyTag(t *testing.T) {
	encoder := &BinaryKeyEncoder{CompressionThreshold: 256}
	payload := makeTier3Data(200000)
	attr := datalog.NewKeyword(":probe/payload")

	for _, held := range []struct {
		name  string
		value datalog.Value
	}{
		{"[]byte", payload},
		{"string", string(payload)},
	} {
		t.Run(held.name, func(t *testing.T) {
			_, _, blobData := datalog.EncodeValue(held.value, 256)
			require.NotNil(t, blobData, "the case must reach tier 3")

			datom := datalog.Datom{
				E: datalog.NewIdentity("probe:" + held.name), A: attr, V: held.value,
				Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
			}
			heldKey := encoder.EncodeKey(VAET, &datom)

			onlyThisDatom := func(prefix []byte) bool {
				return bytes.HasPrefix(heldKey, prefix)
			}
			require.True(t, blobIsReferenced(encoder, blobData.Hash, onlyThisDatom),
				"a reference held only as %s must keep the blob", held.name)
		})
	}

	// And with nothing holding it, the same probe reports unreferenced.
	_, _, blobData := datalog.EncodeValue(payload, 256)
	require.NotNil(t, blobData)
	require.False(t, blobIsReferenced(encoder, blobData.Hash,
		func([]byte) bool { return false }))
}

// TestBlobKeyLayout pins the content-addressed key the sweep deletes by.
func TestBlobKeyLayout(t *testing.T) {
	var hash [20]byte
	copy(hash[:], "0123456789abcdefghij")

	key := blobKey(hash)
	require.Len(t, key[:], blobKeyLen)
	require.Equal(t, blobKeyPrefix, key[0])
	require.Equal(t, hash[:], key[1:])
	require.Greater(t, blobKeyPrefix, byte(TAEV),
		"blobs must sort clear of every index keyspace")
}
