package storage

import "github.com/wbrown/janus-datalog/datalog"

// blobKeyLen is the width of a blob key: the 0xFF prefix and a 20-byte SHA1.
const blobKeyLen = 1 + 20

// blobKey returns the content-addressed key holding the compressed bytes whose
// SHA1 is hash.
func blobKey(hash [20]byte) [blobKeyLen]byte {
	var key [blobKeyLen]byte
	key[0] = blobKeyPrefix
	copy(key[1:], hash[:])
	return key
}

// blobIsReferenced reports whether any index key names the blob at hash, asking
// exists once per hashed type tag.
//
// Both tags are asked because a blob is content-addressed and its key carries no
// type: identical bytes stored as a string and as a []byte compress to the same
// SHA1 and share one blob, while their index keys differ in the tag alone. A
// probe under a single tag therefore reports "unreferenced" for a blob the other
// tag still holds, and deleting on that answer strands a live datom.
//
// exists answers whether any key carries the given prefix. It is the caller's
// because the store owning the keys is the only thing that can seek them, and
// because the caller also owns the exclusion the answer is only valid under —
// a writer commits a tier-3 value's blob alongside its index keys, so an answer
// obtained before that commit is stale the moment it lands.
func blobIsReferenced(encoder *BinaryKeyEncoder, hash [20]byte, exists func(prefix []byte) bool) bool {
	for vType := range datalog.BlobReferenceTypes {
		if exists(encoder.VAETHashPrefix(vType, hash)) {
			return true
		}
	}
	return false
}
