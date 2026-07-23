//go:build !(js && wasm)

package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// scanAddWinsBadger drives the accumulator from a Badger keys-only
// iterator's raw keys with direct, inlinable per-entry calls. Returns
// ok=false when the iterator is not the Badger keys-only type, so the
// caller falls back to Datom() resolution.
func scanAddWinsBadger(encoder *BinaryKeyEncoder, iter Iterator, acc *addWinsAccumulator) (BlobReader, bool, error) {
	it, ok := iter.(*KeyOnlyIterator)
	if !ok {
		return nil, false, nil
	}
	for it.Next() {
		_, _, vBytes, tx, op, _, err := encoder.DecodeKey(EAVT, it.Key())
		if err != nil {
			return nil, true, err
		}
		acc.observeSpan(vBytes, Tx(tx).ToElementID(), datalog.CRDTOp(op))
	}
	return it.blobs, true, nil
}
