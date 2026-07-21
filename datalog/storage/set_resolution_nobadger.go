//go:build js && wasm

package storage

// scanAddWinsBadger has no Badger iterator to match on js/wasm; ok=false
// routes the caller to the memory-store or Datom() paths.
func scanAddWinsBadger(_ *BinaryKeyEncoder, _ Iterator, _ *addWinsAccumulator) (BlobReader, bool, error) {
	return nil, false, nil
}
