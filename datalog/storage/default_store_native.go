//go:build !(js && wasm)

package storage

func openDefaultStore(path string, encoder *BinaryKeyEncoder) (Store, error) {
	return NewBadgerStore(path, encoder)
}
