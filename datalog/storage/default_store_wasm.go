//go:build js && wasm

package storage

func openDefaultStore(_ string, encoder *BinaryKeyEncoder) (Store, error) {
	return NewMemoryStore(encoder), nil
}
