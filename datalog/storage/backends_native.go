//go:build !(js && wasm)

package storage

const defaultBackendName = "badger"

func appendNativeBackends(backends []Backend) []Backend {
	return append(backends, Backend{
		Name:       "badger",
		Persistent: true,
		Open: func(path string, encoder *BinaryKeyEncoder) (Store, error) {
			return NewBadgerStore(path, encoder)
		},
	})
}
