//go:build js && wasm

package storage

// Badger needs a filesystem, so a wasm build has no persistent backend and
// defaults to the tree store: typed datoms, encoded only at the JDZL and EDN
// boundaries.
const defaultBackendName = "memory-trees"

func appendNativeBackends(backends []Backend) []Backend {
	return backends
}
