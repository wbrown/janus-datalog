//go:build js && wasm

package storage

// Badger needs a filesystem, so a wasm build has no persistent backend and
// defaults to the in-memory emulator.
const defaultBackendName = "memory"

func appendNativeBackends(backends []Backend) []Backend {
	return backends
}
