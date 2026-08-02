package storage

import (
	"fmt"
	"strings"
)

// Backend is a store this build can open. Which ones exist is a property of the
// build: Badger needs a filesystem and does not compile under js/wasm, so a
// caller choosing a store — a command-line flag, a JS host, a diagnostic — asks
// AvailableBackends rather than re-deriving it from build tags.
type Backend struct {
	Name string

	// Persistent reports whether the backend keeps its data at Path. A caller
	// that reopens a path expecting its data back needs this; the in-process
	// stores ignore Path entirely.
	Persistent bool

	Open func(path string, encoder *BinaryKeyEncoder) (Store, error)
}

// AvailableBackends lists every store this build can open. The order is
// in-process first, then persistent; DefaultBackend names the default rather
// than the position doing it.
func AvailableBackends() []Backend {
	return appendNativeBackends([]Backend{
		{
			// The Badger emulator: the same binary index keys a disk store
			// holds, in a process that has no disk.
			Name: "memory",
			Open: func(_ string, encoder *BinaryKeyEncoder) (Store, error) {
				return NewMemoryStore(encoder), nil
			},
		},
		{
			// Eight sorted trees of typed datoms, encoding only at the JDZL and
			// EDN boundaries.
			Name: "memory-trees",
			Open: func(_ string, encoder *BinaryKeyEncoder) (Store, error) {
				return NewMemoryTreeStore(encoder), nil
			},
		},
	})
}

// BackendNamed resolves a backend by name. The error lists what this build has,
// because the answer differs between a native binary and a wasm one.
func BackendNamed(name string) (Backend, error) {
	available := AvailableBackends()
	for _, backend := range available {
		if backend.Name == name {
			return backend, nil
		}
	}
	names := make([]string, 0, len(available))
	for _, backend := range available {
		names = append(names, backend.Name)
	}
	return Backend{}, fmt.Errorf("unknown storage backend %q; this build has: %s",
		name, strings.Join(names, ", "))
}

// DefaultBackend is what a Database opened by path uses. Native builds persist
// to Badger; wasm has no filesystem and holds the data in memory.
func DefaultBackend() Backend {
	backend, err := BackendNamed(defaultBackendName)
	if err != nil {
		panic(fmt.Sprintf("default backend %q is not in AvailableBackends", defaultBackendName))
	}
	return backend
}
