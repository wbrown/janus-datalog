//go:build js && wasm

package storage

import "testing"

// Badger needs a filesystem, so a wasm build has no persistent backend.
func expectedBackendNames() []string {
	return []string{"memory", "memory-trees"}
}

func appendNativeReopenCases(cases []reopenBackendCase) []reopenBackendCase {
	return cases
}

func nativeBlobKeys(t *testing.T, _ Store) ([][]byte, bool) {
	t.Helper()
	return nil, false
}

func deleteNativeKeys(t *testing.T, _ Store, _ [][]byte) bool {
	t.Helper()
	return false
}
