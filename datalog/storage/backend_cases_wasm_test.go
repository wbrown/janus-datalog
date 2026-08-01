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

func deleteNativeStoreBlobs(t *testing.T, _ Store) (int, bool) {
	t.Helper()
	return 0, false
}

// The Tier-3 blob reproduction needs Badger. The injected fault case carries
// the boundary assertions here.
func appendNativeBlobFaultCase(cases []queryBoundaryFaultCase) []queryBoundaryFaultCase {
	return cases
}
