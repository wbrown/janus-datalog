//go:build js && wasm

package storage

import "testing"

func appendNativeBackendCases(cases []storeContractCase) []storeContractCase {
	return cases
}

func appendNativeReopenCases(cases []reopenBackendCase) []reopenBackendCase {
	return cases
}

func deleteNativeStoreBlobs(t *testing.T, _ Store) (int, bool) {
	t.Helper()
	return 0, false
}
