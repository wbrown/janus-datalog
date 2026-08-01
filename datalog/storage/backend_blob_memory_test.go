package storage

import "testing"

// deleteStoreBlobs removes every out-of-line value the store holds. A store
// holding whole datoms has no fixed-width key for a large value to escape, so
// it keeps no blobs and reports hasBlobTier false. An unrecognized store is
// fatal rather than a third case.
func deleteStoreBlobs(t *testing.T, store Store) (deleted int, hasBlobTier bool) {
	t.Helper()
	if deleted, ok := deleteNativeStoreBlobs(t, store); ok {
		return deleted, true
	}
	switch typed := store.(type) {
	case *MemoryStore:
		typed.mu.Lock()
		defer typed.mu.Unlock()
		for key := range typed.entries {
			raw := []byte(key)
			if len(raw) == 21 && raw[0] == blobKeyPrefix {
				delete(typed.entries, key)
				deleted++
			}
		}
		return deleted, true
	case *MemoryTreeStore:
		return 0, false
	default:
		t.Fatalf("unsupported store %T", store)
		return 0, false
	}
}
