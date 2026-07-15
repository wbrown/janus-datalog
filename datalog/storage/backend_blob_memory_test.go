package storage

import "testing"

func deleteStoreBlobs(t *testing.T, store Store) int {
	t.Helper()
	if deleted, ok := deleteNativeStoreBlobs(t, store); ok {
		return deleted
	}
	memoryStore, ok := store.(*MemoryStore)
	if !ok {
		t.Fatalf("unsupported store %T", store)
	}
	memoryStore.mu.Lock()
	defer memoryStore.mu.Unlock()
	deleted := 0
	for key := range memoryStore.entries {
		raw := []byte(key)
		if len(raw) == 21 && raw[0] == blobKeyPrefix {
			delete(memoryStore.entries, key)
			deleted++
		}
	}
	return deleted
}
