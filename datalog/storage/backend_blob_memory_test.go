package storage

import "testing"

// blobKeys returns every out-of-line value key the store holds. A store holding
// whole datoms has no fixed-width key for a large value to escape, so it keeps
// no blobs and reports hasBlobTier false. An unrecognized store is fatal rather
// than a third case.
func blobKeys(t *testing.T, store Store) (keys [][]byte, hasBlobTier bool) {
	t.Helper()
	if keys, ok := nativeBlobKeys(t, store); ok {
		return keys, true
	}
	switch typed := store.(type) {
	case *MemoryStore:
		typed.mu.RLock()
		defer typed.mu.RUnlock()
		for key := range typed.entries {
			raw := []byte(key)
			if len(raw) == 21 && raw[0] == blobKeyPrefix {
				keys = append(keys, raw)
			}
		}
		return keys, true
	case *MemoryTreeStore:
		return nil, false
	default:
		t.Fatalf("unsupported store %T", store)
		return nil, false
	}
}

// deleteStoreBlobs removes every out-of-line value the store holds, reporting
// how many went and whether the store keeps blobs at all.
func deleteStoreBlobs(t *testing.T, store Store) (deleted int, hasBlobTier bool) {
	t.Helper()
	keys, hasBlobTier := blobKeys(t, store)
	if !hasBlobTier {
		return 0, false
	}
	if deleteNativeBlobKeys(t, store, keys) {
		return len(keys), true
	}
	memoryStore := store.(*MemoryStore)
	memoryStore.mu.Lock()
	defer memoryStore.mu.Unlock()
	var undo []memoryEntryUndo
	for _, key := range keys {
		deleteMemoryEntry(memoryStore, &undo, string(key))
	}
	return len(keys), true
}
