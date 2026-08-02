//go:build js && wasm

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Persistent is what NewDatabaseWithOptions consults to decide whether Path
// locates the data or would be discarded. Under wasm the default backend answers
// false, which is why opening by path here yields an in-process database rather
// than an error: the caller asked for a location and the build chose what
// answers.
func TestDefaultBackendOnWasmIsMemoryTrees(t *testing.T) {
	backend := DefaultBackend()
	require.Equal(t, "memory-trees", backend.Name)
	require.False(t, backend.Persistent)

	store, err := backend.Open(t.TempDir(), &BinaryKeyEncoder{})
	require.NoError(t, err)
	defer store.Close()
	_, ok := store.(*MemoryTreeStore)
	require.True(t, ok)
}
