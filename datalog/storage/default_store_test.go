//go:build !(js && wasm)

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Persistent is what NewDatabaseWithOptions consults to decide whether Path
// locates the data or would be discarded, so the default backend's answer to it
// is part of what a native build promises a caller who opens by path alone.
func TestDefaultBackendOnNativeIsBadger(t *testing.T) {
	backend := DefaultBackend()
	require.Equal(t, "badger", backend.Name)
	require.True(t, backend.Persistent)

	store, err := backend.Open(t.TempDir(), &BinaryKeyEncoder{})
	require.NoError(t, err)
	defer store.Close()
	_, ok := store.(*BadgerStore)
	require.True(t, ok)
}
