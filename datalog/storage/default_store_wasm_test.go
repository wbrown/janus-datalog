//go:build js && wasm

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenDefaultStoreUsesMemory(t *testing.T) {
	store, err := openDefaultStore(t.TempDir(), &BinaryKeyEncoder{})
	require.NoError(t, err)
	defer store.Close()
	_, ok := store.(*MemoryStore)
	require.True(t, ok)
}
