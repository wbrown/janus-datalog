package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Memory-specific construction smoke; public semantics live in the backend matrix.
func TestMemoryStoreConstructs(t *testing.T) {
	store := NewMemoryStore(&BinaryKeyEncoder{})
	require.NotNil(t, store.Encoder())
	require.NoError(t, store.Close())
	require.NoError(t, store.Close())
}
