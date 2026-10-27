//go:build !(js && wasm)

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A persistent backend's directory held by another open is reported as
// ErrStoreLocked by a second open of the same path, with the backend's own
// diagnostic, the path among it, kept in the error's text, so a caller tells
// a store another process holds from a store that will not open by errors.Is
// and never by matching the backend's prose.
func TestAPersistentBackendHeldByAnotherOpenIsReportedLocked(t *testing.T) {
	for _, backend := range AvailableBackends() {
		if !backend.Persistent {
			continue
		}
		t.Run(backend.Name, func(t *testing.T) {
			path := t.TempDir()
			first, err := backend.Open(path, &BinaryKeyEncoder{})
			require.NoError(t, err)
			defer first.Close()

			_, err = backend.Open(path, &BinaryKeyEncoder{})
			require.ErrorIs(t, err, ErrStoreLocked)
			require.ErrorContains(t, err, path, "the backend's own diagnostic stays in the error's text")
		})
	}
}
