package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBinaryChunkPayloadsDecodeConcurrently pins that concurrent readers each
// get their own chunk back at its recorded length, with its datoms walkable.
func TestBinaryChunkPayloadsDecodeConcurrently(t *testing.T) {
	backend, err := BackendNamed("memory")
	require.NoError(t, err)
	dump, chunks := importFixtureDump(t, contractCaseFor(backend))
	require.Greater(t, chunks, 1, "one chunk cannot contend")

	header, err := readBinaryHeader(dump)
	require.NoError(t, err)
	trailer, err := readBinaryIndex(dump, header.indexOffset)
	require.NoError(t, err)

	var seekMu sync.Mutex
	var wg sync.WaitGroup
	payloads := make([][]byte, len(trailer.entries))
	failures := make([]error, len(trailer.entries))
	for i := range trailer.entries {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payloads[i], failures[i] = readBinaryChunkPayload(dump, trailer.entries[i], &seekMu)
		}(i)
	}
	wg.Wait()

	datoms := 0
	for i := range payloads {
		require.NoError(t, failures[i], "chunk %d", i)
		require.Equal(t, int(trailer.entries[i].uncLen), len(payloads[i]),
			"chunk %d decoded to the wrong length", i)
		cursor := newBinaryChunkCursor(payloads[i])
		for cursor.Next() {
			datoms++
		}
		require.NoError(t, cursor.Err(), "chunk %d", i)
	}
	require.Equal(t, 4000, datoms)
}
