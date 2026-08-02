package storage

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// importFixtureDump exports a dump large enough to span more than one chunk, so
// a test can distinguish "after every chunk" from "after the last one".
func importFixtureDump(t *testing.T, testCase storeContractCase) (*seekBuffer, int) {
	t.Helper()

	source := openContractDatabase(t, testCase, DatabaseOptions{})
	attr := datalog.NewKeyword(":import/payload")
	filler := strings.Repeat("p", 512)
	datoms := make([]datalog.Datom, 4000)
	for i := range datoms {
		datoms[i] = datalog.Datom{
			E:  datalog.NewIdentity(fmt.Sprintf("import:entity:%d", i)),
			A:  attr,
			V:  filler + fmt.Sprintf("%08d", i),
			Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 4},
		}
	}
	require.NoError(t, source.store.Assert(datoms))

	dump := &seekBuffer{}
	require.NoError(t, source.ExportBinary(dump))

	header, err := readBinaryHeader(dump)
	require.NoError(t, err)
	trailer, err := readBinaryIndex(dump, header.indexOffset)
	require.NoError(t, err)
	require.Greater(t, len(trailer.entries), 1,
		"fixture produced one chunk; raise the datom count so the dump spans several")

	return dump, len(trailer.entries)
}

// TestBinaryImportRunsFinalizerOnceAfterEveryChunk pins when the finalizer runs.
//
// A backend that leaves its write open across chunks needs one call at the end
// of the run to complete it, and exactly one: called per chunk it would be the
// per-chunk completion it exists to avoid, and called before the last chunk it
// would leave datoms outside the completed write.
func TestBinaryImportRunsFinalizerOnceAfterEveryChunk(t *testing.T) {
	for _, testCase := range storeContractCases() {
		t.Run(testCase.name, func(t *testing.T) {
			dump, chunks := importFixtureDump(t, testCase)
			require.Greater(t, chunks, 1)

			target := openContractDatabase(t, testCase, DatabaseOptions{})
			finalized := 0
			require.NoError(t, target.ImportBinary(dump, BinaryImportOptions{
				Finalize: func() error {
					finalized++
					return nil
				},
			}))

			require.Equal(t, 1, finalized, "finalizer ran %d times for %d chunks", finalized, chunks)
			// Counted after ImportBinary returns, not inside Finalize: a store
			// that holds its write open has published nothing yet when the
			// finalizer runs, which is the arrangement Finalize exists to serve.
			require.Equal(t, 4000, countStoreIndex(t, target.store, EAVT))
		})
	}
}

// TestBinaryImportSkipsFinalizerWhenAChunkFails pins that a failed import does
// not complete the write. The failure is planted in the second chunk so that at
// least one chunk decodes first; a dump failing on its first chunk would pass
// whatever the finalizer did.
func TestBinaryImportSkipsFinalizerWhenAChunkFails(t *testing.T) {
	for _, testCase := range storeContractCases() {
		t.Run(testCase.name, func(t *testing.T) {
			dump, _ := importFixtureDump(t, testCase)

			header, err := readBinaryHeader(dump)
			require.NoError(t, err)
			trailer, err := readBinaryIndex(dump, header.indexOffset)
			require.NoError(t, err)

			payload := trailer.entries[1].offset + uint64(binaryChunkHeaderSize)
			for i := uint64(0); i < 32; i++ {
				dump.buf[payload+i] ^= 0xFF
			}

			target := openContractDatabase(t, testCase, DatabaseOptions{})
			finalized := 0
			require.Error(t, target.ImportBinary(dump, BinaryImportOptions{
				Finalize: func() error {
					finalized++
					return nil
				},
			}), "corrupted chunk imported cleanly")
			require.Zero(t, finalized, "finalizer completed a failed import")
		})
	}
}
