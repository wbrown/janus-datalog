package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

var (
	dumpTestEntity = datalog.NewIdentity("storage-dump:subject")
	dumpTestAttr   = datalog.NewKeyword(":storage-dump/name")
)

func memoryDatabaseWithOneDatom(t *testing.T) *Database {
	t.Helper()
	db, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(dumpTestEntity, dumpTestAttr, "detected"))
	_, err = tx.Commit()
	require.NoError(t, err)
	return db
}

func requireDumpDatom(t *testing.T, db *Database) {
	t.Helper()
	value, found, err := db.GetString(dumpTestEntity, dumpTestAttr)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "detected", value)
}

func emptyMemoryDatabase(t *testing.T) *Database {
	t.Helper()
	db, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// ImportDump reads the format from the dump's own bytes, so a JDZL export and
// an EDN export both load through one entry point without the caller naming
// which is which.
func TestImportDumpDetectsBothFormats(t *testing.T) {
	source := memoryDatabaseWithOneDatom(t)

	t.Run("jdzl", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "dump.jdzl")
		file, err := os.Create(path)
		require.NoError(t, err)
		require.NoError(t, source.ExportBinary(file))
		require.NoError(t, file.Close())

		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, binaryExportMagic, string(raw[:len(binaryExportMagic)]))

		target := emptyMemoryDatabase(t)
		require.NoError(t, target.ImportDump(bytes.NewReader(raw)))
		requireDumpDatom(t, target)
	})

	t.Run("edn", func(t *testing.T) {
		var edn bytes.Buffer
		require.NoError(t, source.Export(&edn))
		require.NotEqual(t, binaryExportMagic, edn.String()[:len(binaryExportMagic)])

		target := emptyMemoryDatabase(t)
		require.NoError(t, target.ImportDump(bytes.NewReader(edn.Bytes())))
		requireDumpDatom(t, target)
	})

	// Shorter than the magic is EDN of no datoms, not a read error.
	t.Run("empty", func(t *testing.T) {
		require.NoError(t, emptyMemoryDatabase(t).ImportDump(bytes.NewReader(nil)))
	})
}

// RemovePathOnClose makes the database own the directory it was opened on, for
// a caller that created scratch space to hold it. Close takes the directory
// with it, after the store has released its files.
func TestRemovePathOnCloseTakesTheDirectory(t *testing.T) {
	for _, backend := range AvailableBackends() {
		if !backend.Persistent {
			continue
		}
		t.Run(backend.Name, func(t *testing.T) {
			scratch := filepath.Join(t.TempDir(), "scratch")
			require.NoError(t, os.MkdirAll(scratch, 0o755))

			db, err := NewDatabaseWithOptions(DatabaseOptions{
				BackendName:       backend.Name,
				Path:              scratch,
				RemovePathOnClose: true,
			})
			require.NoError(t, err)
			require.NoError(t, db.Close())

			_, err = os.Stat(scratch)
			require.True(t, os.IsNotExist(err), "scratch directory should be gone, got %v", err)
		})
	}
}

func TestOrdinaryPathSurvivesClose(t *testing.T) {
	for _, backend := range AvailableBackends() {
		if !backend.Persistent {
			continue
		}
		t.Run(backend.Name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "kept")
			require.NoError(t, os.MkdirAll(dir, 0o755))

			db, err := NewDatabaseWithOptions(DatabaseOptions{
				BackendName: backend.Name,
				Path:        dir,
			})
			require.NoError(t, err)
			require.NoError(t, db.Close())

			_, err = os.Stat(dir)
			require.NoError(t, err)
		})
	}
}
