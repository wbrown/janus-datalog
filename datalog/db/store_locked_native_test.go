//go:build !(js && wasm)

package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/db"
)

// A directory a database holds open is reported as ErrStoreLocked by a
// second Open of the same path, under the name the db package exports, with
// the path kept in the error's text.
func TestADirectoryHeldByAnotherOpenIsReportedLocked(t *testing.T) {
	path := t.TempDir()
	first, err := db.Open(path, db.WithBackend("badger"))
	require.NoError(t, err)
	defer first.Close()

	_, err = db.Open(path, db.WithBackend("badger"))
	require.ErrorIs(t, err, db.ErrStoreLocked)
	require.ErrorContains(t, err, path, "the backend's own diagnostic stays in the error's text")
}
