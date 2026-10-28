package db_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
)

// A dump loaded into a persistent backend lands in scratch space db.Open makes
// and the database owns. When the dump does not load, Open returns no database
// and the scratch directory is unreachable to the caller, so Open removes it:
// nothing named janus-dump-* is left in the temporary directory.
//
// The dump is the engine's own: an EDN export of an entity whose body is a
// string longer than the EDN reader's one-line buffer, which the reader
// refuses.
func TestADumpThatFailsToLoadLeavesNoScratchDirectory(t *testing.T) {
	chapter := datalog.NewIdentity("scratch:chapter")
	name := datalog.NewKeyword(":name")
	body := datalog.NewKeyword(":chapter/body")
	facts := []struct {
		e datalog.Identity
		a datalog.Keyword
		v interface{}
	}{
		{chapter, name, "the long chapter"},
		{chapter, body, strings.Repeat("a long line of prose. ", 80000)},
	}

	writer, err := db.OpenMemory()
	require.NoError(t, err)
	defer writer.Close()
	tx := writer.NewTransaction()
	for _, f := range facts {
		require.NoError(t, tx.Set(f.e, f.a, f.v))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	var written []string
	require.NoError(t, writer.QueryInto(&written, `[:find ?n :where [?e :name ?n]]`))

	dumpPath := filepath.Join(t.TempDir(), "chapter.edn")
	file, err := os.Create(dumpPath)
	require.NoError(t, err)
	require.NoError(t, writer.Export(file))
	require.NoError(t, file.Close())
	raw, err := os.ReadFile(dumpPath)
	require.NoError(t, err)

	cases := []struct {
		source string
		open   func() (*db.DB, error)
	}{
		{"path", func() (*db.DB, error) { return db.Open(dumpPath, db.WithBackend("badger")) }},
		{"open file", func() (*db.DB, error) {
			f, err := os.Open(dumpPath)
			require.NoError(t, err)
			defer f.Close()
			return db.Open(f, db.WithBackend("badger"))
		}},
		{"bytes", func() (*db.DB, error) { return db.Open(raw, db.WithBackend("badger")) }},
		{"reader", func() (*db.DB, error) { return db.Open(bytes.NewReader(raw), db.WithBackend("badger")) }},
	}
	for _, c := range cases {
		t.Run(c.source, func(t *testing.T) {
			scratch := t.TempDir()
			t.Setenv("TMPDIR", scratch)

			d, openErr := c.open()
			if openErr == nil {
				_ = d.Close()
				t.Fatalf("db.Open of the dump holding %v answered a database; want the reader's refusal", written)
			}

			entries, err := os.ReadDir(scratch)
			require.NoError(t, err)
			left := []string{}
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), "janus-dump-") {
					left = append(left, entry.Name())
				}
			}
			require.Equal(t, []string{}, left,
				"db.Open of the dump holding %v failed (%v) and left its scratch directory behind", written, openErr)
		})
	}
}
