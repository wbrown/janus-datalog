package db_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A dump opened into a persistent backend lands in scratch space db.Open makes,
// and the directory lives as long as the database holding it. An open that
// answers a database answers one whose Close removes the directory. An open
// that answers an error answers no directory, whichever step refused: the
// dump's load, the schema's install after a load, or a refusal before the
// store opens.
func TestAnOpenThatAnswersNoDatabaseLeavesNoScratchDirectory(t *testing.T) {
	loads := writeDump(t, ".edn")
	unreadable := writeUnreadableDump(t)
	engineWord := schema.NewBuilder().Attribute(":db/doc").Type(schema.TypeString).Add().MustBuild()
	refusals := []struct {
		name string
		open func() (*db.DB, error)
	}{
		{"the dump does not load", func() (*db.DB, error) {
			return db.Open(unreadable, db.WithBackend("badger"))
		}},
		{"the dump loads and the install refuses", func() (*db.DB, error) {
			return db.Open(loads, db.WithBackend("badger"), db.WithRefuseUnknownAttributes())
		}},
		{"the schema is refused before the store opens", func() (*db.DB, error) {
			return db.Open(loads, db.WithBackend("badger"), db.WithSchema(engineWord))
		}},
	}
	for _, c := range refusals {
		t.Run(c.name, func(t *testing.T) {
			scratch := t.TempDir()
			t.Setenv("TMPDIR", scratch)

			d, err := c.open()
			if err == nil {
				_ = d.Close()
				t.Fatal("db.Open answered a database; the case wants its refusal")
			}
			require.Empty(t, scratchDirectoriesIn(t, scratch),
				"db.Open refused (%v) and left its scratch directory behind", err)
		})
	}
	t.Run("the open answers a database", func(t *testing.T) {
		scratch := t.TempDir()
		t.Setenv("TMPDIR", scratch)

		d, err := db.Open(loads, db.WithBackend("badger"))
		require.NoError(t, err)
		require.Len(t, scratchDirectoriesIn(t, scratch), 1, "the open database holds its scratch directory")
		require.NoError(t, d.Close())
		require.Empty(t, scratchDirectoriesIn(t, scratch), "Close removes the scratch directory")
	})
}

// writeUnreadableDump exports a database holding a string longer than the EDN
// reader's one-line buffer, which the reader refuses, and returns the path.
func writeUnreadableDump(t *testing.T) string {
	t.Helper()
	source, err := db.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	tx := source.NewTransaction()
	require.NoError(t, tx.Set(datalog.NewIdentity("dump:chapter"), datalog.NewKeyword(":chapter/body"),
		strings.Repeat("a long line of prose. ", 80000)))
	_, err = tx.Commit()
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "chapter.edn")
	file, err := os.Create(path)
	require.NoError(t, err)
	defer file.Close()
	require.NoError(t, source.Export(file))
	return path
}

// scratchDirectoriesIn lists the janus-dump-* entries of dir, the scratch
// directories db.Open makes.
func scratchDirectoriesIn(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var found []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "janus-dump-") {
			found = append(found, entry.Name())
		}
	}
	return found
}
