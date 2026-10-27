package db_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

var (
	dumpEntity = datalog.NewIdentity("dump:subject")
	dumpAttr   = datalog.NewKeyword(":dump/name")
)

// writeDump exports a one-datom database to a file named for the format under
// test and returns the path. JDZL export needs an io.WriteSeeker to patch its
// trailer offset, so both formats go through a real file.
func writeDump(t *testing.T, ext string) string {
	t.Helper()
	source, err := db.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	tx := source.NewTransaction()
	require.NoError(t, tx.Set(dumpEntity, dumpAttr, "round-tripped"))
	_, err = tx.Commit()
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "dump"+ext)
	file, err := os.Create(path)
	require.NoError(t, err)
	defer file.Close()

	if ext == ".jdzl" {
		require.NoError(t, source.ExportBinary(file))
		return path
	}
	require.NoError(t, source.Export(file))
	return path
}

func requireDumpLoaded(t *testing.T, d *db.DB) {
	t.Helper()
	var names []string
	require.NoError(t, d.QueryInto(&names, `[:find ?name :where [?e :dump/name ?name]]`))
	require.Equal(t, []string{"round-tripped"}, names)
}

// A path naming an exported dump is a source to load, not a directory to open.
func TestOpenLoadsADumpPath(t *testing.T) {
	for _, ext := range []string{".jdzl", ".edn"} {
		t.Run(ext, func(t *testing.T) {
			d, err := db.Open(writeDump(t, ext))
			require.NoError(t, err)
			t.Cleanup(func() { _ = d.Close() })
			requireDumpLoaded(t, d)
		})
	}
}

// Open takes the open file itself — the embedded-database case. An fs.File out
// of an embed.FS and the *os.File from os.Open are both io.ReadSeekers, and the
// format comes from the dump's own bytes rather than from a name the reader
// does not carry.
func TestOpenTakesAnOpenDumpFile(t *testing.T) {
	for _, ext := range []string{".jdzl", ".edn"} {
		t.Run(ext, func(t *testing.T) {
			file, err := os.Open(writeDump(t, ext))
			require.NoError(t, err)
			t.Cleanup(func() { _ = file.Close() })

			d, err := db.Open(file)
			require.NoError(t, err)
			t.Cleanup(func() { _ = d.Close() })
			requireDumpLoaded(t, d)
		})
	}
}

// go:embed into a []byte is the common spelling of an embedded database, and
// bytes.Reader is the embed.FS file's in-memory equivalent. Neither touches a
// filesystem on the way in.
func TestOpenTakesAnInMemoryDump(t *testing.T) {
	for _, ext := range []string{".jdzl", ".edn"} {
		t.Run(ext, func(t *testing.T) {
			raw, err := os.ReadFile(writeDump(t, ext))
			require.NoError(t, err)

			t.Run("bytes", func(t *testing.T) {
				d, err := db.Open(raw)
				require.NoError(t, err)
				t.Cleanup(func() { _ = d.Close() })
				requireDumpLoaded(t, d)
			})

			t.Run("reader", func(t *testing.T) {
				d, err := db.Open(bytes.NewReader(raw))
				require.NoError(t, err)
				t.Cleanup(func() { _ = d.Close() })
				requireDumpLoaded(t, d)
			})
		})
	}
}

// A dump says where the data comes from, not which store holds it, so it pairs
// with any backend the build has. An in-process one takes it directly.
func TestDumpHonorsAnInProcessBackend(t *testing.T) {
	for _, backend := range storage.AvailableBackends() {
		if backend.Persistent {
			continue
		}
		t.Run(backend.Name, func(t *testing.T) {
			file, err := os.Open(writeDump(t, ".jdzl"))
			require.NoError(t, err)
			t.Cleanup(func() { _ = file.Close() })

			d, err := db.Open(file, db.WithBackend(backend.Name))
			require.NoError(t, err)
			t.Cleanup(func() { _ = d.Close() })
			require.IsType(t, storeOfKind(t, backend), d.Store())
			requireDumpLoaded(t, d)
		})
	}
}

// A persistent backend needs a directory and a dump brings none, so Open makes
// a temporary one and the database owns it: Close takes the directory with it.
func TestDumpIntoAPersistentBackendUsesATempDir(t *testing.T) {
	for _, backend := range storage.AvailableBackends() {
		if !backend.Persistent {
			continue
		}
		t.Run(backend.Name, func(t *testing.T) {
			file, err := os.Open(writeDump(t, ".jdzl"))
			require.NoError(t, err)
			t.Cleanup(func() { _ = file.Close() })

			d, err := db.Open(file, db.WithBackend(backend.Name))
			require.NoError(t, err)
			require.IsType(t, storeOfKind(t, backend), d.Store())
			requireDumpLoaded(t, d)
			require.NoError(t, d.Close())
		})
	}
}

// The accepted set is closed and says so, rather than treating an unrecognized
// source as an empty path.
func TestOpenRejectsAnUnsupportedSource(t *testing.T) {
	for _, source := range []interface{}{nil, 42, struct{}{}} {
		_, err := db.Open(source)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot open")
	}
}

func TestIsDumpPath(t *testing.T) {
	require.True(t, storage.IsDumpPath("snapshot.jdzl"))
	require.True(t, storage.IsDumpPath("/var/tmp/snapshot.edn"))
	require.False(t, storage.IsDumpPath("/var/lib/janus.db"))
	require.False(t, storage.IsDumpPath(""))
}

var dumpTags = datalog.NewKeyword(":dump/tags")

// declaredDumpSchema declares the set a dump's writer wrote, with the doc
// string given.
func declaredDumpSchema(t *testing.T, doc string) *schema.Schema {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(":dump/tags").Type(schema.TypeString).Many().Doc(doc).Add().
		Build()
	require.NoError(t, err)
	return s
}

// writeDeclaredDump exports a database that declared :dump/tags as a set and
// wrote three members of it, to a file named for the format under test.
func writeDeclaredDump(t *testing.T, ext string) string {
	t.Helper()
	source, err := db.OpenMemory(db.WithSchema(declaredDumpSchema(t, "the subject's tags")))
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	tx := source.NewTransaction()
	for _, member := range []string{"a", "b", "c"} {
		require.NoError(t, tx.Add(dumpEntity, dumpTags, member))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "declared"+ext)
	file, err := os.Create(path)
	require.NoError(t, err)
	defer file.Close()

	if ext == ".jdzl" {
		require.NoError(t, source.ExportBinary(file))
		return path
	}
	require.NoError(t, source.Export(file))
	return path
}

// A dump carries its writer's definitions, so a database loaded from one with
// no schema of its own resolves a set as the set its writer declared.
func TestOpenDumpResolvesDeclaredSets(t *testing.T) {
	for _, ext := range []string{".jdzl", ".edn"} {
		for _, mode := range backendModes {
			t.Run(ext+"/"+mode.name, func(t *testing.T) {
				d, err := db.Open(writeDeclaredDump(t, ext),
					db.WithBackend(mode.backend.Name), db.WithPlannerOptions(mode.plannerOptions()))
				require.NoError(t, err)
				t.Cleanup(func() { _ = d.Close() })

				var members []string
				require.NoError(t, d.QueryInto(&members, `[:find ?t :where [?e :dump/tags ?t]]`))
				require.ElementsMatch(t, []string{"a", "b", "c"}, members)

				def := d.Schema().GetAttribute(dumpTags)
				require.NotNil(t, def, "the dump's definition of :dump/tags is read at load")
				require.Same(t, schema.CardinalityMany, def.Cardinality)
				require.Equal(t, "the subject's tags", def.Doc)
			})
		}
	}
}

// A schema given at open supersedes the dump's definitions for the idents it
// names, so the doc the given schema states is the one the store carries.
func TestOpenDumpWithSchemaSupersedesDumpFacts(t *testing.T) {
	for _, ext := range []string{".jdzl", ".edn"} {
		for _, mode := range backendModes {
			t.Run(ext+"/"+mode.name, func(t *testing.T) {
				d, err := db.Open(writeDeclaredDump(t, ext),
					db.WithSchema(declaredDumpSchema(t, "given")),
					db.WithBackend(mode.backend.Name), db.WithPlannerOptions(mode.plannerOptions()))
				require.NoError(t, err)
				t.Cleanup(func() { _ = d.Close() })

				var docs []string
				require.NoError(t, d.QueryInto(&docs,
					`[:find ?doc :where [?a :db.attribute/declaration ?d] [?d :db/ident :dump/tags] [?d :db/doc ?doc]]`))
				require.Equal(t, []string{"given"}, docs)
				require.Equal(t, "given", d.Schema().GetAttribute(dumpTags).Doc)

				var members []string
				require.NoError(t, d.QueryInto(&members, `[:find ?t :where [?e :dump/tags ?t]]`))
				require.ElementsMatch(t, []string{"a", "b", "c"}, members)
			})
		}
	}
}

// exportDump exports d in the format named and returns the bytes. Both
// formats go through a real file, as writeDump's do.
func exportDump(t *testing.T, d *db.DB, ext string) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "export"+ext)
	file, err := os.Create(path)
	require.NoError(t, err)
	if ext == ".jdzl" {
		require.NoError(t, d.ExportBinary(file))
	} else {
		require.NoError(t, d.Export(file))
	}
	require.NoError(t, file.Close())
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return raw
}

// A dump carries its writer's declarations, so a database opened from it with
// the writer's own schema declares nothing: the store holds the dump's one
// declaration of each attribute, and an export of it is the dump it loaded,
// byte for byte.
func TestOpenDumpWithTheWritersSchemaDeclaresNothing(t *testing.T) {
	for _, ext := range []string{".jdzl", ".edn"} {
		for _, mode := range backendModes {
			t.Run(ext+"/"+mode.name, func(t *testing.T) {
				path := writeDeclaredDump(t, ext)
				d, err := db.Open(path,
					db.WithSchema(declaredDumpSchema(t, "the subject's tags")),
					db.WithBackend(mode.backend.Name), db.WithPlannerOptions(mode.plannerOptions()))
				require.NoError(t, err)
				t.Cleanup(func() { _ = d.Close() })

				rel, err := d.History().Query(`[:find ?tx :where [?d :db/ident :dump/tags ?tx]]`)
				require.NoError(t, err)
				iter := rel.Iterator()
				defer iter.Close()
				declarations := 0
				for iter.Next() {
					declarations++
				}
				require.Equal(t, 1, declarations, "the store holds the dump's declaration of :dump/tags and no other")

				dump, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, dump, exportDump(t, d, ext), "a load-and-export round trip is byte-stable")
			})
		}
	}
}
