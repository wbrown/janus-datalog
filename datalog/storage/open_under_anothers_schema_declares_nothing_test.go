package storage

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// openedUnderTheSchemaOf opens a database on the mode's backend under the
// schema other declares
// (A_DATABASE_OPENED_UNDER_ANOTHERS_SCHEMA_DECLARES_NOTHING.md).
func openedUnderTheSchemaOf(t *testing.T, mode optimizerMode, other *Database) *Database {
	t.Helper()
	return createOptimizerModeDB(t, mode, DatabaseOptions{SchemaOf: other})
}

// datomsTheStoreHolds counts every datom of a store, by a scan of EAVT.
func datomsTheStoreHolds(t *testing.T, store Store) int {
	t.Helper()
	scan, err := store.Scan(ScanBound{Index: EAVT})
	require.NoError(t, err)
	held := 0
	for scan.Next() {
		held++
	}
	require.NoError(t, scan.Error())
	require.NoError(t, scan.Close())
	return held
}

// lentSchema is the schema the lending database of these tests declares: one
// attribute of each cardinality, each typed.
func lentSchema(t *testing.T) *schema.Schema {
	t.Helper()
	declared, err := schema.NewBuilder().
		Attribute(":lend/name").Type(schema.TypeString).Add().
		Attribute(":lend/tags").Type(schema.TypeString).Many().Add().
		Attribute(":lend/steps").Type(schema.TypeLong).Vector().Add().
		Build()
	require.NoError(t, err)
	return declared
}

var (
	lendName  = datalog.NewKeyword(":lend/name")
	lendTags  = datalog.NewKeyword(":lend/tags")
	lendSteps = datalog.NewKeyword(":lend/steps")
	lendLate  = datalog.NewKeyword(":lend/late")
	lendLoose = datalog.NewKeyword(":lend/loose")
)

// requireLentDefinitions checks that running answers each definition of
// lentSchema, :lend/name under the cardinality given.
func requireLentDefinitions(t *testing.T, running schema.SchemaProvider, name datalog.Keyword) {
	t.Helper()
	for attribute, cardinality := range map[datalog.Keyword]datalog.Keyword{
		lendName: name, lendTags: schema.CardinalityMany, lendSteps: schema.CardinalityVector,
	} {
		definition := running.GetAttribute(attribute)
		require.NotNil(t, definition, "%s", attribute)
		require.Same(t, cardinality, definition.Cardinality, "%s", attribute)
	}
}

// writesUnderLentDefinitions writes to d under each definition of lentSchema
// and reads what it wrote: two Adds at the set attribute are a set of two, two
// at the vector attribute a vector of two.
func writesUnderLentDefinitions(t *testing.T, d *Database, entity datalog.Identity) {
	t.Helper()
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(entity, lendTags, "a"))
	require.NoError(t, tx.Add(entity, lendTags, "b"))
	require.NoError(t, tx.Add(entity, lendSteps, int64(7)))
	require.NoError(t, tx.Add(entity, lendSteps, int64(7)))
	_, err := tx.Commit()
	require.NoError(t, err)
	requireLentValues(t, d, entity)
}

// requireLentValues reads what writesUnderLentDefinitions wrote on entity.
func requireLentValues(t *testing.T, d *Database, entity datalog.Identity) {
	t.Helper()
	members, err := d.GetStrings(entity, lendTags)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b"}, members, "two Adds at a set attribute are a set of two")
	length, err := d.GetVectorLength(entity, lendSteps)
	require.NoError(t, err)
	require.Equal(t, int64(2), length, "two Adds at a vector attribute are a vector of two")
}

// requireNoDeclarationStored checks that d's own store states no definition.
func requireNoDeclarationStored(t *testing.T, d *Database) {
	t.Helper()
	stored, err := readSchemaFacts(d)
	require.NoError(t, err)
	require.Empty(t, stored.Attributes(), "the store states no definition of its own")
}

// A database opened under another's schema runs with the definitions the other
// declares and writes none of them into its own store: the store holds no
// datom after the open. Its schema answers each declared definition, a
// transaction writes under them, an attribute the other carries datoms at and
// never declared has no definition in it, and a declaration the other makes
// after the open does not reach it.
func TestADatabaseOpenedUnderAnothersSchemaDeclaresNothing(t *testing.T) {
	declared := lentSchema(t)
	widened, err := schema.NewBuilder().
		Attribute(":lend/name").Type(schema.TypeString).Add().
		Attribute(":lend/tags").Type(schema.TypeString).Many().Add().
		Attribute(":lend/steps").Type(schema.TypeLong).Vector().Add().
		Attribute(":lend/late").Type(schema.TypeLong).Many().Add().
		Build()
	require.NoError(t, err)
	entity := datalog.NewIdentity("lend:e")
	for _, mode := range legsFor(backendModes) {
		t.Run(mode.name, func(t *testing.T) {
			other := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: declared})
			undeclared := other.NewTransaction()
			require.NoError(t, undeclared.Add(entity, lendLoose, "written and never declared"))
			_, err := undeclared.Commit()
			require.NoError(t, err)
			// An install composes the effective schema from the store's ops:
			// the one after the write is the one that infers the attribute.
			require.NoError(t, other.SetSchema(declared))
			require.NotNil(t, other.Schema().GetAttribute(lendLoose), "the other database infers a definition of the attribute it never declared")

			opened := openedUnderTheSchemaOf(t, mode, other)
			assert.Zero(t, datomsTheStoreHolds(t, opened.store), "the open declares nothing: its store holds no datom")

			requireLentDefinitions(t, opened.Schema(), schema.CardinalityOne)
			require.Nil(t, opened.Schema().GetAttribute(lendLoose), "a definition the other's ops infer is the other's alone")

			refused := opened.NewTransaction()
			require.Error(t, refused.Add(entity, lendName, int64(1)), "a value of another type than the declared one is refused")
			require.NoError(t, refused.Rollback())
			writesUnderLentDefinitions(t, opened, entity)
			requireNoDeclarationStored(t, opened)

			require.NoError(t, other.SetSchema(widened))
			require.NotNil(t, other.Schema().GetAttribute(lendLate))
			require.Nil(t, opened.Schema().GetAttribute(lendLate), "a declaration the other makes after the open does not reach it")
		})
	}
}

// The other database may be closed before the one opened under its schema is
// used: the definitions were taken at the open.
func TestADatabaseOpenedUnderAnothersSchemaOutlivesTheOther(t *testing.T) {
	for _, mode := range legsFor(backendModes) {
		t.Run(mode.name, func(t *testing.T) {
			other, err := NewDatabaseWithOptions(backendOptions(t, mode.backend, DatabaseOptions{Schema: lentSchema(t)}))
			require.NoError(t, err)
			opened := openedUnderTheSchemaOf(t, mode, other)
			require.NoError(t, other.Close())

			requireLentDefinitions(t, opened.Schema(), schema.CardinalityOne)
			writesUnderLentDefinitions(t, opened, datalog.NewIdentity("lend:e"))
		})
	}
}

// The other's schema is the only schema of the open: a schema given beside it
// is refused. Under it a refusing database admits the attributes the other
// declares and refuses any other.
func TestADatabaseOpenedUnderAnothersSchemaTakesNoSchemaBesideIt(t *testing.T) {
	for _, mode := range legsFor(backendModes) {
		t.Run(mode.name, func(t *testing.T) {
			other := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: lentSchema(t)})
			_, err := NewDatabaseWithOptions(backendOptions(t, mode.backend, DatabaseOptions{SchemaOf: other, Schema: lentSchema(t)}))
			require.Error(t, err, "a schema given beside another database's")

			refusing := createOptimizerModeDB(t, mode, DatabaseOptions{SchemaOf: other, RefuseUnknownAttributes: true})
			_, err = refusing.Query(`[:find ?v :where [?e :lend/name ?v]]`)
			require.NoError(t, err, "an attribute the other declares")
			_, err = refusing.Query(`[:find ?v :where [?e :lend/unknown ?v]]`)
			requireUnknown(t, err, kw(":lend/unknown"))
			require.NoError(t, refusing.SetSchema(nil), "the definitions the open took are still there to check against")
			_, err = refusing.Query(`[:find ?v :where [?e :lend/name ?v]]`)
			require.NoError(t, err)
		})
	}
}

// dumpFormats are the two dumps a database writes, each as a function that
// writes d's and hands back the dump to load.
var dumpFormats = []struct {
	name string
	dump func(t *testing.T, d *Database) io.ReadSeeker
}{
	{"edn", func(t *testing.T, d *Database) io.ReadSeeker {
		var written bytes.Buffer
		require.NoError(t, d.Export(&written))
		return bytes.NewReader(written.Bytes())
	}},
	{"binary", func(t *testing.T, d *Database) io.ReadSeeker {
		file, err := os.Create(filepath.Join(t.TempDir(), "dump.jdzl"))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, file.Close()) })
		require.NoError(t, d.ExportBinary(file))
		_, err = file.Seek(0, io.SeekStart)
		require.NoError(t, err)
		return file
	}},
}

// A dump of a database opened under another's schema describes itself: loaded
// into a database opened under no schema, it runs with the definitions the
// first ran under and holds what the first wrote. A definition the database
// declared itself after its open supersedes the one it took for that ident,
// in the database and in its dump, and a SetSchema(nil) withdraws neither.
func TestADumpOfADatabaseOpenedUnderAnothersSchemaDescribesItself(t *testing.T) {
	own, err := schema.NewBuilder().
		Attribute(":lend/name").Type(schema.TypeString).Many().Add().
		Attribute(":lend/late").Type(schema.TypeLong).Many().Add().
		Build()
	require.NoError(t, err)
	entity := datalog.NewIdentity("lend:e")
	for _, mode := range legsFor(backendModes) {
		for _, format := range dumpFormats {
			t.Run(mode.name+"/"+format.name, func(t *testing.T) {
				other := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: lentSchema(t)})
				opened := openedUnderTheSchemaOf(t, mode, other)
				writesUnderLentDefinitions(t, opened, entity)

				restored := createOptimizerModeDB(t, mode, DatabaseOptions{Dump: format.dump(t, opened)})
				requireLentDefinitions(t, restored.Schema(), schema.CardinalityOne)
				requireLentValues(t, restored, entity)

				require.NoError(t, opened.SetSchema(own))
				requireLentDefinitions(t, opened.Schema(), schema.CardinalityMany)
				require.NotNil(t, opened.Schema().GetAttribute(lendLate))
				require.Same(t, schema.CardinalityOne, other.Schema().GetAttribute(lendName).Cardinality, "the other database's definition is its own")

				superseded := createOptimizerModeDB(t, mode, DatabaseOptions{Dump: format.dump(t, opened)})
				requireLentDefinitions(t, superseded.Schema(), schema.CardinalityMany)
				require.NotNil(t, superseded.Schema().GetAttribute(lendLate))
				requireLentValues(t, superseded, entity)

				require.NoError(t, opened.SetSchema(nil))
				requireLentDefinitions(t, opened.Schema(), schema.CardinalityMany)
			})
		}
	}
}

// A dump imported into a database opened under another's schema states its
// definitions in the database's store, and they supersede the ones the open
// took for the idents they name.
func TestADumpImportedIntoADatabaseOpenedUnderAnothersSchemaSupersedesIt(t *testing.T) {
	declares, err := schema.NewBuilder().
		Attribute(":lend/name").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	for _, mode := range legsFor(backendModes) {
		t.Run(mode.name, func(t *testing.T) {
			other := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: lentSchema(t)})
			opened := openedUnderTheSchemaOf(t, mode, other)
			declaring := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: declares})

			var dump bytes.Buffer
			require.NoError(t, declaring.Export(&dump))
			require.NoError(t, opened.Import(&dump))
			requireLentDefinitions(t, opened.Schema(), schema.CardinalityMany)
		})
	}
}

// A branch forked from a database opened under another's schema runs under the
// same definitions, and its store states none of them either.
func TestABranchOfADatabaseOpenedUnderAnothersSchemaDeclaresNothing(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	entity := datalog.NewIdentity("lend:e")
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			other := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: lentSchema(t)})
			opened := openedUnderTheSchemaOf(t, mode, other)
			writesUnderLentDefinitions(t, opened, entity)
			_, err := opened.Snapshot("s")
			require.NoError(t, err)

			branch := branchOf(t, opened, "s")
			requireLentDefinitions(t, branch.Schema(), schema.CardinalityOne)
			requireLentValues(t, branch, entity)
			requireNoDeclarationStored(t, branch)
			writesUnderLentDefinitions(t, branch, datalog.NewIdentity("lend:branch"))
		})
	}
}

// An open under another's schema costs what an open under no schema costs,
// where an open under the same schema given declares each attribute.
//
//	go test -run '^$' -bench OpenUnderAnothersSchema ./datalog/storage
func BenchmarkOpenUnderAnothersSchema(b *testing.B) {
	const attributes = 256
	builder := schema.NewBuilder()
	for i := 0; i < attributes; i++ {
		builder = builder.Attribute(fmt.Sprintf(":lend/attribute%d", i)).Type(schema.TypeLong).Add()
	}
	given, err := builder.Build()
	require.NoError(b, err)
	backend, err := BackendNamed("memory-trees")
	require.NoError(b, err)
	other, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: backend.Name, Schema: given})
	require.NoError(b, err)
	defer other.Close()
	for _, c := range []struct {
		name string
		opts DatabaseOptions
	}{
		{"under no schema", DatabaseOptions{BackendName: backend.Name}},
		{"under the schema given", DatabaseOptions{BackendName: backend.Name, Schema: given}},
		{"under another's schema", DatabaseOptions{BackendName: backend.Name, SchemaOf: other}},
	} {
		b.Run(c.name, func(b *testing.B) {
			for b.Loop() {
				opened, err := NewDatabaseWithOptions(c.opts)
				if err != nil {
					b.Fatal(err)
				}
				if err := opened.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
