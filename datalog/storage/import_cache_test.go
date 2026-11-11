package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// importEntryPoints are the ways a dump enters a database: Import reads EDN,
// ImportDump reads either format from the dump's own bytes, and ImportBinary
// reads JDZL. binary is the format the dump is written in.
var importEntryPoints = []struct {
	name   string
	binary bool
	load   func(d *Database, dump []byte) error
}{
	{"Import", false, func(d *Database, dump []byte) error { return d.Import(bytes.NewReader(dump)) }},
	{"ImportDump/edn", false, func(d *Database, dump []byte) error { return d.ImportDump(bytes.NewReader(dump)) }},
	{"ImportDump/jdzl", true, func(d *Database, dump []byte) error { return d.ImportDump(bytes.NewReader(dump)) }},
	{"ImportBinary", true, func(d *Database, dump []byte) error { return d.ImportBinary(bytes.NewReader(dump)) }},
}

// exportDump writes d as EDN, or as JDZL when binary, and returns the bytes.
func exportDump(t *testing.T, d *Database, binary bool) []byte {
	if !binary {
		var buf bytes.Buffer
		require.NoError(t, d.Export(&buf))
		return buf.Bytes()
	}
	path := filepath.Join(t.TempDir(), "dump.jdzl")
	file, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, d.ExportBinary(file))
	require.NoError(t, file.Close())
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return raw
}

// importedItemSchema declares :item/code, a unique cardinality-one string,
// :item/name, a cardinality-one string, and :item/tags, a cardinality-many
// string.
func importedItemSchema() *schema.Schema {
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: datalog.NewKeyword(":item/code"), ValueType: schema.TypeString, Cardinality: schema.CardinalityOne, Unique: schema.UniqueIdentity})
	s.Add(&schema.AttributeDefinition{Ident: datalog.NewKeyword(":item/name"), ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
	s.Add(&schema.AttributeDefinition{Ident: datalog.NewKeyword(":item/tags"), ValueType: schema.TypeString, Cardinality: schema.CardinalityMany})
	return s
}

// An import asserts a dump's datoms into the store, and a read after it
// answers what the dump states. The source writes holder's code, name and a
// tag; the target imports that and reads holder by entity. The source then
// writes a claimant of holder's code, a later name for holder and a second
// tag, and the target imports the source again. Holder's code is the
// claimant's, holder's name is the later one and holder's tags are both,
// through every entry point, with the cache on as with it off.
func TestAReadAfterAnImportAnswersTheImportedFacts(t *testing.T) {
	code := datalog.NewKeyword(":item/code")
	name := datalog.NewKeyword(":item/name")
	tags := datalog.NewKeyword(":item/tags")
	holder := datalog.NewIdentity("item:holder")
	claimant := datalog.NewIdentity("item:claimant")

	for _, entry := range importEntryPoints {
		for _, mode := range legsFor(backendModes) {
			for _, cacheMode := range cacheTestModes {
				t.Run(entry.name+"/"+mode.name+"/"+cacheMode.name, func(t *testing.T) {
					source := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: importedItemSchema()})
					target := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: importedItemSchema(), DisableCache: cacheMode.disableCache})

					tx := source.NewTransaction()
					require.NoError(t, tx.Add(holder, code, "y"))
					require.NoError(t, tx.Add(holder, name, "first"))
					require.NoError(t, tx.Add(holder, tags, "a"))
					_, err := tx.Commit()
					require.NoError(t, err)
					require.NoError(t, entry.load(target, exportDump(t, source, entry.binary)))

					// Read by entity, so the cache holds holder's entries as the
					// first dump stated them.
					var codes, names, held []string
					require.NoError(t, target.QueryInto(&codes, `[:find ?v :in $ ?e :where [?e :item/code ?v]]`, holder))
					require.NoError(t, target.QueryInto(&names, `[:find ?v :in $ ?e :where [?e :item/name ?v]]`, holder))
					require.NoError(t, target.QueryInto(&held, `[:find ?v :in $ ?e :where [?e :item/tags ?v]]`, holder))
					require.Equal(t, []string{"y"}, codes, "holder's code after the first import")
					require.Equal(t, []string{"first"}, names, "holder's name after the first import")
					require.Equal(t, []string{"a"}, held, "holder's tags after the first import")

					tx = source.NewTransaction()
					require.NoError(t, tx.Add(claimant, code, "y"))
					require.NoError(t, tx.Add(holder, name, "second"))
					require.NoError(t, tx.Add(holder, tags, "b"))
					_, err = tx.Commit()
					require.NoError(t, err)
					require.NoError(t, entry.load(target, exportDump(t, source, entry.binary)))

					codes, names, held = nil, nil, nil
					var claimed []string
					require.NoError(t, target.QueryInto(&codes, `[:find ?v :in $ ?e :where [?e :item/code ?v]]`, holder))
					require.NoError(t, target.QueryInto(&claimed, `[:find ?v :in $ ?e :where [?e :item/code ?v]]`, claimant))
					require.NoError(t, target.QueryInto(&names, `[:find ?v :in $ ?e :where [?e :item/name ?v]]`, holder))
					require.NoError(t, target.QueryInto(&held, `[:find ?v :in $ ?e :where [?e :item/tags ?v]]`, holder))
					assert.Empty(t, codes, "holder's code after the second import, which the claimant took")
					assert.Equal(t, []string{"y"}, claimed, "the claimant's code after the second import")
					assert.Equal(t, []string{"second"}, names, "holder's name after the second import")
					assert.ElementsMatch(t, []string{"a", "b"}, held, "holder's tags after the second import")
				})
			}
		}
	}
}
