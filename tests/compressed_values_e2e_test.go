package tests

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// makeTier3Text creates a string that compresses but stays above 60KB compressed.
// Uses pseudo-random printable ASCII so LZ77 finds few long matches.
func makeTier3Text(size int) string {
	data := make([]byte, size)
	state := uint64(99)
	printable := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .,;:!?-_"
	for i := range data {
		state = state*6364136223846793005 + 1442695040888963407
		data[i] = printable[int((state>>33))%len(printable)]
	}
	return string(data)
}

// openCompressedDB opens a compressed-value test database. popts is nil for
// the default planner options, or a mode-specific override from the
// optimizer mode matrix (see docs/wip/OPTIMIZER_MODE_MATRIX.md).
func openCompressedDB(t *testing.T, s schema.SchemaProvider, popts *planner.PlannerOptions) (*storage.Database, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "e2e-compress-*")
	require.NoError(t, err)
	db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
		Path:                 dir,
		Schema:               s,
		CompressionThreshold: 256,
		PlannerOptions:       popts,
	})
	require.NoError(t, err)
	return db, func() { db.Close(); os.RemoveAll(dir) }
}

func longText(prefix string, size int) string {
	base := prefix + " " + strings.Repeat("The quick brown fox jumps over the lazy dog. ", size/45+2)
	if len(base) > size {
		return base[:size]
	}
	return base
}

// TestE2E_CompressedQuery_FindByEntity runs a full Datalog query that reads
// compressed values through the entire pipeline: parser → planner → executor → storage.
func TestE2E_CompressedQuery_FindByEntity(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := openCompressedDB(t, nil, &popts)
			defer cleanup()

			entity := datalog.NewIdentity("alice")
			tx := db.NewTransaction()
			tx.Add(entity, datalog.NewKeyword(":person/name"), "Alice")
			tx.Add(entity, datalog.NewKeyword(":person/bio"), longText("Alice's biography", 800))
			tx.Add(entity, datalog.NewKeyword(":person/age"), int64(30))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Full Datalog query — find bio by entity
			tuples, err := executor.CollectTuples(db.Query(
				`[:find ?bio :in $ ?e :where [?e :person/bio ?bio]]`,
				entity,
			))
			require.NoError(t, err)
			require.Len(t, tuples, 1)
			assert.Equal(t, longText("Alice's biography", 800), tuples[0][0].(string))
		})
	}
}

// TestE2E_CompressedQuery_FindByValue runs an AVET-path query with a compressed
// value as the search term.
func TestE2E_CompressedQuery_FindByValue(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := openCompressedDB(t, nil, &popts)
			defer cleanup()

			bio := longText("unique biography content", 600)
			entity := datalog.NewIdentity("bob")
			tx := db.NewTransaction()
			tx.Add(entity, datalog.NewKeyword(":person/bio"), bio)
			_, err := tx.Commit()
			require.NoError(t, err)

			// Query with value bound — triggers AVET lookup on compressed value
			tuples, err := executor.CollectTuples(db.Query(
				`[:find ?e :in $ ?bio :where [?e :person/bio ?bio]]`,
				bio,
			))
			require.NoError(t, err)
			require.Len(t, tuples, 1)
			assert.Equal(t, entity, tuples[0][0])
		})
	}
}

// TestE2E_CompressedQuery_Join runs a join query where one side has compressed values.
func TestE2E_CompressedQuery_Join(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := openCompressedDB(t, nil, &popts)
			defer cleanup()

			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")

			tx := db.NewTransaction()
			tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice")
			tx.Add(alice, datalog.NewKeyword(":person/bio"), longText("Alice is a software engineer", 500))
			tx.Add(bob, datalog.NewKeyword(":person/name"), "Bob")
			tx.Add(bob, datalog.NewKeyword(":person/bio"), longText("Bob is a data scientist", 500))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Join: find names and bios
			tuples, err := executor.CollectTuples(db.Query(
				`[:find ?name ?bio :where [?e :person/name ?name] [?e :person/bio ?bio]]`,
			))
			require.NoError(t, err)
			assert.Len(t, tuples, 2, "should find both people")

			// Verify both names and bios are present
			names := map[string]string{}
			for _, tuple := range tuples {
				name := tuple[0].(string)
				bio := tuple[1].(string)
				names[name] = bio
			}
			assert.Contains(t, names, "Alice")
			assert.Contains(t, names, "Bob")
			assert.True(t, strings.HasPrefix(names["Alice"], "Alice is a software engineer"))
			assert.True(t, strings.HasPrefix(names["Bob"], "Bob is a data scientist"))
		})
	}
}

// TestE2E_CompressedQuery_CRDT_LWW verifies cardinality-one LWW through
// the full query pipeline with compressed values.
func TestE2E_CompressedQuery_CRDT_LWW(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":doc/content"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})

			popts := mode.plannerOptions()
			db, cleanup := openCompressedDB(t, s, &popts)
			defer cleanup()

			entity := datalog.NewIdentity("doc1")

			// Write version 1
			tx1 := db.NewTransaction()
			tx1.Set(entity, datalog.NewKeyword(":doc/content"), longText("first draft", 500))
			_, err := tx1.Commit()
			require.NoError(t, err)

			// Write version 2
			tx2 := db.NewTransaction()
			tx2.Set(entity, datalog.NewKeyword(":doc/content"), longText("final version", 500))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Query should return only the latest
			tuples, err := executor.CollectTuples(db.Query(
				`[:find ?content :in $ ?e :where [?e :doc/content ?content]]`,
				entity,
			))
			require.NoError(t, err)
			require.Len(t, tuples, 1)
			assert.True(t, strings.HasPrefix(tuples[0][0].(string), "final version"))
		})
	}
}

// TestE2E_CompressedQuery_StrStartsWith verifies str/starts-with? predicate
// works on compressed values (evaluates at Relation layer after decompression).
func TestE2E_CompressedQuery_StrStartsWith(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := openCompressedDB(t, nil, &popts)
			defer cleanup()

			tx := db.NewTransaction()
			for i := 0; i < 5; i++ {
				e := datalog.NewIdentity(strings.Repeat("a", i+1))
				tx.Add(e, datalog.NewKeyword(":doc/content"), longText("Chapter 1: Introduction", 400+i*10))
			}
			for i := 0; i < 5; i++ {
				e := datalog.NewIdentity(strings.Repeat("b", i+1))
				tx.Add(e, datalog.NewKeyword(":doc/content"), longText("Chapter 2: Methods", 400+i*10))
			}
			_, err := tx.Commit()
			require.NoError(t, err)

			// Query with str/starts-with? on compressed values
			tuples, err := executor.CollectTuples(db.Query(
				`[:find ?e :where [?e :doc/content ?c] [(str/starts-with? ?c "Chapter 1")]]`,
			))
			require.NoError(t, err)
			assert.Len(t, tuples, 5, "should find 5 entities with Chapter 1 content")
		})
	}
}

// TestE2E_CompressedExportImportQuery writes data, exports with #lzj,
// imports into a fresh DB, and runs full Datalog queries on the imported data.
func TestE2E_CompressedExportImportQuery(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db1, cleanup1 := openCompressedDB(t, nil, &popts)
			defer cleanup1()

			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")

			tx := db1.NewTransaction()
			tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice")
			tx.Add(alice, datalog.NewKeyword(":person/bio"), longText("Alice bio", 600))
			tx.Add(bob, datalog.NewKeyword(":person/name"), "Bob")
			tx.Add(bob, datalog.NewKeyword(":person/bio"), longText("Bob bio", 600))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Export compressed
			var buf bytes.Buffer
			err = db1.Export(&buf, storage.ExportOptions{Compressed: true})
			require.NoError(t, err)
			assert.Contains(t, buf.String(), "#lzj")

			// Import into fresh DB
			dir2, err := os.MkdirTemp("", "e2e-import-*")
			require.NoError(t, err)
			defer os.RemoveAll(dir2)

			db2Popts := mode.plannerOptions()
			db2, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
				Path:                 dir2,
				CompressionThreshold: 256,
				PlannerOptions:       &db2Popts,
			})
			require.NoError(t, err)
			defer db2.Close()

			err = db2.Import(strings.NewReader(buf.String()))
			require.NoError(t, err)

			// Full Datalog query on imported data
			tuples, err := executor.CollectTuples(db2.Query(
				`[:find ?name ?bio :where [?e :person/name ?name] [?e :person/bio ?bio]]`,
			))
			require.NoError(t, err)
			assert.Len(t, tuples, 2)

			names := map[string]bool{}
			for _, tuple := range tuples {
				name := tuple[0].(string)
				bio := tuple[1].(string)
				names[name] = true
				if name == "Alice" {
					assert.True(t, strings.HasPrefix(bio, "Alice bio"))
				} else if name == "Bob" {
					assert.True(t, strings.HasPrefix(bio, "Bob bio"))
				}
			}
			assert.True(t, names["Alice"])
			assert.True(t, names["Bob"])

			// AVET lookup on imported compressed value
			tuples2, err := executor.CollectTuples(db2.Query(
				`[:find ?e :in $ ?bio :where [?e :person/bio ?bio]]`,
				longText("Alice bio", 600),
			))
			require.NoError(t, err)
			require.Len(t, tuples2, 1, "AVET lookup on imported compressed value should work")
		})
	}
}

// TestE2E_Tier3_FullQuery writes a Tier 3 blob value and queries it through
// the full db.Query() pipeline — parser, planner, executor, storage, blob store.
func TestE2E_Tier3_FullQuery(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := openCompressedDB(t, nil, &popts)
			defer cleanup()

			bigValue := makeTier3Text(100000)

			// Verify this actually reaches Tier 3
			vType, _, _ := datalog.EncodeValue(bigValue, 256)
			if vType != datalog.TypeHashedString {
				t.Skipf("value reaches type 0x%02x not Tier 3 (len=%d)", vType, len(bigValue))
			}

			entity := datalog.NewIdentity("tier3-e2e")
			tx := db.NewTransaction()
			tx.Add(entity, datalog.NewKeyword(":doc/content"), bigValue)
			tx.Add(entity, datalog.NewKeyword(":doc/title"), "Big Document")
			_, err := tx.Commit()
			require.NoError(t, err)

			// Query 1: Find Tier 3 value by entity (E+A bound, V unbound)
			tuples, err := executor.CollectTuples(db.Query(
				`[:find ?content :in $ ?e :where [?e :doc/content ?content]]`,
				entity,
			))
			require.NoError(t, err)
			require.Len(t, tuples, 1)
			assert.Equal(t, bigValue, tuples[0][0].(string), "Tier 3 value should round-trip through db.Query()")

			// Query 2: Join Tier 3 value with another attribute
			tuples2, err := executor.CollectTuples(db.Query(
				`[:find ?title ?content :in $ ?e :where [?e :doc/title ?title] [?e :doc/content ?content]]`,
				entity,
			))
			require.NoError(t, err)
			require.Len(t, tuples2, 1)
			assert.Equal(t, "Big Document", tuples2[0][0].(string))
			assert.Equal(t, bigValue, tuples2[0][1].(string))

			// Query 3: AVET lookup with Tier 3 value as input parameter
			tuples3, err := executor.CollectTuples(db.Query(
				`[:find ?e :in $ ?content :where [?e :doc/content ?content]]`,
				bigValue,
			))
			require.NoError(t, err)
			require.Len(t, tuples3, 1, "AVET lookup on Tier 3 blob value should work")
			assert.Equal(t, entity, tuples3[0][0])
		})
	}
}

// TestE2E_CompressedQuery_JoinOnValue joins on a compressed value — the variable
// ?content appears in two patterns and the executor must match on the decompressed value.
func TestE2E_CompressedQuery_JoinOnValue(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := openCompressedDB(t, nil, &popts)
			defer cleanup()

			sharedContent := longText("shared content for join test", 600)
			differentContent := longText("different content not shared", 600)

			alice := datalog.NewIdentity("alice-join")
			bob := datalog.NewIdentity("bob-join")
			carol := datalog.NewIdentity("carol-join")

			tx := db.NewTransaction()
			// Alice and Bob share the same content value
			tx.Add(alice, datalog.NewKeyword(":doc/content"), sharedContent)
			tx.Add(bob, datalog.NewKeyword(":doc/mirror"), sharedContent)
			// Carol has different content
			tx.Add(carol, datalog.NewKeyword(":doc/content"), differentContent)
			_, err := tx.Commit()
			require.NoError(t, err)

			// Join on ?content — should find alice+bob pair, not carol
			tuples, err := executor.CollectTuples(db.Query(
				`[:find ?e1 ?e2 :where [?e1 :doc/content ?c] [?e2 :doc/mirror ?c]]`,
			))
			require.NoError(t, err)
			require.Len(t, tuples, 1, "join on compressed value should find exactly one pair")
			assert.Equal(t, alice, tuples[0][0])
			assert.Equal(t, bob, tuples[0][1])
		})
	}
}

// TestE2E_Tier3_JoinOnValue joins on a Tier 3 blob value through db.Query().
func TestE2E_Tier3_JoinOnValue(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := openCompressedDB(t, nil, &popts)
			defer cleanup()

			bigValue := makeTier3Text(100000)
			vType, _, _ := datalog.EncodeValue(bigValue, 256)
			if vType != datalog.TypeHashedString {
				t.Skipf("value doesn't reach Tier 3")
			}

			e1 := datalog.NewIdentity("tier3-join-1")
			e2 := datalog.NewIdentity("tier3-join-2")

			tx := db.NewTransaction()
			tx.Add(e1, datalog.NewKeyword(":doc/content"), bigValue)
			tx.Add(e2, datalog.NewKeyword(":doc/mirror"), bigValue)
			_, err := tx.Commit()
			require.NoError(t, err)

			// Join on the Tier 3 value
			tuples, err := executor.CollectTuples(db.Query(
				`[:find ?e1 ?e2 :where [?e1 :doc/content ?c] [?e2 :doc/mirror ?c]]`,
			))
			require.NoError(t, err)
			require.Len(t, tuples, 1, "join on Tier 3 blob value should find exactly one pair")
			assert.Equal(t, e1, tuples[0][0])
			assert.Equal(t, e2, tuples[0][1])
		})
	}
}

// TestE2E_Tier3_ExportImportQuery writes a Tier 3 value, exports with #lzj
// (inline in EDN), imports into a fresh DB, and runs full queries.
func TestE2E_Tier3_ExportImportQuery(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db1, cleanup1 := openCompressedDB(t, nil, &popts)
			defer cleanup1()

			bigValue := makeTier3Text(100000)

			vType, _, _ := datalog.EncodeValue(bigValue, 256)
			if vType != datalog.TypeHashedString {
				t.Skipf("value doesn't reach Tier 3")
			}

			entity := datalog.NewIdentity("tier3-export-e2e")
			tx := db1.NewTransaction()
			tx.Add(entity, datalog.NewKeyword(":doc/content"), bigValue)
			tx.Add(entity, datalog.NewKeyword(":doc/title"), "Exported Blob")
			_, err := tx.Commit()
			require.NoError(t, err)

			// Export compressed — Tier 3 value should be inline #lzj
			var buf bytes.Buffer
			err = db1.Export(&buf, storage.ExportOptions{Compressed: true})
			require.NoError(t, err)
			assert.Contains(t, buf.String(), "#lzj")

			// Import into fresh DB
			dir2, err := os.MkdirTemp("", "tier3-e2e-import-*")
			require.NoError(t, err)
			defer os.RemoveAll(dir2)

			db2Popts := mode.plannerOptions()
			db2, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
				Path:                 dir2,
				CompressionThreshold: 256,
				PlannerOptions:       &db2Popts,
			})
			require.NoError(t, err)
			defer db2.Close()

			err = db2.Import(strings.NewReader(buf.String()))
			require.NoError(t, err)

			// Full query on imported Tier 3 data
			tuples, err := executor.CollectTuples(db2.Query(
				`[:find ?title ?content :in $ ?e :where [?e :doc/title ?title] [?e :doc/content ?content]]`,
				entity,
			))
			require.NoError(t, err)
			require.Len(t, tuples, 1)
			assert.Equal(t, "Exported Blob", tuples[0][0].(string))
			assert.Equal(t, bigValue, tuples[0][1].(string),
				"Tier 3 value should survive export→import→query round-trip")
		})
	}
}
