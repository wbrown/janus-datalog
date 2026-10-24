package storage

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// BenchmarkOpenWithUnchangedSchema measures a reopen per backend: with the
// schema the store already carries, where the definitions are read back,
// compared and nothing written, and with no schema, where they are read back
// and the undeclared attributes inferred. The difference is the writer's
// adoption diff.
func BenchmarkOpenWithUnchangedSchema(b *testing.B) {
	for _, rc := range reopenBackendCases() {
		s := schemaFactsFixture(b)
		first := rc.open(b, DatabaseOptions{Schema: s})
		writeBenchmarkEntities(b, first, 100)

		b.Run(rc.name+"/same-schema", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				rc.open(b, DatabaseOptions{Schema: s})
			}
		})
		b.Run(rc.name+"/no-schema", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				rc.open(b, DatabaseOptions{})
			}
		})
	}
}

// BenchmarkImportInferenceAfterLoad measures an EDN import into a fresh
// in-process database declaring nothing, which ends by reading the
// definitions the dump carries and inferring the rest, and the install step
// alone on a database that already holds the import.
func BenchmarkImportInferenceAfterLoad(b *testing.B) {
	source := openContractDatabase(b, inProcessStores()[0], DatabaseOptions{Schema: schemaFactsFixture(b)})
	writeBenchmarkEntities(b, source, 1000)
	var dump bytes.Buffer
	if err := source.Export(&dump); err != nil {
		b.Fatal(err)
	}

	for _, backend := range inProcessStores() {
		b.Run(backend.name+"/import", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				target := openContractDatabase(b, backend, DatabaseOptions{})
				if err := target.Import(bytes.NewReader(dump.Bytes())); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(backend.name+"/install", func(b *testing.B) {
			target := openContractDatabase(b, backend, DatabaseOptions{})
			if err := target.Import(bytes.NewReader(dump.Bytes())); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := target.installSchema(true); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// writeBenchmarkEntities commits n entities each carrying a declared string,
// a declared set member and an undeclared value, so a reopen and an import
// have definitions to read and ops to infer from.
func writeBenchmarkEntities(b *testing.B, d *Database, n int) {
	tx := d.NewTransaction()
	for i := 0; i < n; i++ {
		entity := datalog.NewIdentity(fmt.Sprintf("holder-%d", i))
		if err := tx.Add(entity, kw(":t/string"), fmt.Sprintf("held-%d", i)); err != nil {
			b.Fatal(err)
		}
		if err := tx.Add(entity, kw(":t/many"), fmt.Sprintf("member-%d", i)); err != nil {
			b.Fatal(err)
		}
		if err := tx.Add(entity, kw(":t/loose"), int64(i)); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
}
