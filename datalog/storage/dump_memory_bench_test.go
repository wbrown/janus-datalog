package storage

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"testing"
)

// dumpPathEnv names a JDZL or EDN dump to load. The benchmarks skip when it is
// unset, so the corpus stays outside the repository and the gate stays
// self-contained.
const dumpPathEnv = "JANUS_DUMP_PATH"

const bytesPerMB = 1 << 20

// BenchmarkDumpLoadMemory reports what holding a dump costs each backend.
//
// Three numbers, because they answer different questions.
//
// sys_total_mb is MemStats.Sys, the memory the whole process has obtained. Sys
// counts address space taken from the OS and never decreases — releasing pages
// does not lower it — so in a run with more than one backend this carries the
// high-water mark of every subtest before it, not this one's. It is the browser
// figure only when the backend runs alone, which under js/wasm it does.
//
// sys_growth_mb is what this load made the process obtain, so it is the
// backend's own cost wherever it runs in the order.
//
// retained_mb is the live heap after two collections: the store itself, once
// the import's garbage is gone. The gap to growth is the import's transient
// cost.
//
// -benchmem already reports cumulative allocation as B/op, so this adds no
// third metric for it. Intended at -benchtime=1x: loading the same dump N times
// measures nothing new.
//
//	JANUS_DUMP_PATH=/path/to/dump.jdzl go test -bench=DumpLoadMemory -benchmem \
//	    -benchtime=1x ./datalog/storage
func BenchmarkDumpLoadMemory(b *testing.B) {
	dumpPath := dumpPathOrSkip(b)

	for _, backend := range AvailableBackends() {
		b.Run(backend.Name, func(b *testing.B) {
			var m importMemory
			for i := 0; i < b.N; i++ {
				m = measureImport(b, backend, dumpPath,
					func(db *Database, r io.ReadSeeker) error { return db.ImportDump(r) })
			}
			m.report(b)
		})
	}
}

// BenchmarkDumpImportWorkers sweeps the one concurrency knob the import has.
//
// Only the chunk decode runs in parallel: the payload reads serialize on the
// shared ReadSeeker and the asserts serialize on the store's write lock, so the
// workers spend their time holding decoded chunks while they queue. That makes
// worker count a peak-memory dial with an unmeasured throughput return, which is
// what this reports.
//
//	JANUS_DUMP_PATH=/path/to/dump.jdzl go test -bench=DumpImportWorkers -benchmem \
//	    -benchtime=1x ./datalog/storage
func BenchmarkDumpImportWorkers(b *testing.B) {
	dumpPath := dumpPathOrSkip(b)
	skipUnlessJDZL(b, dumpPath)

	for _, backend := range AvailableBackends() {
		b.Run(backend.Name, func(b *testing.B) {
			for _, workers := range []int{1, 2, 4, runtime.GOMAXPROCS(0)} {
				b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
					var m importMemory
					for i := 0; i < b.N; i++ {
						m = measureImport(b, backend, dumpPath,
							func(db *Database, r io.ReadSeeker) error {
								return db.ImportBinary(r, BinaryImportOptions{Workers: workers})
							})
					}
					m.report(b)
				})
			}
		})
	}
}

func dumpPathOrSkip(b *testing.B) string {
	b.Helper()
	dumpPath := os.Getenv(dumpPathEnv)
	if dumpPath == "" {
		b.Skipf("%s unset: set it to a dump to measure backend memory", dumpPathEnv)
	}
	return dumpPath
}

// skipUnlessJDZL guards a benchmark that calls ImportBinary directly, which an
// EDN dump has no header for.
func skipUnlessJDZL(b *testing.B, dumpPath string) {
	b.Helper()
	file, err := os.Open(dumpPath)
	if err != nil {
		b.Fatalf("open dump: %v", err)
	}
	defer file.Close()

	var magic [len(binaryExportMagic)]byte
	if _, err := io.ReadFull(file, magic[:]); err != nil || string(magic[:]) != binaryExportMagic {
		b.Skipf("%s is not a JDZL dump; the worker sweep has no EDN equivalent", dumpPath)
	}
}

// importMemory is what one load cost.
type importMemory struct {
	sysTotal  uint64
	sysGrowth uint64
	retained  uint64
}

func (m importMemory) report(b *testing.B) {
	b.Helper()
	b.ReportMetric(float64(m.sysTotal)/bytesPerMB, "sys_total_mb")
	b.ReportMetric(float64(m.sysGrowth)/bytesPerMB, "sys_growth_mb")
	b.ReportMetric(float64(m.retained)/bytesPerMB, "retained_mb")
}

// measureImport loads dumpPath into a fresh database on backend through load,
// and returns what that cost.
//
// The dump is read through the file rather than a []byte so the source does not
// sit in the heap being measured.
func measureImport(
	b *testing.B,
	backend Backend,
	dumpPath string,
	load func(*Database, io.ReadSeeker) error,
) importMemory {
	b.Helper()

	file, err := os.Open(dumpPath)
	if err != nil {
		b.Fatalf("open dump: %v", err)
	}
	defer file.Close()

	opts := DatabaseOptions{BackendName: backend.Name}
	if backend.Persistent {
		opts.Path = b.TempDir()
	}

	b.StopTimer()
	runtime.GC()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	b.StartTimer()

	db, err := NewDatabaseWithOptions(opts)
	if err != nil {
		b.Fatalf("open %s database: %v", backend.Name, err)
	}
	if err := load(db, file); err != nil {
		b.Fatalf("import dump into %s: %v", backend.Name, err)
	}

	b.StopTimer()
	var loaded runtime.MemStats
	runtime.ReadMemStats(&loaded)
	runtime.GC()
	runtime.GC()
	var settled runtime.MemStats
	runtime.ReadMemStats(&settled)

	// The store must outlive the reads above, or the collections take the thing
	// being measured.
	runtime.KeepAlive(db)
	if err := db.Close(); err != nil {
		b.Fatalf("close %s database: %v", backend.Name, err)
	}
	b.StartTimer()

	m := importMemory{sysTotal: max(loaded.Sys, settled.Sys)}
	if m.sysTotal > before.Sys {
		m.sysGrowth = m.sysTotal - before.Sys
	}
	if settled.HeapAlloc > before.HeapAlloc {
		m.retained = settled.HeapAlloc - before.HeapAlloc
	}
	return m
}
