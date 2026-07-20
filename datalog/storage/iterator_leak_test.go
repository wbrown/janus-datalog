package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// TestIteratorLeak_BuiltinPatternDiscoveredEntity reproduces a resource leak
// where get-some, get-else, and missing? fail to close the input relation's
// iterator when the entity is discovered via pattern match (not bound via :in $).
//
// The unclosed iterator holds a BadgerDB transaction that pins the memtable
// skiplist ref count above zero. When db.Close() flushes the memtable, the
// skiplist's OnClose callback (which deletes the WAL file) never fires because
// the ref count never reaches zero.
//
// Detection: after db.Close(), the WAL file (00001.mem, 256 MB) persists on
// disk. This is cross-platform — Go's GC collects unreachable objects but does
// not call skiplist.DecrRef() (Go has no destructors).
//
// Root cause: executor/relation_ops.go — filterWithPredicateAndLookup and
// evaluateExpressionWithLookup call rel.Iterator() without iter.Close().
func TestIteratorLeak_BuiltinPatternDiscoveredEntity(t *testing.T) {
	type testCase struct {
		name  string
		query string
	}

	cases := []testCase{
		{
			name: "get-some",
			query: `[:find ?name
				 :where
				 [?e :entity/type _]
				 [(get-some $ ?e :entity/name :entity/code) ?name]]`,
		},
		{
			name: "get-else",
			query: `[:find ?name
				 :where
				 [?e :entity/type _]
				 [(get-else $ ?e :entity/name "unknown") ?name]]`,
		},
		{
			name: "missing?",
			query: `[:find ?e
				 :where
				 [?e :entity/type _]
				 [(missing? $ ?e :entity/code)]]`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range optimizerModes {
				t.Run(mode.name, func(t *testing.T) {
					dir, err := os.MkdirTemp("", "iter-leak-*")
					if err != nil {
						t.Fatal(err)
					}
					defer os.RemoveAll(dir)

					popts := mode.plannerOptions()
					db, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir, PlannerOptions: &popts})
					if err != nil {
						t.Fatalf("Failed to create database: %v", err)
					}

					// Insert entities — some with :entity/code, some without
					tx := db.NewTransaction()
					for i := range 10 {
						id := datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
						tx.Add(id, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":entity.type/room"))
						tx.Add(id, datalog.NewKeyword(":entity/name"), fmt.Sprintf("Room %d", i))
						if i%2 == 0 {
							tx.Add(id, datalog.NewKeyword(":entity/code"), fmt.Sprintf("R%d", i))
						}
					}
					_, err = tx.Commit()
					if err != nil {
						t.Fatalf("Commit failed: %v", err)
					}

					// Run query with pattern-discovered entities
					var results []any
					err = db.QueryInto(&results, tc.query)
					if err != nil {
						t.Fatalf("Query failed: %v", err)
					}
					if len(results) == 0 {
						t.Fatal("Expected results, got none")
					}

					// Close database
					if err := db.Close(); err != nil {
						t.Fatalf("Close failed: %v", err)
					}
					runtime.GC()

					// After db.Close(), the WAL file (00001.mem) should have been
					// deleted by the memtable skiplist's OnClose callback. If the
					// iterator was leaked, the skiplist ref count never reached zero
					// and the WAL persists.
					entries, err := os.ReadDir(dir)
					if err != nil {
						t.Fatalf("ReadDir failed: %v", err)
					}
					for _, entry := range entries {
						if filepath.Ext(entry.Name()) == ".mem" {
							info, _ := entry.Info()
							size := int64(0)
							if info != nil {
								size = info.Size()
							}
							t.Errorf("WAL file %s (%d MB) persists after db.Close() — iterator not closed",
								entry.Name(), size/(1024*1024))
						}
					}
				})
			}
		})
	}
}
