//go:build !(js && wasm)

package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

func BenchmarkJoinProjectInsertion(b *testing.B) {
	for _, mode := range []struct {
		name    string
		enabled bool
	}{
		{name: "flat", enabled: false},
		{name: "materialized-project", enabled: true},
	} {
		b.Run(mode.name, func(b *testing.B) {
			db, err := NewDatabaseWithOptions(DatabaseOptions{Path: b.TempDir()})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			score := datalog.NewKeyword(":item/score")
			payload := datalog.NewKeyword(":item/payload")
			tx := db.NewTransaction()
			for i := 0; i < 2_000; i++ {
				entity := datalog.NewIdentity(fmt.Sprintf("project-benchmark-%d", i))
				if err := tx.Add(entity, score, int64(i%100)); err != nil {
					b.Fatal(err)
				}
				if err := tx.Add(entity, payload, fmt.Sprintf("payload-%d", i)); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := tx.Commit(); err != nil {
				b.Fatal(err)
			}

			source := `[:find ?entity ?payload
				:where [?entity :item/score ?score]
				       [(> ?score 50)]
				       [?entity :item/payload ?payload]]`
			query, err := db.resolveQuery(source)
			if err != nil {
				b.Fatal(err)
			}
			options := DefaultPlannerOptions()
			options.EnableAlgebraOptimizer = true
			options.EnableJoinProjectInsertion = mode.enabled
			options.Cache = nil
			router := executor.NewSourceRouter(buildSourceMap(nil, db.Matcher()))
			exec := executor.NewExecutorWithOptions(router, db, options)

			warm, err := exec.ExecuteWithRelations(executor.NewContext(nil), query, nil)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := executor.CollectTuples(warm, nil); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := exec.ExecuteWithRelations(executor.NewContext(nil), query, nil)
				if err != nil {
					b.Fatal(err)
				}
				rows, err := executor.CollectTuples(result, nil)
				if err != nil {
					b.Fatal(err)
				}
				if len(rows) != 980 {
					b.Fatalf("got %d rows, want 980", len(rows))
				}
			}
		})
	}
}
