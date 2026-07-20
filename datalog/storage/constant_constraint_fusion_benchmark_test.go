package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func BenchmarkConstantConstraintFusion(b *testing.B) {
	for _, entities := range []int{1_000, 10_000} {
		for _, fusion := range []bool{false, true} {
			name := "match-join"
			if fusion {
				name = "lookup-filter"
			}
			b.Run(fmt.Sprintf("entities=%d/%s", entities, name), func(b *testing.B) {
				options := DefaultPlannerOptions()
				options.EnableAttributeFetchFusion = fusion
				s := schema.NewSchema()
				s.Add(&schema.AttributeDefinition{
					Ident:       datalog.NewKeyword(":bench/group"),
					ValueType:   schema.TypeLong,
					Cardinality: schema.CardinalityOne,
				})
				s.Add(&schema.AttributeDefinition{
					Ident:       datalog.NewKeyword(":bench/status"),
					ValueType:   schema.TypeKeyword,
					Cardinality: schema.CardinalityOne,
				})
				db, err := NewDatabaseWithOptions(DatabaseOptions{
					Path:           b.TempDir(),
					Schema:         s,
					PlannerOptions: &options,
				})
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()

				group := datalog.NewKeyword(":bench/group")
				status := datalog.NewKeyword(":bench/status")
				active := datalog.NewKeyword(":status/active")
				inactive := datalog.NewKeyword(":status/inactive")
				tx := db.NewTransaction()
				for i := 0; i < entities; i++ {
					entity := datalog.NewIdentity(fmt.Sprintf("constraint-benchmark-%d", i))
					if err := tx.Set(entity, group, int64(i%100)); err != nil {
						b.Fatal(err)
					}
					value := inactive
					if i%2 == 0 {
						value = active
					}
					if err := tx.Set(entity, status, value); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := tx.Commit(); err != nil {
					b.Fatal(err)
				}

				queryText := `[:find ?entity
					:where [(q [:find ?entity
					            :where [?entity :bench/group ?group]
					                   [?entity :bench/status :status/active]]
					           $) [[?entity] ...]]]`
				warm, err := db.Query(queryText)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := executor.CollectTuples(warm, nil); err != nil {
					b.Fatal(err)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					result, err := db.Query(queryText)
					if err != nil {
						b.Fatal(err)
					}
					rows, err := executor.CollectTuples(result, nil)
					if err != nil {
						b.Fatal(err)
					}
					if len(rows) != entities/2 {
						b.Fatalf("got %d rows, want %d", len(rows), entities/2)
					}
				}
			})
		}
	}
}
