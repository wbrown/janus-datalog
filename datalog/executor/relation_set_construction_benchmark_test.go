package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

var provenSetConstructionResult Relation

func BenchmarkProvenSetConstruction(b *testing.B) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	symbols := []query.Symbol{entity, value}
	properties := RelationProperties{Keys: [][]query.Symbol{{entity}}}
	for _, count := range []int{10_000, 100_000} {
		tuples := make([]Tuple, count)
		for i := range tuples {
			tuples[i] = Tuple{int64(i), int64(i * 2)}
		}
		b.Run(fmt.Sprintf("tuples=%d/deduplicating", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				relation := NewMaterializedRelationWithProperties(
					symbols,
					tuples,
					ExecutorOptions{},
					properties,
				)
				if relation.Size() != count {
					b.Fatalf("got %d tuples, want %d", relation.Size(), count)
				}
				provenSetConstructionResult = relation
			}
		})
		b.Run(fmt.Sprintf("tuples=%d/proven-set", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				relation := newMaterializedRelationFromSet(
					symbols,
					tuples,
					ExecutorOptions{},
					properties,
				)
				if relation.Size() != count {
					b.Fatalf("got %d tuples, want %d", relation.Size(), count)
				}
				provenSetConstructionResult = relation
			}
		})
	}
}
