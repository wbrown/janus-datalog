package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func BenchmarkOrderedLimit(b *testing.B) {
	for _, rowCount := range []int{10_000, 100_000} {
		symbols, tuples := orderedLimitBenchmarkData(rowCount)
		base := NewMaterializedRelationFromSet(symbols, tuples, ExecutorOptions{})
		orderBy := []query.OrderByClause{{
			Variable:   symbols[0],
			Descending: true,
		}}

		for _, limit := range []int{1, 10, 100} {
			b.Run(fmt.Sprintf("rows_%d/limit_%d/materialized", rowCount, limit), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					result := TopNRelation(base, orderBy, limit)
					if result.Size() != limit {
						b.Fatalf("got %d rows, want %d", result.Size(), limit)
					}
				}
			})

			b.Run(fmt.Sprintf("rows_%d/limit_%d/streaming", rowCount, limit), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					rel := NewStreamingRelation(symbols, base.Iterator())
					result := TopNRelation(rel, orderBy, limit)
					if result.Size() != limit {
						b.Fatalf("got %d rows, want %d", result.Size(), limit)
					}
				}
			})
		}
	}
}

func orderedLimitBenchmarkData(rowCount int) ([]query.Symbol, []Tuple) {
	score := datalog.NewSymbol("?score")
	entity := datalog.NewSymbol("?entity")
	symbols := []query.Symbol{score, entity}
	tuples := make([]Tuple, rowCount)
	for i := range tuples {
		// 7,919 is coprime with both benchmark sizes, producing a stable
		// permutation rather than an already-sorted input.
		value := int64((i * 7_919) % rowCount)
		tuples[i] = Tuple{
			value,
			datalog.NewIdentity(fmt.Sprintf("top-n:%d:%d", rowCount, value)),
		}
	}
	return symbols, tuples
}
