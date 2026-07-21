package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func BenchmarkKeyPreservingJoinProjection(b *testing.B) {
	id := datalog.NewSymbol("?id")
	leftValue := datalog.NewSymbol("?left")
	rightValue := datalog.NewSymbol("?right")
	leftSymbols := []query.Symbol{id, leftValue}
	rightSymbols := []query.Symbol{id, rightValue}
	projectedSymbols := []query.Symbol{id, leftValue}
	opts := ExecutorOptions{
		EnableStreamingJoins: true,
	}

	for _, rowCount := range []int{10_000, 100_000} {
		leftTuples := make([]Tuple, rowCount)
		rightTuples := make([]Tuple, rowCount)
		for i := range rowCount {
			leftTuples[i] = Tuple{int64(i), int64(i * 2)}
			rightTuples[i] = Tuple{int64(i), int64(i * 3)}
		}
		right := NewMaterializedRelationWithProperties(
			rightSymbols,
			rightTuples,
			opts,
			RelationProperties{Keys: [][]query.Symbol{{id}}},
		)

		b.Run(fmt.Sprintf("rows_%d", rowCount), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				leftBase := NewMaterializedRelationFromSet(leftSymbols, leftTuples, ExecutorOptions{})
				left := NewStreamingRelationWithProperties(
					leftSymbols,
					leftBase.Iterator(),
					opts,
					RelationProperties{Keys: [][]query.Symbol{{id}}},
				)
				joined := HashJoinWithOptions(left, right, []query.Symbol{id}, opts)
				projected, err := joined.Project(projectedSymbols)
				if err != nil {
					b.Fatal(err)
				}

				count := 0
				it := projected.Iterator()
				for it.Next() {
					count++
				}
				if err := it.Error(); err != nil {
					b.Fatal(err)
				}
				if err := it.Close(); err != nil {
					b.Fatal(err)
				}
				if count != rowCount {
					b.Fatalf("got %d rows, want %d", count, rowCount)
				}
			}
		})
	}
}

func BenchmarkSemiAntiJoinPropertyPropagation(b *testing.B) {
	id := datalog.NewSymbol("?id")
	value := datalog.NewSymbol("?value")
	leftSymbols := []query.Symbol{id, value}
	rightSymbols := []query.Symbol{id}
	properties := RelationProperties{
		Ordering: []query.OrderByClause{{Variable: id, Descending: false}},
		Keys:     [][]query.Symbol{{id}},
	}

	for _, rowCount := range []int{10_000, 100_000} {
		leftTuples := make([]Tuple, rowCount)
		rightTuples := make([]Tuple, 0, rowCount/2)
		for i := range rowCount {
			leftTuples[i] = Tuple{int64(i), int64(i * 2)}
			if i%2 == 0 {
				rightTuples = append(rightTuples, Tuple{int64(i)})
			}
		}
		left := NewMaterializedRelationWithProperties(
			leftSymbols,
			leftTuples,
			ExecutorOptions{},
			properties,
		)
		right := NewMaterializedRelation(rightSymbols, rightTuples)
		expectedRows := rowCount / 2

		b.Run(fmt.Sprintf("semi/rows_%d", rowCount), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				result := SemiJoin(left, right, []query.Symbol{id})
				if result.Size() != expectedRows {
					b.Fatalf("got %d rows, want %d", result.Size(), expectedRows)
				}
			}
		})

		b.Run(fmt.Sprintf("anti/rows_%d", rowCount), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				result := AntiJoin(left, right, []query.Symbol{id})
				if result.Size() != expectedRows {
					b.Fatalf("got %d rows, want %d", result.Size(), expectedRows)
				}
			}
		})
	}
}
