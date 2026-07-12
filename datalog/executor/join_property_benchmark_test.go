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
		EnableStreamingJoins:      true,
		EnableIteratorComposition: true,
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
				leftBase := NewMaterializedRelationNoDedupe(leftSymbols, leftTuples)
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
