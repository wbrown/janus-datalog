package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestGroupedAggregationPreservesTypedKeys(t *testing.T) {
	groupA := datalog.NewSymbol("?group-a")
	groupB := datalog.NewSymbol("?group-b")
	value := datalog.NewSymbol("?value")
	symbols := []query.Symbol{groupA, groupB, value}
	find := []query.FindElement{
		query.FindVariable{Symbol: groupA},
		query.FindVariable{Symbol: groupB},
		query.FindAggregate{Function: datalog.SymSum, Arg: value},
	}

	modes := []struct {
		name string
		open func([]Tuple) Relation
	}{
		{
			name: "batch",
			open: func(tuples []Tuple) Relation {
				return NewMaterializedRelationWithOptions(
					symbols,
					tuples,
					ExecutorOptions{EnableStreamingAggregation: false},
				)
			},
		},
		{
			name: "streaming",
			open: func(tuples []Tuple) Relation {
				base := NewMaterializedRelationFromSet(symbols, tuples, ExecutorOptions{})
				return NewStreamingRelationWithOptions(
					symbols,
					base.Iterator(),
					ExecutorOptions{EnableStreamingAggregation: true},
				)
			},
		},
	}

	for _, pair := range adversarialTuplePairs {
		t.Run(pair.name, func(t *testing.T) {
			input := []Tuple{
				{pair.a[0], pair.a[1], float64(10)},
				{pair.b[0], pair.b[1], float64(20)},
			}
			want := [][]interface{}{
				{pair.a[0], pair.a[1], float64(10)},
				{pair.b[0], pair.b[1], float64(20)},
			}

			for _, mode := range modes {
				t.Run(mode.name, func(t *testing.T) {
					got, err := CollectTuples(ExecuteAggregations(mode.open(input), find), nil)
					require.NoError(t, err)
					require.ElementsMatch(t, want, got)
				})
			}
		})
	}
}

func BenchmarkGroupedAggregationKeying(b *testing.B) {
	groupA := datalog.NewSymbol("?group-a")
	groupB := datalog.NewSymbol("?group-b")
	value := datalog.NewSymbol("?value")
	symbols := []query.Symbol{groupA, groupB, value}
	find := []query.FindElement{
		query.FindVariable{Symbol: groupA},
		query.FindVariable{Symbol: groupB},
		query.FindAggregate{Function: datalog.SymSum, Arg: value},
		query.FindAggregate{Function: datalog.SymAvg, Arg: value},
	}

	const (
		rowCount   = 10_000
		groupCount = 100
	)
	tuples := make([]Tuple, rowCount)
	for i := range tuples {
		group := i % groupCount
		tuples[i] = Tuple{int64(group), fmt.Sprintf("group-%d", group), float64(i)}
	}
	base := NewMaterializedRelationFromSet(symbols, tuples, ExecutorOptions{})

	b.Run("batch", func(b *testing.B) {
		rel := NewMaterializedRelationFromSet(
			symbols,
			tuples,
			ExecutorOptions{EnableStreamingAggregation: false},
		)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			result := ExecuteAggregations(rel, find)
			if result.Size() != groupCount {
				b.Fatalf("got %d groups, want %d", result.Size(), groupCount)
			}
		}
	})

	b.Run("streaming", func(b *testing.B) {
		opts := ExecutorOptions{EnableStreamingAggregation: true}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			rel := NewStreamingRelationWithOptions(symbols, base.Iterator(), opts)
			result := ExecuteAggregations(rel, find)
			if result.Size() != groupCount {
				b.Fatalf("got %d groups, want %d", result.Size(), groupCount)
			}
		}
	})
}
