package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// ExecuteAggregations answers the relation over its find's elements in
// written order, each aggregate under its display symbol and a pull under its
// variable, a group key as a variable is, on the batch path and on the
// streaming path.
func TestExecuteAggregationsAnswersItsFindInWrittenOrder(t *testing.T) {
	k, v := datalog.NewSymbol("?k"), datalog.NewSymbol("?v")
	count := query.FindAggregate{Function: datalog.SymCount, Arg: v}
	sum := query.FindAggregate{Function: datalog.SymSum, Arg: v}
	countSymbol, sumSymbol := datalog.NewSymbol(count.String()), datalog.NewSymbol(sum.String())

	// tuplesOver is n tuples over ?k ?v, ?k cycling through 0, 1 and 2 and ?v
	// the tuple's ordinal, so every tuple is distinct.
	tuplesOver := func(n int) []Tuple {
		tuples := make([]Tuple, n)
		for i := range tuples {
			tuples[i] = Tuple{int64(i % 3), int64(i)}
		}
		return tuples
	}

	layouts := []struct {
		name    string
		find    []query.FindElement
		written []query.Symbol
		// key, count and sum are the positions of a group's key, count and
		// sum in a result tuple, sum -1 where the find holds no sum.
		key, count, sum int
	}{
		{"a group variable between two aggregates",
			[]query.FindElement{count, query.FindVariable{Symbol: k}, sum},
			[]query.Symbol{countSymbol, k, sumSymbol}, 1, 0, 2},
		{"a pull after an aggregate",
			[]query.FindElement{count, query.FindPull{Variable: k, Pattern: &query.PullPattern{}}},
			[]query.Symbol{countSymbol, k}, 1, 0, -1},
	}
	paths := []struct {
		name   string
		tuples []Tuple
		opts   ExecutorOptions
		path   Relation
	}{
		{"batch", tuplesOver(12), ExecutorOptions{}, &MaterializedRelation{}},
		{"streaming", tuplesOver(StreamingAggregationThreshold), ExecutorOptions{EnableStreamingAggregation: true}, &StreamingAggregateRelation{}},
	}
	for _, p := range paths {
		for _, l := range layouts {
			t.Run(p.name+"/"+l.name, func(t *testing.T) {
				want := map[string]string{}
				counts, sums := map[int64]int64{}, map[int64]int64{}
				for _, tuple := range p.tuples {
					counts[tuple[0].(int64)]++
					sums[tuple[0].(int64)] += tuple[1].(int64)
				}
				for key := range counts {
					want[fmt.Sprint(key)] = fmt.Sprint(counts[key])
					if l.sum >= 0 {
						want[fmt.Sprint(key)] += " " + fmt.Sprint(sums[key])
					}
				}

				rel := NewMaterializedRelationWithOptions([]query.Symbol{k, v}, p.tuples, p.opts)
				result := ExecuteAggregations(rel, l.find)
				require.IsType(t, p.path, result, "the case runs the aggregation path it names")
				require.Equal(t, l.written, result.Symbols(), "the result carries the find's elements in written order")

				tuples, err := CollectTuples(result, nil)
				require.NoError(t, err)
				got := map[string]string{}
				for _, tuple := range tuples {
					got[fmt.Sprint(tuple[l.key])] = fmt.Sprint(tuple[l.count])
					if l.sum >= 0 {
						got[fmt.Sprint(tuple[l.key])] += " " + fmt.Sprint(tuple[l.sum])
					}
				}
				require.Equal(t, want, got, "each tuple holds the key, the count and the sum at the find's positions")
			})
		}
	}
}
