package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// countingSliceIterator (defined in lazy_seq_relation_test.go) tracks how many
// tuples the source produced via its nextCalls pointer, which lets these tests
// prove that a limit stops pulling from the source instead of draining it.

// TestLimitRelationStopsScanAfterN is the core streaming-early-termination
// contract: wrapping a streaming relation of 1000 tuples in a limit of 5 must
// pull the source iterator at most 5 times, not drain it.
func TestLimitRelationStopsScanAfterN(t *testing.T) {
	const total = 1000

	build := func(calls *int) *countingSliceIterator {
		tuples := make([]Tuple, total)
		for i := range tuples {
			tuples[i] = Tuple{int64(i)}
		}
		return &countingSliceIterator{tuples: tuples, nextCalls: calls}
	}

	t.Run("limit below size stops the scan early", func(t *testing.T) {
		calls := 0
		sr := NewStreamingRelation([]query.Symbol{datalog.NewSymbol("?x")}, build(&calls))
		lr := NewLimitRelation(sr, 5)

		if got := lr.Size(); got != 5 {
			t.Errorf("expected Size 5, got %d", got)
		}
		if calls != 5 {
			t.Errorf("expected source pulled exactly 5 times (early termination), got %d", calls)
		}
	})

	t.Run("limit zero pulls nothing", func(t *testing.T) {
		calls := 0
		sr := NewStreamingRelation([]query.Symbol{datalog.NewSymbol("?x")}, build(&calls))
		lr := NewLimitRelation(sr, 0)

		if got := lr.Size(); got != 0 {
			t.Errorf("expected Size 0, got %d", got)
		}
		if calls != 0 {
			t.Errorf("expected source pulled 0 times, got %d", calls)
		}
	})

	t.Run("limit above size returns all without error", func(t *testing.T) {
		calls := 0
		small := &countingSliceIterator{tuples: []Tuple{{int64(1)}, {int64(2)}, {int64(3)}}, nextCalls: &calls}
		sr := NewStreamingRelation([]query.Symbol{datalog.NewSymbol("?x")}, small)
		lr := NewLimitRelation(sr, 10)

		if got := lr.Size(); got != 3 {
			t.Errorf("expected Size 3, got %d", got)
		}
	})

	t.Run("iterator yields exactly the limit", func(t *testing.T) {
		calls := 0
		sr := NewStreamingRelation([]query.Symbol{datalog.NewSymbol("?x")}, build(&calls))
		lr := NewLimitRelation(sr, 7)

		it := lr.Iterator()
		defer it.Close()
		count := 0
		for it.Next() {
			count++
		}
		if count != 7 {
			t.Errorf("expected iterator to yield 7 tuples, got %d", count)
		}
	})
}
