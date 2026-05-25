package executor

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// Contract tests for the canonical consumer (Part 1 of
// docs/proposals/ITERATOR_ERROR_CONTRACT_ENFORCEMENT.md).

func TestForEach_VisitsAllTuplesOnClean(t *testing.T) {
	rel := newFailingRelation(100, Tuple{int64(1)}, Tuple{int64(2)}, Tuple{int64(3)})
	var seen []int64
	err := ForEach(rel, func(tu Tuple) error {
		seen = append(seen, tu[0].(int64))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3}, seen)
}

func TestForEach_EmptyIsNoError(t *testing.T) {
	rel := newFailingRelation(100)
	calls := 0
	err := ForEach(rel, func(Tuple) error { calls++; return nil })
	require.NoError(t, err)
	require.Equal(t, 0, calls)
}

func TestForEach_ReturnsIteratorErrorImmediate(t *testing.T) {
	rel := newFailingRelation(0, Tuple{int64(1)})
	err := ForEach(rel, func(Tuple) error { return nil })
	require.ErrorIs(t, err, errInjectedIterator)
}

func TestForEach_ReturnsIteratorErrorMidStream(t *testing.T) {
	rel := newFailingRelation(1, Tuple{int64(1)}, Tuple{int64(2)}, Tuple{int64(3)})
	calls := 0
	err := ForEach(rel, func(Tuple) error { calls++; return nil })
	require.ErrorIs(t, err, errInjectedIterator)
	require.Equal(t, 1, calls, "fn runs for tuples yielded before the failure")
}

func TestForEach_StopsAndReturnsFnError(t *testing.T) {
	fnErr := errors.New("fn boom")
	rel := newFailingRelation(100, Tuple{int64(1)}, Tuple{int64(2)}, Tuple{int64(3)})
	calls := 0
	err := ForEach(rel, func(Tuple) error {
		calls++
		if calls == 2 {
			return fnErr
		}
		return nil
	})
	require.ErrorIs(t, err, fnErr)
	require.Equal(t, 2, calls, "iteration stops at the fn error")
}

func TestForEach_ReturnsCloseErrorWhenIterationClean(t *testing.T) {
	closeErr := errors.New("close boom")
	rel := newFailingRelation(100, Tuple{int64(1)})
	rel.closeErr = closeErr
	err := ForEach(rel, func(Tuple) error { return nil })
	require.ErrorIs(t, err, closeErr)
}

func TestForEach_IterationErrorBeatsCloseError(t *testing.T) {
	closeErr := errors.New("close boom")
	rel := newFailingRelation(0, Tuple{int64(1)})
	rel.closeErr = closeErr
	err := ForEach(rel, func(Tuple) error { return nil })
	require.ErrorIs(t, err, errInjectedIterator, "iteration error must not be masked by Close()")
}

func TestForEach_FnErrorBeatsCloseError(t *testing.T) {
	fnErr := errors.New("fn boom")
	closeErr := errors.New("close boom")
	rel := newFailingRelation(100, Tuple{int64(1)}, Tuple{int64(2)})
	rel.closeErr = closeErr
	err := ForEach(rel, func(Tuple) error { return fnErr })
	require.ErrorIs(t, err, fnErr, "fn error must not be masked by Close()")
}
