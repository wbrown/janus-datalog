package executor

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestFindOuterRelationLeavesSingleStreamLazy(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	base := NewMaterializedRelation(
		[]query.Symbol{entity},
		[]Tuple{{int64(1)}, {int64(2)}},
	)
	stream := NewStreamingRelation(
		[]query.Symbol{entity},
		base.Iterator(),
	)
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{})

	selected, consumed := exec.findOuterRelation(
		[]query.Symbol{entity},
		Relations{stream},
	)
	require.Same(t, stream, selected)
	require.Equal(t, []int{0}, consumed)
	require.False(t, stream.shouldCache,
		"single outer selection must not request eager materialization")
}

func TestFindOuterRelationCombinesMultipleStreamsReusably(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	leftBase := NewMaterializedRelation([]query.Symbol{x}, []Tuple{{int64(2)}})
	rightBase := NewMaterializedRelation([]query.Symbol{y}, []Tuple{{int64(3)}})
	left := NewStreamingRelation([]query.Symbol{x}, leftBase.Iterator())
	right := NewStreamingRelation([]query.Symbol{y}, rightBase.Iterator())
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{})

	selected, consumed := exec.findOuterRelation(
		[]query.Symbol{x, y},
		Relations{left, right},
	)
	require.Equal(t, []int{0, 1}, consumed)
	tuples, err := CollectTuples(selected, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2), int64(3)}}, tuples)
}

// collectBranch, the boundary at which a branch's result is kept for its
// index, surfaces the result's iteration error and its close error.
func TestCollectBranchPreservesIteratorAndCloseErrors(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	branch := NewMaterializedRelation(
		[]query.Symbol{entity, value},
		[]Tuple{{int64(1), "one"}, {int64(2), "two"}},
	)

	_, iterationFailure := collectBranch(failingRelation{Relation: branch, failAfter: 1})
	require.ErrorIs(t, iterationFailure, errInjectedIterator)

	closeFailure := errors.New("collect branch close failure")
	_, closeResult := collectBranch(failingRelation{Relation: branch, failAfter: 100, closeErr: closeFailure})
	require.ErrorIs(t, closeResult, closeFailure)
}

// A subject's iteration error and its close error are the result's: the
// first surfaces when the result is driven, the second when it is closed.
func TestOrFallbackSurfacesTheSubjectsIteratorAndCloseErrors(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	base := NewMaterializedRelation(
		[]query.Symbol{entity},
		[]Tuple{{int64(1)}, {int64(2)}},
	)

	testCases := []struct {
		name     string
		outer    Relation
		expected error
	}{
		{
			name:     "iteration",
			outer:    failingRelation{Relation: base, failAfter: 1},
			expected: errInjectedIterator,
		},
		{
			name:     "close",
			outer:    failingRelation{Relation: base, failAfter: 100, closeErr: errors.New("subject close failure")},
			expected: errors.New("subject close failure"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			relation := NewOrFallbackRelation(
				newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{}),
				NewContext(),
				[]query.Symbol{entity},
				[]query.Symbol{value},
				[][]query.Clause{{&query.Expression{
					Function: &query.GroundFunction{Value: "fallback"},
					Binding:  value,
				}}},
				testCase.outer,
				ExecutorOptions{},
				true,
			)
			iterator := relation.Iterator()
			for iterator.Next() {
			}
			err := iterator.Error()
			if closeErr := iterator.Close(); err == nil {
				err = closeErr
			}
			require.EqualError(t, err, testCase.expected.Error())
		})
	}
}

// With the EA arm serving the branch, a subject's iteration error and its
// close error are still the result's: the arm reads the window's run, and
// the subject is read by the fill.
func TestOrFallbackSurfacesTheSubjectsErrorsUnderTheEAArm(t *testing.T) {
	entitySymbol := datalog.NewSymbol("?entity")
	valueSymbol := datalog.NewSymbol("?value")
	entity := datalog.NewIdentity("ea-cache-outer-error")
	attr := datalog.NewKeyword(":item/value")
	base := NewMaterializedRelation(
		[]query.Symbol{entitySymbol},
		[]Tuple{{entity}},
	)
	branch := []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entitySymbol},
		query.Constant{Value: attr},
		query.Variable{Name: valueSymbol},
	}}}
	matcher := &bundleLookupMatcher{values: map[bundleLookupKey]interface{}{
		{entity: entity, attr: attr}: "present",
	}}

	testCases := []struct {
		name     string
		outer    Relation
		expected error
	}{
		{
			name:     "iteration",
			outer:    failingRelation{Relation: base, failAfter: 0},
			expected: errInjectedIterator,
		},
		{
			name:     "close",
			outer:    failingRelation{Relation: base, failAfter: 100, closeErr: errors.New("EA cache outer close failure")},
			expected: errors.New("EA cache outer close failure"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			relation := NewOrFallbackRelation(
				newQueryExecutor(matcher, nil, ExecutorOptions{}),
				NewContext(),
				[]query.Symbol{entitySymbol},
				[]query.Symbol{valueSymbol},
				[][]query.Clause{branch},
				testCase.outer,
				ExecutorOptions{},
				true,
			)
			relation.prefetched = true
			iterator := relation.Iterator()
			for iterator.Next() {
			}
			err := iterator.Error()
			if closeErr := iterator.Close(); err == nil {
				err = closeErr
			}
			require.EqualError(t, err, testCase.expected.Error())
		})
	}
}
