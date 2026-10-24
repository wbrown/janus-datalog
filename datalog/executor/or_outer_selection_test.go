package executor

import (
	"errors"
	"fmt"
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

// Two streaming groups sharing no symbol are joined as their product, a
// subject carrying both groups' symbols and consuming both groups.
func TestFindOuterRelationCombinesTwoStreamsSharingNoSymbolAsTheirProduct(t *testing.T) {
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
	syms := selected.Symbols()
	xi, yi := query.SymbolIndex(syms, x), query.SymbolIndex(syms, y)
	require.True(t, xi >= 0 && yi >= 0, "the subject carries both groups' symbols; got %v", syms)
	tuples, err := CollectTuples(selected, nil)
	require.NoError(t, err)
	require.Len(t, tuples, 1)
	require.Equal(t, int64(2), tuples[0][xi])
	require.Equal(t, int64(3), tuples[0][yi])
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
			relation, err := NewOrFallbackRelation(
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
			require.NoError(t, err)
			iterator := relation.Iterator()
			for iterator.Next() {
			}
			err = iterator.Error()
			if closeErr := iterator.Close(); err == nil {
				err = closeErr
			}
			require.EqualError(t, err, testCase.expected.Error())
		})
	}
}

// With the EA arm serving the branch, a subject's iteration error and its
// close error are still the result's: the arm reads the window's run, and
// the subject is read by the window reader. The arm is reached as a query
// reaches it, through the fallback's prefetch trigger under
// EnableEntityPrefetch: under a cap of 128 the subject's seventh window
// holds 64 entities, the first above the threshold, so it is prefetched and
// every later window's branch is built from the cache.
func TestOrFallbackSurfacesTheSubjectsErrorsUnderTheEAArm(t *testing.T) {
	// Under a cap of 128 a subject of 300 is read in windows of 1, 2, 4, 8,
	// 16, 32, 64, 128 and 45 tuples.
	const subjectSize = 300
	const seventhWindow, eighthWindow, ninthWindow = 64, 128, 45
	entitySymbol := datalog.NewSymbol("?entity")
	valueSymbol := datalog.NewSymbol("?value")
	attr := datalog.NewKeyword(":item/value")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	values := map[bundleLookupKey]interface{}{}
	var datoms []datalog.Datom
	subject := make([]Tuple, 0, subjectSize)
	for i := 0; i < subjectSize; i++ {
		entity := datalog.NewIdentity(fmt.Sprintf("ea-cache-outer-error:item%d", i))
		values[bundleLookupKey{entity: entity, attr: attr}] = int64(i)
		datoms = append(datoms, datalog.Datom{E: entity, A: attr, V: int64(i), Tx: tx})
		subject = append(subject, Tuple{entity})
	}
	base := NewMaterializedRelation([]query.Symbol{entitySymbol}, subject)
	branch := []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entitySymbol},
		query.Constant{Value: attr},
		query.Variable{Name: valueSymbol},
	}}}

	testCases := []struct {
		name     string
		outer    Relation
		expected error
		lookups  int // the entities of the windows the arm built before the failure
	}{
		{
			name:     "iteration",
			outer:    failingRelation{Relation: base, failAfter: 200},
			expected: errInjectedIterator,
			lookups:  seventhWindow, // the eighth window fails while it is read
		},
		{
			name:     "close",
			outer:    failingRelation{Relation: base, failAfter: 1000, closeErr: errors.New("EA cache outer close failure")},
			expected: errors.New("EA cache outer close failure"),
			lookups:  seventhWindow + eighthWindow + ninthWindow,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			matcher := &prefetchingMatcher{PatternMatcher: NewMemoryPatternMatcher(datoms), values: values}
			options := ExecutorOptions{EnableEntityPrefetch: true, ScopeWindowCap: 128}
			relation, err := NewOrFallbackRelation(
				newQueryExecutor(matcher, nil, options),
				NewContext(),
				[]query.Symbol{entitySymbol},
				[]query.Symbol{valueSymbol},
				[][]query.Clause{branch},
				testCase.outer,
				options,
				true,
			)
			require.NoError(t, err)
			iterator := relation.Iterator()
			for iterator.Next() {
			}
			err = iterator.Error()
			if closeErr := iterator.Close(); err == nil {
				err = closeErr
			}
			require.EqualError(t, err, testCase.expected.Error())
			require.NotEmpty(t, matcher.prefetched, "the trigger ran before the failure, so the arm served the branch")
			require.Equal(t, testCase.lookups, matcher.lookups, "the arm built every window after the trigger from the cache")
		})
	}
}
