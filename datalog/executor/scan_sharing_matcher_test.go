package executor

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// countingMatcher wraps a PatternMatcher and counts Match() calls.
type countingMatcher struct {
	inner     PatternMatcher
	callCount int32
}

func (m *countingMatcher) Match(pattern *query.DataPattern, bindings Relations) (Relation, error) {
	atomic.AddInt32(&m.callCount, 1)
	return m.inner.Match(pattern, bindings)
}

// fixedMatcher returns a fixed relation for any Match() call.
type fixedMatcher struct {
	tuples  []Tuple
	symbols []query.Symbol
}

func (m *fixedMatcher) Match(pattern *query.DataPattern, bindings Relations) (Relation, error) {
	return NewMaterializedRelation(m.symbols, m.tuples), nil
}

// TestScanSharingMatcher_UnboundScanShared verifies that calling Match()
// twice with the same unbound pattern only calls the inner matcher once.
func TestScanSharingMatcher_UnboundScanShared(t *testing.T) {
	inner := &countingMatcher{inner: &fixedMatcher{
		tuples:  []Tuple{{"e1", "s1"}, {"e2", "s2"}, {"e3", "s3"}},
		symbols: []query.Symbol{datalog.NewSymbol("?t"), datalog.NewSymbol("?s")},
	}}
	reg := NewScanRegistry()
	matcher := NewScanSharingMatcher(inner, reg, nil)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?t")},
			query.Constant{Value: datalog.NewKeyword(":task/root")},
			query.Variable{Name: datalog.NewSymbol("?s")},
		},
	}

	// First call — should hit inner matcher
	rel1, err := matcher.Match(pattern, nil)
	require.NoError(t, err)
	tuples1, err := CollectTuples(rel1, nil)
	require.NoError(t, err)
	assert.Len(t, tuples1, 3)

	// Second call — should hit registry, not inner matcher
	rel2, err := matcher.Match(pattern, nil)
	require.NoError(t, err)
	tuples2, err := CollectTuples(rel2, nil)
	require.NoError(t, err)
	assert.Len(t, tuples2, 3)

	assert.Equal(t, int32(1), atomic.LoadInt32(&inner.callCount),
		"inner matcher should be called exactly once")
}

// TestScanSharingMatcher_BoundScanNotShared verifies that Match() calls
// with non-nil bindings always go to the inner matcher (no caching).
func TestScanSharingMatcher_BoundScanNotShared(t *testing.T) {
	inner := &countingMatcher{inner: &fixedMatcher{
		tuples:  []Tuple{{"e1"}},
		symbols: []query.Symbol{datalog.NewSymbol("?t")},
	}}
	reg := NewScanRegistry()
	matcher := NewScanSharingMatcher(inner, reg, nil)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?t")},
			query.Constant{Value: datalog.NewKeyword(":task/root")},
			query.Variable{Name: datalog.NewSymbol("?s")},
		},
	}

	bindings := Relations{NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?t")},
		[]Tuple{{"e1"}},
	)}

	_, _ = matcher.Match(pattern, bindings)
	_, _ = matcher.Match(pattern, bindings)

	assert.Equal(t, int32(2), atomic.LoadInt32(&inner.callCount),
		"bound scans should not be shared — inner called every time")
}

// TestScanSharingMatcher_DifferentPatternsIndependent verifies that
// different patterns are not shared.
func TestScanSharingMatcher_DifferentPatternsIndependent(t *testing.T) {
	inner := &countingMatcher{inner: &fixedMatcher{
		tuples:  []Tuple{{"e1"}},
		symbols: []query.Symbol{datalog.NewSymbol("?t")},
	}}
	reg := NewScanRegistry()
	matcher := NewScanSharingMatcher(inner, reg, nil)

	p1 := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?t")},
			query.Constant{Value: datalog.NewKeyword(":task/root")},
			query.Variable{Name: datalog.NewSymbol("?s")},
		},
	}
	p2 := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?t")},
			query.Constant{Value: datalog.NewKeyword(":task/status")},
			query.Variable{Name: datalog.NewSymbol("?s")},
		},
	}

	_, _ = matcher.Match(p1, nil)
	_, _ = matcher.Match(p2, nil)

	assert.Equal(t, int32(2), atomic.LoadInt32(&inner.callCount),
		"different patterns should each call inner matcher")
}

// TestScanSharingMatcher_SymbolRemapping verifies that when the second
// call uses different variable names, the returned relation has the
// correct symbols but identical tuple data.
func TestScanSharingMatcher_SymbolRemapping(t *testing.T) {
	inner := &countingMatcher{inner: &fixedMatcher{
		tuples:  []Tuple{{"e1", "v1"}, {"e2", "v2"}},
		symbols: []query.Symbol{datalog.NewSymbol("?t"), datalog.NewSymbol("?s")},
	}}
	reg := NewScanRegistry()
	matcher := NewScanSharingMatcher(inner, reg, nil)

	p1 := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?t")},
			query.Constant{Value: datalog.NewKeyword(":task/root")},
			query.Variable{Name: datalog.NewSymbol("?s")},
		},
	}
	p2 := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?x")},
			query.Constant{Value: datalog.NewKeyword(":task/root")},
			query.Variable{Name: datalog.NewSymbol("?y")},
		},
	}

	rel1, _ := matcher.Match(p1, nil)
	rel2, _ := matcher.Match(p2, nil)

	assert.Equal(t, []query.Symbol{datalog.NewSymbol("?t"), datalog.NewSymbol("?s")}, rel1.Symbols())
	assert.Equal(t, []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}, rel2.Symbols())

	// Same tuple data
	tuples1, _ := CollectTuples(rel1, nil)
	tuples2, _ := CollectTuples(rel2, nil)
	require.Len(t, tuples1, 2)
	require.Len(t, tuples2, 2)
	assert.Equal(t, tuples1[0][0], tuples2[0][0])
	assert.Equal(t, tuples1[0][1], tuples2[0][1])

	assert.Equal(t, int32(1), atomic.LoadInt32(&inner.callCount))
}

// TestScanSharingMatcher_ThreeConsumers verifies that three Match() calls
// with the same pattern all return correct data from a single inner call.
func TestScanSharingMatcher_ThreeConsumers(t *testing.T) {
	tuples := make([]Tuple, 100)
	for i := range tuples {
		tuples[i] = Tuple{i}
	}
	inner := &countingMatcher{inner: &fixedMatcher{
		tuples:  tuples,
		symbols: []query.Symbol{datalog.NewSymbol("?i")},
	}}
	reg := NewScanRegistry()
	matcher := NewScanSharingMatcher(inner, reg, nil)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?i")},
			query.Constant{Value: datalog.NewKeyword(":attr")},
		},
	}

	rel1, _ := matcher.Match(pattern, nil)
	rel2, _ := matcher.Match(pattern, nil)
	rel3, _ := matcher.Match(pattern, nil)

	t1, _ := CollectTuples(rel1, nil)
	t2, _ := CollectTuples(rel2, nil)
	t3, _ := CollectTuples(rel3, nil)

	assert.Len(t, t1, 100)
	assert.Len(t, t2, 100)
	assert.Len(t, t3, 100)

	assert.Equal(t, int32(1), atomic.LoadInt32(&inner.callCount),
		"three consumers should share a single inner Match() call")
}

// TestScanSharingMatcher_AnnotationEvents verifies that cache-miss is
// emitted on first call and cache-hit on second.
func TestScanSharingMatcher_AnnotationEvents(t *testing.T) {
	inner := &fixedMatcher{
		tuples:  []Tuple{{"e1"}},
		symbols: []query.Symbol{datalog.NewSymbol("?t")},
	}
	reg := NewScanRegistry()

	var events []string
	handler := annotations.Handler(func(e annotations.Event) {
		events = append(events, e.Name)
	})
	matcher := NewScanSharingMatcher(inner, reg, handler)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?t")},
			query.Constant{Value: datalog.NewKeyword(":attr")},
		},
	}

	_, _ = matcher.Match(pattern, nil)
	_, _ = matcher.Match(pattern, nil)

	assert.Contains(t, events, "scan-sharing/cache-miss")
	assert.Contains(t, events, "scan-sharing/cache-hit")
}
