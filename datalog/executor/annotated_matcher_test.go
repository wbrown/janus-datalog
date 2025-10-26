package executor

import (
	"errors"
	"sync"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// testMatcher is a simple test matcher for annotation tests
type testMatcher struct {
	matchResult Relation
	matchError  error
	callCount   int
}

func (m *testMatcher) Match(pattern *query.DataPattern, bindings Relations) (Relation, error) {
	m.callCount++
	return m.matchResult, m.matchError
}

// testPredicateMatcher implements PredicateAwareMatcher
type testPredicateMatcher struct {
	testMatcher
	constraintCallCount int
}

func (m *testPredicateMatcher) MatchWithConstraints(
	pattern *query.DataPattern,
	bindings Relations,
	constraints []StorageConstraint,
) (Relation, error) {
	m.constraintCallCount++
	return m.matchResult, m.matchError
}

func TestWrapMatcher_NilHandler(t *testing.T) {
	// Arrange
	mock := &testMatcher{
		matchResult: NewMaterializedRelation(
			[]query.Symbol{"?x"},
			[]Tuple{{1}, {2}, {3}},
		),
	}

	// Act - wrap with nil handler
	wrapped := WrapMatcher(mock, nil)

	// Assert - should return original matcher unchanged
	if wrapped != mock {
		t.Errorf("Expected nil handler to return original matcher, got different instance")
	}
}

func TestWrapMatcher_ZeroOverheadWhenDisabled(t *testing.T) {
	// Arrange
	mock := &testMatcher{
		matchResult: NewMaterializedRelation(
			[]query.Symbol{"?x"},
			[]Tuple{{1}, {2}, {3}},
		),
	}

	// Act - wrap with nil handler
	wrapped := WrapMatcher(mock, nil)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?x"},
			query.Constant{Value: datalog.NewKeyword(":attr")},
			query.Variable{Name: "?v"},
		},
	}

	result, err := wrapped.Match(pattern, nil)

	// Assert
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	if mock.callCount != 1 {
		t.Errorf("Expected exactly 1 call to underlying matcher, got %d", mock.callCount)
	}

	// Verify it's truly zero overhead (same instance)
	if wrapped != mock {
		t.Error("Zero overhead guarantee violated - created wrapper when handler is nil")
	}
}

func TestWrapMatcher_BasicAnnotation(t *testing.T) {
	// Arrange
	mock := &testMatcher{
		matchResult: NewMaterializedRelation(
			[]query.Symbol{"?x", "?v"},
			[]Tuple{{1, "a"}, {2, "b"}},
		),
	}

	var capturedEvents []annotations.Event
	var eventsMu sync.Mutex
	handler := func(e annotations.Event) {
		eventsMu.Lock()
		capturedEvents = append(capturedEvents, e)
		eventsMu.Unlock()
	}

	// Act
	wrapped := WrapMatcher(mock, handler)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?x"},
			query.Constant{Value: datalog.NewKeyword(":attr")},
			query.Variable{Name: "?v"},
		},
	}

	result, err := wrapped.Match(pattern, nil)

	// Assert
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result == nil || result.Size() != 2 {
		t.Errorf("Expected 2 results, got %v", result)
	}

	// Verify annotation event was captured
	if len(capturedEvents) != 1 {
		t.Fatalf("Expected 1 annotation event, got %d", len(capturedEvents))
	}

	event := capturedEvents[0]
	if event.Name != annotations.MatchesToRelations {
		t.Errorf("Expected event name %s, got %s", annotations.MatchesToRelations, event.Name)
	}

	// Verify grouped metrics
	if event.Data["match.count"] != 2 {
		t.Errorf("Expected match.count=2, got %v", event.Data["match.count"])
	}

	if event.Data["success"] != true {
		t.Error("Expected success=true")
	}

	// Verify symbol order preserved
	symbolOrder, ok := event.Data["symbol.order"].([]string)
	if !ok || len(symbolOrder) != 2 {
		t.Errorf("Expected symbol.order with 2 elements, got %v", event.Data["symbol.order"])
	}

	if symbolOrder[0] != "?x" || symbolOrder[1] != "?v" {
		t.Errorf("Expected symbol order [?x ?v], got %v", symbolOrder)
	}
}

func TestWrapMatcher_WithError(t *testing.T) {
	// Arrange
	testError := errors.New("test error")
	mock := &testMatcher{
		matchError: testError,
	}

	var capturedEvents []annotations.Event
	var eventsMu sync.Mutex
	handler := func(e annotations.Event) {
		eventsMu.Lock()
		capturedEvents = append(capturedEvents, e)
		eventsMu.Unlock()
	}

	// Act
	wrapped := WrapMatcher(mock, handler)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?x"},
			query.Constant{Value: datalog.NewKeyword(":attr")},
			query.Variable{Name: "?v"},
		},
	}

	result, err := wrapped.Match(pattern, nil)

	// Assert
	if err != testError {
		t.Errorf("Expected error %v, got %v", testError, err)
	}

	if result != nil {
		t.Error("Expected nil result on error")
	}

	// Verify error was captured in annotation
	if len(capturedEvents) != 1 {
		t.Fatalf("Expected 1 annotation event, got %d", len(capturedEvents))
	}

	event := capturedEvents[0]
	if event.Data["success"] != false {
		t.Error("Expected success=false on error")
	}

	if event.Data["error"] != testError.Error() {
		t.Errorf("Expected error message %q, got %v", testError.Error(), event.Data["error"])
	}
}

func TestWrapMatcher_WithBindings(t *testing.T) {
	// Arrange
	mock := &testMatcher{
		matchResult: NewMaterializedRelation(
			[]query.Symbol{"?x", "?v"},
			[]Tuple{{1, "a"}},
		),
	}

	var capturedEvents []annotations.Event
	var eventsMu sync.Mutex
	handler := func(e annotations.Event) {
		eventsMu.Lock()
		capturedEvents = append(capturedEvents, e)
		eventsMu.Unlock()
	}

	// Act
	wrapped := WrapMatcher(mock, handler)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?x"},
			query.Constant{Value: datalog.NewKeyword(":attr")},
			query.Variable{Name: "?v"},
		},
	}

	// Create binding relation
	bindingRel := NewMaterializedRelation(
		[]query.Symbol{"?x"},
		[]Tuple{{1}, {2}, {3}},
	)

	result, err := wrapped.Match(pattern, Relations{bindingRel})

	// Assert
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	// Verify binding information was captured
	if len(capturedEvents) != 1 {
		t.Fatalf("Expected 1 annotation event, got %d", len(capturedEvents))
	}

	event := capturedEvents[0]

	bindingCols, ok := event.Data["binding.columns"].([]string)
	if !ok || len(bindingCols) != 1 || bindingCols[0] != "?x" {
		t.Errorf("Expected binding.columns=[?x], got %v", event.Data["binding.columns"])
	}

	if event.Data["binding.size"] != 3 {
		t.Errorf("Expected binding.size=3, got %v", event.Data["binding.size"])
	}
}

func TestWrapMatcher_PredicateAwareInterface(t *testing.T) {
	// Arrange
	mock := &testPredicateMatcher{
		testMatcher: testMatcher{
			matchResult: NewMaterializedRelation(
				[]query.Symbol{"?x"},
				[]Tuple{{1}},
			),
		},
	}

	var capturedEvents []annotations.Event
	var eventsMu sync.Mutex
	handler := func(e annotations.Event) {
		eventsMu.Lock()
		capturedEvents = append(capturedEvents, e)
		eventsMu.Unlock()
	}

	// Act
	wrapped := WrapMatcher(mock, handler)

	// Verify it implements PredicateAwareMatcher
	predicateMatcher, ok := wrapped.(PredicateAwareMatcher)
	if !ok {
		t.Fatal("Expected wrapped matcher to implement PredicateAwareMatcher")
	}

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?x"},
			query.Constant{Value: datalog.NewKeyword(":attr")},
			query.Constant{Value: 42},
		},
	}

	// Call MatchWithConstraints
	constraints := []StorageConstraint{} // empty for test
	result, err := predicateMatcher.MatchWithConstraints(pattern, nil, constraints)

	// Assert
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	if mock.constraintCallCount != 1 {
		t.Errorf("Expected 1 call to MatchWithConstraints, got %d", mock.constraintCallCount)
	}

	// Verify annotation captured constraint count
	if len(capturedEvents) != 1 {
		t.Fatalf("Expected 1 annotation event, got %d", len(capturedEvents))
	}

	event := capturedEvents[0]
	if event.Data["constraint.count"] != 0 {
		t.Errorf("Expected constraint.count=0, got %v", event.Data["constraint.count"])
	}
}

func TestWrapMatcher_TransparentForAllInterfaces(t *testing.T) {
	// This test verifies that the decorator is truly transparent -
	// the underlying matcher's interface is preserved

	mock := &testMatcher{
		matchResult: NewMaterializedRelation(
			[]query.Symbol{"?x"},
			[]Tuple{{1}},
		),
	}

	wrapped := WrapMatcher(mock, func(annotations.Event) {})

	// Verify the basic interface is preserved
	_, ok := wrapped.(PatternMatcher)
	if !ok {
		t.Error("Wrapped matcher should implement PatternMatcher")
	}

	// Verify the underlying matcher can still be accessed if needed
	if annotated, ok := wrapped.(*AnnotatedMatcher); ok {
		if annotated.underlying != mock {
			t.Error("Should be able to access underlying matcher")
		}
	} else {
		t.Error("Should be able to cast to AnnotatedMatcher")
	}
}

func BenchmarkWrapMatcher_Overhead(b *testing.B) {
	// Measure the overhead of decoration
	mock := &testMatcher{
		matchResult: NewMaterializedRelation(
			[]query.Symbol{"?x"},
			[]Tuple{{1}, {2}, {3}},
		),
	}

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?x"},
			query.Constant{Value: datalog.NewKeyword(":attr")},
			query.Variable{Name: "?v"},
		},
	}

	b.Run("Unwrapped", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mock.Match(pattern, nil)
		}
	})

	b.Run("Wrapped-Disabled", func(b *testing.B) {
		wrapped := WrapMatcher(mock, nil) // Zero overhead
		for i := 0; i < b.N; i++ {
			wrapped.Match(pattern, nil)
		}
	})

	b.Run("Wrapped-Enabled", func(b *testing.B) {
		wrapped := WrapMatcher(mock, func(annotations.Event) {}) // With annotation
		for i := 0; i < b.N; i++ {
			wrapped.Match(pattern, nil)
		}
	})
}
