package executor

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestHashJoin(t *testing.T) {
	// Left relation: people and their departments
	leftSymbols := []query.Symbol{datalog.NewSymbol("?person"), datalog.NewSymbol("?dept")}
	leftTuples := []Tuple{
		{"Alice", "Engineering"},
		{"Bob", "Sales"},
		{"Charlie", "Engineering"},
	}
	left := NewMaterializedRelation(leftSymbols, leftTuples)

	// Right relation: departments and their locations
	rightSymbols := []query.Symbol{datalog.NewSymbol("?dept"), datalog.NewSymbol("?location")}
	rightTuples := []Tuple{
		{"Engineering", "Building A"},
		{"Sales", "Building B"},
		{"Marketing", "Building C"},
	}
	right := NewMaterializedRelation(rightSymbols, rightTuples)

	// Join on ?dept
	joined := left.HashJoin(right, []query.Symbol{datalog.NewSymbol("?dept")})

	// Expected: 3 results (no Marketing people)
	if joined.Size() != 3 {
		t.Errorf("expected 3 joined tuples, got %d", joined.Size())
	}

	// Check symbols
	expectedSymbols := []query.Symbol{datalog.NewSymbol("?person"), datalog.NewSymbol("?dept"), datalog.NewSymbol("?location")}
	if !reflect.DeepEqual(joined.Symbols(), expectedSymbols) {
		t.Errorf("expected symbols %v, got %v", expectedSymbols, joined.Symbols())
	}

	// Collect results
	results := collectTuples(joined)

	// Verify specific joins
	expected := []Tuple{
		{"Alice", "Engineering", "Building A"},
		{"Bob", "Sales", "Building B"},
		{"Charlie", "Engineering", "Building A"},
	}

	if !tuplesEqual(results, expected) {
		t.Errorf("unexpected join results:\ngot:  %v\nwant: %v", results, expected)
	}
}

func TestJoinMultipleSymbols(t *testing.T) {
	// Test joining on multiple symbols
	leftSymbols := []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")}
	leftTuples := []Tuple{
		{1, 2, "x"},
		{1, 3, "y"},
		{2, 2, "z"},
	}
	left := NewMaterializedRelation(leftSymbols, leftTuples)

	rightSymbols := []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?d")}
	rightTuples := []Tuple{
		{1, 2, "foo"},
		{1, 3, "bar"},
		{2, 3, "baz"},
	}
	right := NewMaterializedRelation(rightSymbols, rightTuples)

	// Join on both ?a and ?b
	joined := left.HashJoin(right, []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")})

	// Should match: (1,2) and (1,3)
	if joined.Size() != 2 {
		t.Errorf("expected 2 joined tuples, got %d", joined.Size())
	}

	results := collectTuples(joined)
	expected := []Tuple{
		{1, 2, "x", "foo"},
		{1, 3, "y", "bar"},
	}

	if !tuplesEqual(results, expected) {
		t.Errorf("unexpected join results:\ngot:  %v\nwant: %v", results, expected)
	}
}

func TestEmptyJoin(t *testing.T) {
	// Join with no common values
	leftSymbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	leftTuples := []Tuple{{1, 2}, {3, 4}}
	left := NewMaterializedRelation(leftSymbols, leftTuples)

	rightSymbols := []query.Symbol{datalog.NewSymbol("?y"), datalog.NewSymbol("?z")}
	rightTuples := []Tuple{{5, 6}, {7, 8}}
	right := NewMaterializedRelation(rightSymbols, rightTuples)

	joined := left.HashJoin(right, []query.Symbol{datalog.NewSymbol("?y")})

	if joined.Materialize().Size() != 0 {
		t.Error("expected empty join result")
	}
}

func TestCrossProduct(t *testing.T) {
	// Test cross product (no common symbols)
	leftSymbols := []query.Symbol{datalog.NewSymbol("?a")}
	leftTuples := []Tuple{{"x"}, {"y"}}
	left := NewMaterializedRelation(leftSymbols, leftTuples)

	rightSymbols := []query.Symbol{datalog.NewSymbol("?b")}
	rightTuples := []Tuple{{1}, {2}}
	right := NewMaterializedRelation(rightSymbols, rightTuples)

	joined := left.Join(right)

	// Should be 2x2 = 4 tuples
	if joined.Size() != 4 {
		t.Errorf("expected 4 tuples in cross product, got %d", joined.Size())
	}

	results := collectTuples(joined)
	expected := []Tuple{
		{"x", 1},
		{"x", 2},
		{"y", 1},
		{"y", 2},
	}

	if !tuplesEqual(results, expected) {
		t.Errorf("unexpected cross product:\ngot:  %v\nwant: %v", results, expected)
	}
}

func TestSemiJoin(t *testing.T) {
	// Left: all people
	leftSymbols := []query.Symbol{datalog.NewSymbol("?person"), datalog.NewSymbol("?dept")}
	leftTuples := []Tuple{
		{"Alice", "Engineering"},
		{"Bob", "Sales"},
		{"Charlie", "Engineering"},
		{"David", "HR"},
	}
	left := NewMaterializedRelation(leftSymbols, leftTuples)

	// Right: active departments
	rightSymbols := []query.Symbol{datalog.NewSymbol("?dept")}
	rightTuples := []Tuple{
		{"Engineering"},
		{"Sales"},
	}
	right := NewMaterializedRelation(rightSymbols, rightTuples)

	// Semi-join: people in active departments
	result := left.SemiJoin(right, []query.Symbol{datalog.NewSymbol("?dept")})

	if result.Size() != 3 {
		t.Errorf("expected 3 people in active departments, got %d", result.Size())
	}

	// David from HR should be filtered out
	results := collectTuples(result)
	for _, tuple := range results {
		if tuple[0] == "David" {
			t.Error("David should have been filtered out")
		}
	}
}

func TestAntiJoin(t *testing.T) {
	// Same setup as SemiJoin
	leftSymbols := []query.Symbol{datalog.NewSymbol("?person"), datalog.NewSymbol("?dept")}
	leftTuples := []Tuple{
		{"Alice", "Engineering"},
		{"Bob", "Sales"},
		{"Charlie", "Engineering"},
		{"David", "HR"},
	}
	left := NewMaterializedRelation(leftSymbols, leftTuples)

	rightSymbols := []query.Symbol{datalog.NewSymbol("?dept")}
	rightTuples := []Tuple{
		{"Engineering"},
		{"Sales"},
	}
	right := NewMaterializedRelation(rightSymbols, rightTuples)

	// Anti-join: people NOT in active departments
	result := left.AntiJoin(right, []query.Symbol{datalog.NewSymbol("?dept")})

	if result.Size() != 1 {
		t.Errorf("expected 1 person not in active departments, got %d", result.Size())
	}

	// Only David from HR should remain
	it := result.Iterator()
	if it.Next() {
		tuple := it.Tuple()
		if tuple[0] != "David" || tuple[1] != "HR" {
			t.Errorf("expected David from HR, got %v", tuple)
		}
	}
	it.Close()
}

// Test utilities

func collectTuples(rel Relation) []Tuple {
	var tuples []Tuple
	it := rel.Iterator()
	defer it.Close()

	for it.Next() {
		tuples = append(tuples, it.Tuple())
	}
	return tuples
}

func tuplesEqual(a, b []Tuple) bool {
	if len(a) != len(b) {
		return false
	}

	// For simplicity, assume order matters
	// In real implementation might want set comparison
	for i := range a {
		if !reflect.DeepEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func TestJoinBuildCopyAnnotation(t *testing.T) {
	// Test that the copy tracking annotation is emitted when a handler is provided

	var copyEvent *annotations.Event
	opts := ExecutorOptions{
		Handler: func(e annotations.Event) {
			if e.Name == annotations.JoinBuildCopy {
				copyEvent = &e
			}
		},
	}

	// MaterializedRelation doesn't require copying (RequiresCopy() = false)
	leftSymbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	leftTuples := []Tuple{
		{1, "a"},
		{2, "b"},
		{3, "c"},
	}
	left := NewMaterializedRelationWithOptions(leftSymbols, leftTuples, opts)

	rightSymbols := []query.Symbol{datalog.NewSymbol("?y"), datalog.NewSymbol("?z")}
	rightTuples := []Tuple{
		{"a", 100},
		{"b", 200},
		{"d", 400},
	}
	right := NewMaterializedRelationWithOptions(rightSymbols, rightTuples, opts)

	// Perform the join
	joined := HashJoinWithOptions(left, right, []query.Symbol{datalog.NewSymbol("?y")}, opts)

	// Materialize to ensure build phase runs
	_ = collectTuples(joined)

	// Check that we got the copy tracking annotation
	if copyEvent == nil {
		t.Fatal("expected JoinBuildCopy annotation to be emitted")
	}
	copyCount, _ := copyEvent.Data["copied"].(int)
	skipCount, _ := copyEvent.Data["passthru"].(int)
	requiresCopy, _ := copyEvent.Data["requires_copy"].(bool)

	// MaterializedRelation doesn't require copying, so we should see passthru > 0, copied = 0
	if requiresCopy {
		t.Error("expected requires_copy=false for MaterializedRelation")
	}
	if copyCount != 0 {
		t.Errorf("expected 0 copies for MaterializedRelation, got %d", copyCount)
	}
	if skipCount == 0 {
		t.Errorf("expected some passthru copies for MaterializedRelation, got %d", skipCount)
	}

	t.Logf("Copy stats: copied=%d, passthru=%d, requires_copy=%v", copyCount, skipCount, requiresCopy)
}

func TestHashJoinEmitsStructuredStrategyBuildAndProbeAnnotations(t *testing.T) {
	// One capture per event under test.
	var strategy, build, probe *annotations.Event
	opts := ExecutorOptions{Handler: func(event annotations.Event) {
		switch event.Name {
		case annotations.JoinStrategy:
			strategy = &event
		case annotations.JoinBuild:
			build = &event
		case annotations.JoinProbe:
			probe = &event
		}
	}}
	joinSymbol := datalog.NewSymbol("?id")
	left := NewMaterializedRelationWithOptions(
		[]query.Symbol{joinSymbol, datalog.NewSymbol("?left")},
		[]Tuple{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}},
		opts,
	)
	right := NewMaterializedRelationWithOptions(
		[]query.Symbol{joinSymbol, datalog.NewSymbol("?right")},
		[]Tuple{{int64(1), "x"}, {int64(2), "y"}, {int64(4), "z"}},
		opts,
	)

	result := HashJoinWithOptions(left, right, []query.Symbol{joinSymbol}, opts)
	require.Len(t, collectTuples(result), 2)

	require.NotNil(t, strategy)
	require.Equal(t, "materialized", strategy.Data["mode"])
	require.Equal(t, "right", strategy.Data["build_side"])

	require.NotNil(t, build)
	require.Equal(t, 3, build.Data["tuple_count"])
	require.Equal(t, false, build.Data["join_key_unique"])

	require.NotNil(t, probe)
	require.Equal(t, 3, probe.Data["tuple_count"])
	require.Equal(t, 2, probe.Data["result_count"])
}

func TestJoinStrategyAnnotationDoesNotConsumeStreamingRelation(t *testing.T) {
	var events []annotations.Event
	handler := func(event annotations.Event) {
		events = append(events, event)
	}
	joinSymbol := datalog.NewSymbol("?id")
	base := NewMaterializedRelationFromSet(
		[]query.Symbol{joinSymbol},
		[]Tuple{{int64(1)}},
		ExecutorOptions{},
	)
	stream := NewStreamingRelationWithOptions(
		base.Symbols(),
		base.Iterator(),
		ExecutorOptions{Handler: handler},
	)

	emitJoinStrategyAnnotation(
		handler,
		stream,
		base,
		[]query.Symbol{joinSymbol},
		"streaming",
		"right",
		false,
	)

	require.False(t, stream.iteratorCalled,
		"emitting a join annotation must not consume a streaming relation")
	require.Len(t, events, 1)
	require.Equal(t, -1, events[0].Data["left_size"])
	require.Equal(t, 1, events[0].Data["right_size"])
}

func TestStreamingJoinProbeAnnotationLifecycle(t *testing.T) {
	join := datalog.NewSymbol("?join")
	leftValue := datalog.NewSymbol("?left")
	rightValue := datalog.NewSymbol("?right")
	open := func(handler annotations.Handler, probe Relation) Relation {
		left := NewMaterializedRelation(
			[]query.Symbol{join, leftValue},
			[]Tuple{{int64(1), "left"}},
		)
		return HashJoinWithOptions(left, probe, []query.Symbol{join}, ExecutorOptions{
			Handler:              handler,
			EnableStreamingJoins: true,
		})
	}
	probe := func() Relation {
		return NewMaterializedRelation(
			[]query.Symbol{join, rightValue},
			[]Tuple{{int64(1), "a"}, {int64(1), "b"}, {int64(1), "c"}},
		)
	}

	t.Run("partial close emits exactly once", func(t *testing.T) {
		var events []annotations.Event
		handler := func(event annotations.Event) {
			if event.Name == annotations.JoinProbe {
				events = append(events, event)
			}
		}
		it := open(handler, probe()).Iterator()
		require.True(t, it.Next())
		require.NoError(t, it.Close())
		require.NoError(t, it.Close())
		require.Len(t, events, 1)
		require.Equal(t, "streaming", events[0].Data["mode"])
		require.IsType(t, 0, events[0].Data["tuple_count"])
		require.IsType(t, 0, events[0].Data["matched_count"])
		require.IsType(t, 0, events[0].Data["result_count"])
		require.Equal(t, 1, events[0].Data["result_count"])
	})

	t.Run("exhaustion then close emits exactly once", func(t *testing.T) {
		var events []annotations.Event
		handler := func(event annotations.Event) {
			if event.Name == annotations.JoinProbe {
				events = append(events, event)
			}
		}
		it := open(handler, probe()).Iterator()
		for it.Next() {
		}
		require.NoError(t, it.Error())
		require.NoError(t, it.Close())
		require.Len(t, events, 1)
		require.Equal(t, 3, events[0].Data["result_count"])
	})

	t.Run("probe failure is emitted once", func(t *testing.T) {
		var events []annotations.Event
		handler := func(event annotations.Event) {
			if event.Name == annotations.JoinProbe {
				events = append(events, event)
			}
		}
		failingProbe := failingRelation{
			Relation:  probe(),
			failAfter: 1,
		}
		it := open(handler, failingProbe).Iterator()
		for it.Next() {
		}
		require.ErrorIs(t, it.Error(), errInjectedIterator)
		require.NoError(t, it.Close())
		require.Len(t, events, 1)
		require.Equal(t, 1, events[0].Data["result_count"])
	})
}

func TestConcurrentStructuredJoinAnnotations(t *testing.T) {
	var eventCount atomic.Int64
	var fieldMu sync.Mutex
	var malformed bool
	// Twenty workers emit through this one handler concurrently, and the engine
	// does not serialize handlers — so the counting and the flag carry their own
	// synchronization here, in the handler that holds them.
	handler := func(event annotations.Event) {
		if event.Name != annotations.JoinStrategy &&
			event.Name != annotations.JoinBuild &&
			event.Name != annotations.JoinProbe {
			return
		}
		eventCount.Add(1)
		fieldMu.Lock()
		defer fieldMu.Unlock()
		if event.Data == nil {
			malformed = true
		}
	}
	join := datalog.NewSymbol("?join")

	var wait sync.WaitGroup
	for worker := 0; worker < 20; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			left := NewMaterializedRelation(
				[]query.Symbol{join},
				[]Tuple{{int64(worker)}},
			)
			right := NewMaterializedRelation(
				[]query.Symbol{join},
				[]Tuple{{int64(worker)}},
			)
			result := HashJoinWithOptions(left, right, []query.Symbol{join}, ExecutorOptions{
				Handler: handler,
			})
			_, err := collectTypedTuples(result)
			require.NoError(t, err)
		}(worker)
	}
	wait.Wait()
	require.Equal(t, int64(60), eventCount.Load())
	fieldMu.Lock()
	require.False(t, malformed)
	fieldMu.Unlock()
}
