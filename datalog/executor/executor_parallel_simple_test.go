package executor

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestParallelExecutorWithManualPhases tests parallel execution with explicitly constructed phases
func TestParallelExecutorWithManualPhases(t *testing.T) {
	// Create test datoms
	person1 := datalog.NewIdentity("person1")
	person2 := datalog.NewIdentity("person2")
	person3 := datalog.NewIdentity("person3")

	nameAttr := datalog.NewKeyword(":person/name")
	ageAttr := datalog.NewKeyword(":person/age")
	cityAttr := datalog.NewKeyword(":person/city")

	datoms := []datalog.Datom{
		{E: person1, A: nameAttr, V: "Alice", Tx: 1},
		{E: person1, A: ageAttr, V: int64(30), Tx: 1},
		{E: person1, A: cityAttr, V: "NYC", Tx: 1},
		{E: person2, A: nameAttr, V: "Bob", Tx: 2},
		{E: person2, A: ageAttr, V: int64(25), Tx: 2},
		{E: person2, A: cityAttr, V: "SF", Tx: 2},
		{E: person3, A: nameAttr, V: "Charlie", Tx: 3},
		{E: person3, A: ageAttr, V: int64(35), Tx: 3},
		{E: person3, A: cityAttr, V: "LA", Tx: 3},
	}

	// Create a slow matcher that delays 100ms per pattern
	baseMatcher := NewMemoryPatternMatcher(datoms)
	slowMatcher := &SlowMatcher{
		PatternMatcher: baseMatcher,
		delay:          100 * time.Millisecond,
	}

	// Create a phase with multiple patterns that share a variable
	// This simulates what the planner would create for patterns that can be in the same phase
	phase := &planner.Phase{
		Patterns: []planner.PatternPlan{
			{
				Pattern: &query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: query.Symbol("?e")},
						query.Constant{Value: nameAttr},
						query.Variable{Name: query.Symbol("?name")},
					},
				},
			},
			{
				Pattern: &query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: query.Symbol("?e")},
						query.Constant{Value: ageAttr},
						query.Variable{Name: query.Symbol("?age")},
					},
				},
			},
			{
				Pattern: &query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: query.Symbol("?e")},
						query.Constant{Value: cityAttr},
						query.Variable{Name: query.Symbol("?city")},
					},
				},
			},
		},
		Provides: []query.Symbol{"?e", "?name", "?age", "?city"},
	}

	ctx := NewContext(nil)

	// Test sequential execution of the phase
	seqExec := NewExecutor(slowMatcher)
	atomic.StoreInt32(&slowMatcher.callCount, 0)

	start := time.Now()
	seqResult, err := seqExec.executePhaseSequential(ctx, phase, 0, nil)
	if err != nil {
		t.Fatalf("Sequential phase execution failed: %v", err)
	}
	seqDuration := time.Since(start)
	seqCallCount := atomic.LoadInt32(&slowMatcher.callCount)

	// Test parallel execution of the phase
	parExec := NewParallelExecutor(slowMatcher, 3)
	atomic.StoreInt32(&slowMatcher.callCount, 0)

	start = time.Now()
	parResult, err := parExec.executePhaseParallel(ctx, phase, 0, nil)
	if err != nil {
		t.Fatalf("Parallel phase execution failed: %v", err)
	}
	parDuration := time.Since(start)
	parCallCount := atomic.LoadInt32(&slowMatcher.callCount)

	// Verify results are the same size
	if seqResult.Size() != parResult.Size() {
		t.Errorf("Result sizes differ: sequential=%d, parallel=%d", seqResult.Size(), parResult.Size())
	}

	// Verify parallel is faster
	speedup := float64(seqDuration) / float64(parDuration)
	t.Logf("Sequential: %v (calls: %d)", seqDuration, seqCallCount)
	t.Logf("Parallel:   %v (calls: %d)", parDuration, parCallCount)
	t.Logf("Speedup:    %.2fx", speedup)

	// With 3 patterns and 100ms delay each, sequential should take ~300ms
	// Parallel should take ~100ms (all in parallel), giving ~3x speedup
	// Allow some overhead, expect at least 2x speedup
	if speedup < 2.0 {
		t.Errorf("Expected at least 2x speedup, got %.2fx", speedup)
	}

	// Verify both made the same number of pattern match calls
	if seqCallCount != parCallCount {
		t.Errorf("Call counts differ: sequential=%d, parallel=%d", seqCallCount, parCallCount)
	}
}
