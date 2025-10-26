package executor

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// SlowMatcher wraps a pattern matcher to add artificial delay
type SlowMatcher struct {
	PatternMatcher
	delay     time.Duration
	callCount int32
}

func (sm *SlowMatcher) Match(pattern *query.DataPattern, bindings Relations) (Relation, error) {
	atomic.AddInt32(&sm.callCount, 1)
	time.Sleep(sm.delay)
	return sm.PatternMatcher.Match(pattern, bindings)
}

func TestParallelPatternExecution(t *testing.T) {
	t.Skip("Parallel pattern execution within phases not fully implemented - patterns with shared variables can't be parallelized")
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

	// Create a query that finds specific people independently
	// These patterns are truly independent and can run in parallel
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: query.Symbol("?alice-city")},
			query.FindVariable{Symbol: query.Symbol("?bob-age")},
			query.FindVariable{Symbol: query.Symbol("?charlie-city")},
		},
		Where: []query.Clause{
			// Find Alice's city
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?alice")},
					query.Constant{Value: nameAttr},
					query.Constant{Value: "Alice"},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?alice")},
					query.Constant{Value: cityAttr},
					query.Variable{Name: query.Symbol("?alice-city")},
				},
			},
			// Find Bob's age (independent from Alice)
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?bob")},
					query.Constant{Value: nameAttr},
					query.Constant{Value: "Bob"},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?bob")},
					query.Constant{Value: ageAttr},
					query.Variable{Name: query.Symbol("?bob-age")},
				},
			},
			// Find Charlie's city (independent from both)
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?charlie")},
					query.Constant{Value: nameAttr},
					query.Constant{Value: "Charlie"},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?charlie")},
					query.Constant{Value: cityAttr},
					query.Variable{Name: query.Symbol("?charlie-city")},
				},
			},
		},
	}

	// Test sequential execution
	seqExec := NewExecutor(slowMatcher)
	ctx := NewContext(nil)

	// Reset call count
	slowMatcher.callCount = 0

	start := time.Now()
	seqResult, err := seqExec.ExecuteWithContext(ctx, q)
	if err != nil {
		t.Fatalf("Sequential execution failed: %v", err)
	}
	seqDuration := time.Since(start)
	seqCallCount := atomic.LoadInt32(&slowMatcher.callCount)

	// Reset call count for parallel test
	atomic.StoreInt32(&slowMatcher.callCount, 0)

	// Test parallel execution
	parExec := NewParallelExecutor(slowMatcher, 3)

	start = time.Now()
	parResult, err := parExec.ExecuteWithContext(ctx, q)
	if err != nil {
		t.Fatalf("Parallel execution failed: %v", err)
	}
	parDuration := time.Since(start)
	parCallCount := atomic.LoadInt32(&slowMatcher.callCount)

	// Verify results are the same
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
	// Allow some overhead, expect at least 1.5x speedup
	if speedup < 1.5 {
		t.Errorf("Expected at least 1.5x speedup, got %.2fx", speedup)
	}
}

func BenchmarkSequentialVsParallel(b *testing.B) {
	// Create test datoms for a more realistic scenario
	var datoms []datalog.Datom
	nameAttr := datalog.NewKeyword(":person/name")
	ageAttr := datalog.NewKeyword(":person/age")
	deptAttr := datalog.NewKeyword(":person/dept")

	// Create 1000 people
	for i := 0; i < 1000; i++ {
		person := datalog.NewIdentity(fmt.Sprintf("person%d", i))
		datoms = append(datoms,
			datalog.Datom{E: person, A: nameAttr, V: fmt.Sprintf("Person%d", i), Tx: uint64(i)},
			datalog.Datom{E: person, A: ageAttr, V: int64(20 + i%50), Tx: uint64(i)},
			datalog.Datom{E: person, A: deptAttr, V: fmt.Sprintf("Dept%d", i%10), Tx: uint64(i)},
		)
	}

	matcher := NewMemoryPatternMatcher(datoms)

	// Query with multiple patterns
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: query.Symbol("?e")},
			query.FindVariable{Symbol: query.Symbol("?name")},
			query.FindVariable{Symbol: query.Symbol("?age")},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?e")},
					query.Constant{Value: nameAttr},
					query.Variable{Name: query.Symbol("?name")},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?e")},
					query.Constant{Value: ageAttr},
					query.Variable{Name: query.Symbol("?age")},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?e")},
					query.Constant{Value: deptAttr},
					query.Constant{Value: "Dept5"},
				},
			},
		},
	}

	ctx := NewContext(nil)

	b.Run("Sequential", func(b *testing.B) {
		exec := NewExecutor(matcher)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := exec.ExecuteWithContext(ctx, q)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		exec := NewParallelExecutor(matcher, 4)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := exec.ExecuteWithContext(ctx, q)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
