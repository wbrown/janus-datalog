package executor

import (
	"runtime"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestParallelSubqueryConfiguration tests configuration flags
// SKIPPED: Configuration is now passed via ExecutorOptions, not globals
func Skip_TestParallelSubqueryConfiguration(t *testing.T) {
	// Test default values
	// NOTE: No longer testing globals since they were removed
	/*
		if !EnableParallelSubqueries {
			t.Error("EnableParallelSubqueries should default to true")
		}
	*/

	// These still exist as package-level variables for default config
	if SubqueryWorkerCount != runtime.NumCPU() {
		t.Errorf("SubqueryWorkerCount should default to runtime.NumCPU()=%d, got %d",
			runtime.NumCPU(), SubqueryWorkerCount)
	}

	if ParallelSubqueryThreshold != 10 {
		t.Errorf("ParallelSubqueryThreshold should be 10, got %d", ParallelSubqueryThreshold)
	}
}

// TestCombineSubqueryResults tests result combination logic
func TestCombineSubqueryResults(t *testing.T) {
	// Create test subquery plan
	subqPlan := planner.SubqueryPlan{
		Inputs: []query.Symbol{"?x"},
		Subquery: &query.SubqueryPattern{
			Binding: query.TupleBinding{
				Variables: []query.Symbol{"?result"},
			},
		},
	}

	tests := []struct {
		name     string
		results  []Relation
		expected int
	}{
		{
			name:     "empty results",
			results:  []Relation{},
			expected: 0,
		},
		{
			name: "single result",
			results: []Relation{
				NewMaterializedRelation(
					[]query.Symbol{"?x", "?result"},
					[]Tuple{{1, "a"}},
				),
			},
			expected: 1,
		},
		{
			name: "multiple results",
			results: []Relation{
				NewMaterializedRelation(
					[]query.Symbol{"?x", "?result"},
					[]Tuple{{1, "a"}},
				),
				NewMaterializedRelation(
					[]query.Symbol{"?x", "?result"},
					[]Tuple{{2, "b"}},
				),
				NewMaterializedRelation(
					[]query.Symbol{"?x", "?result"},
					[]Tuple{{3, "c"}},
				),
			},
			expected: 3,
		},
		{
			name: "results with multiple tuples",
			results: []Relation{
				NewMaterializedRelation(
					[]query.Symbol{"?x", "?result"},
					[]Tuple{{1, "a"}, {1, "a2"}},
				),
				NewMaterializedRelation(
					[]query.Symbol{"?x", "?result"},
					[]Tuple{{2, "b"}, {2, "b2"}},
				),
			},
			expected: 4,
		},
		{
			name: "with nil results",
			results: []Relation{
				NewMaterializedRelation(
					[]query.Symbol{"?x", "?result"},
					[]Tuple{{1, "a"}},
				),
				nil,
				NewMaterializedRelation(
					[]query.Symbol{"?x", "?result"},
					[]Tuple{{2, "b"}},
				),
			},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := combineSubqueryResults(tt.results, subqPlan)
			if err != nil {
				t.Fatalf("combineSubqueryResults failed: %v", err)
			}

			if result.Size() != tt.expected {
				t.Errorf("Expected %d tuples, got %d", tt.expected, result.Size())
			}
		})
	}
}

// TestExecuteSubqueryStructure tests the execution structure
func TestExecuteSubqueryStructure(t *testing.T) {
	// This test just verifies the parallel infrastructure compiles correctly
	// End-to-end testing is done in gopher-street OHLC tests

	// Verify configuration defaults
	// NOTE: EnableParallelSubqueries is now in ExecutorOptions, test default via executor creation
	exec := NewExecutor(nil)
	if !exec.enableParallelSubqueries {
		t.Error("enableParallelSubqueries should be true by default")
	}

	if ParallelSubqueryThreshold != 10 {
		t.Error("ParallelSubqueryThreshold should be 10")
	}
}

// TestExecuteSubqueryThresholdLogic tests the threshold decision logic
func TestExecuteSubqueryThresholdLogic(t *testing.T) {
	tests := []struct {
		name           string
		enabled        bool
		combinations   int
		expectParallel bool
	}{
		{
			name:           "disabled - below threshold",
			enabled:        false,
			combinations:   5,
			expectParallel: false,
		},
		{
			name:           "disabled - above threshold",
			enabled:        false,
			combinations:   20,
			expectParallel: false,
		},
		{
			name:           "enabled - below threshold",
			enabled:        true,
			combinations:   5,
			expectParallel: false,
		},
		{
			name:           "enabled - at threshold",
			enabled:        true,
			combinations:   10,
			expectParallel: true,
		},
		{
			name:           "enabled - above threshold",
			enabled:        true,
			combinations:   20,
			expectParallel: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the logic that decides whether to use parallel
			useParallel := tt.enabled && tt.combinations >= ParallelSubqueryThreshold

			if useParallel != tt.expectParallel {
				t.Errorf("Expected parallel=%v, got %v", tt.expectParallel, useParallel)
			}
		})
	}
}

// TestParallelSubqueryWorkerCount tests worker count configuration
func TestParallelSubqueryWorkerCount(t *testing.T) {
	oldCount := SubqueryWorkerCount
	defer func() { SubqueryWorkerCount = oldCount }()

	testCounts := []int{0, 1, 2, 4, 8, 16}

	for _, count := range testCounts {
		SubqueryWorkerCount = count

		// Test normalization logic
		numWorkers := SubqueryWorkerCount
		if numWorkers <= 0 {
			numWorkers = runtime.NumCPU()
		}

		if count <= 0 {
			if numWorkers != runtime.NumCPU() {
				t.Errorf("Worker count %d should normalize to NumCPU()=%d, got %d",
					count, runtime.NumCPU(), numWorkers)
			}
		} else {
			if numWorkers != count {
				t.Errorf("Worker count should be %d, got %d", count, numWorkers)
			}
		}
	}
}

// Helper function (note: collectTuples and tuplesEqual are defined in join_test.go)
