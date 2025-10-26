package executor

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestStreamingPerformanceDemo demonstrates the performance impact of streaming
func TestStreamingPerformanceDemo(t *testing.T) {
	fmt.Println("\n=== STREAMING PERFORMANCE DEMONSTRATION ===")

	// Test scenarios with different selectivities and operations
	scenarios := []struct {
		name        string
		dataSize    int
		filterRatio float64 // What percentage passes filter
		operations  []string
	}{
		{"High Selectivity (1% pass)", 10000, 0.01, []string{"filter", "project"}},
		{"Medium Selectivity (10% pass)", 10000, 0.10, []string{"filter", "project"}},
		{"Low Selectivity (50% pass)", 10000, 0.50, []string{"filter", "project"}},
		{"Complex Pipeline", 5000, 0.05, []string{"filter", "join", "project"}},
		{"Multiple Filters", 10000, 0.20, []string{"filter1", "filter2", "project"}},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			fmt.Printf("\nScenario: %s\n", scenario.name)
			fmt.Printf("  Data Size: %d tuples\n", scenario.dataSize)
			fmt.Printf("  Filter Ratio: %.0f%%\n", scenario.filterRatio*100)
			fmt.Printf("  Operations: %v\n", scenario.operations)

			// Run with materialized (streaming disabled)
			matTime := runScenarioMaterialized(t, scenario.dataSize, scenario.filterRatio, scenario.operations)

			// Run with streaming enabled
			streamTime := runScenarioStreaming(t, scenario.dataSize, scenario.filterRatio, scenario.operations)

			// Calculate improvement
			improvement := float64(matTime) / float64(streamTime)
			fmt.Printf("  Results:\n")
			fmt.Printf("    Materialized: %d ns\n", matTime)
			fmt.Printf("    Streaming:    %d ns\n", streamTime)
			fmt.Printf("    Improvement:  %.2fx faster\n", improvement)

			// Memory estimation
			expectedMemReduction := (1.0 - scenario.filterRatio) * 100
			fmt.Printf("    Est. Memory Reduction: %.0f%%\n", expectedMemReduction)
		})
	}

	fmt.Println("\n=== END DEMONSTRATION ===")
}

func runScenarioMaterialized(t *testing.T, size int, filterRatio float64, ops []string) int64 {
	// Create options with streaming disabled
	opts := ExecutorOptions{
		EnableIteratorComposition: false,
		EnableTrueStreaming:       false,
	}

	return measureScenarioWithOpts(t, size, filterRatio, ops, opts)
}

func runScenarioStreaming(t *testing.T, size int, filterRatio float64, ops []string) int64 {
	// Create options with streaming enabled
	opts := ExecutorOptions{
		EnableIteratorComposition: true,
		EnableTrueStreaming:       true,
	}

	return measureScenarioWithOpts(t, size, filterRatio, ops, opts)
}

func measureScenarioWithOpts(t *testing.T, size int, filterRatio float64, ops []string, opts ExecutorOptions) int64 {
	// Run multiple iterations for stable measurement
	iterations := 100
	var totalNs int64

	for i := 0; i < iterations; i++ {
		start := nanoTime()
		runPipeline(t, size, filterRatio, ops, opts)
		totalNs += nanoTime() - start
	}

	return totalNs / int64(iterations)
}

func runPipeline(t *testing.T, size int, filterRatio float64, ops []string, opts ExecutorOptions) {
	// Create test data
	var tuples []Tuple
	for i := 0; i < size; i++ {
		tuples = append(tuples, Tuple{i, fmt.Sprintf("name%d", i), i * 10, i * 100})
	}
	columns := []query.Symbol{"?id", "?name", "?score", "?value"}

	// Create initial relation with options
	source := newMockIterator(tuples)
	rel := NewStreamingRelationWithOptions(columns, source, opts)

	// Apply operations
	var current Relation = rel
	for _, op := range ops {
		switch op {
		case "filter", "filter1":
			// Filter based on ratio
			threshold := int(float64(size) * filterRatio)
			filter := NewSimpleFilter(func(t Tuple) bool {
				return t[0].(int) < threshold
			})
			current = current.Filter(filter)

		case "filter2":
			// Second filter (score > 100)
			filter := NewSimpleFilter(func(t Tuple) bool {
				return t[2].(int) > 100
			})
			current = current.Filter(filter)

		case "project":
			// Project to subset of columns
			projected, err := current.Project([]query.Symbol{"?id", "?score"})
			assert.NoError(t, err)
			current = projected

		case "join":
			// Create a small join relation
			var joinTuples []Tuple
			for i := 0; i < size/10; i++ {
				if i%10 < int(filterRatio*10) {
					joinTuples = append(joinTuples, Tuple{i, fmt.Sprintf("city%d", i)})
				}
			}
			joinIter := newMockIterator(joinTuples)
			joinRel := NewStreamingRelation([]query.Symbol{"?id", "?city"}, joinIter)
			current = HashJoin(current, joinRel, []query.Symbol{"?id"})
		}
	}

	// Consume results
	it := current.Iterator()
	count := 0
	for it.Next() {
		count++
		_ = it.Tuple()
	}
	it.Close()
}

// nanoTime returns current time in nanoseconds
func nanoTime() int64 {
	return time.Now().UnixNano()
}

// BenchmarkStreamingScenarios provides detailed benchmarks
func BenchmarkStreamingScenarios(b *testing.B) {
	scenarios := []struct {
		name     string
		dataSize int
		filter   float64
	}{
		{"Small_HighSelectivity", 1000, 0.01},
		{"Medium_HighSelectivity", 5000, 0.01},
		{"Large_HighSelectivity", 10000, 0.01},
		{"Large_MediumSelectivity", 10000, 0.10},
		{"Large_LowSelectivity", 10000, 0.50},
	}

	for _, sc := range scenarios {
		b.Run(sc.name+"_Materialized", func(b *testing.B) {
			opts := ExecutorOptions{
				EnableIteratorComposition: false,
				EnableTrueStreaming:       false,
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkScenarioWithOpts(b, sc.dataSize, sc.filter, opts)
			}
		})

		b.Run(sc.name+"_Streaming", func(b *testing.B) {
			opts := ExecutorOptions{
				EnableIteratorComposition: true,
				EnableTrueStreaming:       true,
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkScenarioWithOpts(b, sc.dataSize, sc.filter, opts)
			}
		})
	}
}

func benchmarkScenarioWithOpts(b *testing.B, size int, filterRatio float64, opts ExecutorOptions) {
	var tuples []Tuple
	for i := 0; i < size; i++ {
		tuples = append(tuples, Tuple{i, i * 10, i * 100})
	}
	columns := []query.Symbol{"?x", "?y", "?z"}

	source := newMockIterator(tuples)
	rel := NewStreamingRelationWithOptions(columns, source, opts)

	// Apply aggressive filter
	threshold := int(float64(size) * filterRatio)
	filtered := rel.Filter(NewSimpleFilter(func(t Tuple) bool {
		return t[0].(int) < threshold
	}))

	// Project
	projected, _ := filtered.Project([]query.Symbol{"?x", "?z"})

	// Consume
	it := projected.Iterator()
	for it.Next() {
		_ = it.Tuple()
	}
	it.Close()
}

// TestMemoryCharacteristics demonstrates memory usage patterns
func TestMemoryCharacteristics(t *testing.T) {
	fmt.Println("\n=== MEMORY CHARACTERISTICS ===")

	testCases := []struct {
		name       string
		inputSize  int
		outputSize int
		desc       string
	}{
		{
			"Aggressive Filter",
			100000,
			100,
			"Filter 100K tuples to 100 (0.1% selectivity)",
		},
		{
			"Moderate Filter",
			10000,
			1000,
			"Filter 10K tuples to 1K (10% selectivity)",
		},
		{
			"Projection Only",
			5000,
			5000,
			"Project 5K tuples from 10 cols to 2 cols",
		},
	}

	for _, tc := range testCases {
		fmt.Printf("%s:\n", tc.name)
		fmt.Printf("  %s\n", tc.desc)

		// Calculate memory impact
		matMemory := tc.inputSize * 10 * 8 // Assume 10 fields, 8 bytes each
		streamMemory := tc.outputSize * 10 * 8
		reduction := float64(matMemory-streamMemory) / float64(matMemory) * 100

		fmt.Printf("  Materialized Memory: ~%d KB\n", matMemory/1024)
		fmt.Printf("  Streaming Memory:    ~%d KB\n", streamMemory/1024)
		fmt.Printf("  Memory Reduction:    %.1f%%\n", reduction)
		fmt.Println()
	}

	fmt.Println("=== END MEMORY ANALYSIS ===")
}
