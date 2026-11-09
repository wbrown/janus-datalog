package executor

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog/query"

	"github.com/wbrown/janus-datalog/datalog"
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

			// Run over a materialized relation
			matTime := measureScenario(t, scenario.dataSize, scenario.filterRatio, scenario.operations, false)

			// Run over a streaming relation
			streamTime := measureScenario(t, scenario.dataSize, scenario.filterRatio, scenario.operations, true)

			// Calculate improvement
			improvement := float64(matTime) / float64(streamTime)
			fmt.Printf("  Results:\n")
			fmt.Printf("    Materialized: %d ns\n", matTime)
			fmt.Printf("    Streaming:    %d ns\n", streamTime)
			fmt.Printf("    Improvement:  %.2fx faster\n", improvement)

			// Memory estimation
			expectedMemReduction := (1.0 - scenario.filterRatio) * 100
			fmt.Printf("    Est. Memory Reduction: %.0f%%\n", expectedMemReduction)

			want := expectedPipelineResult(scenario.dataSize, scenario.filterRatio, scenario.operations)
			for _, streaming := range []bool{false, true} {
				symbols, tuples := runPipeline(t, scenario.dataSize, scenario.filterRatio, scenario.operations, streaming)
				id, score := query.SymbolIndex(symbols, datalog.NewSymbol("?id")), query.SymbolIndex(symbols, datalog.NewSymbol("?score"))
				if len(symbols) != 2 || id < 0 || score < 0 {
					t.Fatalf("the pipeline projects ?id and ?score; got %v", symbols)
				}
				got := make([]Tuple, len(tuples))
				for i, tuple := range tuples {
					got[i] = Tuple{tuple[id], tuple[score]}
				}
				assert.ElementsMatch(t, want, got, "streaming %v", streaming)
			}
		})
	}

	fmt.Println("\n=== END DEMONSTRATION ===")
}

// measureScenario is the mean time of a hundred runs of the pipeline, over a
// StreamingRelation when streaming is set and a MaterializedRelation
// otherwise.
func measureScenario(t *testing.T, size int, filterRatio float64, ops []string, streaming bool) int64 {
	// Run multiple iterations for stable measurement
	iterations := 100
	var totalNs int64

	for i := 0; i < iterations; i++ {
		start := nanoTime()
		runPipeline(t, size, filterRatio, ops, streaming)
		totalNs += nanoTime() - start
	}

	return totalNs / int64(iterations)
}

// runPipeline returns the symbols and tuples of the pipeline's answer, run
// over a StreamingRelation when streaming is set and a MaterializedRelation
// otherwise, the two real relation kinds.
func runPipeline(t *testing.T, size int, filterRatio float64, ops []string, streaming bool) ([]query.Symbol, []Tuple) {
	// Create test data
	var tuples []Tuple
	for i := 0; i < size; i++ {
		tuples = append(tuples, Tuple{i, fmt.Sprintf("name%d", i), i * 10, i * 100})
	}
	symbols := []query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?name"), datalog.NewSymbol("?score"), datalog.NewSymbol("?value")}

	var current Relation
	if streaming {
		current = NewStreamingRelation(symbols, &sliceIterator{tuples: tuples, pos: -1})
	} else {
		current = NewMaterializedRelation(symbols, tuples)
	}
	for _, op := range ops {
		switch op {
		case "filter", "filter1":
			// Filter based on ratio
			threshold := int64(float64(size) * filterRatio)
			current = current.FilterWithPredicate(&query.Comparison{
				Op:    datalog.SymLT,
				Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?id")},
				Right: query.ConstantTerm{Value: threshold},
			})

		case "filter2":
			// Second filter (score > 100)
			current = current.FilterWithPredicate(&query.Comparison{
				Op:    datalog.SymGT,
				Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?score")},
				Right: query.ConstantTerm{Value: int64(100)},
			})

		case "project":
			// Project to subset of symbols
			projected, err := current.Project([]query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?score")})
			assert.NoError(t, err)
			current = projected

		case "join":
			// The join side holds filterRatio of the ids below size/10, read
			// per hundred ids so a ratio under a tenth still keeps some.
			var joinTuples []Tuple
			for i := 0; i < size/10; i++ {
				if i%100 < int(filterRatio*100) {
					joinTuples = append(joinTuples, Tuple{i, fmt.Sprintf("city%d", i)})
				}
			}
			joinIter := &sliceIterator{tuples: joinTuples, pos: -1}
			joinRel := NewStreamingRelation([]query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?city")}, joinIter)
			current = HashJoin(current, joinRel, []query.Symbol{datalog.NewSymbol("?id")})
		}
	}

	it := current.Iterator()
	var answer []Tuple
	for it.Next() {
		tuple := it.Tuple()
		if it.RequiresCopy() {
			tuple = append(Tuple(nil), tuple...)
		}
		answer = append(answer, tuple)
	}
	if err := it.Error(); err != nil {
		t.Fatalf("pipeline %v: %v", ops, err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("pipeline %v: close: %v", ops, err)
	}
	return current.Symbols(), answer
}

// expectedPipelineResult computes runPipeline's answer in Go over the same
// fixture: each ?id the filters keep and the join side holds, with its
// ?score.
func expectedPipelineResult(size int, filterRatio float64, ops []string) []Tuple {
	want := []Tuple{}
	for i := 0; i < size; i++ {
		kept := true
		for _, op := range ops {
			switch op {
			case "filter", "filter1":
				kept = kept && int64(i) < int64(float64(size)*filterRatio)
			case "filter2":
				kept = kept && i*10 > 100
			case "join":
				kept = kept && i < size/10 && i%100 < int(filterRatio*100)
			}
		}
		if kept {
			want = append(want, Tuple{i, i * 10})
		}
	}
	return want
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
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkScenario(b, sc.dataSize, sc.filter, false)
			}
		})

		b.Run(sc.name+"_Streaming", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkScenario(b, sc.dataSize, sc.filter, true)
			}
		})
	}
}

// benchmarkScenario runs the filter-and-project pipeline once, over a
// StreamingRelation when streaming is set and a MaterializedRelation
// otherwise, the two real relation kinds.
func benchmarkScenario(b *testing.B, size int, filterRatio float64, streaming bool) {
	var tuples []Tuple
	for i := 0; i < size; i++ {
		tuples = append(tuples, Tuple{i, i * 10, i * 100})
	}
	symbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?z")}

	var rel Relation
	if streaming {
		rel = NewStreamingRelation(symbols, &sliceIterator{tuples: tuples, pos: -1})
	} else {
		rel = NewMaterializedRelation(symbols, tuples)
	}

	// Apply aggressive filter
	threshold := int64(float64(size) * filterRatio)
	filtered := rel.FilterWithPredicate(&query.Comparison{
		Op:    datalog.SymLT,
		Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?x")},
		Right: query.ConstantTerm{Value: threshold},
	})

	projected, err := filtered.Project([]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?z")})
	if err != nil {
		b.Fatalf("project: %v", err)
	}
	xi := query.SymbolIndex(projected.Symbols(), datalog.NewSymbol("?x"))
	if xi < 0 {
		b.Fatalf("the projection carries ?x; got %v", projected.Symbols())
	}

	// Each tuple's ?x passes the filter, and every ?x below the threshold
	// comes through.
	it := projected.Iterator()
	count := 0
	for it.Next() {
		if x := it.Tuple()[xi].(int); int64(x) >= threshold {
			b.Fatalf("?x %d passed the filter ?x < %d", x, threshold)
		}
		count++
	}
	if err := it.Error(); err != nil {
		b.Fatalf("iterate: %v", err)
	}
	if err := it.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}
	if int64(count) != threshold {
		b.Fatalf("%d tuples passed the filter, want %d", count, threshold)
	}
}

// TestMemoryCharacteristics runs two filters and a projection over tuples of
// ten symbols, where tuple i holds i*10+k at ?sk, on a materialized and on a
// streaming relation, and checks each answer: the filters keep tuples 0
// through outputSize-1 whole, and the projection keeps every tuple's ?s0 and
// ?s1.
func TestMemoryCharacteristics(t *testing.T) {
	fmt.Println("\n=== MEMORY CHARACTERISTICS ===")

	symbols := make([]query.Symbol, 10)
	for k := range symbols {
		symbols[k] = datalog.NewSymbol(fmt.Sprintf("?s%d", k))
	}
	// below keeps the tuples whose ?s0 is under limit*10: tuples 0 through
	// limit-1.
	below := func(limit int) func(Relation) (Relation, error) {
		return func(rel Relation) (Relation, error) {
			return rel.FilterWithPredicate(&query.Comparison{
				Op:    datalog.SymLT,
				Left:  query.VariableTerm{Symbol: symbols[0]},
				Right: query.ConstantTerm{Value: int64(limit * 10)},
			}), nil
		}
	}

	testCases := []struct {
		name       string
		inputSize  int
		outputSize int
		desc       string
		operate    func(Relation) (Relation, error)
		width      int // the answer carries the first width symbols
	}{
		{
			"Aggressive Filter",
			100000,
			100,
			"Filter 100K tuples to 100 (0.1% selectivity)",
			below(100),
			10,
		},
		{
			"Moderate Filter",
			10000,
			1000,
			"Filter 10K tuples to 1K (10% selectivity)",
			below(1000),
			10,
		},
		{
			"Projection Only",
			5000,
			5000,
			"Project 5K tuples from 10 symbols to 2 symbols",
			func(rel Relation) (Relation, error) { return rel.Project(symbols[:2]) },
			2,
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

		input := make([]Tuple, tc.inputSize)
		for i := range input {
			input[i] = make(Tuple, len(symbols))
			for k := range symbols {
				input[i][k] = i*10 + k
			}
		}
		want := make([]Tuple, tc.outputSize)
		for i := range want {
			want[i] = make(Tuple, tc.width)
			for k := range want[i] {
				want[i][k] = i*10 + k
			}
		}

		for _, streaming := range []bool{false, true} {
			var rel Relation
			if streaming {
				rel = NewStreamingRelation(symbols, &sliceIterator{tuples: input, pos: -1})
			} else {
				rel = NewMaterializedRelation(symbols, input)
			}
			answer, err := tc.operate(rel)
			if err != nil {
				t.Fatalf("%s, streaming %v: %v", tc.name, streaming, err)
			}
			if len(answer.Symbols()) != tc.width {
				t.Fatalf("%s, streaming %v: the answer carries %v; want %v",
					tc.name, streaming, answer.Symbols(), symbols[:tc.width])
			}
			positions := make([]int, tc.width)
			for k, sym := range symbols[:tc.width] {
				positions[k] = query.SymbolIndex(answer.Symbols(), sym)
				if positions[k] < 0 {
					t.Fatalf("%s, streaming %v: the answer carries %v; want %v",
						tc.name, streaming, answer.Symbols(), symbols[:tc.width])
				}
			}
			tuples, err := CollectTuples(answer, nil)
			if err != nil {
				t.Fatalf("%s, streaming %v: %v", tc.name, streaming, err)
			}
			got := make([]Tuple, len(tuples))
			for i, tuple := range tuples {
				got[i] = make(Tuple, tc.width)
				for k, position := range positions {
					got[i][k] = tuple[position]
				}
			}
			assert.ElementsMatch(t, want, got, "%s, streaming %v", tc.name, streaming)
		}
	}

	fmt.Println("=== END MEMORY ANALYSIS ===")
}
