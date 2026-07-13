package executor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestExecuteAggregations(t *testing.T) {
	// Create test data
	symbols := []query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?age"), datalog.NewSymbol("?score")}
	tuples := []Tuple{
		{"Alice", int64(30), 85.5},
		{"Bob", int64(25), 92.0},
		{"Charlie", int64(35), 78.5},
		{"Dave", int64(25), 88.0},
	}
	rel := NewMaterializedRelation(symbols, tuples)

	tests := []struct {
		name            string
		findElements    []query.FindElement
		expectedSymbols []query.Symbol
		expectedRows    int
		validate        func(*testing.T, Relation)
	}{
		{
			name: "no aggregates - just projection",
			findElements: []query.FindElement{
				query.FindVariable{Symbol: datalog.NewSymbol("?name")},
				query.FindVariable{Symbol: datalog.NewSymbol("?age")},
			},
			expectedSymbols: []query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?age")},
			expectedRows:    4,
			validate: func(t *testing.T, result Relation) {
				// Should have all 4 tuples with just name and age
				if result.Size() != 4 {
					t.Errorf("expected 4 tuples, got %d", result.Size())
				}
			},
		},
		{
			name: "single aggregation - count",
			findElements: []query.FindElement{
				query.FindAggregate{Function: "count", Arg: datalog.NewSymbol("?name")},
			},
			expectedSymbols: []query.Symbol{datalog.NewSymbol("(count ?name)")},
			expectedRows:    1,
			validate: func(t *testing.T, result Relation) {
				it := result.Iterator()
				defer it.Close()
				if it.Next() {
					tuple := it.Tuple()
					if count, ok := tuple[0].(int64); !ok || count != 4 {
						t.Errorf("expected count of 4, got %v", tuple[0])
					}
				}
			},
		},
		{
			name: "single aggregation - avg",
			findElements: []query.FindElement{
				query.FindAggregate{Function: "avg", Arg: datalog.NewSymbol("?age")},
			},
			expectedSymbols: []query.Symbol{datalog.NewSymbol("(avg ?age)")},
			expectedRows:    1,
			validate: func(t *testing.T, result Relation) {
				it := result.Iterator()
				defer it.Close()
				if it.Next() {
					tuple := it.Tuple()
					if avg, ok := tuple[0].(float64); !ok || avg != 28.75 {
						t.Errorf("expected avg of 28.75, got %v", tuple[0])
					}
				}
			},
		},
		{
			name: "single aggregation - max",
			findElements: []query.FindElement{
				query.FindAggregate{Function: "max", Arg: datalog.NewSymbol("?score")},
			},
			expectedSymbols: []query.Symbol{datalog.NewSymbol("(max ?score)")},
			expectedRows:    1,
			validate: func(t *testing.T, result Relation) {
				it := result.Iterator()
				defer it.Close()
				if it.Next() {
					tuple := it.Tuple()
					if max, ok := tuple[0].(float64); !ok || max != 92.0 {
						t.Errorf("expected max of 92.0, got %v", tuple[0])
					}
				}
			},
		},
		{
			name: "grouped aggregation - age groups",
			findElements: []query.FindElement{
				query.FindVariable{Symbol: datalog.NewSymbol("?age")},
				query.FindAggregate{Function: "count", Arg: datalog.NewSymbol("?name")},
				query.FindAggregate{Function: "avg", Arg: datalog.NewSymbol("?score")},
			},
			expectedSymbols: []query.Symbol{datalog.NewSymbol("?age"), datalog.NewSymbol("(count ?name)"), datalog.NewSymbol("(avg ?score)")},
			expectedRows:    3, // 3 unique ages: 25, 30, 35
			validate: func(t *testing.T, result Relation) {
				// Find the tuple for age 25 (should have count=2, avg=90)
				it := result.Iterator()
				defer it.Close()
				found25 := false
				for it.Next() {
					tuple := it.Tuple()
					if age, ok := tuple[0].(int64); ok && age == 25 {
						found25 = true
						if count, ok := tuple[1].(int64); !ok || count != 2 {
							t.Errorf("age 25: expected count 2, got %v", tuple[1])
						}
						if avg, ok := tuple[2].(float64); !ok || avg != 90.0 {
							t.Errorf("age 25: expected avg 90.0, got %v", tuple[2])
						}
					}
				}
				if !found25 {
					t.Error("didn't find age 25 group")
				}
			},
		},
		{
			name: "multiple aggregates",
			findElements: []query.FindElement{
				query.FindAggregate{Function: "min", Arg: datalog.NewSymbol("?age")},
				query.FindAggregate{Function: "max", Arg: datalog.NewSymbol("?age")},
				query.FindAggregate{Function: "sum", Arg: datalog.NewSymbol("?score")},
			},
			expectedSymbols: []query.Symbol{datalog.NewSymbol("(min ?age)"), datalog.NewSymbol("(max ?age)"), datalog.NewSymbol("(sum ?score)")},
			expectedRows:    1,
			validate: func(t *testing.T, result Relation) {
				it := result.Iterator()
				defer it.Close()
				if it.Next() {
					tuple := it.Tuple()
					if min, ok := tuple[0].(int64); !ok || min != 25 {
						t.Errorf("expected min age 25, got %v", tuple[0])
					}
					if max, ok := tuple[1].(int64); !ok || max != 35 {
						t.Errorf("expected max age 35, got %v", tuple[1])
					}
					if sum, ok := tuple[2].(float64); !ok || sum != 344.0 {
						t.Errorf("expected sum 344.0, got %v", tuple[2])
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExecuteAggregations(rel, tt.findElements)

			// Check symbol count
			if len(result.Symbols()) != len(tt.expectedSymbols) {
				t.Errorf("expected %d symbols, got %d", len(tt.expectedSymbols), len(result.Symbols()))
			}

			// Check tuple count
			if result.Size() != tt.expectedRows {
				t.Errorf("expected %d tuples, got %d", tt.expectedRows, result.Size())
			}

			// Run custom validation
			if tt.validate != nil {
				tt.validate(t, result)
			}
		})
	}
}

func TestAggregationEmitsStructuredStrategyAnnotation(t *testing.T) {
	var events []annotations.Event
	ctx := NewContext(func(event annotations.Event) {
		events = append(events, event)
	})
	value := datalog.NewSymbol("?value")
	rel := NewMaterializedRelationWithOptions(
		[]query.Symbol{value},
		[]Tuple{{int64(1)}, {int64(2)}, {int64(3)}},
		ExecutorOptions{Collector: ctx.Collector()},
	)

	result := ExecuteAggregationsWithContext(ctx, rel, []query.FindElement{
		query.FindAggregate{Function: "sum", Arg: value},
	})
	if result.Size() != 1 {
		t.Fatalf("aggregation result size = %d, want 1", result.Size())
	}

	var strategy annotations.Event
	found := false
	for _, event := range events {
		if event.Name == annotations.AggregationStrategy {
			strategy = event
			found = true
			break
		}
	}
	if !found {
		t.Fatal("aggregation strategy annotation was not emitted")
	}
	if strategy.Data["strategy"] != "batch" {
		t.Fatalf("aggregation strategy = %v, want batch", strategy.Data["strategy"])
	}
	if strategy.Data["aggregate_count"] != 1 {
		t.Fatalf("aggregate_count = %v, want 1", strategy.Data["aggregate_count"])
	}
	if strategy.Data["group_by_count"] != 0 {
		t.Fatalf("group_by_count = %v, want 0", strategy.Data["group_by_count"])
	}
}

func TestStreamingAggregationEmitsMaterializedAnnotation(t *testing.T) {
	var events []annotations.Event
	ctx := NewContext(func(event annotations.Event) {
		events = append(events, event)
	})
	value := datalog.NewSymbol("?value")
	tuples := make([]Tuple, StreamingAggregationThreshold+1)
	for i := range tuples {
		tuples[i] = Tuple{int64(i)}
	}
	base := NewMaterializedRelationNoDedupe([]query.Symbol{value}, tuples)
	stream := NewStreamingRelationWithOptions(
		base.Symbols(),
		base.Iterator(),
		ExecutorOptions{
			EnableStreamingAggregation: true,
		},
	)

	result := ExecuteAggregationsWithContext(ctx, stream, []query.FindElement{
		query.FindAggregate{Function: "sum", Arg: value},
	})
	if stream.iteratorCalled {
		t.Fatal("emitting the aggregation strategy annotation consumed the streaming input")
	}
	if result.Size() != 1 {
		t.Fatalf("streaming aggregation result size = %d, want 1", result.Size())
	}

	foundStrategy := false
	foundMaterialized := false
	for _, event := range events {
		if event.Name == annotations.AggregationStrategy {
			foundStrategy = true
			if event.Data["input_size"] != -1 {
				t.Fatalf("streaming input_size = %v, want -1", event.Data["input_size"])
			}
		}
		if event.Name == annotations.AggregationMaterialized {
			foundMaterialized = true
			if event.Data["input_count"] != StreamingAggregationThreshold+1 {
				t.Fatalf("input_count = %v, want %d",
					event.Data["input_count"], StreamingAggregationThreshold+1)
			}
			if event.Data["result_count"] != 1 {
				t.Fatalf("result_count = %v, want 1", event.Data["result_count"])
			}
		}
	}
	if !foundStrategy {
		t.Error("aggregation strategy annotation was not emitted")
	}
	if !foundMaterialized {
		t.Error("aggregation materialized annotation was not emitted through the context collector")
	}
}

func TestAggregationRejectsMissingGroupSymbolWithoutPanic(t *testing.T) {
	order := datalog.NewSymbol("?order")
	missing := datalog.NewSymbol("?missing")
	tuples := make([]Tuple, StreamingAggregationThreshold+1)
	for i := range tuples {
		tuples[i] = Tuple{int64(i)}
	}
	source := NewStreamingRelationWithOptions(
		[]query.Symbol{order},
		NewMaterializedRelationNoDedupe([]query.Symbol{order}, tuples).Iterator(),
		ExecutorOptions{EnableStreamingAggregation: true},
	)

	var result Relation
	require.NotPanics(t, func() {
		result = ExecuteAggregations(source, []query.FindElement{
			query.FindVariable{Symbol: missing},
			query.FindAggregate{Function: "count", Arg: order},
		})
	})
	require.Error(t, driveErr(result))
	require.Contains(t, driveErr(result).Error(), "group-by symbol ?missing")
}

func TestProjectSymbols(t *testing.T) {
	symbols := []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c"), datalog.NewSymbol("?d")}
	tuples := []Tuple{
		{1, 2, 3, 4},
		{5, 6, 7, 8},
		{9, 10, 11, 12},
	}
	rel := NewMaterializedRelation(symbols, tuples)

	tests := []struct {
		name            string
		projectSymbols  []query.Symbol
		expectedSymbols []query.Symbol
		expectedVals    [][]interface{}
	}{
		{
			name:            "project subset",
			projectSymbols:  []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?c")},
			expectedSymbols: []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?c")},
			expectedVals: [][]interface{}{
				{1, 3},
				{5, 7},
				{9, 11},
			},
		},
		{
			name:            "project reordered",
			projectSymbols:  []query.Symbol{datalog.NewSymbol("?d"), datalog.NewSymbol("?b")},
			expectedSymbols: []query.Symbol{datalog.NewSymbol("?d"), datalog.NewSymbol("?b")},
			expectedVals: [][]interface{}{
				{4, 2},
				{8, 6},
				{12, 10},
			},
		},
		{
			name:            "project all",
			projectSymbols:  []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c"), datalog.NewSymbol("?d")},
			expectedSymbols: []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c"), datalog.NewSymbol("?d")},
			expectedVals: [][]interface{}{
				{1, 2, 3, 4},
				{5, 6, 7, 8},
				{9, 10, 11, 12},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := rel.Project(tt.projectSymbols)
			if err != nil {
				t.Fatalf("Project failed: %v", err)
			}

			// Check symbols
			resultSymbols := result.Symbols()
			if len(resultSymbols) != len(tt.expectedSymbols) {
				t.Fatalf("expected %d symbols, got %d", len(tt.expectedSymbols), len(resultSymbols))
			}
			for i, symbol := range resultSymbols {
				if symbol != tt.expectedSymbols[i] {
					t.Errorf("symbol %d: expected %s, got %s", i, tt.expectedSymbols[i], symbol)
				}
			}

			// Check values
			it := result.Iterator()
			defer it.Close()
			idx := 0
			for it.Next() {
				tuple := it.Tuple()
				if idx >= len(tt.expectedVals) {
					t.Errorf("too many tuples: expected %d", len(tt.expectedVals))
					break
				}
				expected := tt.expectedVals[idx]
				if len(tuple) != len(expected) {
					t.Errorf("tuple %d: expected %d values, got %d", idx, len(expected), len(tuple))
				}
				for i, val := range tuple {
					if val != expected[i] {
						t.Errorf("tuple %d position %d: expected %v, got %v", idx, i, expected[i], val)
					}
				}
				idx++
			}
			if idx != len(tt.expectedVals) {
				t.Errorf("expected %d tuples, got %d", len(tt.expectedVals), idx)
			}
		})
	}
}

func TestAggregationWithTimeValues(t *testing.T) {
	// Test that time values are handled correctly in aggregations
	date1 := time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC)
	date2 := time.Date(2023, 6, 10, 0, 0, 0, 0, time.UTC)
	date3 := time.Date(2023, 3, 20, 0, 0, 0, 0, time.UTC)

	symbols := []query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?date")}
	tuples := []Tuple{
		{"Alice", date1},
		{"Bob", date2},
		{"Charlie", date3},
	}
	rel := NewMaterializedRelation(symbols, tuples)

	// Test min/max with dates
	result := ExecuteAggregations(rel, []query.FindElement{
		query.FindAggregate{Function: "min", Arg: datalog.NewSymbol("?date")},
		query.FindAggregate{Function: "max", Arg: datalog.NewSymbol("?date")},
	})

	if result.Size() != 1 {
		t.Fatalf("expected 1 tuple, got %d", result.Size())
	}

	it := result.Iterator()
	defer it.Close()
	if it.Next() {
		tuple := it.Tuple()
		if minDate, ok := tuple[0].(time.Time); !ok || !minDate.Equal(date1) {
			t.Errorf("expected min date %v, got %v", date1, tuple[0])
		}
		if maxDate, ok := tuple[1].(time.Time); !ok || !maxDate.Equal(date2) {
			t.Errorf("expected max date %v, got %v", date2, tuple[1])
		}
	}
}

func TestAggregationTupleKeysPreserveTypes(t *testing.T) {
	for _, pair := range adversarialTuplePairs {
		t.Run(pair.name, func(t *testing.T) {
			aKey := NewTupleKeyFull(pair.a)
			bKey := NewTupleKeyFull(pair.b)
			if aKey.Equal(bKey) {
				t.Fatalf("distinct typed aggregation keys compare equal: %v and %v", pair.a, pair.b)
			}

			groups := NewTupleKeyMap()
			groups.Put(aKey, "a")
			groups.Put(bKey, "b")
			if got, ok := groups.Get(aKey); !ok || got != "a" {
				t.Fatalf("first typed key was lost: got %v, found %v", got, ok)
			}
			if got, ok := groups.Get(bKey); !ok || got != "b" {
				t.Fatalf("second typed key was lost: got %v, found %v", got, ok)
			}
		})
	}
}

func TestEmptyRelationAggregation(t *testing.T) {
	// Test aggregation on empty relation
	// Following relational theory (C.J. Date): empty input → empty output (no NULL)
	emptyRel := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?x")}, []Tuple{})

	// Following pure relational theory, aggregates on empty relations return empty results
	// This is consistent with Datomic philosophy: attributes exist or don't exist, no NULL placeholders
	result := ExecuteAggregations(emptyRel, []query.FindElement{
		query.FindAggregate{Function: "count", Arg: datalog.NewSymbol("?x")},
	})

	if result.Size() != 0 {
		t.Fatalf("expected 0 tuples (empty result), got %d", result.Size())
	}

	// Other aggregates on empty also return empty results
	result = ExecuteAggregations(emptyRel, []query.FindElement{
		query.FindAggregate{Function: "sum", Arg: datalog.NewSymbol("?x")},
		query.FindAggregate{Function: "avg", Arg: datalog.NewSymbol("?x")},
		query.FindAggregate{Function: "min", Arg: datalog.NewSymbol("?x")},
		query.FindAggregate{Function: "max", Arg: datalog.NewSymbol("?x")},
	})

	if result.Size() != 0 {
		t.Fatalf("expected 0 tuples (empty result), got %d", result.Size())
	}
}
