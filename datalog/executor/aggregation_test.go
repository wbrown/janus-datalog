package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestExecuteAggregations(t *testing.T) {
	// Create test data
	columns := []query.Symbol{"?name", "?age", "?score"}
	tuples := []Tuple{
		{"Alice", int64(30), 85.5},
		{"Bob", int64(25), 92.0},
		{"Charlie", int64(35), 78.5},
		{"Dave", int64(25), 88.0},
	}
	rel := NewMaterializedRelation(columns, tuples)

	tests := []struct {
		name         string
		findElements []query.FindElement
		expectedCols []query.Symbol
		expectedRows int
		validate     func(*testing.T, Relation)
	}{
		{
			name: "no aggregates - just projection",
			findElements: []query.FindElement{
				query.FindVariable{Symbol: "?name"},
				query.FindVariable{Symbol: "?age"},
			},
			expectedCols: []query.Symbol{"?name", "?age"},
			expectedRows: 4,
			validate: func(t *testing.T, result Relation) {
				// Should have all 4 rows with just name and age
				if result.Size() != 4 {
					t.Errorf("expected 4 rows, got %d", result.Size())
				}
			},
		},
		{
			name: "single aggregation - count",
			findElements: []query.FindElement{
				query.FindAggregate{Function: "count", Arg: "?name"},
			},
			expectedCols: []query.Symbol{"(count ?name)"},
			expectedRows: 1,
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
				query.FindAggregate{Function: "avg", Arg: "?age"},
			},
			expectedCols: []query.Symbol{"(avg ?age)"},
			expectedRows: 1,
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
				query.FindAggregate{Function: "max", Arg: "?score"},
			},
			expectedCols: []query.Symbol{"(max ?score)"},
			expectedRows: 1,
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
				query.FindVariable{Symbol: "?age"},
				query.FindAggregate{Function: "count", Arg: "?name"},
				query.FindAggregate{Function: "avg", Arg: "?score"},
			},
			expectedCols: []query.Symbol{"?age", "(count ?name)", "(avg ?score)"},
			expectedRows: 3, // 3 unique ages: 25, 30, 35
			validate: func(t *testing.T, result Relation) {
				// Find the row for age 25 (should have count=2, avg=90)
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
				query.FindAggregate{Function: "min", Arg: "?age"},
				query.FindAggregate{Function: "max", Arg: "?age"},
				query.FindAggregate{Function: "sum", Arg: "?score"},
			},
			expectedCols: []query.Symbol{"(min ?age)", "(max ?age)", "(sum ?score)"},
			expectedRows: 1,
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

			// Check column count
			if len(result.Columns()) != len(tt.expectedCols) {
				t.Errorf("expected %d columns, got %d", len(tt.expectedCols), len(result.Columns()))
			}

			// Check row count
			if result.Size() != tt.expectedRows {
				t.Errorf("expected %d rows, got %d", tt.expectedRows, result.Size())
			}

			// Run custom validation
			if tt.validate != nil {
				tt.validate(t, result)
			}
		})
	}
}

func TestProjectColumns(t *testing.T) {
	columns := []query.Symbol{"?a", "?b", "?c", "?d"}
	tuples := []Tuple{
		{1, 2, 3, 4},
		{5, 6, 7, 8},
		{9, 10, 11, 12},
	}
	rel := NewMaterializedRelation(columns, tuples)

	tests := []struct {
		name         string
		projectCols  []query.Symbol
		expectedCols []query.Symbol
		expectedVals [][]interface{}
	}{
		{
			name:         "project subset",
			projectCols:  []query.Symbol{"?a", "?c"},
			expectedCols: []query.Symbol{"?a", "?c"},
			expectedVals: [][]interface{}{
				{1, 3},
				{5, 7},
				{9, 11},
			},
		},
		{
			name:         "project reordered",
			projectCols:  []query.Symbol{"?d", "?b"},
			expectedCols: []query.Symbol{"?d", "?b"},
			expectedVals: [][]interface{}{
				{4, 2},
				{8, 6},
				{12, 10},
			},
		},
		{
			name:         "project all",
			projectCols:  []query.Symbol{"?a", "?b", "?c", "?d"},
			expectedCols: []query.Symbol{"?a", "?b", "?c", "?d"},
			expectedVals: [][]interface{}{
				{1, 2, 3, 4},
				{5, 6, 7, 8},
				{9, 10, 11, 12},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := rel.Project(tt.projectCols)
			if err != nil {
				t.Fatalf("Project failed: %v", err)
			}

			// Check columns
			resultCols := result.Columns()
			if len(resultCols) != len(tt.expectedCols) {
				t.Fatalf("expected %d columns, got %d", len(tt.expectedCols), len(resultCols))
			}
			for i, col := range resultCols {
				if col != tt.expectedCols[i] {
					t.Errorf("column %d: expected %s, got %s", i, tt.expectedCols[i], col)
				}
			}

			// Check values
			it := result.Iterator()
			defer it.Close()
			row := 0
			for it.Next() {
				tuple := it.Tuple()
				if row >= len(tt.expectedVals) {
					t.Errorf("too many rows: expected %d", len(tt.expectedVals))
					break
				}
				expected := tt.expectedVals[row]
				if len(tuple) != len(expected) {
					t.Errorf("row %d: expected %d values, got %d", row, len(expected), len(tuple))
				}
				for i, val := range tuple {
					if val != expected[i] {
						t.Errorf("row %d col %d: expected %v, got %v", row, i, expected[i], val)
					}
				}
				row++
			}
			if row != len(tt.expectedVals) {
				t.Errorf("expected %d rows, got %d", len(tt.expectedVals), row)
			}
		})
	}
}

func TestAggregationWithTimeValues(t *testing.T) {
	// Test that time values are handled correctly in aggregations
	date1 := time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC)
	date2 := time.Date(2023, 6, 10, 0, 0, 0, 0, time.UTC)
	date3 := time.Date(2023, 3, 20, 0, 0, 0, 0, time.UTC)

	columns := []query.Symbol{"?name", "?date"}
	tuples := []Tuple{
		{"Alice", date1},
		{"Bob", date2},
		{"Charlie", date3},
	}
	rel := NewMaterializedRelation(columns, tuples)

	// Test min/max with dates
	result := ExecuteAggregations(rel, []query.FindElement{
		query.FindAggregate{Function: "min", Arg: "?date"},
		query.FindAggregate{Function: "max", Arg: "?date"},
	})

	if result.Size() != 1 {
		t.Fatalf("expected 1 row, got %d", result.Size())
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

func TestStringifyValue(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{nil, "<nil>"},
		{"hello", "hello"},
		{int64(42), "42"},
		{3.14, "3.14"},
		{true, "true"},
		{time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC), "2023-06-15T00:00:00Z"},
	}

	for _, tt := range tests {
		result := stringifyValue(tt.input)
		if result != tt.expected {
			t.Errorf("stringifyValue(%v) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestEmptyRelationAggregation(t *testing.T) {
	// Test aggregation on empty relation
	// Following relational theory (C.J. Date): empty input → empty output (no NULL)
	emptyRel := NewMaterializedRelation([]query.Symbol{"?x"}, []Tuple{})

	// Following pure relational theory, aggregates on empty relations return empty results
	// This is consistent with Datomic philosophy: attributes exist or don't exist, no NULL placeholders
	result := ExecuteAggregations(emptyRel, []query.FindElement{
		query.FindAggregate{Function: "count", Arg: "?x"},
	})

	if result.Size() != 0 {
		t.Fatalf("expected 0 rows (empty result), got %d", result.Size())
	}

	// Other aggregates on empty also return empty results
	result = ExecuteAggregations(emptyRel, []query.FindElement{
		query.FindAggregate{Function: "sum", Arg: "?x"},
		query.FindAggregate{Function: "avg", Arg: "?x"},
		query.FindAggregate{Function: "min", Arg: "?x"},
		query.FindAggregate{Function: "max", Arg: "?x"},
	})

	if result.Size() != 0 {
		t.Fatalf("expected 0 rows (empty result), got %d", result.Size())
	}
}
