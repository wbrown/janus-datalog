package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestExtractTimeRanges(t *testing.T) {
	tests := []struct {
		name            string
		inputTuples     []Tuple
		inputColumns    []query.Symbol
		correlationKeys []query.Symbol
		expectedRanges  int
		expectNil       bool
		description     string
	}{
		{
			name: "hourly_time_ranges",
			inputTuples: []Tuple{
				{int64(2025), int64(6), int64(20), int64(9)},  // 2025-06-20 09:00
				{int64(2025), int64(6), int64(20), int64(10)}, // 2025-06-20 10:00
				{int64(2025), int64(6), int64(20), int64(11)}, // 2025-06-20 11:00
			},
			inputColumns:    []query.Symbol{"?year", "?month", "?day", "?hour"},
			correlationKeys: []query.Symbol{"$", "?s", "?year", "?month", "?day", "?hour"},
			expectedRanges:  3,
			description:     "Should extract 3 hourly ranges",
		},
		{
			name: "daily_time_ranges",
			inputTuples: []Tuple{
				{int64(2025), int64(6), int64(20)}, // 2025-06-20
				{int64(2025), int64(6), int64(21)}, // 2025-06-21
				{int64(2025), int64(6), int64(22)}, // 2025-06-22
			},
			inputColumns:    []query.Symbol{"?year", "?month", "?day"},
			correlationKeys: []query.Symbol{"$", "?s", "?year", "?month", "?day"},
			expectedRanges:  3,
			description:     "Should extract 3 daily ranges",
		},
		{
			name: "deduplication",
			inputTuples: []Tuple{
				{int64(2025), int64(6), int64(20), int64(9)},
				{int64(2025), int64(6), int64(20), int64(9)}, // Duplicate
				{int64(2025), int64(6), int64(20), int64(10)},
			},
			inputColumns:    []query.Symbol{"?year", "?month", "?day", "?hour"},
			correlationKeys: []query.Symbol{"$", "?s", "?year", "?month", "?day", "?hour"},
			expectedRanges:  2,
			description:     "Should deduplicate identical time ranges",
		},
		{
			name: "no_time_components",
			inputTuples: []Tuple{
				{"CRWV", int64(100)},
			},
			inputColumns:    []query.Symbol{"?symbol", "?value"},
			correlationKeys: []query.Symbol{"$", "?symbol", "?value"},
			expectNil:       true,
			description:     "Should return nil for non-time-based queries",
		},
		{
			name: "partial_time_components",
			inputTuples: []Tuple{
				{int64(2025), int64(6)}, // Only year and month
			},
			inputColumns:    []query.Symbol{"?year", "?month"},
			correlationKeys: []query.Symbol{"$", "?s", "?year", "?month"},
			expectNil:       true,
			description:     "Should return nil when missing required day component",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create input relation
			inputRel := NewMaterializedRelation(tt.inputColumns, tt.inputTuples)

			// Extract time ranges
			ranges, err := extractTimeRanges(inputRel, tt.correlationKeys)
			if err != nil {
				t.Fatalf("extractTimeRanges failed: %v", err)
			}

			if tt.expectNil {
				if ranges != nil && len(ranges) > 0 {
					t.Errorf("%s: expected nil ranges, got %d ranges", tt.description, len(ranges))
				}
				return
			}

			if len(ranges) != tt.expectedRanges {
				t.Errorf("%s: expected %d ranges, got %d", tt.description, tt.expectedRanges, len(ranges))
			}

			// Verify range properties
			for i, r := range ranges {
				if r.End.Before(r.Start) || r.End.Equal(r.Start) {
					t.Errorf("Range %d has invalid interval: [%v, %v)", i, r.Start, r.End)
				}
			}
		})
	}
}

func TestTimeRangeGranularity(t *testing.T) {
	t.Run("hourly_granularity", func(t *testing.T) {
		inputTuples := []Tuple{
			{int64(2025), int64(6), int64(20), int64(9)},
		}
		inputRel := NewMaterializedRelation(
			[]query.Symbol{"?year", "?month", "?day", "?hour"},
			inputTuples,
		)

		ranges, err := extractTimeRanges(inputRel, []query.Symbol{"$", "?s", "?year", "?month", "?day", "?hour"})
		if err != nil {
			t.Fatalf("extractTimeRanges failed: %v", err)
		}

		if len(ranges) != 1 {
			t.Fatalf("expected 1 range, got %d", len(ranges))
		}

		r := ranges[0]
		expected := time.Date(2025, 6, 20, 9, 0, 0, 0, time.UTC)
		if !r.Start.Equal(expected) {
			t.Errorf("expected start %v, got %v", expected, r.Start)
		}

		expectedEnd := expected.Add(1 * time.Hour)
		if !r.End.Equal(expectedEnd) {
			t.Errorf("expected end %v, got %v", expectedEnd, r.End)
		}
	})

	t.Run("daily_granularity", func(t *testing.T) {
		inputTuples := []Tuple{
			{int64(2025), int64(6), int64(20)},
		}
		inputRel := NewMaterializedRelation(
			[]query.Symbol{"?year", "?month", "?day"},
			inputTuples,
		)

		ranges, err := extractTimeRanges(inputRel, []query.Symbol{"$", "?s", "?year", "?month", "?day"})
		if err != nil {
			t.Fatalf("extractTimeRanges failed: %v", err)
		}

		if len(ranges) != 1 {
			t.Fatalf("expected 1 range, got %d", len(ranges))
		}

		r := ranges[0]
		expected := time.Date(2025, 6, 20, 0, 0, 0, 0, time.UTC)
		if !r.Start.Equal(expected) {
			t.Errorf("expected start %v, got %v", expected, r.Start)
		}

		expectedEnd := expected.AddDate(0, 0, 1)
		if !r.End.Equal(expectedEnd) {
			t.Errorf("expected end %v, got %v", expectedEnd, r.End)
		}
	})
}

func TestTimeRangeWithIntTypes(t *testing.T) {
	// Test that we handle both int and int64 properly
	t.Run("int_values", func(t *testing.T) {
		inputTuples := []Tuple{
			{int(2025), int(6), int(20), int(9)},
		}
		inputRel := NewMaterializedRelation(
			[]query.Symbol{"?year", "?month", "?day", "?hour"},
			inputTuples,
		)

		ranges, err := extractTimeRanges(inputRel, []query.Symbol{"$", "?s", "?year", "?month", "?day", "?hour"})
		if err != nil {
			t.Fatalf("extractTimeRanges failed: %v", err)
		}

		if len(ranges) != 1 {
			t.Fatalf("expected 1 range, got %d", len(ranges))
		}

		expected := time.Date(2025, 6, 20, 9, 0, 0, 0, time.UTC)
		if !ranges[0].Start.Equal(expected) {
			t.Errorf("expected start %v, got %v", expected, ranges[0].Start)
		}
	})

	t.Run("mixed_int_types", func(t *testing.T) {
		inputTuples := []Tuple{
			{int64(2025), int(6), int64(20), int(9)},
		}
		inputRel := NewMaterializedRelation(
			[]query.Symbol{"?year", "?month", "?day", "?hour"},
			inputTuples,
		)

		ranges, err := extractTimeRanges(inputRel, []query.Symbol{"$", "?s", "?year", "?month", "?day", "?hour"})
		if err != nil {
			t.Fatalf("extractTimeRanges failed: %v", err)
		}

		if len(ranges) != 1 {
			t.Fatalf("expected 1 range, got %d", len(ranges))
		}
	})
}

func TestTimeRangeOptimization260Hours(t *testing.T) {
	// Simulate the actual hourly OHLC scenario: 260 distinct hours
	var inputTuples []Tuple

	// Generate 260 hours across multiple days
	startDate := time.Date(2025, 6, 20, 9, 0, 0, 0, time.UTC)
	for i := 0; i < 260; i++ {
		dt := startDate.Add(time.Duration(i) * time.Hour)
		inputTuples = append(inputTuples, Tuple{
			int64(dt.Year()),
			int64(dt.Month()),
			int64(dt.Day()),
			int64(dt.Hour()),
		})
	}

	inputRel := NewMaterializedRelation(
		[]query.Symbol{"?year", "?month", "?day", "?hour"},
		inputTuples,
	)

	ranges, err := extractTimeRanges(inputRel, []query.Symbol{"$", "?s", "?year", "?month", "?day", "?hour"})
	if err != nil {
		t.Fatalf("extractTimeRanges failed: %v", err)
	}

	if len(ranges) != 260 {
		t.Errorf("expected 260 ranges for hourly OHLC, got %d", len(ranges))
	}

	// Verify ranges are consecutive hours
	for i := 0; i < len(ranges)-1; i++ {
		// Each range should be 1 hour
		duration := ranges[i].End.Sub(ranges[i].Start)
		if duration != 1*time.Hour {
			t.Errorf("Range %d has duration %v, expected 1 hour", i, duration)
		}
	}
}
