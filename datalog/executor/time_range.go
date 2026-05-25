package executor

import (
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TimeRange represents a half-open time interval [Start, End)
type TimeRange struct {
	Start time.Time
	End   time.Time
}

// timeKey is used for deduplication without string allocations
type timeKey struct {
	year, month, day, hour int64
}

// extractTimeRanges converts correlation key tuples into time ranges.
// This enables semi-join pushdown by constraining scans to only relevant time
// periods (see BadgerMatcher.WithTimeRanges / scanTimeRanges).
func extractTimeRanges(inputRelation Relation, correlationKeys []query.Symbol) ([]TimeRange, error) {
	// Get symbols from input relation
	syms := inputRelation.Symbols()
	if len(syms) == 0 {
		return nil, nil
	}

	// Find symbol indices for time components
	symYear := datalog.NewSymbol("?year")
	symY := datalog.NewSymbol("?y")
	symMonth := datalog.NewSymbol("?month")
	symM := datalog.NewSymbol("?m")
	symDay := datalog.NewSymbol("?day")
	symD := datalog.NewSymbol("?d")
	symHour := datalog.NewSymbol("?hour")
	symH := datalog.NewSymbol("?h")
	symHr := datalog.NewSymbol("?hr")

	var yearIdx, monthIdx, dayIdx, hourIdx int = -1, -1, -1, -1
	for i, sym := range syms {
		if sym == symYear || sym == symY {
			yearIdx = i
		} else if sym == symMonth || sym == symM {
			monthIdx = i
		} else if sym == symDay || sym == symD {
			dayIdx = i
		} else if sym == symHour || sym == symH || sym == symHr {
			hourIdx = i
		}
	}

	// We need at least year, month, day to construct time ranges
	if yearIdx < 0 || monthIdx < 0 || dayIdx < 0 {
		return nil, nil // Not time-based, skip optimization
	}

	// Extract unique time component tuples and convert to ranges
	// Use struct key to avoid fmt.Sprintf allocations
	seen := make(map[timeKey]bool, inputRelation.Size())
	// Pre-allocate ranges slice with exact capacity to avoid reallocation
	ranges := make([]TimeRange, 0, inputRelation.Size())

	it := inputRelation.Iterator()
	defer it.Close()

	for it.Next() {
		tuple := it.Tuple()
		if len(tuple) <= dayIdx {
			continue
		}

		// Extract time components (handle both int64 and int)
		var year, month, day, hour int64

		switch v := tuple[yearIdx].(type) {
		case int64:
			year = v
		case int:
			year = int64(v)
		default:
			continue
		}

		switch v := tuple[monthIdx].(type) {
		case int64:
			month = v
		case int:
			month = int64(v)
		default:
			continue
		}

		switch v := tuple[dayIdx].(type) {
		case int64:
			day = v
		case int:
			day = int64(v)
		default:
			continue
		}

		if hourIdx >= 0 && hourIdx < len(tuple) {
			switch v := tuple[hourIdx].(type) {
			case int64:
				hour = v
			case int:
				hour = int64(v)
			}
		}

		// Create unique key for deduplication (zero allocations with struct key)
		key := timeKey{year: year, month: month, day: day, hour: hour}
		if seen[key] {
			continue
		}
		seen[key] = true

		// Convert to time range
		start := time.Date(int(year), time.Month(month), int(day), int(hour), 0, 0, 0, time.UTC)
		var end time.Time
		if hourIdx >= 0 {
			// Hour-level granularity: [hour:00, hour+1:00)
			end = start.Add(1 * time.Hour)
		} else {
			// Day-level granularity: [day 00:00, day+1 00:00)
			end = start.AddDate(0, 0, 1)
		}

		ranges = append(ranges, TimeRange{Start: start, End: end})
	}

	return ranges, nil
}
