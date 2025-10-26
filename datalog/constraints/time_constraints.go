package constraints

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

// StorageConstraint represents a constraint that can be pushed to storage
type StorageConstraint interface {
	// Evaluate checks if a datom satisfies this constraint
	Evaluate(datom *datalog.Datom) bool
	// String returns a human-readable description
	String() string
}

// TimeRangeConstraint filters datoms by time ranges
// Optimizes predicates like: year(time) = 2025, month(time) = 6, etc.
// by converting to efficient range checks: time >= start AND time < end
type TimeRangeConstraint struct {
	position  int       // Which position in datom (E=0, A=1, V=2, Tx=3)
	startTime time.Time // Inclusive start
	endTime   time.Time // Exclusive end
}

// Evaluate checks if the datom's time value falls within the range
func (c *TimeRangeConstraint) Evaluate(datom *datalog.Datom) bool {
	var t time.Time
	var ok bool

	switch c.position {
	case 2: // Value - most common position for time values
		t, ok = datom.V.(time.Time)
		if !ok {
			return false
		}
	default:
		// Entity, Attribute, and Transaction positions don't typically hold time.Time
		// For now, just pass through (constraint doesn't apply)
		return true
	}

	// Check if time is within range: startTime <= t < endTime
	return !t.Before(c.startTime) && t.Before(c.endTime)
}

func (c *TimeRangeConstraint) String() string {
	return fmt.Sprintf("time[%d] ∈ [%s, %s)", c.position,
		c.startTime.Format("2006-01-02 15:04"),
		c.endTime.Format("2006-01-02 15:04"))
}

// ComposeTimeConstraint combines year/month/day/hour/minute/second into a single range
// Pass nil for unspecified components (e.g., year + month only)
func ComposeTimeConstraint(
	year *int,
	month *int,
	day *int,
	hour *int,
	minute *int,
	second *int,
	position int,
) *TimeRangeConstraint {
	// Start with most specific time, default unspecified parts
	y := 1970
	m := 1
	d := 1
	h := 0
	min := 0
	sec := 0

	if year != nil {
		y = *year
	}
	if month != nil {
		m = *month
	}
	if day != nil {
		d = *day
	}
	if hour != nil {
		h = *hour
	}
	if minute != nil {
		min = *minute
	}
	if second != nil {
		sec = *second
	}

	start := time.Date(y, time.Month(m), d, h, min, sec, 0, time.UTC)

	// Calculate end based on least specific component
	var end time.Time
	if second != nil {
		end = start.Add(time.Second)
	} else if minute != nil {
		end = start.Add(time.Minute)
	} else if hour != nil {
		end = start.Add(time.Hour)
	} else if day != nil {
		end = start.AddDate(0, 0, 1) // Next day
	} else if month != nil {
		end = start.AddDate(0, 1, 0) // Next month
	} else if year != nil {
		end = start.AddDate(1, 0, 0) // Next year
	} else {
		// No constraints - match everything
		end = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	}

	return &TimeRangeConstraint{
		position:  position,
		startTime: start,
		endTime:   end,
	}
}
