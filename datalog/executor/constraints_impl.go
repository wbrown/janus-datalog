package executor

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

// Simple constraint implementations that don't depend on storage package

// equalityConstraint checks if a specific position equals a value
type equalityConstraint struct {
	position int
	value    interface{}
}

func (c *equalityConstraint) Evaluate(datom *datalog.Datom) bool {
	switch c.position {
	case 0: // Entity
		if id, ok := c.value.(datalog.Identity); ok {
			return datom.E.Equal(id)
		}
	case 1: // Attribute
		if kw, ok := c.value.(datalog.Keyword); ok {
			return datom.A.String() == kw.String()
		}
	case 2: // Value
		// Fast path for common integer comparisons
		if iv, ok := c.value.(int64); ok {
			dv, ok := datom.V.(int64)
			return ok && dv == iv
		}
		// Fast path for string comparisons
		if sv, ok := c.value.(string); ok {
			dv, ok := datom.V.(string)
			return ok && dv == sv
		}
		// Fast path for bool comparisons
		if bv, ok := c.value.(bool); ok {
			dv, ok := datom.V.(bool)
			return ok && dv == bv
		}
		// Fast path for symbol comparisons (pointer equality via interning)
		if sv, ok := c.value.(datalog.Symbol); ok {
			dv, ok := datom.V.(datalog.Symbol)
			return ok && dv == sv
		}
		return datalog.ValuesEqual(datom.V, c.value)
	case 3: // Transaction
		if eid, ok := datalog.DerefElementID(c.value); ok {
			return datom.Tx == eid
		}
		// Backward compatibility: also support uint64 comparison by Lamport
		if tx, ok := c.value.(uint64); ok {
			return datom.Tx.Lamport == tx
		}
	}
	return false
}

func (c *equalityConstraint) String() string {
	pos := []string{"E", "A", "V", "T"}[c.position]
	return fmt.Sprintf("%s = %v", pos, c.value)
}

// rangeConstraint checks if a value is within a range
type rangeConstraint struct {
	position   int
	min, max   interface{}
	includeMin bool
	includeMax bool
}

func (c *rangeConstraint) Evaluate(datom *datalog.Datom) bool {
	var value interface{}
	switch c.position {
	case 2: // Value position
		value = datom.V
	case 3: // Transaction
		value = datom.Tx
	default:
		return false
	}

	if c.min != nil {
		cmp := datalog.CompareValues(value, c.min)
		if c.includeMin && cmp < 0 {
			return false
		}
		if !c.includeMin && cmp <= 0 {
			return false
		}
	}

	if c.max != nil {
		cmp := datalog.CompareValues(value, c.max)
		if c.includeMax && cmp > 0 {
			return false
		}
		if !c.includeMax && cmp >= 0 {
			return false
		}
	}

	return true
}

func (c *rangeConstraint) String() string {
	pos := []string{"E", "A", "V", "T"}[c.position]
	if c.min != nil && c.max != nil {
		return fmt.Sprintf("%v %s %s %s %v",
			c.min,
			ifThen(c.includeMin, "<=", "<"),
			pos,
			ifThen(c.includeMax, "<=", "<"),
			c.max)
	} else if c.min != nil {
		return fmt.Sprintf("%s %s %v", pos, ifThen(c.includeMin, ">=", ">"), c.min)
	} else if c.max != nil {
		return fmt.Sprintf("%s %s %v", pos, ifThen(c.includeMax, "<=", "<"), c.max)
	}
	return ""
}

// timeExtractionConstraint handles time-based predicates
type timeExtractionConstraint struct {
	position  int
	extractFn string
	expected  interface{}
}

func (c *timeExtractionConstraint) Evaluate(datom *datalog.Datom) bool {
	// Only support Value position for now
	if c.position != 2 {
		return false
	}

	t, ok := datom.V.(time.Time)
	if !ok {
		return false
	}

	var extracted interface{}
	switch c.extractFn {
	case "year":
		extracted = int64(t.Year())
	case "month":
		extracted = int64(t.Month())
	case "day":
		extracted = int64(t.Day())
	case "hour":
		extracted = int64(t.Hour())
	case "minute":
		extracted = int64(t.Minute())
	case "second":
		extracted = int64(t.Second())
	default:
		return false
	}

	return datalog.ValuesEqual(extracted, c.expected)
}

func (c *timeExtractionConstraint) String() string {
	return fmt.Sprintf("%s(V) = %v", c.extractFn, c.expected)
}

func ifThen(cond bool, ifTrue, ifFalse string) string {
	if cond {
		return ifTrue
	}
	return ifFalse
}
