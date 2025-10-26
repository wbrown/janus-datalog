package executor

import (
	"bytes"
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
		return datalog.ValuesEqual(datom.V, c.value)
	case 3: // Transaction
		if tx, ok := c.value.(uint64); ok {
			return datom.Tx == tx
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
		cmp := compareValuesForConstraints(value, c.min)
		if c.includeMin && cmp < 0 {
			return false
		}
		if !c.includeMin && cmp <= 0 {
			return false
		}
	}

	if c.max != nil {
		cmp := compareValuesForConstraints(value, c.max)
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

// Helper functions

func compareValuesForConstraints(a, b interface{}) int {
	// Handle time.Time specially
	if t1, ok := a.(time.Time); ok {
		if t2, ok := b.(time.Time); ok {
			if t1.Before(t2) {
				return -1
			} else if t1.After(t2) {
				return 1
			}
			return 0
		}
	}

	// Handle numeric types
	var v1, v2 float64
	var v1Ok, v2Ok bool

	switch x := a.(type) {
	case int:
		v1, v1Ok = float64(x), true
	case int64:
		v1, v1Ok = float64(x), true
	case float64:
		v1, v1Ok = x, true
	}

	switch x := b.(type) {
	case int:
		v2, v2Ok = float64(x), true
	case int64:
		v2, v2Ok = float64(x), true
	case float64:
		v2, v2Ok = x, true
	}

	if v1Ok && v2Ok {
		if v1 < v2 {
			return -1
		} else if v1 > v2 {
			return 1
		}
		return 0
	}

	// Fall back to string comparison
	return bytes.Compare([]byte(fmt.Sprintf("%v", a)), []byte(fmt.Sprintf("%v", b)))
}

func valuesEqual(a, b interface{}) bool {
	// Handle Identity comparison
	if id1, ok := a.(datalog.Identity); ok {
		if id2, ok := b.(datalog.Identity); ok {
			return id1.Equal(id2)
		}
		if s, ok := b.(string); ok {
			return id1.String() == s
		}
	}

	// Handle Keyword comparison
	if kw1, ok := a.(datalog.Keyword); ok {
		if kw2, ok := b.(datalog.Keyword); ok {
			return kw1.String() == kw2.String()
		}
		if s, ok := b.(string); ok {
			return kw1.String() == s
		}
	}

	// Handle numeric comparisons with type flexibility
	switch n1 := a.(type) {
	case int64:
		switch n2 := b.(type) {
		case int64:
			return n1 == n2
		case int:
			return n1 == int64(n2)
		case float64:
			return float64(n1) == n2
		}
	case int:
		switch n2 := b.(type) {
		case int:
			return n1 == n2
		case int64:
			return int64(n1) == n2
		case float64:
			return float64(n1) == n2
		}
	case float64:
		switch n2 := b.(type) {
		case float64:
			return n1 == n2
		case int64:
			return n1 == float64(n2)
		case int:
			return n1 == float64(n2)
		}
	case bool:
		if b2, ok := b.(bool); ok {
			return n1 == b2
		}
	case string:
		if s2, ok := b.(string); ok {
			return n1 == s2
		}
	case time.Time:
		if t2, ok := b.(time.Time); ok {
			return n1.Equal(t2)
		}
	}

	// Default equality
	return a == b
}

func ifThen(cond bool, ifTrue, ifFalse string) string {
	if cond {
		return ifTrue
	}
	return ifFalse
}
