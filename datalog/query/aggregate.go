package query

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

// AggregateFunction represents an aggregate function like sum, count, avg
// These are special because they operate on sets of values, not single bindings
type AggregateFunction interface {
	Pattern // Embeds Pattern interface

	// Variable returns the variable this aggregate operates on
	Variable() Symbol

	// FunctionName returns the name of the aggregate function
	FunctionName() string

	// Aggregate performs the aggregation on a set of values
	Aggregate(values []interface{}) (interface{}, error)

	// RequiresValues returns true if this aggregate needs actual values (not just counting)
	RequiresValues() bool
}

// CountAggregate counts the number of values
type CountAggregate struct {
	Var Symbol
}

func (c CountAggregate) Variable() Symbol {
	return c.Var
}

func (c CountAggregate) FunctionName() string {
	return "count"
}

func (c CountAggregate) Aggregate(values []interface{}) (interface{}, error) {
	return int64(len(values)), nil
}

func (c CountAggregate) RequiresValues() bool {
	return false // Count just needs the number of rows
}

func (c CountAggregate) String() string {
	return fmt.Sprintf("(count %s)", c.Var)
}

// SumAggregate sums numeric values
type SumAggregate struct {
	Var Symbol
}

func (s SumAggregate) Variable() Symbol {
	return s.Var
}

func (s SumAggregate) FunctionName() string {
	return "sum"
}

func (s SumAggregate) Aggregate(values []interface{}) (interface{}, error) {
	if len(values) == 0 {
		return int64(0), nil
	}

	// Check if we need float arithmetic
	hasFloat := false
	for _, v := range values {
		if _, ok := v.(float64); ok {
			hasFloat = true
			break
		}
	}

	if hasFloat {
		var sum float64
		for _, v := range values {
			sum += toFloat64(toNumber(v))
		}
		return sum, nil
	} else {
		var sum int64
		for _, v := range values {
			sum += toInt64(toNumber(v))
		}
		return sum, nil
	}
}

func (s SumAggregate) RequiresValues() bool {
	return true
}

func (s SumAggregate) String() string {
	return fmt.Sprintf("(sum %s)", s.Var)
}

// AvgAggregate computes the average of numeric values
type AvgAggregate struct {
	Var Symbol
}

func (a AvgAggregate) Variable() Symbol {
	return a.Var
}

func (a AvgAggregate) FunctionName() string {
	return "avg"
}

func (a AvgAggregate) Aggregate(values []interface{}) (interface{}, error) {
	if len(values) == 0 {
		return float64(0), nil
	}

	var sum float64
	for _, v := range values {
		sum += toFloat64(toNumber(v))
	}
	return sum / float64(len(values)), nil
}

func (a AvgAggregate) RequiresValues() bool {
	return true
}

func (a AvgAggregate) String() string {
	return fmt.Sprintf("(avg %s)", a.Var)
}

// MinAggregate finds the minimum value
type MinAggregate struct {
	Var Symbol
}

func (m MinAggregate) Variable() Symbol {
	return m.Var
}

func (m MinAggregate) FunctionName() string {
	return "min"
}

func (m MinAggregate) Aggregate(values []interface{}) (interface{}, error) {
	if len(values) == 0 {
		return nil, nil
	}

	min := values[0]
	for i := 1; i < len(values); i++ {
		if datalog.CompareValues(values[i], min) < 0 {
			min = values[i]
		}
	}
	return min, nil
}

func (m MinAggregate) RequiresValues() bool {
	return true
}

func (m MinAggregate) String() string {
	return fmt.Sprintf("(min %s)", m.Var)
}

// MaxAggregate finds the maximum value
type MaxAggregate struct {
	Var Symbol
}

func (m MaxAggregate) Variable() Symbol {
	return m.Var
}

func (m MaxAggregate) FunctionName() string {
	return "max"
}

func (m MaxAggregate) Aggregate(values []interface{}) (interface{}, error) {
	if len(values) == 0 {
		return nil, nil
	}

	max := values[0]
	for i := 1; i < len(values); i++ {
		if datalog.CompareValues(values[i], max) > 0 {
			max = values[i]
		}
	}
	return max, nil
}

func (m MaxAggregate) RequiresValues() bool {
	return true
}

func (m MaxAggregate) String() string {
	return fmt.Sprintf("(max %s)", m.Var)
}

// Helper to determine if a value is numeric
func isNumeric(val interface{}) bool {
	switch val.(type) {
	case int, int32, int64, float32, float64:
		return true
	default:
		return false
	}
}

// Helper to determine if a value is a time
func isTime(val interface{}) bool {
	_, ok := val.(time.Time)
	return ok
}
