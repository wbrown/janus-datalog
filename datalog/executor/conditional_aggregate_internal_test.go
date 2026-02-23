package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog/query"

	"github.com/wbrown/janus-datalog/datalog"
)

// TestConditionalAggregateInternalInfrastructure tests the internal conditional aggregate
// execution infrastructure (used by query rewriter, NOT exposed to users)
func TestConditionalAggregateInternalInfrastructure(t *testing.T) {
	// Create a simple relation with hour, filter, and value symbols
	symbols := []query.Symbol{datalog.NewSymbol("?hour"), datalog.NewSymbol("?filter"), datalog.NewSymbol("?value")}
	tuples := []Tuple{
		{int64(10), true, 100.0},
		{int64(10), true, 102.0},
		{int64(10), false, 105.0},
		{int64(10), false, 103.0},
	}
	rel := NewMaterializedRelation(symbols, tuples)

	// Create find elements with conditional aggregate (internal use only)
	findElements := []query.FindElement{
		query.FindVariable{Symbol: datalog.NewSymbol("?hour")},
		query.FindAggregate{
			Function:  "min",
			Arg:       datalog.NewSymbol("?value"),
			Predicate: datalog.NewSymbol("?filter"), // Internal: filter on this symbol
		},
	}

	// Execute aggregation with internal conditional aggregate
	result := ExecuteAggregations(rel, findElements)

	// Verify result
	assert.Equal(t, 1, result.Size(), "should have 1 group (hour=10)")

	it := result.Iterator()
	defer it.Close()

	assert.True(t, it.Next())
	tuple := it.Tuple()
	assert.Equal(t, 2, len(tuple), "should have 2 symbols: hour and min")
	assert.Equal(t, int64(10), tuple[0], "hour should be 10")
	assert.Equal(t, 100.0, tuple[1], "min should be 100.0 (only values where filter=true)")

	assert.False(t, it.Next(), "should be no more tuples")
}

// TestConditionalAggregateEmptyResult tests that conditional aggregates return
// empty result set when no tuples match the filter predicate (relational theory)
func TestConditionalAggregateEmptyResult(t *testing.T) {
	// Create relation where all filter values are false
	symbols := []query.Symbol{datalog.NewSymbol("?filter"), datalog.NewSymbol("?value")}
	tuples := []Tuple{
		{false, 10.0},
		{false, 20.0},
		{false, 30.0},
	}
	rel := NewMaterializedRelation(symbols, tuples)

	// Conditional aggregate with filter that matches nothing
	findElements := []query.FindElement{
		query.FindAggregate{
			Function:  "min",
			Arg:       datalog.NewSymbol("?value"),
			Predicate: datalog.NewSymbol("?filter"), // All false - no matches
		},
	}

	result := ExecuteAggregations(rel, findElements)

	// Relational theory: empty input → empty output
	// Should get zero tuples (not one tuple with NULL)
	assert.Equal(t, 0, result.Size(), "expected empty result set when no tuples match filter")
}

// TestConditionalAggregateMixedTypes tests multiple aggregates with different predicates
func TestConditionalAggregateMixedTypes(t *testing.T) {
	// Create relation with multiple filter symbols
	symbols := []query.Symbol{datalog.NewSymbol("?early"), datalog.NewSymbol("?late"), datalog.NewSymbol("?price")}
	tuples := []Tuple{
		{true, false, 100.0}, // Early
		{true, false, 102.0}, // Early
		{false, true, 105.0}, // Late
		{false, true, 103.0}, // Late
	}
	rel := NewMaterializedRelation(symbols, tuples)

	// Two conditional aggregates with different predicates
	findElements := []query.FindElement{
		query.FindAggregate{
			Function:  "min",
			Arg:       datalog.NewSymbol("?price"),
			Predicate: datalog.NewSymbol("?early"), // Min of early prices
		},
		query.FindAggregate{
			Function:  "max",
			Arg:       datalog.NewSymbol("?price"),
			Predicate: datalog.NewSymbol("?late"), // Max of late prices
		},
	}

	result := ExecuteAggregations(rel, findElements)

	assert.Equal(t, 1, result.Size())

	it := result.Iterator()
	defer it.Close()

	assert.True(t, it.Next())
	tuple := it.Tuple()
	assert.Equal(t, 2, len(tuple))
	assert.Equal(t, 100.0, tuple[0], "min of early prices should be 100.0")
	assert.Equal(t, 105.0, tuple[1], "max of late prices should be 105.0")
}
