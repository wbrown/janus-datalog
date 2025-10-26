package executor

import (
	"fmt"
	"sync"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Note: Streaming aggregation settings are now managed by ExecutorOptions

// Debug flag for aggregation logging
const debugAggregation = false

// StreamingAggregationThreshold is the minimum relation size to use streaming
// For small relations, batch aggregation is faster due to lower overhead
const StreamingAggregationThreshold = 100

// ExecuteAggregations applies aggregation operations to a relation
// This is the main entry point for aggregation logic
func ExecuteAggregations(rel Relation, findElements []query.FindElement) Relation {
	return ExecuteAggregationsWithContext(nil, rel, findElements)
}

// ExecuteAggregationsWithContext applies aggregation operations with annotation support
func ExecuteAggregationsWithContext(ctx Context, rel Relation, findElements []query.FindElement) Relation {
	if debugAggregation {
		fmt.Printf("[ExecuteAggregations] Called with %d find elements, rel columns: %v\n", len(findElements), rel.Columns())
		for i, elem := range findElements {
			switch e := elem.(type) {
			case query.FindAggregate:
				fmt.Printf("[ExecuteAggregations] Element %d: FindAggregate - Function=%s, Arg=%s\n",
					i, e.Function, e.Arg)
			case query.FindVariable:
				fmt.Printf("[ExecuteAggregations] Element %d: FindVariable - Symbol=%s\n",
					i, e.Symbol)
			default:
				fmt.Printf("[ExecuteAggregations] Element %d: Unknown type %T\n", i, elem)
			}
		}
	}

	// Separate variables and aggregates
	var groupByVars []query.Symbol
	var aggregates []query.FindAggregate

	for _, elem := range findElements {
		switch e := elem.(type) {
		case query.FindVariable:
			groupByVars = append(groupByVars, e.Symbol)
		case query.FindAggregate:
			if debugAggregation {
				fmt.Printf("[ExecuteAggregations] Adding aggregate: Function=%s, Arg=%s, Predicate=%s, IsConditional=%v\n",
					e.Function, e.Arg, e.Predicate, e.IsConditional())
			}
			aggregates = append(aggregates, e)
		}
	}

	if debugAggregation {
		fmt.Printf("[ExecuteAggregations] Extracted %d aggregates, %d groupByVars: %v\n", len(aggregates), len(groupByVars), groupByVars)
		for i, agg := range aggregates {
			fmt.Printf("[ExecuteAggregations] Aggregate %d AFTER extraction: Function=%s, Arg=%s, Predicate=%s, IsConditional=%v\n",
				i, agg.Function, agg.Arg, agg.Predicate, agg.IsConditional())
		}
	}

	// If no aggregates, just project the variables
	if len(aggregates) == 0 {
		result, err := rel.Project(groupByVars)
		if err != nil {
			// Return empty relation on error
			opts := rel.Options()
			return NewMaterializedRelationWithOptions(groupByVars, []Tuple{}, opts)
		}
		return result
	}

	// Extract options from relation
	opts := rel.Options()

	// Check if streaming aggregation is applicable and beneficial
	useStreaming := opts.EnableStreamingAggregation &&
		len(aggregates) > 0 &&
		isStreamingEligible(aggregates) &&
		shouldUseStreaming(rel)

	if opts.EnableStreamingAggregationDebug {
		fmt.Printf("[ExecuteAggregations] aggregates=%d, eligible=%v, shouldUse=%v, useStreaming=%v, relType=%T, relSize=%d\n",
			len(aggregates), isStreamingEligible(aggregates), shouldUseStreaming(rel), useStreaming, rel, rel.Size())
	}

	// Emit aggregation annotation with find clause details
	if ctx != nil && ctx.Collector() != nil {
		data := ctx.Collector().GetDataMap()
		data["aggregate_count"] = len(aggregates)
		data["groupby_count"] = len(groupByVars)
		data["groupby_vars"] = groupByVars

		// Record the find elements for debugging
		findElemStrs := make([]string, len(findElements))
		for i, elem := range findElements {
			findElemStrs[i] = elem.String()
		}
		data["find_elements"] = findElemStrs

		// Record which aggregation mode was used (for testing/verification)
		if useStreaming {
			data["aggregation_mode"] = "streaming"
		} else {
			data["aggregation_mode"] = "batch"
		}

		ctx.Collector().AddTiming("aggregation/executed", time.Now(), data)
	}

	// If streaming is enabled and beneficial, use it
	if useStreaming {
		if opts.EnableStreamingAggregationDebug {
			fmt.Printf("[ExecuteAggregations] Using STREAMING aggregation (groupByVars=%v)\n", groupByVars)
		}
		// If no group-by variables, pass empty slice (single global group)
		return NewStreamingAggregateRelation(rel, groupByVars, aggregates)
	}

	if opts.EnableStreamingAggregationDebug {
		fmt.Printf("[ExecuteAggregations] Using BATCH aggregation\n")
	}

	// Otherwise, use batch aggregation (current implementation)
	// If no group-by variables, it's a single aggregation
	if len(groupByVars) == 0 {
		if debugAggregation {
			fmt.Printf("[ExecuteAggregations] Calling executeSingleAggregation with %d aggregates, rel.Size()=%d\n", len(aggregates), rel.Size())
		}
		result := executeSingleAggregation(rel, aggregates)
		if debugAggregation {
			fmt.Printf("[ExecuteAggregations] executeSingleAggregation returned: Size=%d, Columns=%v\n", result.Size(), result.Columns())
			if result.Size() > 0 {
				fmt.Printf("[ExecuteAggregations] First tuple: %v\n", result.Get(0))
			}
		}
		return result
	}

	// Otherwise, group by the variables and aggregate within groups
	return executeGroupedAggregation(rel, groupByVars, aggregates)
}

// isStreamingEligible checks if all aggregates can be computed in streaming fashion
func isStreamingEligible(aggregates []query.FindAggregate) bool {
	for _, agg := range aggregates {
		switch agg.Function {
		case "count", "sum", "avg", "min", "max":
			// These are streamable
			continue
		default:
			// Unsupported aggregate function (e.g., median, percentile)
			return false
		}
	}
	return true
}

// shouldUseStreaming determines if streaming aggregation would be beneficial
func shouldUseStreaming(rel Relation) bool {
	// For materialized relations, check size
	if matRel, ok := rel.(*MaterializedRelation); ok {
		return matRel.Size() >= StreamingAggregationThreshold
	}

	// For streaming relations, always use streaming aggregation
	// (avoids forcing materialization just to check size)
	if _, ok := rel.(*StreamingRelation); ok {
		return true
	}

	// For other relation types, use streaming by default
	return true
}

// executeSingleAggregation computes aggregates over the entire relation
func executeSingleAggregation(rel Relation, aggregates []query.FindAggregate) Relation {
	// Collect all values for each aggregate
	aggValues := make([][]interface{}, len(aggregates))
	for i := range aggValues {
		aggValues[i] = []interface{}{}
	}

	it := rel.Iterator()
	defer it.Close()

	columns := rel.Columns()

	// Find predicate indices for conditional aggregates
	predicateIndices := make([]int, len(aggregates))
	for i, agg := range aggregates {
		predicateIndices[i] = -1 // -1 means no predicate (unconditional)
		if agg.IsConditional() {
			for j, col := range columns {
				if col == agg.Predicate {
					predicateIndices[i] = j
					break
				}
			}
		}
	}

	for it.Next() {
		tuple := it.Tuple()
		for i, agg := range aggregates {
			// Check predicate for conditional aggregates
			predicateIdx := predicateIndices[i]
			if predicateIdx >= 0 {
				// Conditional aggregate - check predicate
				if predicateIdx < len(tuple) {
					// Predicate must be a boolean and true
					if pred, ok := tuple[predicateIdx].(bool); !ok || !pred {
						continue // Skip this value (predicate is false or not boolean)
					}
				} else {
					continue // Predicate column missing, skip
				}
			}

			// Predicate passed (or no predicate), find column index for this aggregate
			found := false
			for j, col := range columns {
				if col == agg.Arg {
					if j < len(tuple) {
						aggValues[i] = append(aggValues[i], tuple[j])
						found = true
					}
					break
				}
			}
			// DEBUG: Log when column not found
			if !found && len(columns) > 0 {
				fmt.Printf("AGGREGATE BUG: Column %v not found in columns %v for aggregate %d\n", agg.Arg, columns, i)
			}
		}
	}

	// Compute aggregates
	results := make(Tuple, len(aggregates))
	hasAnyValues := false
	for i, agg := range aggregates {
		if len(aggValues[i]) > 0 {
			hasAnyValues = true
		}
		results[i] = computeAggregateValues(aggValues[i], agg.Function)
	}

	// Build result columns (aggregate functions as column names)
	resultColumns := make([]query.Symbol, len(aggregates))
	for i, agg := range aggregates {
		// Use String() method which handles conditional vs unconditional formatting
		resultColumns[i] = query.Symbol(agg.String())
	}

	// Relational theory: empty input → empty output
	// If no aggregate has any values (all predicates failed or input was empty),
	// return empty result set instead of a row with nil values
	opts := rel.Options()
	if !hasAnyValues {
		return NewMaterializedRelationWithOptions(resultColumns, []Tuple{}, opts)
	}

	return NewMaterializedRelationWithOptions(resultColumns, []Tuple{results}, opts)
}

// executeGroupedAggregation performs aggregation with grouping
func executeGroupedAggregation(rel Relation, groupByVars []query.Symbol, aggregates []query.FindAggregate) Relation {
	// Create column mapping
	columns := rel.Columns()
	groupIndices := make([]int, len(groupByVars))
	for i, groupVar := range groupByVars {
		for j, col := range columns {
			if col == groupVar {
				groupIndices[i] = j
				break
			}
		}
	}

	aggIndices := make([]int, len(aggregates))
	for i, agg := range aggregates {
		for j, col := range columns {
			if col == agg.Arg {
				aggIndices[i] = j
				break
			}
		}
	}

	// Find predicate indices for conditional aggregates
	predicateIndices := make([]int, len(aggregates))
	for i, agg := range aggregates {
		predicateIndices[i] = -1 // -1 means no predicate (unconditional)
		if agg.IsConditional() {
			for j, col := range columns {
				if col == agg.Predicate {
					predicateIndices[i] = j
					break
				}
			}
			// If predicate column not found, we'll handle it during execution
		}
	}

	// Group tuples
	groups := make(map[string]Tuple)
	groupValues := make(map[string][][]interface{})

	it := rel.Iterator()
	defer it.Close()

	for it.Next() {
		tuple := it.Tuple()

		// Extract group key
		groupKey := ""
		groupTuple := make(Tuple, len(groupIndices))
		for i, idx := range groupIndices {
			if idx < len(tuple) {
				groupTuple[i] = tuple[idx]
				groupKey += stringifyValue(tuple[idx]) + "|"
			}
		}

		// Store group tuple (first occurrence)
		if _, exists := groups[groupKey]; !exists {
			groups[groupKey] = groupTuple
			groupValues[groupKey] = make([][]interface{}, len(aggregates))
			for i := range groupValues[groupKey] {
				groupValues[groupKey][i] = []interface{}{}
			}
		}

		// Collect values for aggregation (with predicate filtering for conditional aggregates)
		for i, idx := range aggIndices {
			if idx < len(tuple) {
				// Check predicate for conditional aggregates
				predicateIdx := predicateIndices[i]
				if predicateIdx >= 0 {
					// Conditional aggregate - check predicate
					if predicateIdx < len(tuple) {
						// Predicate must be a boolean and true
						if pred, ok := tuple[predicateIdx].(bool); !ok || !pred {
							continue // Skip this value (predicate is false or not boolean)
						}
					} else {
						continue // Predicate column missing, skip
					}
				}

				// Predicate passed (or no predicate), collect value
				groupValues[groupKey][i] = append(groupValues[groupKey][i], tuple[idx])
			}
		}
	}

	// Compute aggregates for each group
	var resultTuples []Tuple
	for groupKey, groupTuple := range groups {
		// Relational theory: if all aggregates for this group are empty
		// (all values filtered by predicates), exclude this group from result
		hasAnyValues := false
		for i := range aggregates {
			if len(groupValues[groupKey][i]) > 0 {
				hasAnyValues = true
				break
			}
		}
		if !hasAnyValues {
			continue // Skip this group - no values passed predicates
		}

		resultTuple := make(Tuple, len(groupByVars)+len(aggregates))

		// Add group-by values
		copy(resultTuple, groupTuple)

		// Add aggregate results
		for i, agg := range aggregates {
			resultTuple[len(groupByVars)+i] = computeAggregateValues(groupValues[groupKey][i], agg.Function)
		}

		resultTuples = append(resultTuples, resultTuple)
	}

	// Build result columns
	resultColumns := make([]query.Symbol, len(groupByVars)+len(aggregates))
	copy(resultColumns, groupByVars)
	for i, agg := range aggregates {
		// DEBUG: Print aggregate details
		if debugAggregation {
			fmt.Printf("[ExecuteAggregations] Aggregate %d: Function=%s, Arg=%s, Predicate=%s, IsConditional=%v, String()=%s\n",
				i, agg.Function, agg.Arg, agg.Predicate, agg.IsConditional(), agg.String())
		}
		// Use String() method which handles conditional vs unconditional formatting
		resultColumns[len(groupByVars)+i] = query.Symbol(agg.String())
	}

	opts := rel.Options()
	return NewMaterializedRelationWithOptions(resultColumns, resultTuples, opts)
}

// computeAggregateValues is already defined in executor.go
// We'll leave it there for now and reference it

// stringifyValue converts a value to string for grouping
func stringifyValue(v interface{}) string {
	if v == nil {
		return "<nil>"
	}
	switch val := v.(type) {
	case string:
		return val
	case time.Time:
		return val.Format(time.RFC3339)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ============================================================================
// Streaming Aggregation Implementation
// ============================================================================

// AggregateState maintains running aggregates for a single group
// Supports incremental updates for: sum, count, min, max, avg
type AggregateState struct {
	count int64
	sum   float64
	min   interface{}
	max   interface{}
}

// newAggregateState creates a new aggregate state
func newAggregateState() *AggregateState {
	return &AggregateState{
		count: 0,
		sum:   0,
		min:   nil,
		max:   nil,
	}
}

// Update incrementally updates aggregate state with a new value
func (s *AggregateState) Update(function string, value interface{}) {
	// Skip nil values (SQL semantics)
	if value == nil {
		return
	}

	switch function {
	case "count":
		s.count++

	case "sum", "avg":
		if num, ok := toFloat64(value); ok {
			s.sum += num
			s.count++
		}

	case "min":
		if s.min == nil || datalog.CompareValues(value, s.min) < 0 {
			s.min = value
		}
		s.count++

	case "max":
		if s.max == nil || datalog.CompareValues(value, s.max) > 0 {
			s.max = value
		}
		s.count++
	}
}

// GetResult returns the final aggregate result
func (s *AggregateState) GetResult(function string) interface{} {
	switch function {
	case "count":
		return s.count

	case "sum":
		if s.count == 0 {
			return nil
		}
		return s.sum

	case "avg":
		if s.count == 0 {
			return nil
		}
		return s.sum / float64(s.count)

	case "min":
		if s.count == 0 {
			return nil
		}
		return s.min

	case "max":
		if s.count == 0 {
			return nil
		}
		return s.max

	default:
		return nil
	}
}

// GroupKey represents a unique group for aggregation
type GroupKey struct {
	values []interface{}
}

// String returns a string representation for map keying
func (k GroupKey) String() string {
	result := ""
	for _, v := range k.values {
		result += stringifyValue(v) + "|"
	}
	return result
}

// StreamingAggregateRelation computes aggregates incrementally in a single pass
// This reduces memory usage from O(tuples) to O(groups) and eliminates intermediate
// materialization of all values before aggregation.
type StreamingAggregateRelation struct {
	source      Relation
	groupByVars []query.Symbol
	aggregates  []query.FindAggregate
	options     ExecutorOptions

	// Lazy materialization
	materializeOnce sync.Once
	materialized    *MaterializedRelation
}

// NewStreamingAggregateRelation creates a streaming aggregate relation
func NewStreamingAggregateRelation(source Relation, groupByVars []query.Symbol, aggregates []query.FindAggregate) *StreamingAggregateRelation {
	// Extract options from source relation
	opts := source.Options()

	return &StreamingAggregateRelation{
		source:      source,
		groupByVars: groupByVars,
		aggregates:  aggregates,
		options:     opts,
	}
}

// Columns returns the output columns
func (r *StreamingAggregateRelation) Columns() []query.Symbol {
	resultColumns := make([]query.Symbol, len(r.groupByVars)+len(r.aggregates))
	copy(resultColumns, r.groupByVars)
	for i, agg := range r.aggregates {
		// Use String() method which handles conditional vs unconditional formatting
		resultColumns[len(r.groupByVars)+i] = query.Symbol(agg.String())
	}
	return resultColumns
}

func (r *StreamingAggregateRelation) Symbols() []query.Symbol {
	return r.Columns()
}

// Options returns the executor options for this streaming aggregate relation
func (r *StreamingAggregateRelation) Options() ExecutorOptions {
	return r.options
}

// Iterator returns an iterator over the aggregated results
// Uses lazy materialization: aggregates are computed on first call, cached for subsequent calls
func (r *StreamingAggregateRelation) Iterator() Iterator {
	r.materializeOnce.Do(func() {
		r.materialized = r.materialize()
	})
	return r.materialized.Iterator()
}

// Size returns the number of groups (only known after materialization)
func (r *StreamingAggregateRelation) Size() int {
	// Trigger materialization to know size
	r.Iterator()
	return r.materialized.Size()
}

// IsEmpty returns true if there are no groups
func (r *StreamingAggregateRelation) IsEmpty() bool {
	return r.Size() == 0
}

// Get returns a specific tuple by index (requires materialization)
func (r *StreamingAggregateRelation) Get(i int) Tuple {
	r.Iterator()
	return r.materialized.Get(i)
}

// String returns a string representation (delegates to materialized result)
func (r *StreamingAggregateRelation) String() string {
	r.Iterator()
	return r.materialized.String()
}

// Table returns a table representation (delegates to materialized result)
func (r *StreamingAggregateRelation) Table() string {
	r.Iterator()
	return r.materialized.Table()
}

// ProjectFromPattern creates a new Relation with symbols from the pattern
func (r *StreamingAggregateRelation) ProjectFromPattern(pattern *query.DataPattern) Relation {
	r.Iterator()
	return r.materialized.ProjectFromPattern(pattern)
}

// Sorted returns tuples sorted by the relation's symbols
func (r *StreamingAggregateRelation) Sorted() []Tuple {
	r.Iterator()
	return r.materialized.Sorted()
}

// Project projects specific columns (delegates to materialized result)
func (r *StreamingAggregateRelation) Project(columns []query.Symbol) (Relation, error) {
	r.Iterator()
	return r.materialized.Project(columns)
}

// Materialize returns the materialized relation
func (r *StreamingAggregateRelation) Materialize() Relation {
	r.Iterator()
	return r.materialized
}

// Sort returns a new relation sorted by the specified order-by clauses
func (r *StreamingAggregateRelation) Sort(orderBy []query.OrderByClause) Relation {
	r.Iterator()
	return r.materialized.Sort(orderBy)
}

// Filter applies a filter function (delegates to materialized result)
func (r *StreamingAggregateRelation) Filter(filter Filter) Relation {
	r.Iterator()
	return r.materialized.Filter(filter)
}

// FilterWithPredicate applies a predicate filter (delegates to materialized result)
func (r *StreamingAggregateRelation) FilterWithPredicate(pred query.Predicate) Relation {
	r.Iterator()
	return r.materialized.FilterWithPredicate(pred)
}

// EvaluateFunction evaluates a function and adds result as new column
func (r *StreamingAggregateRelation) EvaluateFunction(fn query.Function, outputColumn query.Symbol) Relation {
	r.Iterator()
	return r.materialized.EvaluateFunction(fn, outputColumn)
}

// Select returns a new relation filtered by predicate
func (r *StreamingAggregateRelation) Select(pred func(Tuple) bool) Relation {
	r.Iterator()
	return r.materialized.Select(pred)
}

// Join performs a natural join with another relation
func (r *StreamingAggregateRelation) Join(other Relation) Relation {
	r.Iterator()
	return r.materialized.Join(other)
}

// HashJoin performs a hash join (delegates to materialized result)
func (r *StreamingAggregateRelation) HashJoin(other Relation, joinCols []query.Symbol) Relation {
	r.Iterator()
	return r.materialized.HashJoin(other, joinCols)
}

// SemiJoin returns tuples from this relation that have matches in the other
func (r *StreamingAggregateRelation) SemiJoin(other Relation, joinCols []query.Symbol) Relation {
	r.Iterator()
	return r.materialized.SemiJoin(other, joinCols)
}

// AntiJoin returns tuples from this relation that have no matches in the other
func (r *StreamingAggregateRelation) AntiJoin(other Relation, joinCols []query.Symbol) Relation {
	r.Iterator()
	return r.materialized.AntiJoin(other, joinCols)
}

// Aggregate applies aggregation (delegates to materialized result)
func (r *StreamingAggregateRelation) Aggregate(findElements []query.FindElement) Relation {
	r.Iterator()
	return r.materialized.Aggregate(findElements)
}

// materialize performs the actual streaming aggregation
func (r *StreamingAggregateRelation) materialize() *MaterializedRelation {
	// Build column index mappings
	columns := r.source.Columns()

	if r.options.EnableStreamingAggregationDebug {
		fmt.Printf("[StreamingAggregateRelation.materialize] Source columns: %v\n", columns)
		fmt.Printf("[StreamingAggregateRelation.materialize] Group-by vars: %v\n", r.groupByVars)
		fmt.Printf("[StreamingAggregateRelation.materialize] Aggregates: %v\n", r.aggregates)
	}

	groupIndices := make([]int, len(r.groupByVars))
	for i := range groupIndices {
		groupIndices[i] = -1 // Initialize to -1 (not found)
	}
	for i, groupVar := range r.groupByVars {
		for j, col := range columns {
			if col == groupVar {
				groupIndices[i] = j
				break
			}
		}
	}

	aggIndices := make([]int, len(r.aggregates))
	for i := range aggIndices {
		aggIndices[i] = -1 // Initialize to -1 (not found)
	}
	for i, agg := range r.aggregates {
		for j, col := range columns {
			if col == agg.Arg {
				aggIndices[i] = j
				break
			}
		}
	}

	if r.options.EnableStreamingAggregationDebug {
		fmt.Printf("[StreamingAggregateRelation.materialize] Group indices: %v\n", groupIndices)
		fmt.Printf("[StreamingAggregateRelation.materialize] Agg indices: %v\n", aggIndices)
	}

	// Find predicate indices for conditional aggregates
	predicateIndices := make([]int, len(r.aggregates))
	for i, agg := range r.aggregates {
		predicateIndices[i] = -1 // -1 means no predicate (unconditional)
		if agg.IsConditional() {
			for j, col := range columns {
				if col == agg.Predicate {
					predicateIndices[i] = j
					break
				}
			}
		}
	}

	// Single pass over source: group and aggregate incrementally
	// Use separate AggregateState per aggregate to support conditional aggregates properly
	groups := make(map[string][]*AggregateState)
	groupKeys := make(map[string]GroupKey)

	it := r.source.Iterator()
	defer it.Close()

	tupleCount := 0
	for it.Next() {
		tuple := it.Tuple()
		tupleCount++

		if r.options.EnableStreamingAggregationDebug && tupleCount <= 3 {
			fmt.Printf("[StreamingAggregateRelation.materialize] Tuple %d: %v (len=%d)\n", tupleCount, tuple, len(tuple))
		}

		// Extract group key
		keyValues := make([]interface{}, len(groupIndices))
		for i, idx := range groupIndices {
			if idx >= 0 && idx < len(tuple) {
				keyValues[i] = tuple[idx]
			}
		}
		key := GroupKey{values: keyValues}
		keyStr := key.String()

		// Get or create aggregate states (one per aggregate)
		states, exists := groups[keyStr]
		if !exists {
			states = make([]*AggregateState, len(r.aggregates))
			for i := range states {
				states[i] = newAggregateState()
			}
			groups[keyStr] = states
			groupKeys[keyStr] = key

			if r.options.EnableStreamingAggregationDebug {
				fmt.Printf("[StreamingAggregateRelation.materialize] Created new group: %s\n", keyStr)
			}
		}

		// Update aggregates incrementally (with predicate filtering for conditional aggregates)
		for i, agg := range r.aggregates {
			idx := aggIndices[i]
			if idx >= 0 && idx < len(tuple) {
				// Check predicate for conditional aggregates
				predicateIdx := predicateIndices[i]
				if predicateIdx >= 0 {
					// Conditional aggregate - check predicate
					if predicateIdx < len(tuple) {
						// Predicate must be a boolean and true
						if pred, ok := tuple[predicateIdx].(bool); !ok || !pred {
							continue // Skip this value (predicate is false or not boolean)
						}
					} else {
						continue // Predicate column missing, skip
					}
				}

				// Predicate passed (or no predicate), update aggregate
				value := tuple[idx]
				if r.options.EnableStreamingAggregationDebug && tupleCount <= 3 {
					fmt.Printf("[StreamingAggregateRelation.materialize] Updating aggregate %d (%s) with value: %v (type=%T)\n", i, agg.Function, value, value)
				}
				states[i].Update(agg.Function, value)
			} else {
				if r.options.EnableStreamingAggregationDebug && tupleCount <= 3 {
					fmt.Printf("[StreamingAggregateRelation.materialize] Skipping aggregate %d: idx=%d, len(tuple)=%d\n", i, idx, len(tuple))
				}
			}
		}
	}

	if r.options.EnableStreamingAggregationDebug {
		fmt.Printf("[StreamingAggregateRelation.materialize] Processed %d tuples, %d groups\n", tupleCount, len(groups))
	}

	// Convert groups to result tuples
	resultTuples := make([]Tuple, 0, len(groups))
	for keyStr, states := range groups {
		key := groupKeys[keyStr]
		resultTuple := make(Tuple, len(r.groupByVars)+len(r.aggregates))

		// Add group-by values
		copy(resultTuple, key.values)

		// Add aggregate results (one per aggregate state)
		for i, agg := range r.aggregates {
			result := states[i].GetResult(agg.Function)
			if r.options.EnableStreamingAggregationDebug {
				fmt.Printf("[StreamingAggregateRelation.materialize] Aggregate %d (%s) GetResult: %v (type=%T), state.count=%d, state.min=%v, state.max=%v\n",
					i, agg.Function, result, result, states[i].count, states[i].min, states[i].max)
			}
			resultTuple[len(r.groupByVars)+i] = result
		}

		resultTuples = append(resultTuples, resultTuple)
	}

	return NewMaterializedRelationWithOptions(r.Columns(), resultTuples, r.options)
}
