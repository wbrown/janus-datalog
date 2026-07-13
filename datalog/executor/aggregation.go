package executor

import (
	"fmt"
	"sync"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Note: Streaming aggregation settings are now managed by ExecutorOptions

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
	// Separate variables and aggregates
	var groupByVars []query.Symbol
	var aggregates []query.FindAggregate

	for _, elem := range findElements {
		switch e := elem.(type) {
		case query.FindVariable:
			groupByVars = append(groupByVars, e.Symbol)
		case query.FindAggregate:
			aggregates = append(aggregates, e)
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

	for _, groupBy := range groupByVars {
		if SymbolIndex(rel, groupBy) < 0 {
			resultSymbols := append([]query.Symbol(nil), groupByVars...)
			for _, aggregate := range aggregates {
				resultSymbols = append(resultSymbols, datalog.NewSymbol(aggregate.String()))
			}
			result := NewMaterializedRelationWithOptions(resultSymbols, nil, rel.Options())
			result.err = fmt.Errorf("group-by symbol %s is not present in source relation", groupBy)
			return result
		}
	}

	// Extract options from relation
	opts := rel.Options()

	// Check if streaming aggregation is applicable and beneficial
	useStreaming := opts.EnableStreamingAggregation &&
		len(aggregates) > 0 &&
		isStreamingEligible(aggregates) &&
		shouldUseStreaming(rel)

	collector := opts.Collector
	if ctx != nil && ctx.Collector() != nil {
		collector = ctx.Collector()
	}
	if collector != nil {
		strategy := "batch"
		if useStreaming {
			strategy = "streaming"
		}
		collector.Add(annotations.Event{
			Name: annotations.AggregationStrategy,
			Data: map[string]interface{}{
				"strategy":        strategy,
				"aggregate_count": len(aggregates),
				"group_by_count":  len(groupByVars),
				"stream_eligible": isStreamingEligible(aggregates),
				"input_size":      materializedSize(rel),
			},
		})
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

		ctx.Collector().AddTiming(annotations.AggregationExecuted, time.Now(), data)
	}

	// If streaming is enabled and beneficial, use it
	if useStreaming {
		// If no group-by variables, pass empty slice (single global group)
		result := NewStreamingAggregateRelation(rel, groupByVars, aggregates)
		result.options.Collector = collector
		return result
	}

	// Otherwise, use batch aggregation (current implementation)
	// If no group-by variables, it's a single aggregation
	if len(groupByVars) == 0 {
		return executeSingleAggregation(rel, aggregates)
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
func executeSingleAggregation(rel Relation, aggregates []query.FindAggregate) (result Relation) {
	// Collect all values for each aggregate
	aggValues := make([][]interface{}, len(aggregates))
	for i := range aggValues {
		aggValues[i] = []interface{}{}
	}

	it := rel.Iterator()
	var aggErr error
	defer func() {
		if closeErr := it.Close(); aggErr == nil {
			aggErr = closeErr
		}
		if aggErr != nil && result != nil {
			if materialized, ok := result.(*MaterializedRelation); ok && materialized.err == nil {
				materialized.err = aggErr
			}
		}
	}()

	symbols := rel.Symbols()

	// Find predicate indices for conditional aggregates
	predicateIndices := make([]int, len(aggregates))
	for i, agg := range aggregates {
		predicateIndices[i] = -1 // -1 means no predicate (unconditional)
		if agg.IsConditional() {
			for j, sym := range symbols {
				if sym == agg.Predicate {
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
					continue // Predicate symbol missing, skip
				}
			}

			// Predicate passed (or no predicate), find symbol index for this aggregate
			for j, sym := range symbols {
				if sym == agg.Arg {
					if j < len(tuple) {
						aggValues[i] = append(aggValues[i], tuple[j])
					}
					break
				}
			}
		}
	}

	// Capture any deferred error from the input scan: a failed scan must not be
	// silently aggregated into an empty/zero result.
	aggErr = it.Error()

	// Compute aggregates
	results := make(Tuple, len(aggregates))
	hasAnyValues := false
	for i, agg := range aggregates {
		if len(aggValues[i]) > 0 {
			hasAnyValues = true
		}
		results[i] = computeAggregateValues(aggValues[i], agg.Function)
	}

	// Build result symbols (aggregate functions as symbol names)
	resultSymbols := make([]query.Symbol, len(aggregates))
	for i, agg := range aggregates {
		// Use String() method which handles conditional vs unconditional formatting
		resultSymbols[i] = datalog.NewSymbol(agg.String())
	}

	// Relational theory: empty input → empty output
	// If no aggregate has any values (all predicates failed or input was empty),
	// return empty result set instead of a tuple with nil values
	opts := rel.Options()
	if !hasAnyValues {
		empty := NewMaterializedRelationWithOptions(resultSymbols, []Tuple{}, opts)
		empty.err = aggErr
		return empty
	}

	result = NewMaterializedRelationWithOptions(resultSymbols, []Tuple{results}, opts)
	return result
}

// executeGroupedAggregation performs aggregation with grouping
func executeGroupedAggregation(
	rel Relation,
	groupByVars []query.Symbol,
	aggregates []query.FindAggregate,
) (result Relation) {
	// Create symbol mapping
	symbols := rel.Symbols()
	groupIndices := make([]int, len(groupByVars))
	for i, groupVar := range groupByVars {
		for j, sym := range symbols {
			if sym == groupVar {
				groupIndices[i] = j
				break
			}
		}
	}

	aggIndices := make([]int, len(aggregates))
	for i, agg := range aggregates {
		for j, sym := range symbols {
			if sym == agg.Arg {
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
			for j, sym := range symbols {
				if sym == agg.Predicate {
					predicateIndices[i] = j
					break
				}
			}
			// If predicate symbol not found, we'll handle it during execution
		}
	}

	// Group tuples by their typed values. TupleKeyMap uses the same hashing and
	// ValuesEqual semantics as joins and deduplication, so distinct datalog
	// values cannot collapse through string formatting or delimiter ambiguity.
	groups := NewTupleKeyMap()
	var groupOrder []*batchAggregateGroup

	it := rel.Iterator()
	var aggregateErr error
	defer func() {
		if closeErr := it.Close(); aggregateErr == nil {
			aggregateErr = closeErr
		}
		if aggregateErr != nil && result != nil {
			if materialized, ok := result.(*MaterializedRelation); ok && materialized.err == nil {
				materialized.err = aggregateErr
			}
		}
	}()

	for it.Next() {
		tuple := it.Tuple()

		key := NewTupleKey(tuple, groupIndices)
		groupValue, exists := groups.Get(key)
		var group *batchAggregateGroup
		if exists {
			group = groupValue.(*batchAggregateGroup)
		} else {
			group = &batchAggregateGroup{
				key:    Tuple(key.values),
				values: make([][]interface{}, len(aggregates)),
			}
			groups.Put(key, group)
			groupOrder = append(groupOrder, group)
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
						continue // Predicate symbol missing, skip
					}
				}

				// Predicate passed (or no predicate), collect value
				group.values[i] = append(group.values[i], tuple[idx])
			}
		}
	}

	// Compute aggregates for each group
	resultTuples := make([]Tuple, 0, len(groupOrder))
	for _, group := range groupOrder {
		// Relational theory: if all aggregates for this group are empty
		// (all values filtered by predicates), exclude this group from result
		hasAnyValues := false
		for i := range aggregates {
			if len(group.values[i]) > 0 {
				hasAnyValues = true
				break
			}
		}
		if !hasAnyValues {
			continue // Skip this group - no values passed predicates
		}

		resultTuple := make(Tuple, len(groupByVars)+len(aggregates))

		// Add group-by values
		copy(resultTuple, group.key)

		// Add aggregate results
		for i, agg := range aggregates {
			resultTuple[len(groupByVars)+i] = computeAggregateValues(group.values[i], agg.Function)
		}

		resultTuples = append(resultTuples, resultTuple)
	}

	// Build result symbols
	resultSymbols := make([]query.Symbol, len(groupByVars)+len(aggregates))
	copy(resultSymbols, groupByVars)
	for i, agg := range aggregates {
		// Use String() method which handles conditional vs unconditional formatting
		resultSymbols[len(groupByVars)+i] = datalog.NewSymbol(agg.String())
	}

	opts := rel.Options()
	result = NewMaterializedRelationWithOptions(resultSymbols, resultTuples, opts)
	// Carry any deferred error from the grouped input scan so a failed scan
	// isn't laundered into a partial/empty grouped result.
	aggregateErr = it.Error()
	return result
}

type batchAggregateGroup struct {
	key    Tuple
	values [][]interface{}
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

// Symbols returns the output symbols
func (r *StreamingAggregateRelation) Symbols() []query.Symbol {
	resultSymbols := make([]query.Symbol, len(r.groupByVars)+len(r.aggregates))
	copy(resultSymbols, r.groupByVars)
	for i, agg := range r.aggregates {
		// Use String() method which handles conditional vs unconditional formatting
		resultSymbols[len(r.groupByVars)+i] = datalog.NewSymbol(agg.String())
	}
	return resultSymbols
}

func (r *StreamingAggregateRelation) Properties() RelationProperties {
	if len(r.groupByVars) == 0 {
		return RelationProperties{}
	}
	return RelationProperties{Keys: [][]query.Symbol{
		append([]query.Symbol(nil), r.groupByVars...),
	}}
}

// Options returns the executor options for this streaming aggregate relation
func (r *StreamingAggregateRelation) Options() ExecutorOptions {
	return r.options
}

// RequiresCopy returns false because StreamingAggregateRelation materializes
// all results internally before returning them via Iterator(). The tuples
// are stored in a slice and not reused across iterations.
func (r *StreamingAggregateRelation) RequiresCopy() bool { return false }

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
func (r *StreamingAggregateRelation) Sorted() ([]Tuple, error) {
	r.Iterator()
	return r.materialized.Sorted()
}

// Project projects specific symbols (delegates to materialized result)
func (r *StreamingAggregateRelation) Project(symbols []query.Symbol) (Relation, error) {
	r.Iterator()
	return r.materialized.Project(symbols)
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

// EvaluateFunction evaluates a function and adds result as new symbol
func (r *StreamingAggregateRelation) EvaluateFunction(fn query.Function, outputSymbol query.Symbol) Relation {
	r.Iterator()
	return r.materialized.EvaluateFunction(fn, outputSymbol)
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
func (r *StreamingAggregateRelation) HashJoin(other Relation, joinSyms []query.Symbol) Relation {
	r.Iterator()
	return r.materialized.HashJoin(other, joinSyms)
}

// SemiJoin returns tuples from this relation that have matches in the other
func (r *StreamingAggregateRelation) SemiJoin(other Relation, joinSyms []query.Symbol) Relation {
	r.Iterator()
	return r.materialized.SemiJoin(other, joinSyms)
}

// AntiJoin returns tuples from this relation that have no matches in the other
func (r *StreamingAggregateRelation) AntiJoin(other Relation, joinSyms []query.Symbol) Relation {
	r.Iterator()
	return r.materialized.AntiJoin(other, joinSyms)
}

// Aggregate applies aggregation (delegates to materialized result)
func (r *StreamingAggregateRelation) Aggregate(findElements []query.FindElement) Relation {
	r.Iterator()
	return r.materialized.Aggregate(findElements)
}

// materialize performs the actual streaming aggregation
func (r *StreamingAggregateRelation) materialize() (result *MaterializedRelation) {
	// Build symbol index mappings
	symbols := r.source.Symbols()

	groupIndices := make([]int, len(r.groupByVars))
	for i := range groupIndices {
		groupIndices[i] = -1 // Initialize to -1 (not found)
	}
	for i, groupVar := range r.groupByVars {
		for j, sym := range symbols {
			if sym == groupVar {
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
		for j, sym := range symbols {
			if sym == agg.Arg {
				aggIndices[i] = j
				break
			}
		}
	}

	// Find predicate indices for conditional aggregates
	predicateIndices := make([]int, len(r.aggregates))
	for i, agg := range r.aggregates {
		predicateIndices[i] = -1 // -1 means no predicate (unconditional)
		if agg.IsConditional() {
			for j, sym := range symbols {
				if sym == agg.Predicate {
					predicateIndices[i] = j
					break
				}
			}
		}
	}

	// Single pass over source: group and aggregate incrementally. TupleKeyMap
	// preserves typed datalog equality without allocating formatted string keys.
	groups := NewTupleKeyMap()
	var groupOrder []*streamingAggregateGroup

	it := r.source.Iterator()
	var aggregateErr error
	defer func() {
		if closeErr := it.Close(); aggregateErr == nil {
			aggregateErr = closeErr
		}
		if aggregateErr != nil && result != nil && result.err == nil {
			result.err = aggregateErr
		}
	}()

	tupleCount := 0
	for it.Next() {
		tuple := it.Tuple()
		tupleCount++

		key := NewTupleKey(tuple, groupIndices)
		groupValue, exists := groups.Get(key)
		var group *streamingAggregateGroup
		if exists {
			group = groupValue.(*streamingAggregateGroup)
		} else {
			group = &streamingAggregateGroup{
				key:    Tuple(key.values),
				states: make([]*AggregateState, len(r.aggregates)),
			}
			for i := range group.states {
				group.states[i] = newAggregateState()
			}
			groups.Put(key, group)
			groupOrder = append(groupOrder, group)
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
						continue // Predicate symbol missing, skip
					}
				}

				// Predicate passed (or no predicate), update aggregate
				value := tuple[idx]
				group.states[i].Update(agg.Function, value)
			}
		}
	}

	// Convert groups to result tuples
	resultTuples := make([]Tuple, 0, len(groupOrder))
	for _, group := range groupOrder {
		resultTuple := make(Tuple, len(r.groupByVars)+len(r.aggregates))

		// Add group-by values
		copy(resultTuple, group.key)

		// Add aggregate results (one per aggregate state)
		for i, agg := range r.aggregates {
			result := group.states[i].GetResult(agg.Function)
			resultTuple[len(r.groupByVars)+i] = result
		}

		resultTuples = append(resultTuples, resultTuple)
	}

	if r.options.Collector != nil {
		r.options.Collector.Add(annotations.Event{
			Name: annotations.AggregationMaterialized,
			Data: map[string]interface{}{
				"input_count":  tupleCount,
				"group_count":  len(groupOrder),
				"result_count": len(resultTuples),
			},
		})
	}
	aggregateErr = it.Error()
	result = NewMaterializedRelationWithOptions(r.Symbols(), resultTuples, r.options)
	return result
}

type streamingAggregateGroup struct {
	key    Tuple
	states []*AggregateState
}
