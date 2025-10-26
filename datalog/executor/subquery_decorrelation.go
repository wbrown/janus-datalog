package executor

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// executeDecorrelatedSubqueries executes a group of decorrelated subqueries
//
// This implements Selinger's subquery decorrelation optimization:
// Instead of executing each subquery once per input tuple (e.g., 260 hours × 4 subqueries = 1,040 executions),
// we execute one merged query per filter group (e.g., 3 filter groups = 3 executions).
//
// The merged queries compute ALL groups via GROUP BY, then we hash join the results.
func executeDecorrelatedSubqueries(ctx Context,
	exec *Executor,
	decorPlan *planner.DecorrelatedSubqueryPlan,
	inputRelation Relation) (Relation, error) {

	start := time.Now()
	collector := ctx.Collector()

	// Add begin event with full details
	if collector != nil {
		collector.Add(annotations.Event{
			Name:  "decorrelated_subqueries/begin",
			Start: start,
			Data: map[string]interface{}{
				"signature_hash":      decorPlan.SignatureHash,
				"total_subqueries":    decorPlan.TotalSubqueries,
				"decorrelated_count":  decorPlan.DecorrelatedCount,
				"filter_groups":       len(decorPlan.MergedPlans),
				"original_subqueries": len(decorPlan.OriginalSubqueries),
				"input_size":          inputRelation.Size(),
				"correlation_keys":    decorPlan.CorrelationKeys,
			},
		})
	}

	// Execute each merged query ONCE (computes ALL groups via GROUP BY)
	var groupResults []Relation

	// OPTIMIZATION: Extract time ranges from input relation for semi-join pushdown
	// Convert correlation keys like (year, month, day, hour) into time ranges
	// This allows merged queries to scan only relevant time periods instead of ALL bars
	//
	// IMPORTANT: Skip extraction if input relation is too small (<50 tuples)
	// The optimization has a threshold of >50 ranges in the matcher, so extracting
	// time ranges for smaller input relations just adds overhead with no benefit.
	var timeRanges []TimeRange
	var err error

	if inputRelation.Size() >= 50 {
		timeRanges, err = extractTimeRanges(inputRelation, decorPlan.CorrelationKeys)
		if err != nil {
			return nil, fmt.Errorf("failed to extract time ranges: %w", err)
		}
	}

	// Add time ranges to each merged query's metadata
	if len(timeRanges) > 0 {
		for _, plan := range decorPlan.MergedPlans {
			if plan.Metadata == nil {
				plan.Metadata = make(map[string]interface{})
			}
			plan.Metadata["time_ranges"] = timeRanges
		}

		if collector != nil {
			collector.Add(annotations.Event{
				Name:  "decorrelated_subqueries/time_range_pushdown",
				Start: start,
				Data: map[string]interface{}{
					"num_ranges":       len(timeRanges),
					"optimization":     "semi-join via time ranges",
					"correlation_keys": decorPlan.CorrelationKeys,
				},
			})
		}
	}

	// Check if parallel execution is enabled
	enableParallel := exec.planner != nil && exec.planner.Options().EnableParallelDecorrelation

	if enableParallel {
		// PARALLEL EXECUTION: Use worker pool pattern with concurrency limit
		numQueries := len(decorPlan.MergedPlans)
		groupResults = make([]Relation, numQueries)
		errors := make([]error, numQueries)
		timings := make([]time.Time, numQueries)

		// Determine number of workers (same pattern as RelationInput parallel execution)
		numWorkers := exec.maxSubqueryWorkers
		if numWorkers <= 0 {
			numWorkers = runtime.NumCPU()
		}

		// Worker pool using semaphore pattern
		var wg sync.WaitGroup
		sem := make(chan struct{}, numWorkers)

		for i, mergedPlan := range decorPlan.MergedPlans {
			wg.Add(1)
			go func(idx int, plan *planner.QueryPlan) {
				defer wg.Done()

				// Acquire semaphore
				sem <- struct{}{}
				defer func() { <-sem }()

				timings[idx] = time.Now()

				result, err := executePhasesWithInputs(ctx, exec, plan, []Relation{})
				if err != nil {
					errors[idx] = fmt.Errorf("merged query %d failed: %w", idx, err)
					return
				}
				groupResults[idx] = result

				if collector != nil {
					collector.AddTiming(fmt.Sprintf("decorrelated_subqueries/merged_query_%d", idx), timings[idx], map[string]interface{}{
						"result_size":    result.Size(),
						"result_columns": result.Columns(),
					})
				}
			}(i, mergedPlan)
		}
		wg.Wait()

		// Check for errors from parallel execution
		for _, err := range errors {
			if err != nil {
				return nil, err
			}
		}
	} else {
		// SEQUENTIAL EXECUTION: Execute merged queries one at a time
		for i, mergedPlan := range decorPlan.MergedPlans {
			mergedStart := time.Now()

			result, err := executePhasesWithInputs(ctx, exec, mergedPlan, []Relation{})
			if err != nil {
				return nil, fmt.Errorf("merged query %d failed: %w", i, err)
			}
			groupResults = append(groupResults, result)

			if collector != nil {
				collector.AddTiming(fmt.Sprintf("decorrelated_subqueries/merged_query_%d", i), mergedStart, map[string]interface{}{
					"result_size":    result.Size(),
					"result_columns": result.Columns(),
				})
			}
		}
	}

	// Join all results on their grouping vars
	// For OHLC: groupResults[0] has [?sym ?py ?pm ?pd ?ph ...], groupResults[1] has [?sym ?py ?pm ?pd ?ph ...]
	// We join them on [?sym ?py ?pm ?pd ?ph]
	var joinKeys []query.Symbol
	if len(decorPlan.GroupingVars) > 0 {
		joinKeys = decorPlan.GroupingVars[0] // Use first filter group's grouping vars
	}

	combinedResult, err := joinDecorrelatedResults(groupResults, joinKeys)
	if err != nil {
		return nil, fmt.Errorf("joining decorrelated results failed: %w", err)
	}

	if collector != nil {
		collector.Add(annotations.Event{
			Name:  "decorrelated_subqueries/combined",
			Start: start,
			Data: map[string]interface{}{
				"combined_size":    combinedResult.Size(),
				"combined_columns": combinedResult.Columns(),
			},
		})
	}

	// Join with input relation using correlation keys from outer query
	// The merged result has grouping vars (e.g., ?sym, ?py, ?pm, ?pd, ?ph)
	// The input has correlation keys (e.g., ?s, ?year, ?month, ?day, ?hour)
	// We need to join on these corresponding variables
	joined := hashJoinWithMapping(inputRelation, combinedResult,
		decorPlan.CorrelationKeys, joinKeys)

	if collector != nil {
		collector.Add(annotations.Event{
			Name:  "decorrelated_subqueries/joined_with_input",
			Start: start,
			Data: map[string]interface{}{
				"joined_size":    joined.Size(),
				"joined_columns": joined.Columns(),
			},
		})
	}

	// Rename and reorder columns in one step to match original subquery order
	// This fixes the parallel decorrelation column ordering bug
	finalResult := applyBindingRenamesAndReorder(joined, groupResults, decorPlan, inputRelation.Columns())

	// Add completion event
	if collector != nil {
		collector.AddTiming("decorrelated_subqueries/complete", start, map[string]interface{}{
			"filter_groups": len(decorPlan.MergedPlans),
			"result_size":   finalResult.Size(),
		})
	}

	return finalResult, nil
}

// applyBindingRenamesAndReorder renames and reorders columns in one operation.
//
// This function fixes the parallel decorrelation column ordering bug by:
// 1. Building the final column list in original subquery order (from :where clause)
// 2. Creating tuples with values in that order
//
// The joined result has: [input columns] + [aggregate columns in join order]
// We transform to: [input columns] + [aggregate columns in original subquery order]
func applyBindingRenamesAndReorder(joined Relation, groupResults []Relation, decorPlan *planner.DecorrelatedSubqueryPlan,
	inputColumns []query.Symbol) Relation {

	// Find the maximum original subquery index to determine how many subqueries there were
	maxSubqIdx := -1
	for subqIdx := range decorPlan.ColumnMapping {
		if subqIdx > maxSubqIdx {
			maxSubqIdx = subqIdx
		}
	}

	// Build final columns in correct order: [input columns] + [binding vars in original subquery order]
	finalColumns := make([]query.Symbol, len(inputColumns))
	copy(finalColumns, inputColumns)

	// Collect binding variables in original subquery order
	var orderedBindingVars []query.Symbol
	for subqIdx := 0; subqIdx <= maxSubqIdx; subqIdx++ {
		if resultMap, exists := decorPlan.ColumnMapping[subqIdx]; exists {
			orderedBindingVars = append(orderedBindingVars, resultMap.BindingVars...)
		}
	}
	finalColumns = append(finalColumns, orderedBindingVars...)

	// Build mapping from binding variables to aggregate column symbols
	// We need to look at each filter group's output to know which aggregate corresponds to which binding var
	bindingToSymbol := make(map[query.Symbol]query.Symbol)

	for subqIdx := 0; subqIdx <= maxSubqIdx; subqIdx++ {
		if resultMap, exists := decorPlan.ColumnMapping[subqIdx]; exists {
			// Get this filter group's result columns
			filterGroupResult := groupResults[resultMap.FilterGroupIdx]
			filterGroupCols := filterGroupResult.Columns()

			// Map each binding variable to its actual column symbol in the filter group
			for i, bindingVar := range resultMap.BindingVars {
				if i < len(resultMap.ColumnIndices) {
					colIdx := resultMap.ColumnIndices[i]
					if colIdx < len(filterGroupCols) {
						// The actual column symbol (like "(max ?h)") in the filter group
						bindingToSymbol[bindingVar] = filterGroupCols[colIdx]
					}
				}
			}
		}
	}

	// Build index mapping: for each final column position, where to find it in joined relation
	oldColumns := joined.Columns()
	indexMapping := make([]int, len(finalColumns))

	for finalIdx, col := range finalColumns {
		// Check if this is a binding variable that maps to an aggregate
		if aggSymbol, isBound := bindingToSymbol[col]; isBound {
			// Find the aggregate column in the joined result
			for joinedIdx, joinedCol := range oldColumns {
				if aggSymbol == joinedCol {
					indexMapping[finalIdx] = joinedIdx
					break
				}
			}
		} else {
			// This is an input column - find by name directly
			for joinedIdx, joinedCol := range oldColumns {
				if col == joinedCol {
					indexMapping[finalIdx] = joinedIdx
					break
				}
			}
		}
	}

	// Reorder tuples according to the mapping
	tuples := make([]Tuple, joined.Size())
	it := joined.Iterator()
	idx := 0
	for it.Next() {
		oldTuple := it.Tuple()
		newTuple := make(Tuple, len(finalColumns))
		for newIdx, oldIdx := range indexMapping {
			if oldIdx < len(oldTuple) {
				newTuple[newIdx] = oldTuple[oldIdx]
			}
		}
		tuples[idx] = newTuple
		idx++
	}
	it.Close()

	return NewMaterializedRelation(finalColumns, tuples)
}

// joinDecorrelatedResults joins multiple decorrelated query results
func joinDecorrelatedResults(results []Relation, keys []query.Symbol) (Relation, error) {
	if len(results) == 0 {
		return nil, fmt.Errorf("no results to join")
	}

	if len(results) == 1 {
		return results[0], nil
	}

	// Join first two
	combined := hashJoinOnKeys(results[0], results[1], keys)

	// Join remaining
	for i := 2; i < len(results); i++ {
		combined = hashJoinOnKeys(combined, results[i], keys)
	}

	return combined, nil
}

// hashJoinWithMapping performs hash join with different key names on each side
//
// leftKeys: key column names in left relation
// rightKeys: key column names in right relation (must correspond positionally to leftKeys)
func hashJoinWithMapping(left, right Relation, leftKeys, rightKeys []query.Symbol) Relation {
	// Filter out $ from keys
	var filteredLeftKeys, filteredRightKeys []query.Symbol
	rightIdx := 0
	for _, key := range leftKeys {
		if key == "$" {
			continue
		}
		filteredLeftKeys = append(filteredLeftKeys, key)
		if rightIdx < len(rightKeys) {
			filteredRightKeys = append(filteredRightKeys, rightKeys[rightIdx])
			rightIdx++
		}
	}

	// Find key column indices in both relations
	leftIndices := make([]int, len(filteredLeftKeys))
	rightIndices := make([]int, len(filteredRightKeys))

	for i := range filteredLeftKeys {
		leftIndices[i] = ColumnIndex(left, filteredLeftKeys[i])
		rightIndices[i] = ColumnIndex(right, filteredRightKeys[i])

		// If key not found in either relation, return empty
		if leftIndices[i] < 0 || rightIndices[i] < 0 {
			resultColumns := append(left.Columns(), filterColumns(right.Columns(), filteredRightKeys)...)
			return NewMaterializedRelation(resultColumns, []Tuple{})
		}
	}

	// Build hash table from smaller relation
	var buildRel, probeRel Relation
	var buildIndices, probeIndices []int
	var buildIsLeft bool

	if left.Size() <= right.Size() {
		buildRel = left
		probeRel = right
		buildIndices = leftIndices
		probeIndices = rightIndices
		buildIsLeft = true
	} else {
		buildRel = right
		probeRel = left
		buildIndices = rightIndices
		probeIndices = leftIndices
		buildIsLeft = false
	}

	// Build hash table: key -> tuples
	hashTable := make(map[string][]Tuple)

	it := buildRel.Iterator()
	for it.Next() {
		tuple := it.Tuple()
		key := makeJoinKey(tuple, buildIndices)
		hashTable[key] = append(hashTable[key], tuple)
	}
	it.Close()

	// Probe and build result
	var resultTuples []Tuple
	resultColumns := append(left.Columns(), filterColumns(right.Columns(), filteredRightKeys)...)

	it = probeRel.Iterator()
	for it.Next() {
		probeTuple := it.Tuple()
		key := makeJoinKey(probeTuple, probeIndices)

		if buildTuples, found := hashTable[key]; found {
			for _, buildTuple := range buildTuples {
				// Combine tuples, filtering out duplicate key columns from right side
				var combined Tuple
				if buildIsLeft {
					// left is build, right is probe
					// Result: left + (right - keys)
					combined = append(buildTuple, filterTupleValues(probeTuple, probeIndices)...)
				} else {
					// right is build, left is probe
					// Result: left + (right - keys)
					combined = append(probeTuple, filterTupleValues(buildTuple, buildIndices)...)
				}
				resultTuples = append(resultTuples, combined)
			}
		}
	}
	it.Close()

	return NewMaterializedRelation(resultColumns, resultTuples)
}

// hashJoinOnKeys performs hash join on specified key columns
//
// This is a standard hash join implementation:
// 1. Build hash table from smaller relation
// 2. Probe with larger relation
// 3. Filter out duplicate key columns from result
func hashJoinOnKeys(left, right Relation, keys []query.Symbol) Relation {
	// Find key column indices in both relations
	leftIndices := make([]int, len(keys))
	rightIndices := make([]int, len(keys))

	for i, key := range keys {
		leftIndices[i] = ColumnIndex(left, key)
		rightIndices[i] = ColumnIndex(right, key)

		// If key not found in either relation, this is an error
		// But we'll handle it gracefully by treating it as no match
		if leftIndices[i] < 0 || rightIndices[i] < 0 {
			// Return empty relation
			resultColumns := append(left.Columns(), filterColumns(right.Columns(), keys)...)
			return NewMaterializedRelation(resultColumns, []Tuple{})
		}
	}

	// Build hash table from smaller relation
	var buildRel, probeRel Relation
	var buildIndices, probeIndices []int
	var buildIsLeft bool

	if left.Size() <= right.Size() {
		buildRel = left
		probeRel = right
		buildIndices = leftIndices
		probeIndices = rightIndices
		buildIsLeft = true
	} else {
		buildRel = right
		probeRel = left
		buildIndices = rightIndices
		probeIndices = leftIndices
		buildIsLeft = false
	}

	// Build hash table: key -> tuples
	hashTable := make(map[string][]Tuple)

	it := buildRel.Iterator()
	for it.Next() {
		tuple := it.Tuple()
		key := makeJoinKey(tuple, buildIndices)
		hashTable[key] = append(hashTable[key], tuple)
	}
	it.Close()

	// Probe and build result
	var resultTuples []Tuple
	resultColumns := append(left.Columns(), filterColumns(right.Columns(), keys)...)

	it = probeRel.Iterator()
	for it.Next() {
		probeTuple := it.Tuple()
		key := makeJoinKey(probeTuple, probeIndices)

		if buildTuples, found := hashTable[key]; found {
			for _, buildTuple := range buildTuples {
				// Combine tuples, filtering out duplicate key columns
				var combined Tuple
				if buildIsLeft {
					// left is build, right is probe
					// Result: left + (right - keys)
					combined = append(buildTuple, filterTupleValues(probeTuple, probeIndices)...)
				} else {
					// right is build, left is probe
					// Result: left + (right - keys)
					combined = append(probeTuple, filterTupleValues(buildTuple, buildIndices)...)
				}
				resultTuples = append(resultTuples, combined)
			}
		}
	}
	it.Close()

	return NewMaterializedRelation(resultColumns, resultTuples)
}

// makeJoinKey creates a string key from tuple values at specified indices
func makeJoinKey(tuple Tuple, indices []int) string {
	var parts []string
	for _, idx := range indices {
		if idx >= 0 && idx < len(tuple) {
			parts = append(parts, fmt.Sprintf("%v", tuple[idx]))
		} else {
			parts = append(parts, "NULL")
		}
	}
	return strings.Join(parts, "|")
}

// filterColumns removes key columns from column list (to avoid duplicates in join result)
func filterColumns(columns []query.Symbol, keys []query.Symbol) []query.Symbol {
	keySet := make(map[query.Symbol]bool)
	for _, key := range keys {
		keySet[key] = true
	}

	var result []query.Symbol
	for _, col := range columns {
		if !keySet[col] {
			result = append(result, col)
		}
	}
	return result
}

// filterTupleValues removes values at key indices from tuple (to avoid duplicates in join result)
func filterTupleValues(tuple Tuple, indices []int) Tuple {
	indexSet := make(map[int]bool)
	for _, idx := range indices {
		indexSet[idx] = true
	}

	var result Tuple
	for i, val := range tuple {
		if !indexSet[i] {
			result = append(result, val)
		}
	}
	return result
}

// TimeRange represents a half-open time interval [Start, End)
type TimeRange struct {
	Start time.Time
	End   time.Time
}

// timeKey is used for deduplication without string allocations
type timeKey struct {
	year, month, day, hour int64
}

// extractTimeRanges converts correlation key tuples into time ranges
// This enables semi-join pushdown by constraining merged queries to scan only relevant time periods
func extractTimeRanges(inputRelation Relation, correlationKeys []query.Symbol) ([]TimeRange, error) {
	// Get columns from input relation
	cols := inputRelation.Columns()
	if len(cols) == 0 {
		return nil, nil
	}

	// Find column indices for time components
	var colYearIdx, colMonthIdx, colDayIdx, colHourIdx int = -1, -1, -1, -1
	for i, col := range cols {
		colStr := string(col)
		if colStr == "?year" || colStr == "?y" {
			colYearIdx = i
		} else if colStr == "?month" || colStr == "?m" {
			colMonthIdx = i
		} else if colStr == "?day" || colStr == "?d" {
			colDayIdx = i
		} else if colStr == "?hour" || colStr == "?h" || colStr == "?hr" {
			colHourIdx = i
		}
	}

	// We need at least year, month, day to construct time ranges
	if colYearIdx < 0 || colMonthIdx < 0 || colDayIdx < 0 {
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
		if len(tuple) <= colDayIdx {
			continue
		}

		// Extract time components (handle both int64 and int)
		var year, month, day, hour int64

		switch v := tuple[colYearIdx].(type) {
		case int64:
			year = v
		case int:
			year = int64(v)
		default:
			continue
		}

		switch v := tuple[colMonthIdx].(type) {
		case int64:
			month = v
		case int:
			month = int64(v)
		default:
			continue
		}

		switch v := tuple[colDayIdx].(type) {
		case int64:
			day = v
		case int:
			day = int64(v)
		default:
			continue
		}

		if colHourIdx >= 0 && colHourIdx < len(tuple) {
			switch v := tuple[colHourIdx].(type) {
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
		if colHourIdx >= 0 {
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
