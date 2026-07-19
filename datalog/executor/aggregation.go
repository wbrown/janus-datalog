package executor

import (
	"fmt"
	"math"
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
			result := NewMaterializedRelationWithOptions(
				aggregateResultSymbols(groupByVars, aggregates), nil, rel.Options())
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

// aggregateResultSymbols is the output schema of an aggregation: the group-by
// symbols followed by one symbol per aggregate, named by its display form.
func aggregateResultSymbols(groupByVars []query.Symbol, aggregates []query.FindAggregate) []query.Symbol {
	symbols := make([]query.Symbol, 0, len(groupByVars)+len(aggregates))
	symbols = append(symbols, groupByVars...)
	for _, agg := range aggregates {
		// Use String() method which handles conditional vs unconditional formatting
		symbols = append(symbols, datalog.NewSymbol(agg.String()))
	}
	return symbols
}

// isStreamingEligible checks if all aggregates can be computed in streaming
// fashion. Every resolvable aggregate streams; an unresolvable function symbol
// falls to the batch path, which surfaces the resolution error loudly.
func isStreamingEligible(aggregates []query.FindAggregate) bool {
	for _, agg := range aggregates {
		if _, err := resolveAggregate(agg.Function); err != nil {
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
	resultSymbols := aggregateResultSymbols(nil, aggregates)

	// Resolve each aggregate's behavior once, before any iteration.
	ops, opsErr := resolveAggregates(aggregates)
	if opsErr != nil {
		errRel := NewMaterializedRelationWithOptions(resultSymbols, nil, rel.Options())
		errRel.err = opsErr
		return errRel
	}

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
			predicateIndices[i] = query.SymbolIndex(symbols, agg.Predicate)
		}
	}

	// Hoist each aggregate's argument position out of the per-tuple loop; -1
	// (argument symbol absent) collects nothing, as before.
	argIndices := make([]int, len(aggregates))
	for i, agg := range aggregates {
		argIndices[i] = query.SymbolIndex(symbols, agg.Arg)
	}

	for it.Next() {
		tuple := it.Tuple()
		for i := range aggregates {
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

			// Predicate passed (or no predicate); collect the argument value.
			if j := argIndices[i]; j >= 0 && j < len(tuple) {
				aggValues[i] = append(aggValues[i], tuple[j])
			}
		}
	}

	// Capture any deferred error from the input scan: a failed scan must not be
	// silently aggregated into an empty/zero result.
	aggErr = it.Error()

	// Compute aggregates
	results := make(Tuple, len(aggregates))
	hasAnyValues := false
	for i := range aggregates {
		if len(aggValues[i]) > 0 {
			hasAnyValues = true
		}
		v, aggValErr := foldAggregateValues(ops[i], aggValues[i])
		if aggValErr != nil && aggErr == nil {
			aggErr = aggValErr
		}
		results[i] = v
	}

	// Relational theory: empty input → empty output
	// If no aggregate has any values (all predicates failed or input was empty),
	// return empty result set instead of a tuple with nil values
	opts := rel.Options()
	if !hasAnyValues {
		empty := newMaterializedRelationFromSet(
			resultSymbols,
			nil,
			opts,
			RelationProperties{},
		)
		empty.err = aggErr
		return empty
	}

	result = newMaterializedRelationFromSet(
		resultSymbols,
		[]Tuple{results},
		opts,
		RelationProperties{},
	)
	return result
}

// executeGroupedAggregation performs aggregation with grouping
func executeGroupedAggregation(
	rel Relation,
	groupByVars []query.Symbol,
	aggregates []query.FindAggregate,
) (result Relation) {
	resultSymbols := aggregateResultSymbols(groupByVars, aggregates)

	// Resolve each aggregate's behavior once, before any iteration.
	ops, opsErr := resolveAggregates(aggregates)
	if opsErr != nil {
		errRel := NewMaterializedRelationWithOptions(resultSymbols, nil, rel.Options())
		errRel.err = opsErr
		return errRel
	}

	// Create symbol mapping. Group-by symbols are validated present by
	// ExecuteAggregationsWithContext, so no table entry is -1.
	symbols := rel.Symbols()
	groupIndices := query.SymbolIndexTable(symbols, groupByVars)

	// An aggIndices zero value means an absent argument symbol reads tuple
	// position 0 — long-standing behavior, preserved by this consolidation.
	// (The single-aggregation path collects nothing instead; see
	// executeSingleAggregation.)
	aggIndices := make([]int, len(aggregates))
	for i, agg := range aggregates {
		if j := query.SymbolIndex(symbols, agg.Arg); j >= 0 {
			aggIndices[i] = j
		}
	}

	// Find predicate indices for conditional aggregates
	predicateIndices := make([]int, len(aggregates))
	for i, agg := range aggregates {
		predicateIndices[i] = -1 // -1 means no predicate (unconditional)
		if agg.IsConditional() {
			predicateIndices[i] = query.SymbolIndex(symbols, agg.Predicate)
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
		for i := range aggregates {
			v, aggValErr := foldAggregateValues(ops[i], group.values[i])
			if aggValErr != nil && aggregateErr == nil {
				aggregateErr = aggValErr
			}
			resultTuple[len(groupByVars)+i] = v
		}

		resultTuples = append(resultTuples, resultTuple)
	}

	opts := rel.Options()
	result = newMaterializedRelationFromSet(
		resultSymbols,
		resultTuples,
		opts,
		RelationProperties{Keys: [][]query.Symbol{
			append([]query.Symbol(nil), groupByVars...),
		}},
	)
	// Carry any deferred error from the grouped input scan so a failed scan
	// isn't laundered into a partial/empty grouped result. First error wins:
	// an aggregate finalization error must not be clobbered by a clean scan.
	if scanErr := it.Error(); aggregateErr == nil {
		aggregateErr = scanErr
	}
	return result
}

type batchAggregateGroup struct {
	key    Tuple
	values [][]interface{}
}

// ============================================================================
// Streaming Aggregation Implementation
// ============================================================================

// aggregateAccumulator is one aggregate's running state for one group: flat
// value state, no behavior. The streaming path updates one per (group,
// aggregate) pair per tuple; batch aggregation folds collected values through
// one via foldAggregateValues.
//
// Sums preserve integer typing: an all-int64 group accumulates in int64
// (exact at any magnitude — no float64 round-trip that collapses adjacent
// values above 2^53); the first float input promotes the accumulator to
// float64.
type aggregateAccumulator struct {
	count    int64
	sumInt   int64
	sumFloat float64
	isFloat  bool
	min      interface{}
	max      interface{}
}

// aggregateOps is one aggregate's behavior, resolved once per query from the
// interned function symbol: update folds one value into an accumulator,
// result finalizes it. Aggregation is a producer boundary for NaN — Inf and
// -Inf inputs cancel in a sum — so result errors rather than emitting a
// non-value.
type aggregateOps struct {
	update func(*aggregateAccumulator, interface{})
	result func(*aggregateAccumulator) (interface{}, error)
}

// resolveAggregate maps an interned aggregate function symbol to its
// behavior by pointer equality. An unknown symbol is a loud error, never a
// silent nil aggregate.
func resolveAggregate(fn query.Symbol) (aggregateOps, error) {
	switch fn {
	case datalog.SymCount:
		return aggregateOps{update: updateCount, result: resultCount}, nil
	case datalog.SymSum:
		return aggregateOps{update: updateSum, result: resultSum}, nil
	case datalog.SymAvg:
		return aggregateOps{update: updateSum, result: resultAvg}, nil
	case datalog.SymMin:
		return aggregateOps{update: updateMin, result: resultMin}, nil
	case datalog.SymMax:
		return aggregateOps{update: updateMax, result: resultMax}, nil
	default:
		return aggregateOps{}, fmt.Errorf("unknown aggregate function: %v", fn)
	}
}

// resolveAggregates resolves behavior for a query's aggregate list, once per
// query — never inside a tuple loop.
func resolveAggregates(aggregates []query.FindAggregate) ([]aggregateOps, error) {
	ops := make([]aggregateOps, len(aggregates))
	for i, agg := range aggregates {
		op, err := resolveAggregate(agg.Function)
		if err != nil {
			return nil, err
		}
		ops[i] = op
	}
	return ops, nil
}

// updateCount counts non-nil values (SQL semantics).
func updateCount(acc *aggregateAccumulator, value interface{}) {
	if value == nil {
		return
	}
	acc.count++
}

// updateSum accumulates for sum and avg. Values are boundary-normalized, so
// int64 and float64 are the only numeric shapes in relational flow; nil and
// non-numeric values are skipped, as before.
func updateSum(acc *aggregateAccumulator, value interface{}) {
	switch n := value.(type) {
	case int64:
		if acc.isFloat {
			acc.sumFloat += float64(n)
		} else {
			acc.sumInt += n
		}
		acc.count++
	case float64:
		if !acc.isFloat {
			acc.sumFloat = float64(acc.sumInt)
			acc.isFloat = true
		}
		acc.sumFloat += n
		acc.count++
	}
}

func updateMin(acc *aggregateAccumulator, value interface{}) {
	if value == nil {
		return
	}
	if acc.min == nil || datalog.CompareValues(value, acc.min) < 0 {
		acc.min = value
	}
	acc.count++
}

func updateMax(acc *aggregateAccumulator, value interface{}) {
	if value == nil {
		return
	}
	if acc.max == nil || datalog.CompareValues(value, acc.max) > 0 {
		acc.max = value
	}
	acc.count++
}

func resultCount(acc *aggregateAccumulator) (interface{}, error) {
	return acc.count, nil
}

func resultSum(acc *aggregateAccumulator) (interface{}, error) {
	if acc.count == 0 {
		return nil, nil
	}
	if acc.isFloat {
		if math.IsNaN(acc.sumFloat) {
			return nil, fmt.Errorf("sum produced NaN (Inf and -Inf inputs cancel), which is not a datalog value")
		}
		return acc.sumFloat, nil
	}
	return acc.sumInt, nil
}

func resultAvg(acc *aggregateAccumulator) (interface{}, error) {
	if acc.count == 0 {
		return nil, nil
	}
	total := acc.sumFloat
	if !acc.isFloat {
		total = float64(acc.sumInt)
	}
	avg := total / float64(acc.count)
	if math.IsNaN(avg) {
		return nil, fmt.Errorf("avg produced NaN (Inf and -Inf inputs cancel), which is not a datalog value")
	}
	return avg, nil
}

func resultMin(acc *aggregateAccumulator) (interface{}, error) {
	if acc.count == 0 {
		return nil, nil
	}
	return acc.min, nil
}

func resultMax(acc *aggregateAccumulator) (interface{}, error) {
	if acc.count == 0 {
		return nil, nil
	}
	return acc.max, nil
}

// foldAggregateValues folds a batch of collected values through one
// accumulator — the batch form of the one aggregation algorithm.
func foldAggregateValues(op aggregateOps, values []interface{}) (interface{}, error) {
	var acc aggregateAccumulator
	for _, v := range values {
		op.update(&acc, v)
	}
	return op.result(&acc)
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
	return aggregateResultSymbols(r.groupByVars, r.aggregates)
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
	// Resolve each aggregate's behavior once, before any iteration.
	ops, opsErr := resolveAggregates(r.aggregates)
	if opsErr != nil {
		errRel := newMaterializedRelationFromSet(r.Symbols(), nil, r.options, r.Properties())
		errRel.err = opsErr
		return errRel
	}

	// Build symbol index mappings; -1 means not found.
	symbols := r.source.Symbols()

	groupIndices := query.SymbolIndexTable(symbols, r.groupByVars)

	aggIndices := make([]int, len(r.aggregates))
	for i, agg := range r.aggregates {
		aggIndices[i] = query.SymbolIndex(symbols, agg.Arg)
	}

	// Find predicate indices for conditional aggregates
	predicateIndices := make([]int, len(r.aggregates))
	for i, agg := range r.aggregates {
		predicateIndices[i] = -1 // -1 means no predicate (unconditional)
		if agg.IsConditional() {
			predicateIndices[i] = query.SymbolIndex(symbols, agg.Predicate)
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
				states: make([]aggregateAccumulator, len(r.aggregates)),
			}
			groups.Put(key, group)
			groupOrder = append(groupOrder, group)
		}

		// Update aggregates incrementally (with predicate filtering for conditional aggregates)
		for i := range r.aggregates {
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
				ops[i].update(&group.states[i], tuple[idx])
			}
		}
	}

	// Convert groups to result tuples
	resultTuples := make([]Tuple, 0, len(groupOrder))
	for _, group := range groupOrder {
		resultTuple := make(Tuple, len(r.groupByVars)+len(r.aggregates))

		// Add group-by values
		copy(resultTuple, group.key)

		// Add aggregate results (one per aggregate accumulator)
		for i := range r.aggregates {
			v, resultErr := ops[i].result(&group.states[i])
			if resultErr != nil && aggregateErr == nil {
				aggregateErr = resultErr
			}
			resultTuple[len(r.groupByVars)+i] = v
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
	// First error wins: an aggregate finalization error must not be clobbered
	// by a clean scan.
	if scanErr := it.Error(); aggregateErr == nil {
		aggregateErr = scanErr
	}
	result = newMaterializedRelationFromSet(
		r.Symbols(),
		resultTuples,
		r.options,
		r.Properties(),
	)
	return result
}

type streamingAggregateGroup struct {
	key    Tuple
	states []aggregateAccumulator
}
