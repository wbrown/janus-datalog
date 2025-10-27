package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// QueryExecutor executes a single Datalog query with input relations
// This is the universal interface for query execution at all levels:
// - Phase execution (multi-phase queries)
// - Subquery execution (nested queries)
// - Top-level execution (user queries)
//
// Key semantic: Returns []Relation (potentially multiple disjoint groups)
// Relations are collapsed progressively but may remain disjoint if they share no symbols.
type QueryExecutor interface {
	// Execute a Datalog query with input relations and return relation groups
	// ctx: Execution context with annotation support
	// q: Parsed Datalog query to execute
	// inputs: Input relation groups from previous phase (empty for first phase)
	// Returns: Relation groups (disjoint if they share no symbols)
	Execute(ctx Context, q *query.Query, inputs []Relation) ([]Relation, error)
}

// DefaultQueryExecutor implements QueryExecutor using the PatternMatcher interface
type DefaultQueryExecutor struct {
	matcher PatternMatcher
	options ExecutorOptions
}

// NewQueryExecutor creates a new DefaultQueryExecutor
func NewQueryExecutor(matcher PatternMatcher, options ExecutorOptions) *DefaultQueryExecutor {
	return &DefaultQueryExecutor{
		matcher: matcher,
		options: options,
	}
}

// Execute implements QueryExecutor interface
// Executes clauses progressively with collapse after each step
func (e *DefaultQueryExecutor) Execute(ctx Context, q *query.Query, inputs []Relation) ([]Relation, error) {
	ctx.QueryBegin(q.String())
	defer func(start int64) {
		ctx.QueryComplete(0, 0, nil) // TODO: Add proper tuple count
	}(0)

	// Check if decorrelation path should be used
	if e.options.EnableSubqueryDecorrelation && shouldDecorrelate(q.Where) {
		return e.executeWithDecorrelation(ctx, q, inputs)
	}

	// Simple path: clause-by-clause execution
	// Start with input relation groups (may be multiple disjoint groups)
	groups := Relations(inputs)

	// Execute each clause in the :where section
	// Patterns/Subqueries produce NEW relations (append + collapse)
	// Expressions/Predicates TRANSFORM relations (replace groups + collapse)
	for i, clause := range q.Where {
		switch c := clause.(type) {
		case *query.DataPattern:
			newRel, err := e.executePattern(ctx, c, groups)
			if err != nil {
				return nil, fmt.Errorf("clause %d (pattern) failed: %w", i, err)
			}
			// Always append the relation - don't check IsEmpty() as that consumes iterators
			// Collapse will handle empty relations correctly
			if newRel != nil {
				groups = append(groups, newRel)
			}
			groups = Relations(ctx.CollapseRelations([]Relation(groups), func() []Relation {
				return []Relation(groups.Collapse(ctx))
			}))

		case *query.Expression:
			var err error
			groups, err = e.executeExpression(ctx, c, groups)
			if err != nil {
				return nil, fmt.Errorf("clause %d (expression) failed: %w", i, err)
			}
			groups = Relations(ctx.CollapseRelations([]Relation(groups), func() []Relation {
				return []Relation(groups.Collapse(ctx))
			}))

		case query.Predicate:
			var err error
			groups, err = e.executePredicate(ctx, c, groups)
			if err != nil {
				return nil, fmt.Errorf("clause %d (predicate) failed: %w", i, err)
			}

		case *query.SubqueryPattern:
			newRel, err := e.executeSubquery(ctx, c, groups)
			if err != nil {
				return nil, fmt.Errorf("clause %d (subquery) failed: %w", i, err)
			}
			// Always append the relation - don't check IsEmpty() as that consumes iterators
			// Collapse will handle empty relations correctly
			if newRel != nil {
				groups = append(groups, newRel)
			}
			groups = Relations(ctx.CollapseRelations([]Relation(groups), func() []Relation {
				return []Relation(groups.Collapse(ctx))
			}))

		default:
			return nil, fmt.Errorf("unsupported clause type: %T", clause)
		}

		// Early termination on empty
		if len(groups) == 0 {
			return []Relation{}, nil
		}
	}

	// Apply :find clause - check for aggregates
	if len(q.Find) == 0 {
		return groups, nil
	}

	hasAggregates := false
	for _, elem := range q.Find {
		if elem.IsAggregate() {
			hasAggregates = true
			break
		}
	}

	if hasAggregates {
		// Must have single relation for aggregation
		if len(groups) > 1 {
			return nil, fmt.Errorf("cannot aggregate over %d disjoint relations", len(groups))
		}
		if len(groups) == 0 {
			return []Relation{}, nil
		}

		// Apply aggregations using existing function
		result := ExecuteAggregationsWithContext(ctx, groups[0], q.Find)
		return []Relation{result}, nil

	} else {
		// Simple projection to :find symbols
		findSymbols := extractFindSymbols(q.Find)

		// Check if all :find symbols are available across the groups
		// If symbols span multiple groups, we need to Product() them first
		if len(groups) > 1 {
			// Check which groups contain which :find symbols
			groupsHaveSymbols := make([][]bool, len(groups))
			for i, group := range groups {
				groupsHaveSymbols[i] = make([]bool, len(findSymbols))
				cols := group.Columns()
				for j, sym := range findSymbols {
					for _, col := range cols {
						if col == sym {
							groupsHaveSymbols[i][j] = true
							break
						}
					}
				}
			}

			// Check if any :find symbol is missing from all groups
			for j, sym := range findSymbols {
				found := false
				for i := range groups {
					if groupsHaveSymbols[i][j] {
						found = true
						break
					}
				}
				if !found {
					return nil, fmt.Errorf("projection failed: symbol %v not found in any relation group", sym)
				}
			}

			// Check if any :find symbol spans multiple groups
			// If so, we need to take the Product() of those groups
			needsProduct := false
			for j := range findSymbols {
				count := 0
				for i := range groups {
					if groupsHaveSymbols[i][j] {
						count++
					}
				}
				if count > 1 {
					// Symbol appears in multiple groups - this shouldn't happen after collapse
					return nil, fmt.Errorf("projection failed: symbol %v appears in multiple groups", findSymbols[j])
				}
			}

			// Check if ALL :find symbols can be found in a SINGLE group
			// If not, we need to Product() the groups together
			for i, group := range groups {
				allFound := true
				for j := range findSymbols {
					if !groupsHaveSymbols[i][j] {
						allFound = false
						break
					}
				}
				if allFound {
					// This group has all symbols - project it and return
					projected, err := group.Project(findSymbols)
					if err != nil {
						return nil, fmt.Errorf("projection failed: %w", err)
					}
					return []Relation{projected}, nil
				}
			}

			// :find symbols span multiple groups - need Cartesian product
			// This is the case for our test: [?e, ?name] and [?max-age] are disjoint
			needsProduct = true

			if needsProduct {
				// Take Product() of all groups to create a single relation
				combined := Relations(groups).Product()
				projected, err := combined.Project(findSymbols)
				if err != nil {
					return nil, fmt.Errorf("projection failed after product: %w", err)
				}
				return []Relation{projected}, nil
			}
		}

		// Single group or each group projects independently
		for i, group := range groups {
			projected, err := group.Project(findSymbols)
			if err != nil {
				return nil, fmt.Errorf("projection of group %d failed: %w", i, err)
			}
			groups[i] = projected
		}
		return groups, nil
	}
}

// executePattern executes a data pattern using the PatternMatcher
// Patterns produce new relations from storage that get joined with existing groups
func (e *DefaultQueryExecutor) executePattern(ctx Context, pattern *query.DataPattern, groups []Relation) (Relation, error) {
	// Materialize groups that share symbols with the pattern
	// These groups will be: (1) used for binding-based filtering, (2) joined with the result
	// Materializing allows them to be iterated multiple times without consuming the iterator
	bindings := materializeRelationsForPattern(pattern, Relations(groups))

	// Use PatternMatcher with current groups as bindings
	// NOTE: bindings are used for pattern selection heuristics (FindBestForPattern)
	// and potentially for batch scanning - they will also be joined with the result later
	rel, err := e.matcher.Match(pattern, bindings)
	if err != nil {
		return nil, err
	}
	return rel, nil
}

// executeExpression evaluates an expression clause
// Expressions TRANSFORM groups - may use Product() for multi-relation expressions
func (e *DefaultQueryExecutor) executeExpression(ctx Context, expr *query.Expression, groups []Relation) ([]Relation, error) {
	// Find relations with any required symbols
	var relevantRels []Relation
	var otherRels []Relation

	requiredSyms := expr.Function.RequiredSymbols()
	for _, rel := range groups {
		hasAny := false
		relCols := rel.Columns()
		for _, sym := range requiredSyms {
			for _, col := range relCols {
				if col == sym {
					hasAny = true
					break
				}
			}
			if hasAny {
				break
			}
		}
		if hasAny {
			relevantRels = append(relevantRels, rel)
		} else {
			otherRels = append(otherRels, rel)
		}
	}

	if len(relevantRels) == 0 {
		// No relation has required symbols - skip expression
		return groups, nil
	}

	// Create product of relevant relations (streaming)
	// Product() handles single relation passthrough
	joined := Relations(relevantRels).Product()

	// Evaluate expression
	result := evaluateExpressionNew(joined, expr)

	// Return result + unchanged relations
	return append([]Relation{result}, otherRels...), nil
}

// executePredicate filters relations using a predicate
// Predicates TRANSFORM groups - may use Product() for multi-relation predicates
func (e *DefaultQueryExecutor) executePredicate(ctx Context, pred query.Predicate, groups []Relation) ([]Relation, error) {
	// Find relations with ANY required symbols (same logic as executeExpression)
	var relevantRels []Relation
	var otherRels []Relation

	requiredSyms := pred.RequiredSymbols()
	for _, rel := range groups {
		hasAny := false
		relCols := rel.Columns()
		for _, sym := range requiredSyms {
			for _, col := range relCols {
				if col == sym {
					hasAny = true
					break
				}
			}
			if hasAny {
				break
			}
		}
		if hasAny {
			relevantRels = append(relevantRels, rel)
		} else {
			otherRels = append(otherRels, rel)
		}
	}

	if len(relevantRels) == 0 {
		// No relation has required symbols - skip predicate
		return groups, nil
	}

	// Create product of relevant relations (streaming)
	// Product() handles single relation passthrough
	joined := Relations(relevantRels).Product()

	// Filter using predicate
	result := filterWithPredicate(joined, pred)

	// Return result + unchanged relations
	return append([]Relation{result}, otherRels...), nil
}

// executeSubquery executes a nested subquery
// Subqueries produce new relations from nested query execution
func (e *DefaultQueryExecutor) executeSubquery(ctx Context, subq *query.SubqueryPattern, groups []Relation) (Relation, error) {
	// Check if componentized path is enabled
	if e.options.UseComponentizedSubquery {
		return e.executeSubqueryComponentized(ctx, subq, groups)
	}

	// Log that we're using the legacy path
	if collector := ctx.Collector(); collector != nil {
		collector.Add(annotations.Event{
			Name: "subquery/executor-path",
			Data: map[string]interface{}{
				"path":          "Legacy QueryExecutor",
				"query":         subq.Query.String(),
				"input_count":   len(subq.Inputs),
				"groups_count":  len(groups),
			},
		})
	}

	// CRITICAL: Materialize groups FIRST to prevent iterator consumption
	// When we create Product() and materialize it, that will consume the underlying iterators
	// We need to preserve groups for later use in the outer query
	materializedGroups := make([]Relation, len(groups))
	for i, g := range groups {
		materializedGroups[i] = g.Materialize()
	}

	// Combine all groups into a single relation for extracting input combinations
	var combinedRel Relation
	if len(materializedGroups) == 0 {
		return nil, fmt.Errorf("no input groups for subquery")
	} else if len(materializedGroups) == 1 {
		combinedRel = materializedGroups[0]
	} else {
		// Multiple groups - need to combine them
		combinedRel = Relations(materializedGroups).Product()
	}

	// Extract which input symbols we need from the outer query
	var inputSymbols []query.Symbol
	for _, input := range subq.Inputs {
		switch inp := input.(type) {
		case query.Variable:
			inputSymbols = append(inputSymbols, inp.Name)
		case query.Constant:
			// Check if it's the database marker
			if sym, ok := inp.Value.(query.Symbol); ok && sym == "$" {
				inputSymbols = append(inputSymbols, sym)
			}
			// Other constants don't need extraction
		}
	}

	// Materialize combined relation since getUniqueInputCombinations will consume it
	combinedRel = combinedRel.Materialize()

	// Get unique combinations of input values
	inputCombinations := getUniqueInputCombinations(combinedRel, inputSymbols)

	// Execute subquery for each combination
	var allResults []Relation

	for _, inputValues := range inputCombinations {
		// Create input relations for this combination
		inputRelations := createInputRelationsForSubqueryWithOptions(subq, inputValues, e.options)

		// DEBUG: Log input relations
		if collector := ctx.Collector(); collector != nil {
			for i, rel := range inputRelations {
				collector.Add(annotations.Event{
					Name: "subquery/input-relation",
					Data: map[string]interface{}{
						"index":   i,
						"columns": rel.Columns(),
						"size":    rel.Size(),
					},
				})
			}
		}

		// Execute the nested query recursively using QueryExecutor
		nestedGroups, err := e.Execute(ctx, subq.Query, inputRelations)
		if err != nil {
			return nil, fmt.Errorf("nested query execution failed: %w", err)
		}

		// For subqueries, we expect a single result group (aggregations should have collapsed)
		if len(nestedGroups) == 0 {
			// Empty result - skip this combination
			continue
		}
		if len(nestedGroups) > 1 {
			return nil, fmt.Errorf("subquery returned %d disjoint groups - expected 1", len(nestedGroups))
		}

		nestedResult := nestedGroups[0]

		// Apply binding form to join results with outer query values
		boundResult, err := applyBindingForm(nestedResult, subq.Binding, inputValues, inputSymbols)
		if err != nil {
			return nil, fmt.Errorf("binding form application failed: %w", err)
		}

		allResults = append(allResults, boundResult)
	}

	// Combine all results
	if len(allResults) == 0 {
		// No results - return empty relation with appropriate columns
		return NewMaterializedRelation(extractBindingSymbols(subq.Binding), []Tuple{}), nil
	}

	// Union all results (they should have the same schema)
	return combineSubqueryResultsSimple(allResults), nil
}

// createInputRelationsForSubquery creates input relations from subquery inputs and outer values
func createInputRelationsForSubquery(subq *query.SubqueryPattern, outerValues map[query.Symbol]interface{}) []Relation {
	return createInputRelationsFromPattern(subq, outerValues)
}

func createInputRelationsForSubqueryWithOptions(subq *query.SubqueryPattern, outerValues map[query.Symbol]interface{}, opts ExecutorOptions) []Relation {
	return createInputRelationsFromPatternWithOptions(subq, outerValues, opts)
}

// combineSubqueryResultsSimple combines multiple subquery results into a single relation
func combineSubqueryResultsSimple(results []Relation) Relation {
	if len(results) == 0 {
		return nil
	}
	if len(results) == 1 {
		return results[0]
	}

	// Collect all tuples
	columns := results[0].Columns()
	var allTuples []Tuple

	for _, result := range results {
		it := result.Iterator()
		defer it.Close()
		for it.Next() {
			allTuples = append(allTuples, it.Tuple())
		}
	}

	return NewMaterializedRelation(columns, allTuples)
}

// extractBindingSymbols extracts symbols from a binding form
func extractBindingSymbols(binding query.BindingForm) []query.Symbol {
	switch b := binding.(type) {
	case query.TupleBinding:
		return b.Variables
	case query.RelationBinding:
		return b.Variables
	case query.CollectionBinding:
		return []query.Symbol{b.Variable}
	default:
		return nil
	}
}

// executeSubqueryComponentized executes subquery using component-based optimization
// This uses: SubqueryStrategySelector, SubqueryBatcher, WorkerPool, StreamingUnionBuilder
func (e *DefaultQueryExecutor) executeSubqueryComponentized(ctx Context, subq *query.SubqueryPattern, groups []Relation) (Relation, error) {
	// Initialize components (could be cached on executor for reuse)
	selector := NewSubqueryStrategySelector(100) // Default threshold
	batcher := NewSubqueryBatcher()
	workerPool := NewWorkerPool(e.options.MaxSubqueryWorkers)
	unionBuilder := NewStreamingUnionBuilder(e.options)

	// Combine all groups into a single relation
	var combinedRel Relation
	if len(groups) == 0 {
		return nil, fmt.Errorf("no input groups for subquery")
	} else if len(groups) == 1 {
		combinedRel = groups[0]
	} else {
		combinedRel = Relations(groups).Product()
	}

	// Extract input symbols using batcher
	inputSymbols := batcher.ExtractInputSymbols(subq.Query.In)

	// Get unique input combinations
	inputCombinations := getUniqueInputCombinations(combinedRel, inputSymbols)

	// Select execution strategy
	strategy := selector.SelectStrategy(subq.Query, len(inputCombinations), e.options)

	// Log strategy selection
	if collector := ctx.Collector(); collector != nil {
		collector.Add(annotations.Event{
			Name: "subquery/componentized-strategy",
			Data: map[string]interface{}{
				"strategy":           strategy.String(),
				"input_combinations": len(inputCombinations),
				"query":              subq.Query.String(),
			},
		})
	}

	// Execute based on strategy
	switch strategy {
	case StrategyBatched:
		return e.executeSubqueryBatched(ctx, subq, inputCombinations, inputSymbols, batcher)

	case StrategyParallel:
		return e.executeSubqueryParallel(ctx, subq, inputCombinations, inputSymbols, workerPool, unionBuilder)

	case StrategySequential:
		return e.executeSubquerySequential(ctx, subq, inputCombinations, inputSymbols, unionBuilder)

	default:
		return nil, fmt.Errorf("unknown strategy: %v", strategy)
	}
}

// executeSubqueryBatched executes subquery once with all inputs as RelationInput
func (e *DefaultQueryExecutor) executeSubqueryBatched(
	ctx Context,
	subq *query.SubqueryPattern,
	combinations []map[query.Symbol]interface{},
	inputSymbols []query.Symbol,
	batcher *SubqueryBatcher,
) (Relation, error) {
	if len(combinations) == 0 {
		return NewMaterializedRelation(extractBindingSymbols(subq.Binding), []Tuple{}), nil
	}

	// Build batched input relation
	batchedInput := batcher.BuildBatchedInput(combinations, inputSymbols)

	// Create input relations for the subquery
	inputRelations := []Relation{batchedInput}

	// Execute once with batched input
	nestedGroups, err := e.Execute(ctx, subq.Query, inputRelations)
	if err != nil {
		return nil, fmt.Errorf("batched subquery execution failed: %w", err)
	}

	if len(nestedGroups) == 0 {
		return NewMaterializedRelation(extractBindingSymbols(subq.Binding), []Tuple{}), nil
	}
	if len(nestedGroups) > 1 {
		return nil, fmt.Errorf("batched subquery returned %d groups, expected 1", len(nestedGroups))
	}

	// Apply binding form (no per-tuple input values for batched)
	return applyBindingForm(nestedGroups[0], subq.Binding, nil, inputSymbols)
}

// executeSubqueryParallel executes subquery iterations in parallel using WorkerPool
func (e *DefaultQueryExecutor) executeSubqueryParallel(
	ctx Context,
	subq *query.SubqueryPattern,
	combinations []map[query.Symbol]interface{},
	inputSymbols []query.Symbol,
	workerPool *WorkerPool,
	unionBuilder *StreamingUnionBuilder,
) (Relation, error) {
	if len(combinations) == 0 {
		return NewMaterializedRelation(extractBindingSymbols(subq.Binding), []Tuple{}), nil
	}

	// Convert combinations to []interface{} for worker pool
	inputs := make([]interface{}, len(combinations))
	for i, combo := range combinations {
		inputs[i] = combo
	}

	// Execute in parallel using worker pool
	results, err := workerPool.ExecuteParallel(
		ctx,
		inputs,
		func(ctx Context, input interface{}) (interface{}, error) {
			inputValues := input.(map[query.Symbol]interface{})

			// Create input relations and execute
			inputRelations := createInputRelationsForSubqueryWithOptions(subq, inputValues, e.options)
			nestedGroups, err := e.Execute(ctx, subq.Query, inputRelations)
			if err != nil {
				return nil, err
			}

			if len(nestedGroups) == 0 {
				// Empty result - return empty relation
				return NewMaterializedRelation(extractBindingSymbols(subq.Binding), []Tuple{}), nil
			}
			if len(nestedGroups) > 1 {
				return nil, fmt.Errorf("subquery returned %d groups, expected 1", len(nestedGroups))
			}

			// Apply binding form
			return applyBindingForm(nestedGroups[0], subq.Binding, inputValues, inputSymbols)
		},
	)

	if err != nil {
		return nil, fmt.Errorf("parallel subquery execution failed: %w", err)
	}

	// Convert results back to []Relation
	relations := make([]Relation, len(results))
	for i, r := range results {
		relations[i] = r.(Relation)
	}

	// Union results using StreamingUnionBuilder
	return unionBuilder.Union(relations), nil
}

// executeSubquerySequential executes subquery iterations sequentially
func (e *DefaultQueryExecutor) executeSubquerySequential(
	ctx Context,
	subq *query.SubqueryPattern,
	combinations []map[query.Symbol]interface{},
	inputSymbols []query.Symbol,
	unionBuilder *StreamingUnionBuilder,
) (Relation, error) {
	if len(combinations) == 0 {
		return NewMaterializedRelation(extractBindingSymbols(subq.Binding), []Tuple{}), nil
	}

	var allResults []Relation

	for _, inputValues := range combinations {
		// Create input relations and execute
		inputRelations := createInputRelationsForSubqueryWithOptions(subq, inputValues, e.options)
		nestedGroups, err := e.Execute(ctx, subq.Query, inputRelations)
		if err != nil {
			return nil, fmt.Errorf("sequential subquery execution failed: %w", err)
		}

		if len(nestedGroups) == 0 {
			// Empty result - skip this combination
			continue
		}
		if len(nestedGroups) > 1 {
			return nil, fmt.Errorf("subquery returned %d groups, expected 1", len(nestedGroups))
		}

		// Apply binding form
		boundResult, err := applyBindingForm(nestedGroups[0], subq.Binding, inputValues, inputSymbols)
		if err != nil {
			return nil, fmt.Errorf("binding form application failed: %w", err)
		}

		allResults = append(allResults, boundResult)
	}

	if len(allResults) == 0 {
		return NewMaterializedRelation(extractBindingSymbols(subq.Binding), []Tuple{}), nil
	}

	// Union results using StreamingUnionBuilder
	return unionBuilder.Union(allResults), nil
}

// extractFindSymbols extracts symbols from FindElements for projection
func extractFindSymbols(find []query.FindElement) []query.Symbol {
	var symbols []query.Symbol
	for _, elem := range find {
		switch e := elem.(type) {
		case query.FindVariable:
			symbols = append(symbols, e.Symbol)
		case query.FindAggregate:
			// For aggregates, include the argument variable
			symbols = append(symbols, e.Arg)
		}
	}
	return symbols
}
