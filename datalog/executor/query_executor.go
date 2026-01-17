package executor

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
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

		case *query.OrClause:
			// OR clauses produce new relations (union of branches) that get joined with existing
			newRel, err := e.executeOrClause(ctx, c, groups)
			if err != nil {
				return nil, fmt.Errorf("clause %d (or) failed: %w", i, err)
			}
			if newRel != nil {
				groups = append(groups, newRel)
			}
			groups = Relations(ctx.CollapseRelations([]Relation(groups), func() []Relation {
				return []Relation(groups.Collapse(ctx))
			}))

		case *query.OrJoinClause:
			// OR-JOIN clauses produce new relations with explicit join variables
			newRel, err := e.executeOrJoinClause(ctx, c, groups)
			if err != nil {
				return nil, fmt.Errorf("clause %d (or-join) failed: %w", i, err)
			}
			if newRel != nil {
				groups = append(groups, newRel)
			}
			groups = Relations(ctx.CollapseRelations([]Relation(groups), func() []Relation {
				return []Relation(groups.Collapse(ctx))
			}))

		case *query.NotClause:
			// NOT clauses filter existing relations (anti-join)
			var err error
			groups, err = e.executeNotClause(ctx, c, groups)
			if err != nil {
				return nil, fmt.Errorf("clause %d (not) failed: %w", i, err)
			}

		case *query.NotJoinClause:
			// NOT-JOIN clauses filter with explicit join variables
			var err error
			groups, err = e.executeNotJoinClause(ctx, c, groups)
			if err != nil {
				return nil, fmt.Errorf("clause %d (not-join) failed: %w", i, err)
			}

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

		// Check if we have pulls to execute
		needsPulls := hasPulls(q.Find)

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
					// Apply pulls if needed
					if needsPulls {
						projected, err = e.executePulls(projected, q.Find)
						if err != nil {
							return nil, err
						}
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
				// Apply pulls if needed
				if needsPulls {
					projected, err = e.executePulls(projected, q.Find)
					if err != nil {
						return nil, err
					}
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
			// Apply pulls if needed
			if needsPulls {
				projected, err = e.executePulls(projected, q.Find)
				if err != nil {
					return nil, err
				}
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

	// Special case: if groups is empty and expression has no required symbols
	// (e.g., ground expression), evaluate once and create a single-tuple result
	if len(groups) == 0 && len(requiredSyms) == 0 {
		result, err := expr.Function.Eval(make(map[query.Symbol]interface{}))
		if err != nil {
			return nil, err
		}
		columns := []query.Symbol{expr.Binding}
		tuples := []Tuple{{result}}
		return []Relation{NewMaterializedRelationWithOptions(columns, tuples, e.options)}, nil
	}

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
		// No relation has required symbols
		if len(requiredSyms) == 0 && len(groups) > 0 {
			// Ground expression with existing groups - evaluate once and add column to each tuple
			// This is used in OR fallback: (or [subquery] [(ground 0) ?count])
			result, err := expr.Function.Eval(make(map[query.Symbol]interface{}))
			if err != nil {
				return nil, err
			}

			// Add the new column to each relation in groups
			var resultRels []Relation
			for _, rel := range groups {
				// Add the bound variable as a new column with constant value
				newCols := append(rel.Columns(), expr.Binding)
				var newTuples []Tuple
				iter := rel.Iterator()
				for iter.Next() {
					oldTuple := iter.Tuple()
					newTuple := make(Tuple, len(oldTuple)+1)
					copy(newTuple, oldTuple)
					newTuple[len(oldTuple)] = result
					newTuples = append(newTuples, newTuple)
				}
				iter.Close()
				resultRels = append(resultRels, NewMaterializedRelationWithOptions(newCols, newTuples, e.options))
			}
			return resultRels, nil
		}
		// Skip expression if no relevant relations and expression needs symbols
		return groups, nil
	}

	// Create product of relevant relations (streaming)
	// Product() handles single relation passthrough
	joined := Relations(relevantRels).Product()

	// Check if this expression might need database access (database functions)
	// If the matcher supports EntityLookup, pass it to the expression evaluator
	var lookup query.EntityLookup
	if lookupMatcher, ok := e.matcher.(EntityLookupMatcher); ok {
		lookup = entityLookupAdapter{lookupMatcher}
	}

	// Evaluate expression with optional lookup support
	result := evaluateExpressionWithLookup(joined, expr, lookup)

	// Return result + unchanged relations
	return append([]Relation{result}, otherRels...), nil
}

// entityLookupAdapter adapts EntityLookupMatcher to query.EntityLookup
type entityLookupAdapter struct {
	matcher EntityLookupMatcher
}

func (a entityLookupAdapter) LookupAttribute(entity datalog.Identity, attr datalog.Keyword) (interface{}, bool) {
	return a.matcher.LookupAttribute(entity, attr)
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

	// Check if this predicate might need database access (DatabaseFunctionPredicate)
	// If the matcher supports EntityLookup, pass it to the predicate evaluator
	var lookup query.EntityLookup
	if lookupMatcher, ok := e.matcher.(EntityLookupMatcher); ok {
		lookup = entityLookupAdapter{lookupMatcher}
	}

	// Filter using predicate with optional lookup support
	result := filterWithPredicateAndLookup(joined, pred, lookup)

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
		// No input groups - check if subquery has variable inputs that need outer bindings
		hasVariableInputs := false
		for _, input := range subq.Inputs {
			if _, ok := input.(query.Variable); ok {
				hasVariableInputs = true
				break
			}
		}
		if hasVariableInputs {
			return nil, fmt.Errorf("no input groups for subquery with variable inputs")
		}
		// No variable inputs - execute subquery once with empty input combination
		inputRelations := createInputRelationsForSubqueryWithOptions(subq, make(map[query.Symbol]interface{}), e.options)
		nestedGroups, err := e.Execute(ctx, subq.Query, inputRelations)
		if err != nil {
			return nil, fmt.Errorf("nested query execution failed: %w", err)
		}
		if len(nestedGroups) == 0 {
			// Empty result - return empty relation with binding columns
			return NewMaterializedRelationWithOptions(extractBindingSymbols(subq.Binding), nil, e.options), nil
		}
		if len(nestedGroups) > 1 {
			return nil, fmt.Errorf("subquery returned %d disjoint groups - expected 1", len(nestedGroups))
		}
		// Apply binding form (no outer input values to join with)
		boundResult, err := applyBindingForm(nestedGroups[0], subq.Binding, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("binding form application failed: %w", err)
		}
		return boundResult, nil
	} else if len(materializedGroups) == 1 {
		combinedRel = materializedGroups[0]
	} else {
		// Multiple groups - need to combine them
		combinedRel = Relations(materializedGroups).Product()
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
		case query.FindPull:
			// For pulls, include the pulled variable
			symbols = append(symbols, e.Variable)
		}
	}
	return symbols
}

// hasPulls checks if any find element is a pull expression
func hasPulls(find []query.FindElement) bool {
	for _, elem := range find {
		if _, ok := elem.(query.FindPull); ok {
			return true
		}
	}
	return false
}

// executePulls executes pull expressions on a relation
// For each tuple in the relation, it replaces entity values with pulled maps
// based on the FindPull patterns in the find clause.
func (e *DefaultQueryExecutor) executePulls(rel Relation, find []query.FindElement) (Relation, error) {
	// Build mapping: column index -> pull pattern
	columns := rel.Columns()
	pullSpecs := make(map[int]query.FindPull)

	for _, elem := range find {
		if pull, ok := elem.(query.FindPull); ok {
			// Find the column index for this pull variable
			for i, col := range columns {
				if col == pull.Variable {
					pullSpecs[i] = pull
					break
				}
			}
		}
	}

	if len(pullSpecs) == 0 {
		return rel, nil // No pulls to execute
	}

	// Create pull executor
	puller := NewPullExecutor(e.matcher)

	// Process tuples and execute pulls
	var resultTuples []Tuple
	it := rel.Iterator()
	defer it.Close()

	for it.Next() {
		tuple := it.Tuple()
		// Make a copy to modify
		newTuple := make(Tuple, len(tuple))
		copy(newTuple, tuple)

		// Execute pulls for each pull column
		for colIdx, pull := range pullSpecs {
			if colIdx >= len(tuple) {
				continue
			}

			// Get the entity value
			entity, ok := tuple[colIdx].(datalog.Identity)
			if !ok || entity == nil {
				// Value is not an entity - keep it as is
				continue
			}

			// Execute pull
			pulled, err := puller.Pull(entity, pull.Pattern)
			if err != nil {
				return nil, fmt.Errorf("pull failed for %s: %w", pull.Variable, err)
			}

			// Replace entity with pulled map (nil if entity not found)
			newTuple[colIdx] = pulled
		}

		resultTuples = append(resultTuples, newTuple)
	}

	// Return relation without deduplication - pulled maps (map[string]interface{})
	// are not comparable and would panic during deduplication
	return &MaterializedRelation{
		columns: columns,
		tuples:  resultTuples,
		options: rel.Options(),
	}, nil
}

// executeOrClause performs union of OR branches, or fallback semantics for expression branches
func (e *DefaultQueryExecutor) executeOrClause(ctx Context, clause *query.OrClause, groups Relations) (Relation, error) {
	start := time.Now()
	collector := ctx.Collector()

	if len(clause.Branches) == 0 {
		return NewMaterializedRelationWithOptions(nil, nil, e.options), nil
	}

	// Emit begin event
	if collector != nil {
		collector.Add(annotations.Event{
			Name:  annotations.OrClauseBegin,
			Start: start,
			Data: map[string]interface{}{
				"branch_count":    len(clause.Branches),
				"has_expressions": query.OrHasExpressions(clause.Branches),
			},
		})
	}

	var result Relation
	var err error
	var semantics string

	// Check if any branch has expressions - use fallback semantics if so
	if query.OrHasExpressions(clause.Branches) {
		semantics = "fallback"
		result, err = e.executeOrClauseFallback(ctx, clause, groups)
	} else {
		// Standard union semantics for pattern-only OR
		semantics = "union"
		result, err = e.executeOrClauseUnion(ctx, clause, groups)
	}

	// Emit complete event
	if collector != nil {
		end := time.Now()
		data := map[string]interface{}{
			"semantics": semantics,
			"success":   err == nil,
		}
		if err != nil {
			data["error"] = err.Error()
		} else if result != nil {
			data["result_size"] = result.Size()
		}
		collector.Add(annotations.Event{
			Name:    annotations.OrClauseComplete,
			Start:   start,
			End:     end,
			Latency: end.Sub(start),
			Data:    data,
		})
	}

	return result, err
}

// executeOrClauseFallback implements Clojure-style fallback semantics:
// For each input tuple, try branches in order until one returns a result.
// This is truly streaming - we process one tuple at a time with short-circuit evaluation.
func (e *DefaultQueryExecutor) executeOrClauseFallback(ctx Context, clause *query.OrClause, groups Relations) (Relation, error) {
	collector := ctx.Collector()

	// Emit fallback mode annotation
	if collector != nil {
		collector.Add(annotations.Event{
			Name:  annotations.OrClauseFallback,
			Start: time.Now(),
			Data: map[string]interface{}{
				"branch_count": len(clause.Branches),
				"groups_count": len(groups),
			},
		})
	}

	// Find which symbols the OR branches need from outer context
	neededSymbols := collectOrBranchRequiredSymbols(clause)

	// If no outer bindings needed, do global fallback
	if len(neededSymbols) == 0 || len(groups) == 0 {
		return e.executeOrClauseFallbackGlobal(ctx, clause)
	}

	// Find the group that provides the needed symbols and materialize it
	// We need to materialize because: (1) we iterate per-tuple for short-circuit evaluation,
	// and (2) the outer query needs this relation for the final collapse
	var outerBindingIdx int = -1
	for i, rel := range groups {
		if containsAll(rel.Columns(), neededSymbols) {
			outerBindingIdx = i
			break
		}
	}
	if outerBindingIdx < 0 {
		// No group has the needed symbols - try global fallback
		return e.executeOrClauseFallbackGlobal(ctx, clause)
	}

	// Materialize and replace in groups so outer query can still use it
	groups[outerBindingIdx] = groups[outerBindingIdx].Materialize()
	outerBindingRel := groups[outerBindingIdx]

	// Get the columns we'll output (needed symbols + any symbols branches provide)
	// For now, determine output columns from first branch execution
	var outputCols []query.Symbol
	var resultTuples []Tuple

	// Stream through outer tuples one at a time (now safe - materialized relation)
	outerIter := outerBindingRel.Iterator()
	defer outerIter.Close()

	for outerIter.Next() {
		outerTuple := outerIter.Tuple()

		// Build single-tuple relation for this input
		singleTupleRel := NewMaterializedRelationWithOptions(
			outerBindingRel.Columns(),
			[]Tuple{outerTuple},
			e.options,
		)

		// Try each branch in order until one returns a result
		var tupleResult Relation
		for i, branch := range clause.Branches {
			branchResult, err := e.executeInnerClauses(ctx, branch, singleTupleRel)
			if err != nil {
				return nil, fmt.Errorf("OR branch %d execution failed: %w", i+1, err)
			}

			if branchResult != nil && branchResult.Size() > 0 {
				tupleResult = branchResult
				break // Short-circuit: first non-empty result wins
			}
		}

		// Collect results from this tuple
		if tupleResult != nil {
			// Set output columns from first result
			if outputCols == nil {
				outputCols = tupleResult.Columns()
			}

			// Add all tuples from result
			resultIter := tupleResult.Iterator()
			for resultIter.Next() {
				resultTuples = append(resultTuples, resultIter.Tuple())
			}
			resultIter.Close()
		}
	}

	if len(resultTuples) == 0 {
		return NewMaterializedRelationWithOptions(nil, nil, e.options), nil
	}

	return NewMaterializedRelationWithOptions(outputCols, resultTuples, e.options), nil
}

// findCommonColumns returns columns that exist in all relations
func findCommonColumns(relations []Relation) []query.Symbol {
	if len(relations) == 0 {
		return nil
	}

	// Start with first relation's columns
	colSet := make(map[query.Symbol]bool)
	for _, col := range relations[0].Columns() {
		colSet[col] = true
	}

	// Intersect with each subsequent relation
	for i := 1; i < len(relations); i++ {
		relCols := make(map[query.Symbol]bool)
		for _, col := range relations[i].Columns() {
			relCols[col] = true
		}
		// Keep only columns that exist in both
		for col := range colSet {
			if !relCols[col] {
				delete(colSet, col)
			}
		}
	}

	// Preserve order from first relation
	var result []query.Symbol
	for _, col := range relations[0].Columns() {
		if colSet[col] {
			result = append(result, col)
		}
	}
	return result
}

// antiJoinOnSymbols returns tuples from left that have no matching tuple in right on the given symbols
func antiJoinOnSymbols(left, right Relation, symbols []query.Symbol) Relation {
	if left == nil || right == nil || len(symbols) == 0 {
		return left
	}

	// Build set of key values from right
	rightKeys := make(map[string]bool)
	rightIter := right.Iterator()
	rightCols := right.Columns()
	for rightIter.Next() {
		tuple := rightIter.Tuple()
		key := extractKeyFromTuple(tuple, rightCols, symbols)
		rightKeys[key] = true
	}
	rightIter.Close()

	// Filter left to only tuples not in right
	var remaining []Tuple
	leftIter := left.Iterator()
	leftCols := left.Columns()
	for leftIter.Next() {
		tuple := leftIter.Tuple()
		key := extractKeyFromTuple(tuple, leftCols, symbols)
		if !rightKeys[key] {
			remaining = append(remaining, tuple)
		}
	}
	leftIter.Close()

	return NewMaterializedRelationWithOptions(leftCols, remaining, left.Options())
}

// extractKeyFromTuple extracts a string key from tuple for the given symbols
func extractKeyFromTuple(tuple Tuple, cols []query.Symbol, symbols []query.Symbol) string {
	colIdx := make(map[query.Symbol]int)
	for i, col := range cols {
		colIdx[col] = i
	}

	var key string
	for _, sym := range symbols {
		if idx, ok := colIdx[sym]; ok && idx < len(tuple) {
			key += fmt.Sprintf("%v|", tuple[idx])
		}
	}
	return key
}

// crossJoinWithOuter produces the cross product of outer tuples with branch result tuples
// Used when a fallback branch (like ground expression) doesn't include outer context
func crossJoinWithOuter(outer, branch Relation, opts ExecutorOptions) Relation {
	if outer == nil || branch == nil {
		return branch
	}

	outerCols := outer.Columns()
	branchCols := branch.Columns()

	// Combined columns: outer columns + branch columns
	combinedCols := make([]query.Symbol, 0, len(outerCols)+len(branchCols))
	combinedCols = append(combinedCols, outerCols...)
	combinedCols = append(combinedCols, branchCols...)

	// Materialize branch result (usually small, like a single ground value)
	var branchTuples []Tuple
	branchIter := branch.Iterator()
	for branchIter.Next() {
		branchTuples = append(branchTuples, branchIter.Tuple())
	}
	branchIter.Close()

	// Cross join: for each outer tuple, combine with each branch tuple
	var resultTuples []Tuple
	outerIter := outer.Iterator()
	for outerIter.Next() {
		outerTuple := outerIter.Tuple()
		for _, branchTuple := range branchTuples {
			combined := make(Tuple, len(outerTuple)+len(branchTuple))
			copy(combined, outerTuple)
			copy(combined[len(outerTuple):], branchTuple)
			resultTuples = append(resultTuples, combined)
		}
	}
	outerIter.Close()

	return NewMaterializedRelationWithOptions(combinedCols, resultTuples, opts)
}

// executeOrClauseFallbackGlobal does global fallback when no outer bindings needed
func (e *DefaultQueryExecutor) executeOrClauseFallbackGlobal(ctx Context, clause *query.OrClause) (Relation, error) {
	for i, branch := range clause.Branches {
		branchResult, err := e.executeInnerClauses(ctx, branch, nil)
		if err != nil {
			return nil, fmt.Errorf("OR branch %d execution failed: %w", i+1, err)
		}

		if branchResult != nil && branchResult.Size() > 0 {
			return branchResult, nil
		}
	}

	return NewMaterializedRelationWithOptions(nil, nil, e.options), nil
}

// executeOrClauseUnion implements standard Datalog union semantics
func (e *DefaultQueryExecutor) executeOrClauseUnion(ctx Context, clause *query.OrClause, groups Relations) (Relation, error) {
	collector := ctx.Collector()

	// Emit union mode annotation
	if collector != nil {
		collector.Add(annotations.Event{
			Name:  annotations.OrClauseUnion,
			Start: time.Now(),
			Data: map[string]interface{}{
				"branch_count": len(clause.Branches),
			},
		})
	}

	// Execute each branch and collect results
	var branchResults []Relation
	var commonCols []query.Symbol

	for i, branch := range clause.Branches {
		branchStart := time.Now()
		// Execute this branch's clauses against storage (no prior bindings)
		branchResult, err := e.executeInnerClauses(ctx, branch, nil)
		if err != nil {
			return nil, fmt.Errorf("OR branch %d execution failed: %w", i+1, err)
		}

		// Emit branch complete annotation
		if collector != nil {
			branchEnd := time.Now()
			resultSize := 0
			if branchResult != nil {
				resultSize = branchResult.Size()
			}
			collector.Add(annotations.Event{
				Name:    annotations.OrClauseBranchComplete,
				Start:   branchStart,
				End:     branchEnd,
				Latency: branchEnd.Sub(branchStart),
				Data: map[string]interface{}{
					"branch_index": i,
					"result_size":  resultSize,
					"mode":         "union",
				},
			})
		}

		if branchResult == nil {
			continue
		}

		// Track columns for intersection
		if len(branchResults) == 0 {
			commonCols = branchResult.Columns()
		} else {
			// Intersect columns
			branchColSet := make(map[query.Symbol]bool)
			for _, col := range branchResult.Columns() {
				branchColSet[col] = true
			}
			var newCommon []query.Symbol
			for _, col := range commonCols {
				if branchColSet[col] {
					newCommon = append(newCommon, col)
				}
			}
			commonCols = newCommon
		}

		branchResults = append(branchResults, branchResult)
	}

	if len(branchResults) == 0 || len(commonCols) == 0 {
		return NewMaterializedRelationWithOptions(nil, nil, e.options), nil
	}

	// Union all branch results, projecting to common columns
	return unionRelations(branchResults, commonCols, e.options), nil
}

// executeOrJoinClause performs union with explicit join variables, or fallback for expressions
func (e *DefaultQueryExecutor) executeOrJoinClause(ctx Context, clause *query.OrJoinClause, groups Relations) (Relation, error) {
	if len(clause.Branches) == 0 {
		return NewMaterializedRelationWithOptions(nil, nil, e.options), nil
	}

	joinVars := clause.JoinVars
	if len(joinVars) == 0 {
		return nil, fmt.Errorf("OR-JOIN clause has no join variables")
	}

	// Check if any branch has expressions - use fallback semantics if so
	if query.OrHasExpressions(clause.Branches) {
		return e.executeOrJoinClauseFallback(ctx, clause, groups)
	}

	var branchResults []Relation

	for i, branch := range clause.Branches {
		branchResult, err := e.executeInnerClauses(ctx, branch, nil)
		if err != nil {
			return nil, fmt.Errorf("OR-JOIN branch %d execution failed: %w", i+1, err)
		}

		if branchResult != nil {
			branchResults = append(branchResults, branchResult)
		}
	}

	if len(branchResults) == 0 {
		return NewMaterializedRelationWithOptions(joinVars, nil, e.options), nil
	}

	// Union all branch results, projecting to join vars
	return unionRelations(branchResults, joinVars, e.options), nil
}

// executeOrJoinClauseFallback implements fallback semantics for or-join with expressions
func (e *DefaultQueryExecutor) executeOrJoinClauseFallback(ctx Context, clause *query.OrJoinClause, groups Relations) (Relation, error) {
	joinVars := clause.JoinVars

	for i, branch := range clause.Branches {
		branchResult, err := e.executeInnerClauses(ctx, branch, nil)
		if err != nil {
			return nil, fmt.Errorf("OR-JOIN branch %d execution failed: %w", i+1, err)
		}

		// Return first non-empty result, projected to join vars
		if branchResult != nil && branchResult.Size() > 0 {
			return projectToColumns(branchResult, joinVars, e.options), nil
		}
	}

	// All branches empty
	return NewMaterializedRelationWithOptions(joinVars, nil, e.options), nil
}

// collectOrBranchRequiredSymbols collects symbols that OR branches need from outer context
func collectOrBranchRequiredSymbols(clause *query.OrClause) []query.Symbol {
	seen := make(map[query.Symbol]bool)
	var result []query.Symbol

	for _, branch := range clause.Branches {
		// Track which symbols this branch provides (to distinguish from required)
		branchProvides := make(map[query.Symbol]bool)

		// First pass: collect what each clause provides
		for _, c := range branch {
			switch clause := c.(type) {
			case *query.DataPattern:
				for _, elem := range clause.Elements {
					if v, ok := elem.(query.Variable); ok {
						branchProvides[v.Name] = true
					}
				}
			case *query.Expression:
				if clause.Binding != "" {
					branchProvides[clause.Binding] = true
				}
			case *query.SubqueryPattern:
				// Subquery provides its binding variables
				switch b := clause.Binding.(type) {
				case query.ScalarBinding:
					branchProvides[b.Variable] = true
				case query.TupleBinding:
					for _, v := range b.Variables {
						branchProvides[v] = true
					}
				case query.RelationBinding:
					for _, v := range b.Variables {
						branchProvides[v] = true
					}
				case query.CollectionBinding:
					branchProvides[b.Variable] = true
				}
			}
		}

		// Second pass: collect symbols that are used but must come from outer context
		for _, c := range branch {
			switch clause := c.(type) {
			case *query.DataPattern:
				// Variables in patterns that would make the pattern more selective
				// when bound from outer context (typically entity variables)
				for _, elem := range clause.Elements {
					if v, ok := elem.(query.Variable); ok {
						// If this variable appears in the pattern AND could be bound
						// from outside (not guaranteed to be provided by this pattern alone),
						// we should consider it as potentially required
						if !seen[v.Name] {
							seen[v.Name] = true
							result = append(result, v.Name)
						}
					}
				}
			case *query.SubqueryPattern:
				// Subquery needs variable inputs from outer context
				for _, input := range clause.Inputs {
					if v, ok := input.(query.Variable); ok {
						if !seen[v.Name] {
							seen[v.Name] = true
							result = append(result, v.Name)
						}
					}
				}
			case *query.Expression:
				// Expression needs its required symbols
				for _, sym := range clause.Function.RequiredSymbols() {
					if !seen[sym] {
						seen[sym] = true
						result = append(result, sym)
					}
				}
			}
		}
	}

	return result
}

// executeNotClause performs anti-join filtering on groups
func (e *DefaultQueryExecutor) executeNotClause(ctx Context, clause *query.NotClause, groups Relations) (Relations, error) {
	if len(groups) == 0 {
		return groups, nil
	}

	// Collect all variables from inner clauses to determine join keys
	joinVars := collectInnerVars(clause.Clauses)
	if len(joinVars) == 0 {
		return nil, fmt.Errorf("NOT clause has no variables to join on")
	}

	// Apply NOT filter to each group
	var result Relations
	for _, group := range groups {
		filtered, err := e.filterWithNotClause(ctx, clause, group, joinVars)
		if err != nil {
			return nil, err
		}
		if filtered != nil {
			result = append(result, filtered)
		}
	}

	return result, nil
}

// executeNotJoinClause performs anti-join with explicit join variables
func (e *DefaultQueryExecutor) executeNotJoinClause(ctx Context, clause *query.NotJoinClause, groups Relations) (Relations, error) {
	if len(groups) == 0 {
		return groups, nil
	}

	if len(clause.JoinVars) == 0 {
		return nil, fmt.Errorf("NOT-JOIN clause has no join variables")
	}

	// Apply NOT-JOIN filter to each group
	var result Relations
	for _, group := range groups {
		filtered, err := e.filterWithNotJoinClause(ctx, clause, group)
		if err != nil {
			return nil, err
		}
		if filtered != nil {
			result = append(result, filtered)
		}
	}

	return result, nil
}

// filterWithNotClause applies anti-join filtering to a single relation
func (e *DefaultQueryExecutor) filterWithNotClause(ctx Context, clause *query.NotClause, input Relation, joinVars []query.Symbol) (Relation, error) {
	if input == nil {
		return nil, nil
	}

	// Materialize input since we need to iterate multiple times
	input = input.Materialize()

	// Filter to only variables present in input relation
	inputCols := input.Columns()
	inputColSet := make(map[query.Symbol]bool)
	for _, col := range inputCols {
		inputColSet[col] = true
	}

	var actualJoinVars []query.Symbol
	for _, v := range joinVars {
		if inputColSet[v] {
			actualJoinVars = append(actualJoinVars, v)
		}
	}

	if len(actualJoinVars) == 0 {
		return nil, fmt.Errorf("NOT clause variables not found in input relation")
	}

	// Get unique combinations of join variables from input
	uniqueCombos := getUniqueCombinations(input, actualJoinVars)

	// Track which key combinations matched the inner clauses
	matchedKeys := make(map[string]bool)

	// For each unique combination, execute inner clauses
	for _, combo := range uniqueCombos {
		// Create a single-tuple relation with the combo values for binding
		bindingRel := NewMaterializedRelationWithOptions(actualJoinVars, []Tuple{combo}, e.options)

		// Execute inner clauses with this binding
		innerResult, err := e.executeInnerClauses(ctx, clause.Clauses, bindingRel)
		if err != nil {
			return nil, fmt.Errorf("NOT inner clause execution failed: %w", err)
		}

		// If inner produced any results, this combo is "matched" and should be excluded
		if innerResult != nil && innerResult.Size() > 0 {
			key := notOrTupleKey(combo)
			matchedKeys[key] = true
		}
	}

	// Build join key column indices for input
	keyIndices := make([]int, len(actualJoinVars))
	for i, v := range actualJoinVars {
		for j, col := range inputCols {
			if col == v {
				keyIndices[i] = j
				break
			}
		}
	}

	// Filter input: keep tuples whose join key is NOT in matchedKeys
	var filtered []Tuple
	iter := input.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		// Extract key values
		keyVals := make(Tuple, len(keyIndices))
		for i, idx := range keyIndices {
			keyVals[i] = tuple[idx]
		}
		key := notOrTupleKey(keyVals)

		if !matchedKeys[key] {
			filtered = append(filtered, tuple)
		}
	}
	iter.Close()

	return NewMaterializedRelationWithOptions(inputCols, filtered, e.options), nil
}

// filterWithNotJoinClause applies anti-join with explicit join vars to a single relation
func (e *DefaultQueryExecutor) filterWithNotJoinClause(ctx Context, clause *query.NotJoinClause, input Relation) (Relation, error) {
	if input == nil {
		return nil, nil
	}

	// Materialize input
	input = input.Materialize()
	inputCols := input.Columns()

	// Verify join vars exist in input
	inputColSet := make(map[query.Symbol]bool)
	for _, col := range inputCols {
		inputColSet[col] = true
	}

	for _, v := range clause.JoinVars {
		if !inputColSet[v] {
			return nil, fmt.Errorf("NOT-JOIN variable %s not found in input relation", v)
		}
	}

	// Get unique combinations of join variables
	uniqueCombos := getUniqueCombinations(input, clause.JoinVars)

	// Track matched keys
	matchedKeys := make(map[string]bool)

	for _, combo := range uniqueCombos {
		bindingRel := NewMaterializedRelationWithOptions(clause.JoinVars, []Tuple{combo}, e.options)

		innerResult, err := e.executeInnerClauses(ctx, clause.Clauses, bindingRel)
		if err != nil {
			return nil, fmt.Errorf("NOT-JOIN inner clause execution failed: %w", err)
		}

		if innerResult != nil && innerResult.Size() > 0 {
			key := notOrTupleKey(combo)
			matchedKeys[key] = true
		}
	}

	// Build key indices
	keyIndices := make([]int, len(clause.JoinVars))
	for i, v := range clause.JoinVars {
		for j, col := range inputCols {
			if col == v {
				keyIndices[i] = j
				break
			}
		}
	}

	// Filter
	var filtered []Tuple
	iter := input.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		keyVals := make(Tuple, len(keyIndices))
		for i, idx := range keyIndices {
			keyVals[i] = tuple[idx]
		}
		key := notOrTupleKey(keyVals)

		if !matchedKeys[key] {
			filtered = append(filtered, tuple)
		}
	}
	iter.Close()

	return NewMaterializedRelationWithOptions(inputCols, filtered, e.options), nil
}

// executeInnerClauses executes a list of clauses and returns the result
// Used by NOT and OR to execute their inner clauses
func (e *DefaultQueryExecutor) executeInnerClauses(ctx Context, clauses []query.Clause, binding Relation) (Relation, error) {
	if len(clauses) == 0 {
		return binding, nil
	}

	// Start with binding relation (or empty)
	var groups Relations
	if binding != nil {
		groups = Relations{binding}
	}

	// Execute each clause
	for _, clause := range clauses {
		switch c := clause.(type) {
		case *query.DataPattern:
			newRel, err := e.executePattern(ctx, c, groups)
			if err != nil {
				return nil, err
			}
			if newRel != nil {
				groups = append(groups, newRel)
			}
			groups = groups.Collapse(ctx)

		case *query.Expression:
			var err error
			groups, err = e.executeExpression(ctx, c, groups)
			if err != nil {
				return nil, err
			}
			groups = groups.Collapse(ctx)

		case query.Predicate:
			var err error
			groups, err = e.executePredicate(ctx, c, groups)
			if err != nil {
				return nil, err
			}

		case *query.SubqueryPattern:
			newRel, err := e.executeSubquery(ctx, c, groups)
			if err != nil {
				return nil, err
			}
			if newRel != nil {
				groups = append(groups, newRel)
			}
			groups = groups.Collapse(ctx)

		default:
			return nil, fmt.Errorf("unsupported inner clause type: %T", clause)
		}

		// Early termination on empty
		if len(groups) == 0 {
			return nil, nil
		}
	}

	// Return collapsed result
	if len(groups) == 0 {
		return nil, nil
	}
	if len(groups) == 1 {
		return groups[0], nil
	}

	// Multiple disjoint groups - take product for inner clause result
	return groups.Product(), nil
}
