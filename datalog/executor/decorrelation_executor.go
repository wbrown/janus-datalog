package executor

import (
	"fmt"
	"sort"
	"strings"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// CorrelationSignature identifies a unique subquery pattern for batching
type CorrelationSignature struct {
	// Input variable symbols (the correlation variables)
	InputVars []query.Symbol

	// Pattern fingerprint (to ensure query structure matches)
	PatternHash string

	// Whether this is a grouped aggregation (required for decorrelation)
	IsGroupedAggregate bool
}

// SubqueryGroup represents subqueries that can be batched together
type SubqueryGroup struct {
	Signature CorrelationSignature
	Indices   []int // Indices in the original clause list
}

// Hash creates a unique string representation for grouping
func (cs CorrelationSignature) Hash() string {
	varStrs := make([]string, len(cs.InputVars))
	for i, v := range cs.InputVars {
		varStrs[i] = string(v)
	}
	sort.Strings(varStrs)

	return fmt.Sprintf("%s|%s|grouped=%v",
		strings.Join(varStrs, ","),
		cs.PatternHash,
		cs.IsGroupedAggregate)
}

// shouldDecorrelate determines if a query should use the decorrelation path
func shouldDecorrelate(clauses []query.Clause) bool {
	subqueryCount := 0
	for _, clause := range clauses {
		if _, ok := clause.(*query.SubqueryPattern); ok {
			subqueryCount++
		}
	}

	// Only worth decorrelating if we have multiple subqueries
	return subqueryCount >= 2
}

// analyzeSubqueries separates subqueries from other clauses and groups them by signature
func analyzeSubqueries(clauses []query.Clause) (
	subqueries []*query.SubqueryPattern,
	subqueryIndices []int, // Original indices in clause list
	otherClauses []query.Clause,
	groups map[string]*SubqueryGroup,
) {
	subqueries = make([]*query.SubqueryPattern, 0)
	subqueryIndices = make([]int, 0)
	otherClauses = make([]query.Clause, 0)
	groups = make(map[string]*SubqueryGroup)

	for i, clause := range clauses {
		if subq, ok := clause.(*query.SubqueryPattern); ok {
			// This is a subquery - extract signature and group
			sig := extractCorrelationSignature(subq)
			hash := sig.Hash()

			// Add to subquery list
			subqueryIndices = append(subqueryIndices, i)
			subqueryIdx := len(subqueries)
			subqueries = append(subqueries, subq)

			// Add to group
			if group, exists := groups[hash]; exists {
				group.Indices = append(group.Indices, subqueryIdx)
			} else {
				groups[hash] = &SubqueryGroup{
					Signature: sig,
					Indices:   []int{subqueryIdx},
				}
			}
		} else {
			// Not a subquery - keep as-is
			otherClauses = append(otherClauses, clause)
		}
	}

	return subqueries, subqueryIndices, otherClauses, groups
}

// extractCorrelationSignature analyzes a subquery to determine its batching signature
func extractCorrelationSignature(subq *query.SubqueryPattern) CorrelationSignature {
	// Extract input variables (correlation variables)
	var inputVars []query.Symbol
	for _, input := range subq.Inputs {
		if v, ok := input.(query.Variable); ok {
			inputVars = append(inputVars, v.Name)
		}
	}

	// Sort for consistent hashing
	sortedVars := make([]query.Symbol, len(inputVars))
	copy(sortedVars, inputVars)
	sort.Slice(sortedVars, func(i, j int) bool {
		return sortedVars[i] < sortedVars[j]
	})

	// Create pattern fingerprint from the nested query
	patternHash := createPatternFingerprint(subq.Query)

	// Determine if this is a grouped aggregation
	isGroupedAgg := isGroupedAggregation(subq.Query)

	return CorrelationSignature{
		InputVars:          sortedVars,
		PatternHash:        patternHash,
		IsGroupedAggregate: isGroupedAgg,
	}
}

// createPatternFingerprint creates a hash of the query structure
func createPatternFingerprint(q *query.Query) string {
	var parts []string

	// Hash the WHERE clause structure
	for _, clause := range q.Where {
		parts = append(parts, clauseFingerprint(clause))
	}

	// Hash the FIND clause structure
	findParts := make([]string, len(q.Find))
	for i, elem := range q.Find {
		findParts[i] = fmt.Sprintf("%T", elem)
	}

	return strings.Join(parts, "|") + "||" + strings.Join(findParts, ",")
}

// clauseFingerprint creates a fingerprint for a single clause
func clauseFingerprint(clause query.Clause) string {
	switch c := clause.(type) {
	case *query.DataPattern:
		// Hash: attribute + element types
		var elemTypes []string
		for _, elem := range c.Elements {
			elemTypes = append(elemTypes, fmt.Sprintf("%T", elem))
		}
		return fmt.Sprintf("DataPattern[%s]", strings.Join(elemTypes, ","))

	case *query.Expression:
		return fmt.Sprintf("Expression[%T]", c.Function)

	case query.Predicate:
		return fmt.Sprintf("Predicate[%T]", c)

	default:
		return fmt.Sprintf("%T", clause)
	}
}

// isGroupedAggregation determines if a query is a grouped aggregation
// Grouped aggregations have BOTH aggregates AND non-aggregate variables in :find
// Pure aggregations (only aggregates) should NOT be decorrelated
func isGroupedAggregation(q *query.Query) bool {
	hasAggregates := false
	hasNonAggregateVars := false

	for _, elem := range q.Find {
		if elem.IsAggregate() {
			hasAggregates = true
		} else if _, ok := elem.(query.FindVariable); ok {
			hasNonAggregateVars = true
		}
	}

	// Only decorrelate GROUPED aggregations, not pure aggregations
	return hasAggregates && hasNonAggregateVars
}

// getBatchableGroups returns groups that should be batched (2+ subqueries, grouped aggregation)
func getBatchableGroups(groups map[string]*SubqueryGroup) []*SubqueryGroup {
	var batchable []*SubqueryGroup

	for _, group := range groups {
		// Must have multiple subqueries
		if len(group.Indices) < 2 {
			continue
		}

		// Must be grouped aggregation (not pure aggregation)
		if !group.Signature.IsGroupedAggregate {
			continue
		}

		batchable = append(batchable, group)
	}

	return batchable
}

// isBatched checks if a subquery index is part of any batch group
func isBatched(subqueryIdx int, batchGroups []*SubqueryGroup) bool {
	for _, group := range batchGroups {
		for _, idx := range group.Indices {
			if idx == subqueryIdx {
				return true
			}
		}
	}
	return false
}

// executeWithDecorrelation is the main entry point for decorrelated execution
// It processes the query in two phases:
// 1. Execute all non-subquery clauses to build up correlation variables
// 2. Execute subqueries in batched form where beneficial
func (e *DefaultQueryExecutor) executeWithDecorrelation(ctx Context, q *query.Query, inputs []Relation) ([]Relation, error) {
	// Analyze query structure
	subqueries, _, otherClauses, groupMap := analyzeSubqueries(q.Where)

	// Phase 1: Execute all non-subquery clauses
	// This builds up the relation groups that provide correlation variables
	groups := Relations(inputs)

	for i, clause := range otherClauses {
		switch c := clause.(type) {
		case *query.DataPattern:
			newRel, err := e.executePattern(ctx, c, groups)
			if err != nil {
				return nil, fmt.Errorf("clause %d (pattern) failed: %w", i, err)
			}
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

		default:
			return nil, fmt.Errorf("unsupported clause type in decorrelation: %T", clause)
		}

		// Early termination on empty
		if len(groups) == 0 {
			return []Relation{}, nil
		}
	}

	// Phase 2: Execute subqueries in batched form
	// Identify which groups can be batched
	batchableGroups := getBatchableGroups(groupMap)

	// Execute batched groups
	batchedResults := make(map[int]Relation) // subquery index → result

	for _, batchGroup := range batchableGroups {
		results, err := e.executeBatchedGroup(ctx, subqueries, batchGroup, groups)
		if err != nil {
			return nil, fmt.Errorf("batched group execution failed: %w", err)
		}

		// Store results for each subquery in the group
		for subqIdx, result := range results {
			batchedResults[subqIdx] = result
		}
	}

	// Execute non-batched subqueries normally
	for idx, subq := range subqueries {
		if _, batched := batchedResults[idx]; batched {
			// Already executed in batch
			continue
		}

		// Execute normally
		result, err := e.executeSubquery(ctx, subq, groups)
		if err != nil {
			return nil, fmt.Errorf("subquery %d failed: %w", idx, err)
		}

		if result != nil {
			groups = append(groups, result)
			groups = Relations(ctx.CollapseRelations([]Relation(groups), func() []Relation {
				return []Relation(groups.Collapse(ctx))
			}))
		}
	}

	// Add batched results to groups
	for _, result := range batchedResults {
		if result != nil {
			groups = append(groups, result)
			groups = Relations(ctx.CollapseRelations([]Relation(groups), func() []Relation {
				return []Relation(groups.Collapse(ctx))
			}))
		}
	}

	// Apply :find clause (same as simple path)
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
		// Apply aggregations
		collapsed := groups.Collapse(ctx)
		if len(collapsed) == 0 {
			return []Relation{}, nil
		}

		combined := collapsed[0]
		if len(collapsed) > 1 {
			combined = Relations(collapsed).Product()
		}

		aggregated := ExecuteAggregationsWithContext(ctx, combined, q.Find)
		return []Relation{aggregated}, nil
	}

	// No aggregates - just project
	var findVars []query.Symbol
	for _, elem := range q.Find {
		if v, ok := elem.(query.FindVariable); ok {
			findVars = append(findVars, v.Symbol)
		}
	}

	// Handle projection using the same logic as the simple path
	if len(groups) == 0 {
		return []Relation{}, nil
	}

	if len(groups) == 1 {
		// Single group - project directly
		projected, err := groups[0].Project(findVars)
		if err != nil {
			return nil, fmt.Errorf("projection failed: %w", err)
		}
		return []Relation{projected}, nil
	}

	// Multiple groups - check if ALL :find symbols are in a single group
	// Check which groups contain which :find symbols
	groupsHaveSymbols := make([][]bool, len(groups))
	for i, group := range groups {
		groupsHaveSymbols[i] = make([]bool, len(findVars))
		cols := group.Columns()
		for j, sym := range findVars {
			for _, col := range cols {
				if col == sym {
					groupsHaveSymbols[i][j] = true
					break
				}
			}
		}
	}

	// Check if any :find symbol is missing from all groups
	for j, sym := range findVars {
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

	// Check if ALL :find symbols can be found in a SINGLE group
	for i, group := range groups {
		allFound := true
		for j := range findVars {
			if !groupsHaveSymbols[i][j] {
				allFound = false
				break
			}
		}
		if allFound {
			// This group has all symbols - project it and return
			projected, err := group.Project(findVars)
			if err != nil {
				return nil, fmt.Errorf("projection failed: %w", err)
			}
			return []Relation{projected}, nil
		}
	}

	// :find symbols span multiple groups - take Product() first, then project
	// This handles the case where subquery results are in separate groups
	combined := Relations(groups).Product()
	projected, err := combined.Project(findVars)
	if err != nil {
		return nil, fmt.Errorf("projection failed after product: %w", err)
	}
	return []Relation{projected}, nil
}

// executeBatchedGroup executes a group of subqueries with the same signature as a single batch
func (e *DefaultQueryExecutor) executeBatchedGroup(
	ctx Context,
	subqueries []*query.SubqueryPattern,
	batchGroup *SubqueryGroup,
	groups []Relation,
) (map[int]Relation, error) {

	// Use the first subquery as the representative
	if len(batchGroup.Indices) == 0 {
		return nil, fmt.Errorf("empty batch group")
	}

	representative := subqueries[batchGroup.Indices[0]]

	// Extract all unique input combinations from the current relation groups
	inputSymbols := extractInputSymbols(representative)
	combinedRel := combineGroups(groups)

	// Get unique combinations of input values
	inputCombinations := getUniqueInputCombinations(combinedRel, inputSymbols)

	if len(inputCombinations) == 0 {
		// No input combinations - return empty results for all subqueries
		results := make(map[int]Relation)
		for _, subqIdx := range batchGroup.Indices {
			subq := subqueries[subqIdx]
			columns := extractBindingSymbols(subq.Binding)
			results[subqIdx] = NewMaterializedRelation(columns, []Tuple{})
		}
		return results, nil
	}

	// Create a batched input relation with ALL input combinations
	// This allows each subquery to be executed ONCE instead of N times
	batchedInputRel := createBatchedInputRelation(inputSymbols, inputCombinations)

	// Execute each subquery in the batch with the batched input relation
	// This reduces N×M executions to M executions (where N=combinations, M=subqueries)
	results := make(map[int]Relation)

	for _, subqIdx := range batchGroup.Indices {
		subq := subqueries[subqIdx]

		// Check if this subquery can use batched execution
		// It needs to accept RelationInput in its :in clause
		if canUseBatchedInput(subq.Query) {
			// Execute with batched input relation
			result, err := e.executeBatchedSubquery(ctx, subq, batchedInputRel, inputSymbols)
			if err != nil {
				return nil, fmt.Errorf("batched subquery %d execution failed: %w", subqIdx, err)
			}
			results[subqIdx] = result
		} else {
			// Fall back to sequential execution for this subquery
			result, err := e.executeSubquery(ctx, subq, groups)
			if err != nil {
				return nil, fmt.Errorf("subquery %d in batch failed: %w", subqIdx, err)
			}
			results[subqIdx] = result
		}
	}

	return results, nil
}

// extractInputSymbols extracts the input variable symbols from a subquery
func extractInputSymbols(subq *query.SubqueryPattern) []query.Symbol {
	var symbols []query.Symbol
	for _, input := range subq.Inputs {
		switch inp := input.(type) {
		case query.Variable:
			symbols = append(symbols, inp.Name)
		case query.Constant:
			// Check if it's the database marker
			if sym, ok := inp.Value.(query.Symbol); ok && sym == "$" {
				symbols = append(symbols, sym)
			}
		}
	}
	return symbols
}

// combineGroups combines multiple relation groups into one (for extracting input combinations)
func combineGroups(groups []Relation) Relation {
	if len(groups) == 0 {
		return nil
	}
	if len(groups) == 1 {
		return groups[0]
	}
	// Multiple groups - combine via product
	return Relations(groups).Product()
}

// createBatchedInputRelation creates a relation containing all input combinations
func createBatchedInputRelation(inputSymbols []query.Symbol, combinations []map[query.Symbol]interface{}) Relation {
	// Filter out database marker from columns
	var columns []query.Symbol
	for _, sym := range inputSymbols {
		if sym != "$" {
			columns = append(columns, sym)
		}
	}

	// Build tuples from all combinations
	var tuples []Tuple
	for _, values := range combinations {
		tuple := make(Tuple, len(columns))
		for i, col := range columns {
			if val, ok := values[col]; ok {
				tuple[i] = val
			}
		}
		tuples = append(tuples, tuple)
	}

	return NewMaterializedRelation(columns, tuples)
}

// canUseBatchedInput checks if a subquery can accept batched RelationInput
func canUseBatchedInput(q *query.Query) bool {
	// Check if the query has a RelationInput in its :in clause
	hasDatabase := false
	for _, input := range q.In {
		switch input.(type) {
		case query.DatabaseInput:
			hasDatabase = true
		case query.RelationInput:
			return hasDatabase // Must have database before relation
		}
	}
	return false
}

// executeBatchedSubquery executes a single subquery with batched input relation
func (e *DefaultQueryExecutor) executeBatchedSubquery(
	ctx Context,
	subq *query.SubqueryPattern,
	batchedInputRel Relation,
	inputSymbols []query.Symbol,
) (Relation, error) {
	// Create input relations for the subquery
	// We need to pass $ and the batched relation
	var inputRelations []Relation

	// The subquery should have :in $ [[?sym ?d]] format
	// We pass the batched relation as the RelationInput
	for _, input := range subq.Query.In {
		switch input.(type) {
		case query.DatabaseInput:
			// Database doesn't need a relation
			continue
		case query.RelationInput:
			// This is where we pass our batched relation
			inputRelations = append(inputRelations, batchedInputRel)
		}
	}

	// Execute the nested query recursively using QueryExecutor
	nestedGroups, err := e.Execute(ctx, subq.Query, inputRelations)
	if err != nil {
		return nil, fmt.Errorf("batched nested query execution failed: %w", err)
	}

	// For batched execution, we expect a single result group
	if len(nestedGroups) == 0 {
		// Empty result - return empty relation with appropriate columns
		columns := extractBindingSymbols(subq.Binding)
		return NewMaterializedRelation(columns, []Tuple{}), nil
	}
	if len(nestedGroups) > 1 {
		return nil, fmt.Errorf("batched subquery returned %d disjoint groups - expected 1", len(nestedGroups))
	}

	nestedResult := nestedGroups[0]

	// For batched execution, apply binding form with empty input values
	// The result should already have all rows with correlation columns
	boundResult, err := applyBindingFormBatched(nestedResult, subq.Binding, inputSymbols)
	if err != nil {
		return nil, fmt.Errorf("batched binding form application failed: %w", err)
	}

	return boundResult, nil
}

// applyBindingFormBatched applies binding form to batched subquery results
func applyBindingFormBatched(result Relation, binding query.BindingForm, inputSymbols []query.Symbol) (Relation, error) {
	// For batched execution, the result already contains correlation columns
	// We just need to ensure column ordering is correct
	switch binding.(type) {
	case query.TupleBinding:
		// For batched TupleBinding, result should have [correlation_vars..., binding_vars...]
		// This is already the correct format
		return result, nil

	case query.RelationBinding:
		// For batched RelationBinding, result should have [correlation_vars..., binding_vars...]
		// This is already the correct format
		return result, nil

	default:
		return nil, fmt.Errorf("unsupported binding form for batched execution: %T", binding)
	}
}
