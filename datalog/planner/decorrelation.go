package planner

import (
	"fmt"
	"sort"
	"strings"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// DecorrelationGroup represents a group of subqueries that can be optimized together
type DecorrelationGroup struct {
	Signature  CorrelationSignature
	Subqueries []int // Indices in Phase.Subqueries
}

// extractPatternFingerprint creates a simplified representation for matching
func extractPatternFingerprint(pattern query.Pattern) PatternFingerprint {
	dp, ok := pattern.(*query.DataPattern)
	if !ok {
		return PatternFingerprint{}
	}

	var attributes []string
	var bound []query.Symbol

	// Extract attribute (Element[1])
	if len(dp.Elements) > 1 {
		if c, ok := dp.Elements[1].(query.Constant); ok {
			attributes = append(attributes, fmt.Sprintf("%v", c.Value))
		}
	}

	// Extract bound variables
	for _, elem := range dp.Elements {
		if v, ok := elem.(query.Variable); ok {
			bound = append(bound, query.Symbol(v.Name))
		}
	}

	return PatternFingerprint{
		Attributes: attributes,
		Bound:      bound,
	}
}

// Hash creates a unique string representation for comparison
func (pf PatternFingerprint) Hash() string {
	return fmt.Sprintf("%v|%v", pf.Attributes, pf.Bound)
}

// extractCorrelationSignature analyzes a subquery to create its signature
func extractCorrelationSignature(subqPlan *SubqueryPlan) CorrelationSignature {
	var basePatterns []PatternFingerprint

	// Analyze patterns in nested query
	for _, phase := range subqPlan.NestedPlan.Phases {
		for _, patPlan := range phase.Patterns {
			fp := extractPatternFingerprint(patPlan.Pattern)
			basePatterns = append(basePatterns, fp)
		}
	}

	// Check if query is a GROUPED aggregate (has both aggregates AND non-aggregate variables)
	// Pure aggregations (only aggregates, no grouping vars) should NOT be decorrelated
	// because adding grouping keys changes them from single aggregation to grouped aggregation
	hasAggregates := false
	hasNonAggregateVars := false
	for _, findElem := range subqPlan.NestedPlan.Query.Find {
		if findElem.IsAggregate() {
			hasAggregates = true
		} else if _, ok := findElem.(query.FindVariable); ok {
			hasNonAggregateVars = true
		}
	}

	// Only decorrelate GROUPED aggregations, not pure aggregations
	isGroupedAggregate := hasAggregates && hasNonAggregateVars

	return CorrelationSignature{
		BasePatterns:    basePatterns,
		CorrelationVars: subqPlan.Inputs,
		IsAggregate:     isGroupedAggregate,
	}
}

// Hash creates a unique string representation for the signature
// Note: We only hash correlation vars and aggregate status, NOT base patterns.
// This allows subqueries that access different attributes but use the same
// grouping keys to be decorrelated together (which is the whole point!).
func (cs CorrelationSignature) Hash() string {
	var varNames []string
	for _, v := range cs.CorrelationVars {
		varNames = append(varNames, string(v))
	}
	sort.Strings(varNames)

	return fmt.Sprintf("%v|%v",
		strings.Join(varNames, ","),
		cs.IsAggregate)
}

// Hash creates a unique string for filter group comparison
func (fg FilterGroup) Hash() string {
	var predStrs []string
	for _, pred := range fg.FilterPredicates {
		predStrs = append(predStrs, pred.String())
	}
	sort.Strings(predStrs)

	// Include accessed attributes in hash
	var attrStrs []string
	attrStrs = append(attrStrs, fg.AccessedAttributes...)
	sort.Strings(attrStrs)

	return strings.Join(predStrs, "|") + "||" + strings.Join(attrStrs, "|")
}

// StructureHash creates a hash that ignores specific attributes (for CSE)
// This allows grouping subqueries with the same structure but different accessed attributes
func (fg FilterGroup) StructureHash() string {
	var predStrs []string
	for _, pred := range fg.FilterPredicates {
		predStrs = append(predStrs, pred.String())
	}
	sort.Strings(predStrs)

	// DON'T include accessed attributes - only structure matters for CSE
	return strings.Join(predStrs, "|")
}

// mergeFilterGroupsWithCSE merges filter groups with identical structure but different accessed attributes
// This implements Common Subexpression Elimination for decorrelated subqueries
func mergeFilterGroupsWithCSE(filterGroups []FilterGroup) []FilterGroup {
	// Group by structure hash
	structureGroups := make(map[string][]int)
	for i, fg := range filterGroups {
		hash := fg.StructureHash()
		structureGroups[hash] = append(structureGroups[hash], i)
	}

	// Merge groups with same structure
	var mergedGroups []FilterGroup
	processed := make(map[int]bool)

	for _, indices := range structureGroups {
		if len(indices) == 1 {
			// No merging needed
			mergedGroups = append(mergedGroups, filterGroups[indices[0]])
			processed[indices[0]] = true
		} else {
			// Merge multiple filter groups with same structure
			merged := filterGroups[indices[0]]

			// Combine accessed attributes from all groups
			allAttrs := make(map[string]bool)
			for _, attr := range merged.AccessedAttributes {
				allAttrs[attr] = true
			}

			// Merge subqueries and aggregate functions from other groups
			for _, idx := range indices[1:] {
				fg := filterGroups[idx]

				// Add accessed attributes
				for _, attr := range fg.AccessedAttributes {
					if !allAttrs[attr] {
						allAttrs[attr] = true
						merged.AccessedAttributes = append(merged.AccessedAttributes, attr)
					}
				}

				// Add subqueries
				merged.Subqueries = append(merged.Subqueries, fg.Subqueries...)

				// Merge aggregate functions
				for subqIdx, aggFuncs := range fg.AggFunctions {
					merged.AggFunctions[subqIdx] = aggFuncs
				}

				processed[idx] = true
			}

			processed[indices[0]] = true
			mergedGroups = append(mergedGroups, merged)
		}
	}

	return mergedGroups
}

// isCorrelationPredicate checks if predicate is a correlation constraint
// Example: [(= ?py ?y)] where ?y is an input variable
func isCorrelationPredicate(predPlan PredicatePlan, correlationVars []query.Symbol) bool {
	// Check if this is an equality predicate between two variables
	if predPlan.Type != PredicateEquality {
		return false
	}

	// Check if both required vars are present and one is a correlation var
	if len(predPlan.RequiredVars) != 2 {
		return false
	}

	// Check if one of the variables is a correlation variable
	hasCorrelationVar := false
	for _, reqVar := range predPlan.RequiredVars {
		for _, corrVar := range correlationVars {
			if reqVar == corrVar {
				hasCorrelationVar = true
				break
			}
		}
	}

	return hasCorrelationVar
}

// groupSubqueriesByFilters groups subqueries with same correlation signature by filters
func groupSubqueriesByFilters(subqueries []*SubqueryPlan, indices []int,
	correlationVars []query.Symbol) []FilterGroup {

	// Map filter hash -> FilterGroup
	groups := make(map[string]*FilterGroup)

	for _, idx := range indices {
		subq := subqueries[idx]

		// Extract filter predicates from subquery (exclude correlation predicates)
		var filterPreds []query.Predicate
		for _, phase := range subq.NestedPlan.Phases {
			for _, predPlan := range phase.Predicates {
				// Skip correlation predicates (e.g., [(= ?py ?y)])
				if !isCorrelationPredicate(predPlan, correlationVars) {
					filterPreds = append(filterPreds, predPlan.Predicate)
				}
			}
		}

		// Extract accessed attributes (patterns) - these must match for queries to merge
		var accessedAttributes []string
		for _, phase := range subq.NestedPlan.Phases {
			for _, patPlan := range phase.Patterns {
				fp := extractPatternFingerprint(patPlan.Pattern)
				accessedAttributes = append(accessedAttributes, fp.Hash())
			}
		}

		// Create filter group
		fg := FilterGroup{
			FilterPredicates:   filterPreds,
			AccessedAttributes: accessedAttributes,
			Subqueries:         []int{idx},
			AggFunctions:       make(map[int][]string),
		}

		// Extract aggregate functions
		var aggFuncs []string
		for _, findElem := range subq.NestedPlan.Query.Find {
			if findElem.IsAggregate() {
				aggFuncs = append(aggFuncs, findElem.String())
			}
		}
		fg.AggFunctions[idx] = aggFuncs

		hash := fg.Hash()
		if existing, found := groups[hash]; found {
			existing.Subqueries = append(existing.Subqueries, idx)
			existing.AggFunctions[idx] = aggFuncs
		} else {
			groups[hash] = &fg
		}
	}

	// Convert map to slice
	var result []FilterGroup
	for _, fg := range groups {
		result = append(result, *fg)
	}

	return result
}

// detectDecorrelationOpportunities finds groups of subqueries that can be optimized
func detectDecorrelationOpportunities(phase *Phase) []DecorrelationGroup {
	// Map signature hash -> subquery indices
	signatureGroups := make(map[string][]int)

	for i := range phase.Subqueries {
		subqPlan := &phase.Subqueries[i]

		sig := extractCorrelationSignature(subqPlan)

		// Only aggregate queries can be decorrelated
		if !sig.IsAggregate {
			continue
		}

		// Need correlation variables to decorrelate
		if len(sig.CorrelationVars) == 0 {
			continue
		}

		hash := sig.Hash()
		signatureGroups[hash] = append(signatureGroups[hash], i)
	}

	// Find groups with 2+ subqueries (worth decorrelating)
	var opportunities []DecorrelationGroup
	for hash, indices := range signatureGroups {
		if len(indices) >= 2 {
			// Get signature from first subquery
			sig := extractCorrelationSignature(&phase.Subqueries[indices[0]])

			opportunities = append(opportunities, DecorrelationGroup{
				Signature:  sig,
				Subqueries: indices,
			})
		} else if len(indices) == 1 {
			// Single subquery - not worth decorrelating
			// This is useful for debugging why subqueries don't group
			sig := extractCorrelationSignature(&phase.Subqueries[indices[0]])
			_ = hash // Annotation could capture hash:indices mapping
			_ = sig  // Annotation could capture signature details
		}
	}

	return opportunities
}

// createDecorrelatedPlan creates an optimized plan for a group of subqueries
func (p *Planner) createDecorrelatedPlan(phase *Phase, group DecorrelationGroup) (*DecorrelatedSubqueryPlan, error) {
	subqueries := make([]*SubqueryPlan, len(group.Subqueries))
	for i, idx := range group.Subqueries {
		subqueries[i] = &phase.Subqueries[idx]
	}

	// Create indices for the new subqueries array (0, 1, 2, ...)
	localIndices := make([]int, len(subqueries))
	for i := range localIndices {
		localIndices[i] = i
	}

	// Group subqueries by filter patterns
	filterGroups := groupSubqueriesByFilters(subqueries, localIndices, group.Signature.CorrelationVars)

	// Apply CSE: merge filter groups with same structure but different accessed attributes
	if p.options.EnableCSE {
		filterGroups = mergeFilterGroupsWithCSE(filterGroups)
	}

	// Create merged query plan for each filter group
	var mergedPlans []*QueryPlan
	var allGroupingVars [][]query.Symbol
	columnMapping := make(map[int]ResultMap)

	for groupIdx, fg := range filterGroups {
		// Merge subqueries in this filter group
		mergedQuery, colMap, groupingVars, err := mergeSubqueriesInGroup(subqueries, fg, group.Signature)
		if err != nil {
			return nil, fmt.Errorf("failed to merge subqueries in filter group %d: %w", groupIdx, err)
		}

		// Plan the merged query
		mergedPlan, err := p.Plan(mergedQuery)
		if err != nil {
			return nil, fmt.Errorf("failed to plan merged query for filter group %d: %w", groupIdx, err)
		}

		mergedPlans = append(mergedPlans, mergedPlan)
		allGroupingVars = append(allGroupingVars, groupingVars)

		// Update column mapping (map local indices back to original phase indices)
		for localIdx, cols := range colMap {
			originalIdx := group.Subqueries[localIdx]

			// Extract binding variable names from the original subquery
			var bindingVars []query.Symbol
			if localIdx < len(subqueries) {
				subq := subqueries[localIdx]
				if subq.Subquery != nil {
					switch binding := subq.Subquery.Binding.(type) {
					case query.TupleBinding:
						bindingVars = binding.Variables
					case query.RelationBinding:
						bindingVars = binding.Variables
					}
				}
			}

			columnMapping[originalIdx] = ResultMap{
				FilterGroupIdx: groupIdx,
				ColumnIndices:  cols,
				BindingVars:    bindingVars,
			}
		}
	}

	return &DecorrelatedSubqueryPlan{
		OriginalSubqueries: group.Subqueries,
		FilterGroups:       filterGroups,
		MergedPlans:        mergedPlans,
		CorrelationKeys:    group.Signature.CorrelationVars,
		GroupingVars:       allGroupingVars,
		ColumnMapping:      columnMapping,
		// Metadata for annotations
		SignatureHash:     group.Signature.Hash(),
		TotalSubqueries:   len(phase.Subqueries),
		DecorrelatedCount: len(group.Subqueries),
	}, nil
}

// mergeSubqueriesInGroup merges subqueries in a filter group into single query
// Returns: merged query, column mapping, grouping variables, error
func mergeSubqueriesInGroup(subqueries []*SubqueryPlan, fg FilterGroup,
	sig CorrelationSignature) (*query.Query, map[int][]int, []query.Symbol, error) {

	// Start with first subquery as base
	if len(fg.Subqueries) == 0 {
		return nil, nil, nil, fmt.Errorf("empty filter group")
	}

	baseSubq := subqueries[fg.Subqueries[0]]
	baseQuery := baseSubq.NestedPlan.Query

	// Extract formal parameter names from the subquery's :in clause
	// (NOT the actual arguments from the outer query)
	var formalParams []query.Symbol
	for _, inputSpec := range baseSubq.Subquery.Query.In {
		switch inp := inputSpec.(type) {
		case query.DatabaseInput:
			formalParams = append(formalParams, "$")
		case query.ScalarInput:
			formalParams = append(formalParams, inp.Symbol)
		case query.CollectionInput:
			formalParams = append(formalParams, inp.Symbol)
		case query.TupleInput:
			formalParams = append(formalParams, inp.Symbols...)
		case query.RelationInput:
			formalParams = append(formalParams, inp.Symbols...)
		}
	}

	// Collect all aggregate expressions
	var allFindElements []query.FindElement
	columnMapping := make(map[int][]int)

	// Build mapping from formal parameters to pattern variables using correlation predicates
	// Example: [(= ?py ?y)] means ?y (formal param) maps to ?py (pattern var)
	formalToPattern := make(map[query.Symbol]query.Symbol)
	for _, clause := range baseQuery.Where {
		if comp, ok := clause.(*query.Comparison); ok {
			if comp.Op == query.OpEQ {
				leftVar, leftIsVar := comp.Left.(query.VariableTerm)
				rightVar, rightIsVar := comp.Right.(query.VariableTerm)

				if leftIsVar && rightIsVar {
					// Check if one is a formal parameter
					for _, param := range formalParams {
						if param == "$" {
							continue
						}
						if leftVar.Symbol == param {
							formalToPattern[param] = rightVar.Symbol
						} else if rightVar.Symbol == param {
							formalToPattern[param] = leftVar.Symbol
						}
					}
				}
			}
		}
	}

	// Add grouping keys to :find (use pattern variables, not formal parameters)
	groupingVars := make([]query.Symbol, 0)
	for _, key := range formalParams {
		if key == "$" {
			continue // Skip database marker
		}
		// Use the pattern variable that corresponds to this formal parameter
		if patternVar, found := formalToPattern[key]; found {
			groupingVars = append(groupingVars, patternVar)
			allFindElements = append(allFindElements, query.FindVariable{Symbol: patternVar})
		} else {
			// Formal parameter is directly used in patterns
			groupingVars = append(groupingVars, key)
			allFindElements = append(allFindElements, query.FindVariable{Symbol: key})
		}
	}
	groupKeyCount := len(groupingVars)

	nextColIdx := groupKeyCount // After grouping keys

	// Add aggregate expressions from each subquery
	for _, subqIdx := range fg.Subqueries {
		subq := subqueries[subqIdx]

		var colIndices []int
		for _, findElem := range subq.NestedPlan.Query.Find {
			if findElem.IsAggregate() {
				allFindElements = append(allFindElements, findElem)
				colIndices = append(colIndices, nextColIdx)
				nextColIdx++
			}
		}

		columnMapping[subqIdx] = colIndices
	}

	// Build WHERE clause: Collect patterns from ALL subqueries in the filter group
	// This is critical for CSE - when filter groups are merged, we need ALL their patterns
	var whereClauses []query.Clause

	// Collect unique patterns from all subqueries
	patternSet := make(map[string]query.Clause) // fingerprint -> pattern clause
	for _, subqIdx := range fg.Subqueries {
		subq := subqueries[subqIdx]
		for _, clause := range subq.NestedPlan.Query.Where {
			// Check for concrete pattern types
			if dataPat, ok := clause.(*query.DataPattern); ok {
				fp := extractPatternFingerprint(dataPat)
				patternSet[fp.Hash()] = clause
			} else if subqPat, ok := clause.(*query.SubqueryPattern); ok {
				fp := extractPatternFingerprint(subqPat)
				patternSet[fp.Hash()] = clause
			}
		}
	}

	// Add all unique patterns to WHERE clause
	for _, clause := range patternSet {
		whereClauses = append(whereClauses, clause)
	}

	// Add non-correlation predicates from base query
	for _, clause := range baseQuery.Where {
		// Skip patterns (already added above) - check for concrete pattern types
		if _, ok := clause.(*query.DataPattern); ok {
			continue
		}
		if _, ok := clause.(*query.SubqueryPattern); ok {
			continue
		}

		// Check if this is a correlation predicate
		// A correlation predicate is an equality between two variables where one is a formal parameter
		if comp, ok := clause.(*query.Comparison); ok {
			if comp.Op == query.OpEQ {
				// Check if both operands are variables
				leftVar, leftIsVar := comp.Left.(query.VariableTerm)
				rightVar, rightIsVar := comp.Right.(query.VariableTerm)

				if leftIsVar && rightIsVar {
					// Check if one of them is a formal parameter (excluding $)
					hasFormalParam := false
					for _, param := range formalParams {
						if param == "$" {
							continue // Skip database marker
						}
						if leftVar.Symbol == param || rightVar.Symbol == param {
							hasFormalParam = true
							break
						}
					}

					if hasFormalParam {
						// This is a correlation predicate - skip it
						continue
					}
				}
			}
		}

		// Keep all other clauses (expressions, filter predicates)
		whereClauses = append(whereClauses, clause)
	}

	// Create merged query
	mergedQuery := &query.Query{
		Find:  allFindElements,
		In:    []query.InputSpec{query.DatabaseInput{}}, // Just database, no correlation inputs
		Where: whereClauses,                             // Filtered WHERE clauses
	}

	return mergedQuery, columnMapping, groupingVars, nil
}

// detectAndPlanDecorrelation detects and plans decorrelated subqueries in a phase
func (p *Planner) detectAndPlanDecorrelation(phase *Phase) error {
	// Check if decorrelation is enabled
	if !p.options.EnableSubqueryDecorrelation {
		return nil
	}

	// Analyze all subqueries and capture signature information
	signatureInfo := make(map[string][]int) // signature hash -> subquery indices
	for i := range phase.Subqueries {
		subqPlan := &phase.Subqueries[i]
		sig := extractCorrelationSignature(subqPlan)

		// Store in metadata for potential annotation
		if sig.IsAggregate && len(sig.CorrelationVars) > 0 {
			hash := sig.Hash()
			signatureInfo[hash] = append(signatureInfo[hash], i)
		}
	}

	// Store signature analysis in phase metadata
	phase.Metadata = map[string]interface{}{
		"decorrelation_analysis": map[string]interface{}{
			"total_subqueries": len(phase.Subqueries),
			"signature_groups": len(signatureInfo),
			"signatures":       signatureInfo,
		},
	}

	// Detect opportunities
	opportunities := detectDecorrelationOpportunities(phase)

	// Create decorrelated plans
	decorrelationErrors := make(map[string]string) // signature -> error
	for _, opp := range opportunities {
		decorPlan, err := p.createDecorrelatedPlan(phase, opp)
		if err != nil {
			// Log error but don't fail - fall back to sequential execution
			decorrelationErrors[opp.Signature.Hash()] = err.Error()
			continue
		}

		phase.DecorrelatedSubqueries = append(phase.DecorrelatedSubqueries, *decorPlan)

		// Mark original subqueries as decorrelated (skip in execution)
		for _, idx := range opp.Subqueries {
			phase.Subqueries[idx].Decorrelated = true
		}
	}

	// Update metadata with any errors
	if len(decorrelationErrors) > 0 {
		if meta, ok := phase.Metadata["decorrelation_analysis"].(map[string]interface{}); ok {
			meta["errors"] = decorrelationErrors
		}
	}

	return nil
}
