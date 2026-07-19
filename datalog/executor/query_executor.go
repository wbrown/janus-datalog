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
	matcher          PatternMatcher
	entityResolver   EntityResolver
	options          ExecutorOptions
	constantBindings map[query.Symbol]interface{} // Scalar inputs resolved as constants (not relation symbols)
}

// newQueryExecutor creates a new DefaultQueryExecutor.
//
// INTERNAL USE ONLY: This bypasses the query planner. Tests should use NewExecutor
// or NewExecutorWithOptions which go through the planner. The only legitimate use
// of this function in tests is for unit testing internal executor methods like
// executePattern or executeExpression. End-to-end and integration tests must use
// the public API to ensure the planner is exercised.
func newQueryExecutor(matcher PatternMatcher, resolver EntityResolver, options ExecutorOptions) *DefaultQueryExecutor {
	return &DefaultQueryExecutor{
		matcher:        matcher,
		entityResolver: resolver,
		options:        options,
	}
}

// Execute implements QueryExecutor interface
// Executes clauses progressively with collapse after each step
func (e *DefaultQueryExecutor) Execute(ctx Context, q *query.Query, inputs []Relation) ([]Relation, error) {
	ctx.QueryBegin(q.String())
	defer func(start int64) {
		ctx.QueryComplete(0, 0, nil) // TODO: Add proper tuple count
	}(0)

	// Simple path: clause-by-clause execution
	// Start with input relation groups (may be multiple disjoint groups)
	groups := Relations(inputs)

	// Execute each clause in the :where section
	// Patterns/Subqueries produce NEW relations (append + collapse)
	// Expressions/Predicates TRANSFORM relations (replace groups + collapse)
	for i := 0; i < len(q.Where); i++ {
		clause := q.Where[i]
		switch c := clause.(type) {
		case *query.DataPattern:
			// Same-entity attribute fetches on an already-bound ?e are
			// per-tuple attribute attaches, not new relations to hash-join.
			// Consume a contiguous run in one traversal when every pattern
			// satisfies the existing cardinality and temporal gates.
			fusedCount := 0
			if e.options.EnableAttributeFetchFusion {
				newGroups, count, err := e.tryFuseAttributeFetchBundle(ctx, q.Where[i:], groups)
				if err != nil {
					return nil, fmt.Errorf("clause %d (attribute fetch bundle) failed: %w", i, err)
				}
				if count > 0 {
					groups = Relations(newGroups)
					fusedCount = count
				}
			}
			if fusedCount == 0 {
				patternQuery := &query.Query{
					Find:  q.Find,
					In:    q.In,
					Where: []query.Clause{c},
				}
				if len(q.Where) == 1 {
					patternQuery.OrderBy = q.OrderBy
					patternQuery.Limit = q.Limit
				}
				newRel, err := e.executePattern(ctx, patternQuery, c, groups)
				if err != nil {
					return nil, fmt.Errorf("clause %d (pattern) failed: %w", i, err)
				}
				// Always append the relation - don't check IsEmpty() as that consumes iterators
				// Collapse will handle empty relations correctly
				if newRel != nil {
					groups = append(groups, newRel)
				}
			}
			groups = Relations(ctx.CollapseRelations([]Relation(groups), func() []Relation {
				return []Relation(groups.Collapse(ctx))
			}))

			// Prefetch entity attributes into EA cache after first DataPattern.
			// Subsequent patterns calling Match() with bindings will hit
			// matchWithBindingsFromCache → GetOrResolve → cache hit (O(1))
			// instead of cold GetOrResolve → per-(E,A) EATV scan.
			// Materializes streaming relations to extract entity IDs — this
			// materialization would happen anyway when the next pattern joins.
			if e.options.EnableEntityPrefetch && i == 0 && len(groups) > 0 {
				if prefetcher, ok := e.matcher.(EntityPrefetcher); ok {
					g := groups[len(groups)-1]
					if _, isStreaming := g.(*StreamingRelation); isStreaming {
						g = g.Materialize()
						groups[len(groups)-1] = g
					}
					entities := extractEntityIDs(g, g.Symbols())
					if len(entities) > 50 {
						if collector := ctx.Collector(); collector != nil {
							collector.Add(annotations.Event{
								Name: "prefetch/trigger",
								Data: map[string]interface{}{
									"entity_count": len(entities),
									"symbols":      fmt.Sprintf("%v", g.Symbols()),
								},
							})
						}
						prefetcher.PrefetchEntities(entities)
					}
				}
			}
			if fusedCount > 1 {
				i += fusedCount - 1
			}

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
				groups = replaceConsumedOrGroups(ctx, groups, newRel)
			}
			groups = Relations(ctx.CollapseRelations([]Relation(groups), func() []Relation {
				return []Relation(groups.Collapse(ctx))
			}))

		case *query.OrJoinClause:
			newRel, err := e.executeOrJoinClause(ctx, c, groups)
			if err != nil {
				return nil, fmt.Errorf("clause %d (or-join) failed: %w", i, err)
			}
			if newRel != nil {
				groups = replaceConsumedOrGroups(ctx, groups, newRel)
			}
			groups = Relations(ctx.CollapseRelations([]Relation(groups), func() []Relation {
				return []Relation(groups.Collapse(ctx))
			}))

		case *query.OrDefaultClause:
			newRel, err := e.executeOrDefaultClause(ctx, c, groups)
			if err != nil {
				return nil, fmt.Errorf("clause %d (or-default) failed: %w", i, err)
			}
			if newRel != nil {
				groups = replaceConsumedOrGroups(ctx, groups, newRel)
			}
			groups = Relations(ctx.CollapseRelations([]Relation(groups), func() []Relation {
				return []Relation(groups.Collapse(ctx))
			}))

		case *query.OrDefaultJoinClause:
			newRel, err := e.executeOrDefaultJoinClause(ctx, c, groups)
			if err != nil {
				return nil, fmt.Errorf("clause %d (or-default-join) failed: %w", i, err)
			}
			if newRel != nil {
				groups = replaceConsumedOrGroups(ctx, groups, newRel)
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
		if materialized, ok := result.(*MaterializedRelation); ok && materialized.err != nil {
			return nil, materialized.err
		}
		return []Relation{result}, nil

	} else {
		// Simple projection to :find symbols. Pull find elements project
		// their entity variable like any other symbol (Identity values):
		// pull rendering is result presentation and happens at the result
		// boundary in ExecuteRealized, after sort/strip/limit — never
		// inside relational flow. See
		// docs/bugs/resolved/BUG_PULL_WITH_ORDER_BY_PANICS.md.
		findSymbols := extractFindSymbols(q.Find)

		// Check if all :find symbols are available across the groups
		// If symbols span multiple groups, we need to Product() them first
		if len(groups) > 1 {
			// Check which groups contain which :find symbols
			groupsHaveSymbols := make([][]bool, len(groups))
			for i, group := range groups {
				groupsHaveSymbols[i] = make([]bool, len(findSymbols))
				grpSyms := group.Symbols()
				for j, fs := range findSymbols {
					groupsHaveSymbols[i][j] = query.ContainsSymbol(grpSyms, fs)
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
					// This group has all symbols - project it and return.
					// Pull rendering happens at the result boundary
					// (ExecuteRealized), downstream of every path here.
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
				// Take Product() of all groups to create a single relation.
				// Pull rendering happens at the result boundary
				// (ExecuteRealized), downstream of every path here.
				combined := Relations(groups).Product()
				projected, err := combined.Project(findSymbols)
				if err != nil {
					return nil, fmt.Errorf("projection failed after product: %w", err)
				}
				return []Relation{projected}, nil
			}
		}

		// Single group or each group projects independently. Pull rendering
		// happens at the result boundary (ExecuteRealized, after
		// sort/strip/limit), not here: entity bindings stay Identity through
		// the relational pipeline.
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
func (e *DefaultQueryExecutor) executePattern(
	ctx Context,
	q *query.Query,
	pattern *query.DataPattern,
	groups []Relation,
) (Relation, error) {
	// Materialize groups that share symbols with the pattern
	// These groups will be: (1) used for binding-based filtering, (2) joined with the result
	// Materializing allows them to be iterated multiple times without consuming the iterator
	bindings := materializeRelationsForPattern(pattern, Relations(groups))

	// Use PatternMatcher with current groups as bindings
	// NOTE: bindings are used for pattern selection heuristics (FindBestForPattern)
	// and potentially for batch scanning - they will also be joined with the result later
	rel, err := e.matcher.Match(q, bindings)
	if err != nil {
		return nil, err
	}
	return rel, nil
}

type attributeFetchSpec struct {
	attr         datalog.Keyword
	output       query.Symbol
	expected     interface{}
	isConstraint bool
}

func replaceConsumedOrGroups(
	ctx Context,
	groups Relations,
	relation Relation,
) Relations {
	orRelation, ok := relation.(*OrFallbackRelation)
	if !ok || len(orRelation.consumedGroups) == 0 {
		return append(groups, relation)
	}

	consumed := make(map[int]bool, len(orRelation.consumedGroups))
	insertAt := len(groups)
	for _, index := range orRelation.consumedGroups {
		if index < 0 || index >= len(groups) {
			continue
		}
		consumed[index] = true
		if index < insertAt {
			insertAt = index
		}
	}
	if len(consumed) == 0 {
		return append(groups, relation)
	}

	result := make(Relations, 0, len(groups)-len(consumed)+1)
	inserted := false
	for index, group := range groups {
		if index == insertAt {
			result = append(result, relation)
			inserted = true
		}
		if !consumed[index] {
			result = append(result, group)
		}
	}
	if !inserted {
		result = append(result, relation)
	}

	if collector := ctx.Collector(); collector != nil {
		collector.Add(annotations.Event{
			Name: "or/outer-replaced",
			Data: map[string]interface{}{
				"consumed_groups":  len(consumed),
				"remaining_groups": len(result),
			},
		})
	}
	return result
}

// tryFuseAttributeFetchBundle recognizes a contiguous run of same-entity
// cardinality-one patterns and executes all of them as one per-tuple attribute
// attach. It preserves the existing single-pattern gates and event stream while
// avoiding one relation traversal and materialization per additional attribute.
//
// Returns the replacement groups and number of consumed clauses. A zero count
// means the first clause does not qualify and must use normal pattern matching.
func (e *DefaultQueryExecutor) tryFuseAttributeFetchBundle(
	ctx Context,
	clauses []query.Clause,
	groups []Relation,
) ([]Relation, int, error) {
	if len(clauses) == 0 {
		return nil, 0, nil
	}
	first, ok := clauses[0].(*query.DataPattern)
	if !ok || len(first.Elements) != 3 || first.Source != nil {
		return nil, 0, nil
	}
	entityVar, ok := first.GetE().(query.Variable)
	if !ok {
		return nil, 0, nil
	}

	lookupMatcher, ok := e.matcher.(EntityLookupMatcher)
	if !ok {
		return nil, 0, nil
	}
	fusable, ok := e.matcher.(AttributeFetchFusable)
	if !ok {
		return nil, 0, nil
	}

	boundSymbols := make(map[query.Symbol]bool)
	entityGroup := -1
	for groupIndex, group := range groups {
		for _, symbol := range group.Symbols() {
			boundSymbols[symbol] = true
			if symbol == entityVar.Name {
				if entityGroup >= 0 {
					return nil, 0, nil
				}
				entityGroup = groupIndex
			}
		}
	}
	if entityGroup < 0 {
		return nil, 0, nil
	}

	var fetches []attributeFetchSpec
fetchLoop:
	for _, clause := range clauses {
		pattern, ok := clause.(*query.DataPattern)
		if !ok || len(pattern.Elements) != 3 || pattern.Source != nil {
			break
		}
		candidateEntity, ok := pattern.GetE().(query.Variable)
		if !ok || candidateEntity.Name != entityVar.Name {
			break
		}
		attrConstant, ok := pattern.GetA().(query.Constant)
		if !ok {
			break
		}
		attr, ok := attrConstant.Value.(datalog.Keyword)
		if !ok || !fusable.CanFuseAttributeFetch(attr) {
			break
		}
		spec := attributeFetchSpec{attr: attr}
		switch value := pattern.GetV().(type) {
		case query.Variable:
			if boundSymbols[value.Name] {
				return nil, 0, nil
			}
			spec.output = value.Name
			boundSymbols[value.Name] = true
		case query.Constant:
			spec.expected = value.Value
			spec.isConstraint = true
		default:
			break fetchLoop
		}

		fetches = append(fetches, spec)
	}
	if len(fetches) == 0 {
		return nil, 0, nil
	}

	input := groups[entityGroup]
	inputSymbols := input.Symbols()
	entityIndex := query.SymbolIndex(inputSymbols, entityVar.Name)

	outputSymbols := make([]query.Symbol, 0, len(inputSymbols)+len(fetches))
	outputSymbols = append(outputSymbols, inputSymbols...)
	for _, fetch := range fetches {
		if fetch.output != nil {
			outputSymbols = append(outputSymbols, fetch.output)
		}
	}

	var output []Tuple
	if size := input.Size(); size > 0 {
		output = make([]Tuple, 0, size)
	}
	inputCounts := make([]int, len(fetches))
	outputCounts := make([]int, len(fetches))
	var lookupErr error

	it := input.Iterator()
	for it.Next() {
		tuple := it.Tuple()
		if entityIndex >= len(tuple) {
			inputCounts[0]++
			continue
		}
		entity, ok := tuple[entityIndex].(datalog.Identity)
		if !ok {
			inputCounts[0]++
			continue
		}

		var expanded Tuple
		outputIndex := len(inputSymbols)
		complete := true
		for i, fetch := range fetches {
			inputCounts[i]++
			value, found, err := lookupMatcher.LookupAttribute(entity, fetch.attr)
			lookupErr = err
			if lookupErr != nil {
				complete = false
				break
			}
			if !found {
				complete = false
				break
			}
			if fetch.isConstraint && !datalog.ValuesEqual(value, fetch.expected) {
				complete = false
				break
			}
			outputCounts[i]++
			if fetch.output != nil {
				if expanded == nil {
					expanded = make(Tuple, len(outputSymbols))
					copy(expanded, tuple)
				}
				expanded[outputIndex] = value
				outputIndex++
			}
		}
		if complete {
			if expanded == nil {
				if input.RequiresCopy() {
					expanded = copyTuple(tuple)
				} else {
					expanded = tuple
				}
			}
			output = append(output, expanded)
		}
		if lookupErr != nil {
			break
		}
	}
	iterErr := lookupErr
	if iterErr == nil {
		iterErr = it.Error()
	}
	if closeErr := it.Close(); iterErr == nil {
		iterErr = closeErr
	}
	if iterErr != nil {
		return nil, 0, iterErr
	}

	if collector := ctx.Collector(); collector != nil {
		for i, fetch := range fetches {
			eventName := "pattern/fused-fetch"
			if fetch.isConstraint {
				eventName = "pattern/fused-constraint"
			}
			collector.Add(annotations.Event{
				Name: eventName,
				Data: map[string]interface{}{
					"attr": fetch.attr.String(),
					"in":   inputCounts[i],
					"out":  outputCounts[i],
				},
			})
		}
	}

	newGroups := make([]Relation, len(groups))
	copy(newGroups, groups)
	properties := input.Properties()
	for _, fetch := range fetches {
		properties = properties.addSymbol(fetch.output)
	}
	newGroups[entityGroup] = newMaterializedRelationFromSet(
		outputSymbols,
		output,
		e.options,
		properties,
	)
	return newGroups, len(fetches), nil
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
		var result interface{}
		var err error
		if dbFunc, ok := expr.Function.(query.DatabaseFunction); ok {
			if lookupMatcher, ok := e.matcher.(EntityLookupMatcher); ok {
				lookup := entityLookupAdapter{lookupMatcher}
				result, err = dbFunc.EvalWithLookup(make(map[query.Symbol]interface{}), lookup)
			} else {
				return nil, fmt.Errorf("expression requires database lookup but matcher doesn't support it")
			}
		} else {
			result, err = expr.Function.Eval(make(map[query.Symbol]interface{}))
		}
		if err != nil {
			return nil, err
		}
		if err := admitExpressionResult(expr.Function, result); err != nil {
			return nil, err
		}

		// Handle both scalar and tuple bindings
		switch binding := expr.Binding.(type) {
		case query.TupleBinding:
			values, ok := result.([]interface{})
			if !ok {
				return nil, fmt.Errorf("tuple binding requires tuple result, got %T", result)
			}
			if len(values) != len(binding.Variables) {
				return nil, fmt.Errorf("tuple mismatch: %d values, %d variables",
					len(values), len(binding.Variables))
			}
			symbols := binding.Variables
			tuple := make(Tuple, len(values))
			copy(tuple, values)
			return []Relation{NewMaterializedRelationWithOptions(symbols, []Tuple{tuple}, e.options)}, nil

		case query.Symbol:
			if binding == nil {
				return groups, nil
			}
			symbols := []query.Symbol{binding}
			tuples := []Tuple{{result}}
			return []Relation{NewMaterializedRelationWithOptions(symbols, tuples, e.options)}, nil

		default:
			return nil, fmt.Errorf("unsupported binding type: %T", expr.Binding)
		}
	}

	// Filter out symbols already resolved as constants
	var unresolvedExprSyms []query.Symbol
	for _, sym := range requiredSyms {
		if _, isConstant := e.constantBindings[sym]; !isConstant {
			unresolvedExprSyms = append(unresolvedExprSyms, sym)
		}
	}

	for _, rel := range groups {
		hasAny := false
		relSyms := rel.Symbols()
		for _, sym := range unresolvedExprSyms {
			if query.ContainsSymbol(relSyms, sym) {
				hasAny = true
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
		if len(unresolvedExprSyms) == 0 && len(groups) > 0 {
			// Ground expression (or all-constants) with existing groups - evaluate once and add symbol(s)
			// This handles OR fallback: (or [subquery] [(ground 0) ?count])
			// and constant-bindable scalar expressions: [(+ ?age ?bonus) ?adjusted] where ?bonus is constant
			evalBindings := make(map[query.Symbol]interface{})
			for sym, val := range e.constantBindings {
				evalBindings[sym] = val
			}
			var result interface{}
			var err error
			if dbFunc, ok := expr.Function.(query.DatabaseFunction); ok {
				if lookupMatcher, ok := e.matcher.(EntityLookupMatcher); ok {
					lookup := entityLookupAdapter{lookupMatcher}
					result, err = dbFunc.EvalWithLookup(evalBindings, lookup)
				} else {
					return nil, fmt.Errorf("expression requires database lookup but matcher doesn't support it")
				}
			} else {
				result, err = expr.Function.Eval(evalBindings)
			}
			if err != nil {
				return nil, err
			}
			if err := admitExpressionResult(expr.Function, result); err != nil {
				return nil, err
			}

			// Determine binding symbols and values
			var bindingSyms []query.Symbol
			var bindingValues []interface{}
			switch binding := expr.Binding.(type) {
			case query.TupleBinding:
				bindingSyms = binding.Variables
				values, ok := result.([]interface{})
				if !ok {
					return nil, fmt.Errorf("tuple binding requires tuple result, got %T", result)
				}
				if len(values) != len(binding.Variables) {
					return nil, fmt.Errorf("tuple mismatch: %d values, %d variables",
						len(values), len(binding.Variables))
				}
				bindingValues = values
			case query.Symbol:
				if binding != nil {
					bindingSyms = []query.Symbol{binding}
					bindingValues = []interface{}{result}
				}
			}

			// For each binding symbol: if already in the relation, filter
			// (unify) instead of extending. If not present, extend.
			var resultRels []Relation
			for _, rel := range groups {
				relSyms := rel.Symbols()

				// Partition binding symbols into existing (filter) and new (extend)
				var filterIdx []int    // indices into bindingSyms that already exist in rel
				var filterRelIdx []int // corresponding positions in relSyms
				var extendIdx []int    // indices into bindingSyms that are new

				for i, bs := range bindingSyms {
					found := false
					for j, rs := range relSyms {
						if bs == rs {
							filterIdx = append(filterIdx, i)
							filterRelIdx = append(filterRelIdx, j)
							found = true
							break
						}
					}
					if !found {
						extendIdx = append(extendIdx, i)
					}
				}

				// Build output symbols: existing + only the new binding symbols
				outputSyms := make([]query.Symbol, len(relSyms))
				copy(outputSyms, relSyms)
				for _, ei := range extendIdx {
					outputSyms = append(outputSyms, bindingSyms[ei])
				}

				var outputTuples []Tuple
				iter := rel.Iterator()
				for iter.Next() {
					oldTuple := iter.Tuple()

					// Check filter conditions: existing symbols must match ground values
					match := true
					for k, fi := range filterIdx {
						_ = k
						if filterRelIdx[k] < len(oldTuple) {
							if !datalog.ValuesEqual(oldTuple[filterRelIdx[k]], bindingValues[fi]) {
								match = false
								break
							}
						}
					}
					if !match {
						continue
					}

					// Build output tuple: old values + new extension values
					newTuple := make(Tuple, len(outputSyms))
					copy(newTuple, oldTuple)
					for i, ei := range extendIdx {
						newTuple[len(relSyms)+i] = bindingValues[ei]
					}
					outputTuples = append(outputTuples, newTuple)
				}
				iter.Close()
				resultRels = append(resultRels, NewMaterializedRelationWithOptions(outputSyms, outputTuples, e.options))
			}
			return resultRels, nil
		}

		// Handle all-constants expression with no groups (e.g., get-some with scalar input only)
		// This occurs when all required symbols are constant-bindable and there are no data patterns.
		if len(unresolvedExprSyms) == 0 && len(groups) == 0 {
			evalBindings := make(map[query.Symbol]interface{})
			for sym, val := range e.constantBindings {
				evalBindings[sym] = val
			}

			// Evaluate the expression - check for database function needing lookup
			var result interface{}
			var err error
			if dbFunc, ok := expr.Function.(query.DatabaseFunction); ok {
				if lookupMatcher, ok := e.matcher.(EntityLookupMatcher); ok {
					lookup := entityLookupAdapter{lookupMatcher}
					result, err = dbFunc.EvalWithLookup(evalBindings, lookup)
				} else {
					return nil, fmt.Errorf("expression requires database lookup but matcher doesn't support it")
				}
			} else {
				result, err = expr.Function.Eval(evalBindings)
			}
			if err != nil {
				return nil, err
			}

			// Handle get-some result type. Found=false signals "no attribute
			// matched"; emit an empty relation rather than a tuple.
			if gsr, ok := result.(*query.GetSomeResult); ok {
				if !gsr.Found {
					return []Relation{}, nil
				}
				result = gsr.Value
			}

			if err := admitExpressionResult(expr.Function, result); err != nil {
				return nil, err
			}

			// Create result relation based on binding type
			switch binding := expr.Binding.(type) {
			case query.TupleBinding:
				values, ok := result.([]interface{})
				if !ok {
					return nil, fmt.Errorf("tuple binding requires tuple result, got %T", result)
				}
				if len(values) != len(binding.Variables) {
					return nil, fmt.Errorf("tuple mismatch: %d values, %d variables",
						len(values), len(binding.Variables))
				}
				return []Relation{NewMaterializedRelationWithOptions(binding.Variables, []Tuple{values}, e.options)}, nil
			case query.Symbol:
				if binding != nil {
					return []Relation{NewMaterializedRelationWithOptions([]query.Symbol{binding}, []Tuple{{result}}, e.options)}, nil
				}
			}
			// No binding - return empty groups (expression evaluated for side effect only)
			return []Relation{}, nil
		}

		// Skip expression if no relevant relations and expression needs symbols
		return groups, nil
	}

	// Check if this expression might need database access (database functions)
	var lookup query.EntityLookup
	if lookupMatcher, ok := e.matcher.(EntityLookupMatcher); ok {
		lookup = entityLookupAdapter{lookupMatcher}
	}

	var result Relation
	if len(relevantRels) == 1 {
		// Single relation — evaluate directly, no join needed
		result = evaluateExpressionWithLookup(relevantRels[0], expr, lookup, e.constantBindings)
	} else {
		// Multiple disjoint relations — cross-join with expression evaluation
		// Uses BufferedIterator for inner re-iteration instead of Product()
		result = crossJoinWithExpression(relevantRels, expr, lookup, e.constantBindings, e.options)
	}

	// Return result + unchanged relations
	return append([]Relation{result}, otherRels...), nil
}

// entityLookupAdapter adapts EntityLookupMatcher to query.EntityLookup
type entityLookupAdapter struct {
	matcher EntityLookupMatcher
}

func (a entityLookupAdapter) LookupAttribute(
	entity datalog.Identity,
	attr datalog.Keyword,
) (interface{}, bool, error) {
	return a.matcher.LookupAttribute(entity, attr)
}

func (a entityLookupAdapter) TypeDefault(attr datalog.Keyword, defaultVal interface{}) interface{} {
	if td, ok := a.matcher.(query.TypedDefaulter); ok {
		return td.TypeDefault(attr, defaultVal)
	}
	return defaultVal
}

// executePredicate filters relations using a predicate
// Predicates TRANSFORM groups - may use Product() for multi-relation predicates
func (e *DefaultQueryExecutor) executePredicate(ctx Context, pred query.Predicate, groups []Relation) ([]Relation, error) {
	// Find relations with ANY required symbols (same logic as executeExpression)
	// Exclude symbols resolved as constants — they don't need a relation group
	var relevantRels []Relation
	var otherRels []Relation

	requiredSyms := pred.RequiredSymbols()

	// Filter out symbols already resolved as constants
	var unresolvedSyms []query.Symbol
	for _, sym := range requiredSyms {
		if _, isConstant := e.constantBindings[sym]; !isConstant {
			unresolvedSyms = append(unresolvedSyms, sym)
		}
	}

	for _, rel := range groups {
		hasAny := false
		relSyms := rel.Symbols()
		for _, sym := range unresolvedSyms {
			if query.ContainsSymbol(relSyms, sym) {
				hasAny = true
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

	// Check if this predicate might need database access (DatabaseFunctionPredicate)
	var lookup query.EntityLookup
	if lookupMatcher, ok := e.matcher.(EntityLookupMatcher); ok {
		lookup = entityLookupAdapter{lookupMatcher}
	}

	var result Relation
	if len(relevantRels) == 1 {
		// Single relation — filter directly, no join needed
		result = filterWithPredicateAndLookup(relevantRels[0], pred, lookup, e.constantBindings)
	} else {
		// Multiple disjoint relations — theta-join with predicate filter
		// Uses BufferedIterator for inner re-iteration instead of Product()
		result = thetaJoinWithPredicate(relevantRels, pred, lookup, e.constantBindings, e.options)
	}

	// Return result + unchanged relations
	return append([]Relation{result}, otherRels...), nil
}

// executeSubquery executes a nested subquery
// Subqueries produce new relations from nested query execution
func (e *DefaultQueryExecutor) executeSubquery(ctx Context, subq *query.SubqueryPattern, groups []Relation) (Relation, error) {
	// Record the per-input-combination execution path.
	if collector := ctx.Collector(); collector != nil {
		collector.Add(annotations.Event{
			Name: "subquery/executor-path",
			Data: map[string]interface{}{
				"path":         "Per-combination QueryExecutor",
				"query":        subq.Query.String(),
				"input_count":  len(subq.Inputs),
				"groups_count": len(groups),
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
			// Check if it's a source marker
			if sym, ok := inp.Value.(query.Symbol); ok && sym.IsSource() {
				inputSymbols = append(inputSymbols, sym)
			}
			// Other constants don't need extraction
		}
	}

	// Mark groups cacheable before extracting input combinations so the outer
	// query can replay them later. Materialize() on StreamingRelation enables
	// lazy caching; it does not drain the iterator here.
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
		inputRelations, err := createInputRelationsForSubqueryWithOptions(subq, make(map[query.Symbol]interface{}), e.options)
		if err != nil {
			return nil, fmt.Errorf("subquery input binding failed: %w", err)
		}
		nestedGroups, err := e.Execute(ctx, subq.Query, inputRelations)
		if err != nil {
			return nil, fmt.Errorf("nested query execution failed: %w", err)
		}
		if len(nestedGroups) == 0 {
			// Empty result - return empty relation with binding symbols
			return NewMaterializedRelationWithOptions(subq.Binding.BoundVariables(), nil, e.options), nil
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

	// Get unique combinations of input values
	inputCombinations, err := getUniqueInputCombinations(combinedRel, inputSymbols)
	if err != nil {
		return nil, fmt.Errorf("subquery input extraction failed: %w", err)
	}
	if collector := ctx.Collector(); collector != nil {
		collector.Add(annotations.Event{
			Name: "subquery/input-combinations",
			Data: map[string]interface{}{
				"relation_groups":    len(materializedGroups),
				"product":            len(materializedGroups) > 1,
				"eager_materialized": false,
				"combination_count":  len(inputCombinations),
			},
		})
	}

	// Execute subquery for each combination
	var allResults []Relation

	for _, inputValues := range inputCombinations {
		// Create input relations for this combination
		inputRelations, err := createInputRelationsForSubqueryWithOptions(subq, inputValues, e.options)
		if err != nil {
			return nil, fmt.Errorf("subquery input binding failed: %w", err)
		}

		// DEBUG: Log input relations
		if collector := ctx.Collector(); collector != nil {
			for i, rel := range inputRelations {
				collector.Add(annotations.Event{
					Name: "subquery/input-relation",
					Data: map[string]interface{}{
						"index":   i,
						"symbols": rel.Symbols(),
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
		var nestedResult Relation
		if len(nestedGroups) == 0 {
			// No result groups — create empty relation with the subquery's output symbols
			// so applyBindingForm can produce a properly-typed empty result.
			nestedResult = NewMaterializedRelation(subq.Binding.BoundVariables(), []Tuple{})
		} else if len(nestedGroups) > 1 {
			return nil, fmt.Errorf("subquery returned %d disjoint groups - expected 1", len(nestedGroups))
		} else {
			nestedResult = nestedGroups[0]
		}

		// Apply binding form to join results with outer query values
		boundResult, err := applyBindingForm(nestedResult, subq.Binding, inputValues, inputSymbols)
		if err != nil {
			return nil, fmt.Errorf("binding form application failed: %w", err)
		}

		allResults = append(allResults, boundResult)
	}

	// Combine all results
	if len(allResults) == 0 {
		// No results - return empty relation with appropriate symbols
		return NewMaterializedRelation(subq.Binding.BoundVariables(), []Tuple{}), nil
	}

	// Union all results (they should have the same schema)
	return combineSubqueryResultsSimple(allResults), nil
}

func createInputRelationsForSubqueryWithOptions(subq *query.SubqueryPattern, outerValues map[query.Symbol]interface{}, opts ExecutorOptions) ([]Relation, error) {
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
	symbols := results[0].Symbols()
	var allTuples []Tuple

	var collectErr error
	for _, result := range results {
		if err := collectTuplesInto(&allTuples, result); err != nil && collectErr == nil {
			collectErr = err
		}
	}

	mat := NewMaterializedRelation(symbols, allTuples)
	if collectErr != nil {
		mat.err = collectErr
	}
	return mat
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

// applyFindPulls renders pull expressions on a relation, replacing entity
// values with pulled maps per the FindPull patterns in the find clause.
// Pulled maps are result presentation, not datalog values; the approved
// design (docs/bugs/resolved/BUG_PULL_WITH_ORDER_BY_PANICS.md) runs this only at the
// result boundary, after sort/strip/limit.
func applyFindPulls(matcher PatternMatcher, entityResolver EntityResolver, rel Relation, find []query.FindElement) (Relation, error) {
	symbols := rel.Symbols()
	type pullRequest struct {
		symbolIndex int
		pull        query.FindPull
	}
	var requests []pullRequest
	for _, elem := range find {
		if pull, ok := elem.(query.FindPull); ok {
			if i := query.SymbolIndex(symbols, pull.Variable); i >= 0 {
				requests = append(requests, pullRequest{
					symbolIndex: i,
					pull:        pull,
				})
			}
		}
	}
	if len(requests) == 0 {
		return rel, nil
	}

	puller := NewPullExecutor(matcher, entityResolver)
	if collector := rel.Options().Collector; collector != nil {
		puller = NewPullExecutorWithHandler(matcher, entityResolver, collector.Handler())
	}

	var resultTuples []Tuple
	it := rel.Iterator()
	for it.Next() {
		tuple := it.Tuple()
		newTuple := make(Tuple, len(tuple))
		copy(newTuple, tuple)
		resultTuples = append(resultTuples, newTuple)
	}
	iterErr := it.Error()
	closeErr := it.Close()
	if iterErr != nil {
		return nil, iterErr
	}
	if closeErr != nil {
		return nil, closeErr
	}

	for _, request := range requests {
		var entities []datalog.Identity
		var tupleIndexes []int
		for tupleIndex, tuple := range resultTuples {
			if request.symbolIndex >= len(tuple) {
				continue
			}
			entity, ok := tuple[request.symbolIndex].(datalog.Identity)
			if !ok || entity == nil {
				continue
			}
			entities = append(entities, entity)
			tupleIndexes = append(tupleIndexes, tupleIndex)
		}
		pulledResults, err := puller.PullMany(entities, request.pull.Pattern)
		if err != nil {
			return nil, fmt.Errorf("pull failed for %s: %w", request.pull.Variable, err)
		}
		for i, pulled := range pulledResults {
			entity := entities[i]
			if pulled != nil {
				pulled[query.DBIDKey] = entity
			}
			resultTuples[tupleIndexes[i]][request.symbolIndex] = pulled
		}
	}

	// Return relation without deduplication - pulled maps (map[string]interface{})
	// are not comparable and would panic during deduplication
	return &MaterializedRelation{
		symbols: symbols,
		tuples:  resultTuples,
		options: rel.Options(),
	}, nil
}

// executeOrClause performs union of OR branches.
// Routes to correlated union (per-tuple, all branches) when branches reference
// outer symbols, or uncorrelated union (independent execution) otherwise.
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
				"branch_count": len(clause.Branches),
			},
		})
	}

	var result Relation
	var err error
	var semantics string

	// Route to correlated union only when branches have expression clauses
	// that require outer bindings. Pattern-only branches work fine in
	// uncorrelated union — the join with outer context happens during collapse.
	if branchesNeedCorrelatedExecution(clause.Branches) {
		semantics = "correlated-union"
		result, err = e.executeOrClauseCorrelatedUnion(ctx, clause.Branches, groups)
	} else {
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

// executeOrClauseCorrelatedUnion evaluates all branches per outer tuple and unions results.
func (e *DefaultQueryExecutor) executeOrClauseCorrelatedUnion(ctx Context, branches [][]query.Clause, groups Relations) (Relation, error) {
	neededSymbols := collectOrBranchRequiredSymbols(branches)
	outerRel, consumed := e.findOuterRelation(neededSymbols, groups)
	rel := NewOrFallbackRelation(e, ctx, branches, outerRel, e.options, false)
	rel.consumedGroups = consumed
	return rel, nil
}

// executeOrDefaultClause implements fallback semantics for or-default clauses:
// For each input tuple, try branches in order until one returns a result.
func (e *DefaultQueryExecutor) executeOrDefaultClause(ctx Context, clause *query.OrDefaultClause, groups Relations) (Relation, error) {
	neededSymbols := collectOrBranchRequiredSymbols(clause.Branches)
	outerRel, consumed := e.findOuterRelation(neededSymbols, groups)
	rel := NewOrFallbackRelation(e, ctx, clause.Branches, outerRel, e.options, true)
	rel.consumedGroups = consumed
	return rel, nil
}

// executeOrDefaultJoinClause implements fallback semantics for or-default-join clauses.
func (e *DefaultQueryExecutor) executeOrDefaultJoinClause(ctx Context, clause *query.OrDefaultJoinClause, groups Relations) (Relation, error) {
	joinVarSet := make(map[query.Symbol]bool, len(clause.JoinVars))
	for _, v := range clause.JoinVars {
		joinVarSet[v] = true
	}

	outerRel, consumed := e.findOuterRelationBySymbols(joinVarSet, groups)

	prefetched := false
	rel := NewOrFallbackRelation(e, ctx, clause.Branches, outerRel, e.options, true)
	rel.joinSyms = clause.JoinVars
	rel.prefetched = prefetched
	rel.consumedGroups = consumed
	return rel, nil
}

// branchesNeedCorrelatedExecution returns true if any branch contains
// expression clauses (at any nesting depth) that require outer bindings
// to evaluate. Pattern-only branches don't need correlated execution —
// they can be evaluated independently and joined with outer context
// during collapse.
func branchesNeedCorrelatedExecution(branches [][]query.Clause) bool {
	for _, branch := range branches {
		if clausesNeedCorrelation(branch) {
			return true
		}
	}
	return false
}

// clausesNeedCorrelation recursively checks whether any clause in the
// list (or nested within it) contains expressions or correlated predicates.
func clausesNeedCorrelation(clauses []query.Clause) bool {
	needs := false
	query.WalkClauses(clauses, func(clause query.Clause) bool {
		if needs {
			return false // decided; skip remaining descent
		}
		switch cl := clause.(type) {
		case *query.Expression, *query.SubqueryPattern:
			needs = true
			return false
		case query.Predicate:
			if len(cl.RequiredSymbols()) > 0 {
				needs = true
				return false
			}
		}
		return true
	})
	return needs
}

// findOuterRelation collects and joins all groups that provide any of the
// needed symbols into a single outer relation. Returns unit relation if none found.
func (e *DefaultQueryExecutor) findOuterRelation(neededSymbols []query.Symbol, groups Relations) (Relation, []int) {
	if len(groups) == 0 || len(neededSymbols) == 0 {
		return NewUnitRelation(e.options), nil
	}

	var consumed []int
	for i, rel := range groups {
		if containsAny(rel.Symbols(), neededSymbols) {
			consumed = append(consumed, i)
		}
	}
	if len(consumed) == 0 {
		return NewUnitRelation(e.options), nil
	}
	if len(consumed) == 1 {
		return groups[consumed[0]], consumed
	}
	result := groups[consumed[0]].Materialize()
	for _, index := range consumed[1:] {
		result = result.Join(groups[index].Materialize())
	}
	return result, consumed
}

// findOuterRelationBySymbols collects and joins all groups that provide any
// of the specified symbols into a single outer relation. Returns unit relation if none found.
func (e *DefaultQueryExecutor) findOuterRelationBySymbols(symSet map[query.Symbol]bool, groups Relations) (Relation, []int) {
	var consumed []int
	for i, rel := range groups {
		for _, sym := range rel.Symbols() {
			if symSet[sym] {
				consumed = append(consumed, i)
				break
			}
		}
	}
	if len(consumed) == 0 {
		return NewUnitRelation(e.options), nil
	}
	if len(consumed) == 1 {
		return groups[consumed[0]], consumed
	}
	result := groups[consumed[0]].Materialize()
	for _, index := range consumed[1:] {
		result = result.Join(groups[index].Materialize())
	}
	return result, consumed
}

// findCommonSymbols returns symbols that exist in all relations
func findCommonSymbols(relations []Relation) []query.Symbol {
	if len(relations) == 0 {
		return nil
	}

	// Start with first relation's symbols
	symSet := make(map[query.Symbol]bool)
	for _, sym := range relations[0].Symbols() {
		symSet[sym] = true
	}

	// Intersect with each subsequent relation
	for i := 1; i < len(relations); i++ {
		relSyms := make(map[query.Symbol]bool)
		for _, sym := range relations[i].Symbols() {
			relSyms[sym] = true
		}
		// Keep only symbols that exist in both
		for sym := range symSet {
			if !relSyms[sym] {
				delete(symSet, sym)
			}
		}
	}

	// Preserve order from first relation
	var result []query.Symbol
	for _, sym := range relations[0].Symbols() {
		if symSet[sym] {
			result = append(result, sym)
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
	rightSyms := right.Symbols()
	for rightIter.Next() {
		tuple := rightIter.Tuple()
		key := extractKeyFromTuple(tuple, rightSyms, symbols)
		rightKeys[key] = true
	}
	rightIter.Close()

	// Filter left to only tuples not in right
	var remaining []Tuple
	leftIter := left.Iterator()
	leftSyms := left.Symbols()
	for leftIter.Next() {
		tuple := leftIter.Tuple()
		key := extractKeyFromTuple(tuple, leftSyms, symbols)
		if !rightKeys[key] {
			remaining = append(remaining, tuple)
		}
	}
	leftIter.Close()

	return NewMaterializedRelationWithOptions(leftSyms, remaining, left.Options())
}

// extractKeyFromTuple extracts a string key from tuple for the given symbols
func extractKeyFromTuple(tuple Tuple, syms []query.Symbol, symbols []query.Symbol) string {
	symIdx := make(map[query.Symbol]int)
	for i, sym := range syms {
		symIdx[sym] = i
	}

	var key string
	for _, sym := range symbols {
		if idx, ok := symIdx[sym]; ok && idx < len(tuple) {
			key += fmt.Sprintf("%v|", tuple[idx])
		}
	}
	return key
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
	var commonSyms []query.Symbol

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

		// Track symbols for intersection
		if len(branchResults) == 0 {
			commonSyms = branchResult.Symbols()
		} else {
			// Intersect symbols
			branchSymSet := make(map[query.Symbol]bool)
			for _, sym := range branchResult.Symbols() {
				branchSymSet[sym] = true
			}
			var newCommon []query.Symbol
			for _, sym := range commonSyms {
				if branchSymSet[sym] {
					newCommon = append(newCommon, sym)
				}
			}
			commonSyms = newCommon
		}

		branchResults = append(branchResults, branchResult)
	}

	if len(branchResults) == 0 || len(commonSyms) == 0 {
		return NewMaterializedRelationWithOptions(nil, nil, e.options), nil
	}

	// Union all branch results, projecting to common symbols
	return unionRelations(branchResults, commonSyms, e.options), nil
}

// executeOrJoinClause performs union with explicit join variables.
// Routes to correlated union when branches reference outer symbols.
func (e *DefaultQueryExecutor) executeOrJoinClause(ctx Context, clause *query.OrJoinClause, groups Relations) (Relation, error) {
	if len(clause.Branches) == 0 {
		return NewMaterializedRelationWithOptions(nil, nil, e.options), nil
	}

	if len(clause.JoinVars) == 0 {
		return nil, fmt.Errorf("OR-JOIN clause has no join variables")
	}

	// Or-join always uses correlated execution. Its purpose is to vary
	// which clause matches while preserving outer-bound join variables.
	return e.executeOrJoinClauseCorrelatedUnion(ctx, clause, groups)
}

// executeOrJoinClauseCorrelatedUnion evaluates all branches per outer tuple
// for or-join clauses where branches reference outer symbols.
func (e *DefaultQueryExecutor) executeOrJoinClauseCorrelatedUnion(ctx Context, clause *query.OrJoinClause, groups Relations) (Relation, error) {
	joinVarSet := make(map[query.Symbol]bool, len(clause.JoinVars))
	for _, v := range clause.JoinVars {
		joinVarSet[v] = true
	}

	outerRel, consumed := e.findOuterRelationBySymbols(joinVarSet, groups)

	rel := NewOrFallbackRelation(e, ctx, clause.Branches, outerRel, e.options, false)
	rel.joinSyms = clause.JoinVars
	rel.consumedGroups = consumed
	return rel, nil
}

// extractEntityIDs extracts datalog.Identity values from the specified symbol
// bindings of a materialized relation. Used to collect entity IDs for prefetch.
func extractEntityIDs(rel Relation, syms []query.Symbol) []datalog.Identity {
	symIdx := make(map[query.Symbol]int)
	for i, s := range rel.Symbols() {
		symIdx[s] = i
	}

	var indices []int
	for _, s := range syms {
		if idx, ok := symIdx[s]; ok {
			indices = append(indices, idx)
		}
	}
	if len(indices) == 0 {
		return nil
	}

	seen := make(map[datalog.Identity]bool)
	var entities []datalog.Identity

	it := rel.Iterator()
	for it.Next() {
		t := it.Tuple()
		for _, idx := range indices {
			if idx < len(t) {
				if eid, ok := t[idx].(datalog.Identity); ok && !seen[eid] {
					seen[eid] = true
					entities = append(entities, eid)
				}
			}
		}
	}
	it.Close()

	return entities
}

// collectOrBranchRequiredSymbols collects symbols that OR branches need from outer context
func collectOrBranchRequiredSymbols(branches [][]query.Clause) []query.Symbol {
	seen := make(map[query.Symbol]bool)
	var result []query.Symbol

	for _, branch := range branches {
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
				switch b := clause.Binding.(type) {
				case query.Symbol:
					if b != nil {
						branchProvides[b] = true
					}
				case query.TupleBinding:
					for _, v := range b.Variables {
						branchProvides[v] = true
					}
				}
			case *query.SubqueryPattern:
				// Subquery provides its binding variables
				for _, v := range clause.Binding.BoundVariables() {
					branchProvides[v] = true
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
	inputSyms := input.Symbols()
	inputSymSet := make(map[query.Symbol]bool)
	for _, sym := range inputSyms {
		inputSymSet[sym] = true
	}

	var actualJoinVars []query.Symbol
	for _, v := range joinVars {
		if inputSymSet[v] {
			actualJoinVars = append(actualJoinVars, v)
		}
	}

	if len(actualJoinVars) == 0 {
		return nil, fmt.Errorf("NOT clause variables not found in input relation")
	}

	// Get unique combinations of join variables from input
	uniqueCombos, err := getUniqueCombinations(input, actualJoinVars)
	if err != nil {
		return nil, fmt.Errorf("NOT input combinations failed: %w", err)
	}

	// Track which key combinations matched the inner clauses
	matchedKeys := NewTupleKeyMap()

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
			key := NewTupleKeyFull(combo)
			matchedKeys.Put(key, struct{}{})
		}
	}

	// Build join key symbol indices for input
	keyIndices := query.SymbolIndexTable(inputSyms, actualJoinVars)

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
		key := NewTupleKeyFull(keyVals)

		if !matchedKeys.Exists(key) {
			filtered = append(filtered, tuple)
		}
	}
	iter.Close()

	return NewMaterializedRelationWithOptions(inputSyms, filtered, e.options), nil
}

// filterWithNotJoinClause applies anti-join with explicit join vars to a single relation
func (e *DefaultQueryExecutor) filterWithNotJoinClause(ctx Context, clause *query.NotJoinClause, input Relation) (Relation, error) {
	if input == nil {
		return nil, nil
	}

	// Materialize input
	input = input.Materialize()
	inputSyms := input.Symbols()

	// Verify join vars exist in input
	inputSymSet := make(map[query.Symbol]bool)
	for _, sym := range inputSyms {
		inputSymSet[sym] = true
	}

	for _, v := range clause.JoinVars {
		if !inputSymSet[v] {
			return nil, fmt.Errorf("NOT-JOIN variable %s not found in input relation", v)
		}
	}

	// Get unique combinations of join variables
	uniqueCombos, err := getUniqueCombinations(input, clause.JoinVars)
	if err != nil {
		return nil, fmt.Errorf("NOT-JOIN input combinations failed: %w", err)
	}

	// Track matched keys
	matchedKeys := NewTupleKeyMap()

	for _, combo := range uniqueCombos {
		bindingRel := NewMaterializedRelationWithOptions(clause.JoinVars, []Tuple{combo}, e.options)

		innerResult, err := e.executeInnerClauses(ctx, clause.Clauses, bindingRel)
		if err != nil {
			return nil, fmt.Errorf("NOT-JOIN inner clause execution failed: %w", err)
		}

		// Count inner results via ForEach so a failed inner scan surfaces as an
		// error rather than looking like "no match" — which would wrongly
		// un-exclude this combo and silently corrupt the NOT-JOIN result.
		matched := false
		if innerResult != nil {
			count := 0
			if ferr := ForEach(innerResult, func(Tuple) error { count++; return nil }); ferr != nil {
				return nil, fmt.Errorf("NOT-JOIN inner clause execution failed: %w", ferr)
			}
			matched = count > 0
		}

		if matched {
			key := NewTupleKeyFull(combo)
			matchedKeys.Put(key, struct{}{})
		}
	}

	// Build key indices; join vars are validated present in inputSyms above.
	keyIndices := query.SymbolIndexTable(inputSyms, clause.JoinVars)

	// Filter
	var filtered []Tuple
	iter := input.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		keyVals := make(Tuple, len(keyIndices))
		for i, idx := range keyIndices {
			keyVals[i] = tuple[idx]
		}
		key := NewTupleKeyFull(keyVals)

		if !matchedKeys.Exists(key) {
			filtered = append(filtered, tuple)
		}
	}
	iter.Close()

	return NewMaterializedRelationWithOptions(inputSyms, filtered, e.options), nil
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
			patternQuery := &query.Query{Where: []query.Clause{c}}
			newRel, err := e.executePattern(ctx, patternQuery, c, groups)
			if err != nil {
				return nil, err
			}
			if newRel != nil {
				groups = replaceConsumedOrGroups(ctx, groups, newRel)
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

		case *query.NotClause:
			var err error
			groups, err = e.executeNotClause(ctx, c, groups)
			if err != nil {
				return nil, err
			}

		case *query.NotJoinClause:
			var err error
			groups, err = e.executeNotJoinClause(ctx, c, groups)
			if err != nil {
				return nil, err
			}

		case *query.OrClause:
			newRel, err := e.executeOrClause(ctx, c, groups)
			if err != nil {
				return nil, err
			}
			if newRel != nil {
				groups = append(groups, newRel)
			}
			groups = groups.Collapse(ctx)

		case *query.OrJoinClause:
			newRel, err := e.executeOrJoinClause(ctx, c, groups)
			if err != nil {
				return nil, err
			}
			if newRel != nil {
				groups = replaceConsumedOrGroups(ctx, groups, newRel)
			}
			groups = groups.Collapse(ctx)

		case *query.OrDefaultClause:
			newRel, err := e.executeOrDefaultClause(ctx, c, groups)
			if err != nil {
				return nil, err
			}
			if newRel != nil {
				groups = replaceConsumedOrGroups(ctx, groups, newRel)
			}
			groups = groups.Collapse(ctx)

		case *query.OrDefaultJoinClause:
			newRel, err := e.executeOrDefaultJoinClause(ctx, c, groups)
			if err != nil {
				return nil, err
			}
			if newRel != nil {
				groups = replaceConsumedOrGroups(ctx, groups, newRel)
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
