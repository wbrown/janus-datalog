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

// DefaultQueryExecutor implements QueryExecutor using the PatternMatcher interface.
// The executing query scope's environment (single-valued :in parameter values)
// rides the Context, not the executor: Execute is re-entrant across query
// scopes (phases, subqueries, or-branch bodies), and each scope binds its own
// environment at its boundary.
type DefaultQueryExecutor struct {
	matcher        PatternMatcher
	entityResolver EntityResolver
	options        ExecutorOptions
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
					entities, err := extractEntityIDs(g, g.Symbols())
					if err != nil {
						return nil, fmt.Errorf("prefetch entity extraction failed: %w", err)
					}
					if len(entities) > 50 {
						if collector := ctx.Collector(); collector != nil {
							collector.Add(annotations.Event{
								Name: annotations.PrefetchTrigger,
								Data: map[string]interface{}{
									"entity_count": len(entities),
									"symbols":      fmt.Sprintf("%v", g.Symbols()),
								},
							})
						}
						if err := prefetcher.PrefetchEntities(entities); err != nil {
							return nil, fmt.Errorf("entity prefetch failed: %w", err)
						}
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

	// A zero-symbol group is a constant-only verdict, not data. With one
	// empty tuple it is the join identity: once data groups exist it gates
	// nothing further, so it is absorbed here (unit × R = R). With zero
	// tuples it is the join zero: the whole result is empty (∅ × R = ∅).
	// Left in place, projection would silently drop it — losing a fail
	// verdict, or erroring aggregation on "disjoint" groups.
	//
	// A group whose symbols are ALL environment symbols is the same shape
	// one step up: the environment riding as a group. Any group carrying an
	// env symbol has been collapse-joined with the bound-input group, so an
	// all-env group holds at most the one env tuple — the join identity
	// decorated with constants. Absorb it identically: non-empty → drop,
	// empty → annihilate. Without this, the residual bound-input group left
	// after or-branches consumed its parameters trips disjoint-groups
	// aggregation errors.
	if len(groups) > 1 {
		env := ctx.Environment()
		var envSymbols []query.Symbol
		if env != nil {
			envSymbols = env.Symbols()
		}
		allEnvironment := func(rel Relation) bool {
			symbols := rel.Symbols()
			if len(symbols) == 0 || len(envSymbols) == 0 {
				return false
			}
			for _, sym := range symbols {
				if !query.ContainsSymbol(envSymbols, sym) {
					return false
				}
			}
			return true
		}
		dataGroups := make([]Relation, 0, len(groups))
		for _, rel := range groups {
			if len(rel.Symbols()) > 0 && !allEnvironment(rel) {
				dataGroups = append(dataGroups, rel)
				continue
			}
			verdict := rel.Materialize()
			if verdict.Size() == 0 {
				return []Relation{}, nil
			}
		}
		groups = dataGroups
	}

	// Constant :in bindings are environment, not data — except where the
	// find clause consumes one, which makes it result data. Render those
	// values into a relation group before aggregation/projection consume
	// the symbols: projection treats a missing symbol as empty, so any
	// later rendering would silently drop tuples.
	rendered, renderErr := e.renderConstantFindSymbols(ctx.Environment(), q, groups)
	if renderErr != nil {
		return nil, renderErr
	}
	groups = rendered

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

// renderConstantFindSymbols extends one relation group with find-consumed
// symbols that were resolved as constant :in bindings and are absent from
// every group. Constants are environment, not data — :find membership is the
// one place environment becomes result data, so the value is rendered into
// the relation at the find boundary, where variables, pull entity variables,
// and aggregate arguments are about to be consumed. Extension preserves set
// semantics: distinct tuples stay distinct when the added symbol takes the
// same value in every one of them.
func (e *DefaultQueryExecutor) renderConstantFindSymbols(env Relation, q *query.Query, groups []Relation) ([]Relation, error) {
	if env == nil || len(groups) == 0 {
		return groups, nil
	}
	envSymbols := env.Symbols()

	var consumed []query.Symbol
	for _, elem := range q.Find {
		switch f := elem.(type) {
		case query.FindVariable:
			consumed = append(consumed, f.Symbol)
		case query.FindAggregate:
			consumed = append(consumed, f.Arg)
		case query.FindPull:
			consumed = append(consumed, f.Variable)
		}
	}

	var missing []query.Symbol
	for _, sym := range consumed {
		if !query.ContainsSymbol(envSymbols, sym) {
			continue
		}
		present := false
		for _, rel := range groups {
			if query.ContainsSymbol(rel.Symbols(), sym) {
				present = true
				break
			}
		}
		if !present && !query.ContainsSymbol(missing, sym) {
			missing = append(missing, sym)
		}
	}
	if len(missing) == 0 {
		return groups, nil
	}

	// Extend exactly one group — the same symbol in two groups would read as
	// a join key it is not. Pick the group already carrying the most
	// find-consumed symbols so the extension doesn't manufacture a product
	// between otherwise-independent groups.
	target := 0
	best := -1
	for i, rel := range groups {
		count := 0
		for _, sym := range consumed {
			if query.ContainsSymbol(rel.Symbols(), sym) {
				count++
			}
		}
		if count > best {
			best = count
			target = i
		}
	}

	// The join of the group with the environment's missing symbols: the
	// symbol sets are disjoint by construction (missing = absent from every
	// group), so this is an N×1 decoration — every tuple gains the constant
	// values at the added positions, set semantics preserved.
	projected, err := env.Project(missing)
	if err != nil {
		return nil, fmt.Errorf("environment projection for find rendering failed: %w", err)
	}
	groups[target] = groups[target].Join(projected)
	return groups, nil
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

	result := replaceGroups(groups, orRelation.consumedGroups, relation)

	if collector := ctx.Collector(); collector != nil {
		collector.Add(annotations.Event{
			Name: annotations.OrOuterReplaced,
			Data: map[string]interface{}{
				"consumed_groups":  len(orRelation.consumedGroups),
				"remaining_groups": len(result),
			},
		})
	}
	return result
}

// replaceGroups substitutes the groups at the consumed indices with their
// single replacement relation, inserted at the first consumed position;
// every other group passes through in order. Out-of-range indices are
// ignored; with none in range the replacement appends.
func replaceGroups(groups Relations, consumed []int, replacement Relation) Relations {
	consumedSet := make(map[int]bool, len(consumed))
	insertAt := len(groups)
	for _, index := range consumed {
		if index < 0 || index >= len(groups) {
			continue
		}
		consumedSet[index] = true
		if index < insertAt {
			insertAt = index
		}
	}
	if len(consumedSet) == 0 {
		return append(groups, replacement)
	}

	result := make(Relations, 0, len(groups)-len(consumedSet)+1)
	inserted := false
	for index, group := range groups {
		if index == insertAt {
			result = append(result, replacement)
			inserted = true
		}
		if !consumedSet[index] {
			result = append(result, group)
		}
	}
	if !inserted {
		result = append(result, replacement)
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
		value, found, err := admitExpressionResult(expr.Function, result)
		if err != nil {
			return nil, err
		}
		if !found {
			// Absence with no groups: the expression produces no tuples.
			return []Relation{}, nil
		}

		// Handle both scalar and tuple bindings
		switch binding := expr.Binding.(type) {
		case query.TupleBinding:
			values, ok := value.([]interface{})
			if !ok {
				return nil, fmt.Errorf("tuple binding requires tuple result, got %T", value)
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
			tuples := []Tuple{{value}}
			return []Relation{NewMaterializedRelationWithOptions(symbols, tuples, e.options)}, nil

		default:
			return nil, fmt.Errorf("unsupported binding type: %T", expr.Binding)
		}
	}

	// Symbols the environment binds (single-valued :in parameters) need no
	// relation group: the environment relation binds into the operator.
	env := ctx.Environment()
	envSymbols, envTuple := environmentTuple(env)
	var unresolvedExprSyms []query.Symbol
	for _, sym := range requiredSyms {
		if !query.ContainsSymbol(envSymbols, sym) {
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
			evalBindings := make(map[query.Symbol]interface{}, len(envSymbols))
			for i := range envSymbols {
				evalBindings[envSymbols[i]] = envTuple[i]
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
			value, found, err := admitExpressionResult(expr.Function, result)
			if err != nil {
				return nil, err
			}

			// Determine binding symbols, and values when a binding was
			// produced. Absence still derives the symbols: the emptied
			// groups below keep the schema this expression provides.
			var bindingSyms []query.Symbol
			var bindingValues []interface{}
			switch binding := expr.Binding.(type) {
			case query.TupleBinding:
				bindingSyms = binding.Variables
				if found {
					values, ok := value.([]interface{})
					if !ok {
						return nil, fmt.Errorf("tuple binding requires tuple result, got %T", value)
					}
					if len(values) != len(binding.Variables) {
						return nil, fmt.Errorf("tuple mismatch: %d values, %d variables",
							len(values), len(binding.Variables))
					}
					bindingValues = values
				}
			case query.Symbol:
				if binding != nil {
					bindingSyms = []query.Symbol{binding}
					if found {
						bindingValues = []interface{}{value}
					}
				}
			}

			// For each binding symbol: bound positions unify (filter), new
			// symbols extend the tuple. alignBinding is the single home of
			// this semantics — shared with the per-tuple path.
			var resultRels []Relation
			for _, rel := range groups {
				align := alignBinding(rel.Symbols(), bindingSyms)
				// Absence against the single environment tuple drops every
				// tuple of every group — a universally-false filter. The
				// emptied relation keeps the aligned symbols so the phase
				// contract's Provides survives.
				if !found {
					resultRels = append(resultRels, NewMaterializedRelationWithOptions(align.symbols, nil, e.options))
					continue
				}
				// Pass-through tuples (no extension) must be copied out of
				// a workspace-reusing source; extension allocates fresh.
				needsCopy := rel.RequiresCopy()

				var outputTuples []Tuple
				iter := rel.Iterator()
				for iter.Next() {
					if out, ok := align.apply(iter.Tuple(), bindingValues); ok {
						if needsCopy && !align.extendsTuple() {
							out = copyTuple(out)
						}
						outputTuples = append(outputTuples, out)
					}
				}
				scanErr := iter.Error()
				if closeErr := iter.Close(); scanErr == nil {
					scanErr = closeErr
				}
				if scanErr != nil {
					return nil, scanErr
				}
				resultRels = append(resultRels, NewMaterializedRelationWithOptions(align.symbols, outputTuples, e.options))
			}
			return resultRels, nil
		}

		// Handle all-constants expression with no groups (e.g., get-some with scalar input only)
		// This occurs when all required symbols are constant-bindable and there are no data patterns.
		if len(unresolvedExprSyms) == 0 && len(groups) == 0 {
			evalBindings := make(map[query.Symbol]interface{}, len(envSymbols))
			for i := range envSymbols {
				evalBindings[envSymbols[i]] = envTuple[i]
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

			value, found, err := admitExpressionResult(expr.Function, result)
			if err != nil {
				return nil, err
			}
			if !found {
				// Absence: emit an empty result rather than a tuple.
				return []Relation{}, nil
			}

			// Create result relation based on binding type
			switch binding := expr.Binding.(type) {
			case query.TupleBinding:
				values, ok := value.([]interface{})
				if !ok {
					return nil, fmt.Errorf("tuple binding requires tuple result, got %T", value)
				}
				if len(values) != len(binding.Variables) {
					return nil, fmt.Errorf("tuple mismatch: %d values, %d variables",
						len(values), len(binding.Variables))
				}
				return []Relation{NewMaterializedRelationWithOptions(binding.Variables, []Tuple{values}, e.options)}, nil
			case query.Symbol:
				if binding != nil {
					return []Relation{NewMaterializedRelationWithOptions([]query.Symbol{binding}, []Tuple{{value}}, e.options)}, nil
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
	var err error
	if len(relevantRels) == 1 {
		// Single relation — evaluate directly, no join needed
		result, err = evaluateExpressionWithLookup(relevantRels[0], expr, lookup, env)
	} else {
		// Multiple disjoint relations — cross-join with expression evaluation
		// Uses BufferedIterator for inner re-iteration instead of Product()
		result, err = crossJoinWithExpression(relevantRels, expr, lookup, env, e.options)
	}
	if err != nil {
		return nil, err
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

	// Symbols the environment binds (single-valued :in parameters) need no
	// relation group: the environment relation binds into the operator.
	env := ctx.Environment()
	envSymbols, _ := environmentTuple(env)
	var unresolvedSyms []query.Symbol
	for _, sym := range requiredSyms {
		if !query.ContainsSymbol(envSymbols, sym) {
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
		if len(unresolvedSyms) > 0 {
			// The planner assigns predicates where their symbols are
			// Available; unresolved symbols no group provides means the
			// contract is broken upstream. Skipping would silently drop
			// the filter and report unfiltered tuples as the answer.
			return nil, fmt.Errorf("predicate %s requires symbols %v that no relation group provides",
				pred.String(), unresolvedSyms)
		}

		// Every required symbol is environment-bound: the environment alone
		// decides the predicate. Filter its single tuple once — one
		// evaluation, and a surviving tuple is a uniform pass. With no
		// environment (a symbol-free predicate), the zero-symbol unit is the
		// subject: one evaluation with no bindings, the same verdict shape.
		var lookup query.EntityLookup
		if lookupMatcher, ok := e.matcher.(EntityLookupMatcher); ok {
			lookup = entityLookupAdapter{lookupMatcher}
		}
		subject := env
		if subject == nil {
			subject = NewUnitRelation(e.options)
		}
		verdict, err := filterWithPredicateAndLookup(subject, pred, lookup, nil)
		if err != nil {
			return nil, err
		}
		passes := verdict.Size() == 1
		if !passes {
			// Uniform fail. With data groups, empty each one
			// schema-preserving so downstream clauses and Keep projection
			// see the declared symbols. With no groups, nothing can match:
			// signal it with empty groups — the same early-termination
			// convention every other annihilating clause uses.
			if len(groups) == 0 {
				return []Relation{}, nil
			}
			emptied := make([]Relation, len(groups))
			for i, rel := range groups {
				emptied[i] = NewMaterializedRelationWithProperties(rel.Symbols(), nil, rel.Options(), rel.Properties())
			}
			return emptied, nil
		}
		if len(groups) == 0 {
			// Uniform pass with no data groups: the verdict itself is the
			// relation — the zero-symbol unit (join identity, one empty
			// tuple). It renders constant :find symbols in consumer-only
			// queries and is absorbed at the find boundary once data
			// groups exist.
			return []Relation{NewMaterializedRelationWithOptions([]query.Symbol{}, []Tuple{{}}, e.options)}, nil
		}
		return groups, nil
	}

	// Check if this predicate might need database access (DatabaseFunctionPredicate)
	var lookup query.EntityLookup
	if lookupMatcher, ok := e.matcher.(EntityLookupMatcher); ok {
		lookup = entityLookupAdapter{lookupMatcher}
	}

	var result Relation
	var err error
	if len(relevantRels) == 1 {
		// Single relation — filter directly, no join needed
		result, err = filterWithPredicateAndLookup(relevantRels[0], pred, lookup, env)
	} else {
		// Multiple disjoint relations — theta-join with predicate filter
		// Uses BufferedIterator for inner re-iteration instead of Product()
		result, err = thetaJoinWithPredicate(relevantRels, pred, lookup, env, e.options)
	}
	if err != nil {
		return nil, err
	}

	// Return result + unchanged relations
	return append([]Relation{result}, otherRels...), nil
}

// executeSubquery executes a nested subquery
// Subqueries produce new relations from nested query execution
func (e *DefaultQueryExecutor) executeSubquery(ctx Context, subq *query.SubqueryPattern, groups []Relation) (result Relation, resultErr error) {
	// Record the per-input-combination execution path.
	if collector := ctx.Collector(); collector != nil {
		collector.Add(annotations.Event{
			Name: annotations.SubqueryExecutorPath,
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
		inputRelations, err := subqueryInputRelations(subq, nil, nil, e.options)
		if err != nil {
			return nil, fmt.Errorf("subquery input binding failed: %w", err)
		}
		// The subquery is its own query scope: its environment comes from its
		// own :in bindings, never the enclosing scope's (whose parameters an
		// inner name collision must not capture).
		innerCtx := ctx.WithEnvironment(environmentRelationFromInputs(subq.Query.In, inputRelations))
		nestedGroups, err := e.Execute(innerCtx, subq.Query, inputRelations)
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

	// The unique input combinations ARE a relation: the outer relation
	// projected onto the data input symbols (source markers are execution
	// context, not data). Projection's set semantics is the dedup — typed
	// value identity, never string rendering. With no data inputs the
	// zero-symbol unit is the one (empty) combination.
	dataSymbols := filterSourceSymbols(inputSymbols)
	var combos Relation
	if len(dataSymbols) == 0 {
		combos = NewUnitRelation(e.options)
	} else {
		projected, err := combinedRel.Project(dataSymbols)
		if err != nil {
			return nil, fmt.Errorf("subquery input projection failed: %w", err)
		}
		combos = projected
	}
	// Execute subquery for each combination — each tuple of the projected
	// relation. Projection over a product streams, so the combination count
	// is known only after the pass; the annotation is emitted once the
	// combinations have been consumed.
	var allResults []Relation
	combinationCount := 0
	combosRequireCopy := combos.RequiresCopy()

	comboIter := combos.Iterator()
	// A failed Close must not present a clean result: capture the Close error
	// on every path, without double-closing.
	defer func() {
		if closeErr := comboIter.Close(); closeErr != nil && resultErr == nil {
			result = nil
			resultErr = fmt.Errorf("subquery input combinations failed: %w", closeErr)
		}
	}()
	for comboIter.Next() {
		combo := comboIter.Tuple()
		// Each combination outlives its iteration (applyBindingForm carries it
		// into the bound result), so copy when the source iterator reuses its
		// tuple workspace.
		if combosRequireCopy {
			combo = copyTuple(combo)
		}
		combinationCount++
		// Create input relations for this combination
		inputRelations, err := subqueryInputRelations(subq, dataSymbols, combo, e.options)
		if err != nil {
			return nil, fmt.Errorf("subquery input binding failed: %w", err)
		}

		// One event per input relation, naming where it sits in the list.
		//
		// "relation.position", not "index": in this engine an index is one of the
		// eight physical orderings, and annotations.KeyIndex carries an
		// IndexType. A second meaning under the same key would reach
		// Database.Analyze, which prints Data[KeyIndex] for every event it traces
		// and would render this ordinal as though it named a run.
		if collector := ctx.Collector(); collector != nil {
			for i, rel := range inputRelations {
				collector.Add(annotations.Event{
					Name: annotations.SubqueryInputRelation,
					Data: map[string]interface{}{
						"relation.position": i,
						"symbols":           rel.Symbols(),
						"size":              rel.Size(),
					},
				})
			}
		}

		// Execute the nested query recursively using QueryExecutor. The
		// subquery is its own query scope: its environment comes from its own
		// :in bindings for this combination, never the enclosing scope's.
		innerCtx := ctx.WithEnvironment(environmentRelationFromInputs(subq.Query.In, inputRelations))
		nestedGroups, err := e.Execute(innerCtx, subq.Query, inputRelations)
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
		boundResult, err := applyBindingForm(nestedResult, subq.Binding, dataSymbols, combo)
		if err != nil {
			return nil, fmt.Errorf("binding form application failed: %w", err)
		}

		allResults = append(allResults, boundResult)
	}
	if err := comboIter.Error(); err != nil {
		return nil, fmt.Errorf("subquery input combinations failed: %w", err)
	}
	if collector := ctx.Collector(); collector != nil {
		collector.Add(annotations.Event{
			Name: annotations.SubqueryInputCombinations,
			Data: map[string]interface{}{
				"relation_groups":    len(materializedGroups),
				"product":            len(materializedGroups) > 1,
				"eager_materialized": false,
				"combination_count":  combinationCount,
			},
		})
	}

	// Combine all results
	if len(allResults) == 0 {
		// No results - return empty relation with appropriate symbols
		return NewMaterializedRelation(subq.Binding.BoundVariables(), []Tuple{}), nil
	}

	// Union all results (they should have the same schema)
	return combineSubqueryResultsSimple(allResults), nil
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
	// Inference forms unify on every shared free variable (the language
	// rule), so every branch free variable — including correlates consumed
	// only by a branch NOT or predicate — selects its binding group into
	// the outer relation.
	outerRel, consumed := e.findOuterRelation(branchFreeVariables(branches), groups)
	rel := NewOrFallbackRelation(e, ctx, branches, outerRel, e.options, false)
	rel.consumedGroups = consumed
	return rel, nil
}

// executeOrDefaultClause implements fallback semantics for or-default clauses:
// For each input tuple, try branches in order until one returns a result.
func (e *DefaultQueryExecutor) executeOrDefaultClause(ctx Context, clause *query.OrDefaultClause, groups Relations) (Relation, error) {
	// Inference form: outer-group selection by every branch free variable,
	// as in executeOrClauseCorrelatedUnion above.
	outerRel, consumed := e.findOuterRelation(branchFreeVariables(clause.Branches), groups)
	rel := NewOrFallbackRelation(e, ctx, clause.Branches, outerRel, e.options, true)
	rel.consumedGroups = consumed
	return rel, nil
}

// executeOrDefaultJoinClause implements fallback semantics for or-default-join
// clauses. The declared required vars are the per-group correlation keys and
// must be bound at entry; with none declared, the fallback decision is global.
func (e *DefaultQueryExecutor) executeOrDefaultJoinClause(ctx Context, clause *query.OrDefaultJoinClause, groups Relations) (Relation, error) {
	if err := clause.Validate(); err != nil {
		return nil, err
	}

	// Outer-group selection stays keyed on the correlates alone (the
	// declared RequiredVars — Validate forces branch externals into them):
	// OutputVars are branch-produced, not unification demands on the outer.
	// The canonical scope (Provides = OutputVars, Correlates = RequiredVars)
	// drives branch visibility, matching the declared interface exactly.
	scope := query.ScopeOf(clause)
	requiredSet := make(map[query.Symbol]bool, len(scope.Correlates))
	for _, v := range scope.Correlates {
		requiredSet[v] = true
	}

	outerRel, consumed := e.findOuterRelationBySymbols(requiredSet, groups)
	outerSyms := outerRel.Symbols()
	for _, v := range clause.RequiredVars {
		if !query.ContainsSymbol(outerSyms, v) {
			return nil, fmt.Errorf("or-default-join required variable %s is not bound", v)
		}
	}

	prefetched := false
	rel := NewOrFallbackRelation(e, ctx, clause.Branches, outerRel, e.options, true)
	rel.joinSyms = clause.RequiredVars
	rel.scope = &scope
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

// clausesNeedCorrelation recursively checks whether any clause in the list
// (or nested within it) is a correlated form: expressions, subqueries,
// predicates with inputs, and NOT/not-join — whose anti-join consumes outer
// bindings by definition (ScopeOf gives them Correlates). A branch containing
// any of these must evaluate per outer tuple; executing it blind turns the
// correlation existential and silently widens or narrows the branch.
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
		case *query.NotClause, *query.NotJoinClause:
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
	// The clause's canonical interface (Provides ∪ Correlates — the header
	// plus branch externals) selects the outer groups it unifies with, so a
	// correlate consumed only by a branch NOT/predicate pulls its binding
	// group into the outer relation instead of turning existential.
	scope := query.ScopeOf(clause)
	interfaceSet := make(map[query.Symbol]bool, len(scope.Provides)+len(scope.Correlates))
	for _, v := range scope.Provides {
		interfaceSet[v] = true
	}
	for _, v := range scope.Correlates {
		interfaceSet[v] = true
	}

	outerRel, consumed := e.findOuterRelationBySymbols(interfaceSet, groups)

	rel := NewOrFallbackRelation(e, ctx, clause.Branches, outerRel, e.options, false)
	rel.joinSyms = clause.JoinVars
	rel.scope = &scope
	rel.consumedGroups = consumed
	return rel, nil
}

// extractEntityIDs extracts datalog.Identity values from the specified symbol
// bindings of a materialized relation. Used to collect entity IDs for prefetch.
// A failed scan surfaces as an error and the caller fails the query: an error
// from the store indicates something wrong with the store, whether or not the
// operation that hit it was optional.
func extractEntityIDs(rel Relation, syms []query.Symbol) ([]datalog.Identity, error) {
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
		return nil, nil
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
	scanErr := it.Error()
	if closeErr := it.Close(); scanErr == nil {
		scanErr = closeErr
	}
	if scanErr != nil {
		return nil, scanErr
	}

	return entities, nil
}

// executeNotClause performs anti-join filtering on groups
func (e *DefaultQueryExecutor) executeNotClause(ctx Context, clause *query.NotClause, groups Relations) (Relations, error) {
	if len(groups) == 0 {
		return groups, nil
	}

	// Anti-join keys are the body's free variables — the same scope
	// interface the planner schedules on. filterWithNotClause intersects
	// them with the subject's schema: bound variables unify, the rest are
	// existential (Datomic's unification rule).
	joinVars := query.FreeVariables(clause.Clauses)
	if len(joinVars) == 0 {
		return nil, fmt.Errorf("not clause has no variables to join on")
	}

	// The groups carrying anti-join keys are the clause's subject; when the
	// keys span several disjoint groups, the clause's correlation is their
	// connector — join them once and anti-join the joined relation on the
	// full key set, the anti-join analog of a bridging predicate's
	// theta-join. Anti-joining each group separately instead wipes a
	// correlate-only group with a partially-bound body and turns the
	// entity side existential
	// (BUG_NOT_SPANNING_DISJOINT_GROUPS_APPLIED_PER_GROUP). Groups
	// carrying no key are unrelated to the clause and pass through.
	subject, consumed := e.findOuterRelation(joinVars, groups)
	if len(consumed) == 0 {
		return nil, fmt.Errorf("not clause variables are bound in no relation group")
	}

	filtered, err := e.filterWithNotClause(ctx, clause, subject, joinVars)
	if err != nil {
		return nil, err
	}
	return replaceGroups(groups, consumed, filtered), nil
}

// executeNotJoinClause performs anti-join with explicit join variables
func (e *DefaultQueryExecutor) executeNotJoinClause(ctx Context, clause *query.NotJoinClause, groups Relations) (Relations, error) {
	if len(groups) == 0 {
		return groups, nil
	}

	if len(clause.JoinVars) == 0 {
		return nil, fmt.Errorf("not-join clause has no join variables")
	}

	// Same subject selection and bridging as executeNotClause above, keyed
	// on the declared header. filterWithNotJoinClause classifies each
	// declared variable: subject-carried variables key the anti-join,
	// environment-bound variables constrain the body through its binding,
	// and a variable bound in neither domain stays a loud error.
	subject, consumed := e.findOuterRelation(clause.JoinVars, groups)
	if len(consumed) == 0 {
		return nil, fmt.Errorf("not-join header variables are bound in no relation group")
	}

	filtered, err := e.filterWithNotJoinClause(ctx, clause, subject)
	if err != nil {
		return nil, err
	}
	return replaceGroups(groups, consumed, filtered), nil
}

// notBodyBinding returns the binding symbols for a not/not-join body's
// per-combination evaluation: the anti-join keys extended with the
// environment's tuple, restricted to body-consumed symbols the keys don't
// already carry. The body is a clause scope, and the environment renders
// into its binding at the boundary — as at the top level, subquery entry,
// and or-branch evaluation. The extension tuple is a reference into the
// projected environment relation's own tuple, never a copy.
func notBodyBinding(ctx Context, bodyVars, keys []query.Symbol) ([]query.Symbol, Tuple, error) {
	env := ctx.Environment()
	if env == nil {
		return keys, nil, nil
	}
	var extra []query.Symbol
	for _, sym := range env.Symbols() {
		if query.ContainsSymbol(bodyVars, sym) && !query.ContainsSymbol(keys, sym) {
			extra = append(extra, sym)
		}
	}
	if len(extra) == 0 {
		return keys, nil, nil
	}
	projected, err := env.Project(extra)
	if err != nil {
		return nil, nil, fmt.Errorf("environment projection for anti-join body binding failed: %w", err)
	}
	bindingSyms := append(append([]query.Symbol(nil), keys...), extra...)
	return bindingSyms, projected.Get(0), nil
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
		return nil, fmt.Errorf("not clause variables are bound in no relation group")
	}

	// The not body is a clause scope: the environment joins into its
	// binding, exactly as the top-level, subquery, and or-branch boundaries
	// render it into theirs. Restricted to body-consumed symbols the
	// anti-join keys don't already carry.
	bindingSyms, envExt, err := notBodyBinding(ctx, joinVars, actualJoinVars)
	if err != nil {
		return nil, err
	}

	// Get unique combinations of join variables from input
	uniqueCombos, err := getUniqueCombinations(input, actualJoinVars)
	if err != nil {
		return nil, fmt.Errorf("not clause input combinations failed: %w", err)
	}

	// Track which key combinations matched the inner clauses
	matchedKeys := NewTupleKeyMap()

	// For each unique combination, execute inner clauses
	for _, combo := range uniqueCombos {
		// Create a single-tuple relation binding the combo values plus the
		// environment's tuple
		binding := combo
		if len(envExt) > 0 {
			binding = make(Tuple, 0, len(combo)+len(envExt))
			binding = append(binding, combo...)
			binding = append(binding, envExt...)
		}
		bindingRel := NewMaterializedRelationWithOptions(bindingSyms, []Tuple{binding}, e.options)

		// Execute inner clauses with this binding
		innerResult, err := e.executeInnerClauses(ctx, clause.Clauses, bindingRel)
		if err != nil {
			return nil, fmt.Errorf("not clause body execution failed: %w", err)
		}

		// Count inner results via ForEach so streaming inner relations
		// (Size() == -1) register as matches and a failed inner scan
		// surfaces as an error rather than looking like "no match" — which
		// would wrongly un-exclude this combo and silently corrupt the
		// anti-join result.
		matched := false
		if innerResult != nil {
			count := 0
			if ferr := ForEach(innerResult, func(Tuple) error { count++; return nil }); ferr != nil {
				return nil, fmt.Errorf("not clause body execution failed: %w", ferr)
			}
			matched = count > 0
		}
		if matched {
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
	scanErr := iter.Error()
	if closeErr := iter.Close(); scanErr == nil {
		scanErr = closeErr
	}
	if scanErr != nil {
		return nil, scanErr
	}

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

	// Classify each declared header variable by the mechanism that enforces
	// it. A symbol the body never mentions declares a correlation that does
	// not exist — rejected with the algebra compiler's own message, so both
	// planning modes agree (the or-join family enforces the mirror rule:
	// every declared header symbol must be bound by the branches).
	// Subject-carried variables discriminate the anti-join key.
	// Environment-bound variables constrain the body through its binding
	// (notBodyBinding below); their key contribution would be a constant
	// equal on both sides, so they stay out of the key entirely. A variable
	// in neither domain is unbound — a loud error, never a silent
	// existential.
	inputSymSet := make(map[query.Symbol]bool)
	for _, sym := range inputSyms {
		inputSymSet[sym] = true
	}
	env := ctx.Environment()
	bodyVars := query.FreeVariables(clause.Clauses)
	var keyVars []query.Symbol
	for _, v := range clause.JoinVars {
		if !query.ContainsSymbol(bodyVars, v) {
			return nil, fmt.Errorf("not-join header symbol %s is neither produced nor consumed by the body", v)
		}
		if inputSymSet[v] {
			keyVars = append(keyVars, v)
			continue
		}
		if env != nil && query.ContainsSymbol(env.Symbols(), v) {
			continue
		}
		return nil, fmt.Errorf("not-join variable %s is not bound in the subject relation", v)
	}
	if len(keyVars) == 0 {
		return nil, fmt.Errorf(
			"not-join header %v shares no variable with the subject relation; the anti-join must unify with the enclosing query through at least one data variable",
			clause.JoinVars,
		)
	}

	// The not-join body is a clause scope: the environment joins into its
	// binding alongside the declared header, as at every scope boundary.
	bindingSyms, envExt, err := notBodyBinding(ctx, bodyVars, keyVars)
	if err != nil {
		return nil, err
	}

	// Get unique combinations of the subject-carried key variables
	uniqueCombos, err := getUniqueCombinations(input, keyVars)
	if err != nil {
		return nil, fmt.Errorf("not-join input combinations failed: %w", err)
	}

	// Track matched keys
	matchedKeys := NewTupleKeyMap()

	for _, combo := range uniqueCombos {
		binding := combo
		if len(envExt) > 0 {
			binding = make(Tuple, 0, len(combo)+len(envExt))
			binding = append(binding, combo...)
			binding = append(binding, envExt...)
		}
		bindingRel := NewMaterializedRelationWithOptions(bindingSyms, []Tuple{binding}, e.options)

		innerResult, err := e.executeInnerClauses(ctx, clause.Clauses, bindingRel)
		if err != nil {
			return nil, fmt.Errorf("not-join inner clause execution failed: %w", err)
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

	// Build key indices over the subject-carried key variables, validated
	// present in inputSyms above.
	keyIndices := query.SymbolIndexTable(inputSyms, keyVars)

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
	scanErr := iter.Error()
	if closeErr := iter.Close(); scanErr == nil {
		scanErr = closeErr
	}
	if scanErr != nil {
		return nil, scanErr
	}

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
