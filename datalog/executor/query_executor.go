package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/internal/errjoin"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// prefetchEntityThreshold is the entity count a relation must exceed before
// its entities are prefetched into the EA cache under EnableEntityPrefetch:
// the one threshold every trigger reads, the executor's after the first data
// pattern and a fallback's per window (Rule 6 in docs/ALGEBRA.md,
// docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.10).
const prefetchEntityThreshold = 50

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
	Execute(ctx *Context, q *query.Query, inputs []Relation) ([]Relation, error)
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

	// executor is the executor whose plan this one runs clauses for. An
	// invocation clause runs a fixpoint through it, and a form plans its
	// scopes through it.
	executor *Executor

	// plan is the plan whose phases this executor runs the clauses of. An
	// invocation clause reads what the plan planned for it
	// (planner.RealizedPlan.Evaluations).
	plan *planner.RealizedPlan
}

// newQueryExecutor builds a clause executor for tests of the executor's
// internals, over an executor on the default planner profile carrying the
// options' handler. A query it is given runs clause by clause in the written
// order, bypassing the planner. A form inside those clauses plans each of its
// scopes through the executor's planner, and an invocation clause inside a
// scope reads the scope's plan. An invocation clause at the top level of the
// query has no plan to read its evaluation from; a test that runs one plans
// the query. End-to-end and integration tests use NewExecutor or
// NewExecutorWithOptions, which plan the query.
func newQueryExecutor(matcher PatternMatcher, resolver EntityResolver, options ExecutorOptions) *DefaultQueryExecutor {
	plannerOptions := defaultPlannerOptions()
	plannerOptions.Handler = options.Handler
	executor := &Executor{
		matcher:        matcher,
		entityResolver: resolver,
		planner:        planner.CreatePlanner(nil, plannerOptions),
		options:        options,
		unit:           NewUnitRelation(options),
	}
	return executor.queryExecutor(nil)
}

// Execute implements QueryExecutor interface
// Executes clauses progressively with collapse after each step
// Every production caller is a phase-loop driver, so this span is reported as
// phase/begin and phase/complete by the loop that owns it — with the phase index
// and a real tuple count. It does not also report itself under the query family's
// names.
func (e *DefaultQueryExecutor) Execute(ctx *Context, q *query.Query, inputs []Relation) ([]Relation, error) {
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
			if fusedCount == 0 && streamsThrough(c.Symbols(), groups) {
				var err error
				groups, err = e.executeBoundPattern(ctx, c, groups)
				if err != nil {
					return nil, fmt.Errorf("clause %d (pattern) failed: %w", i, err)
				}
			} else if fusedCount == 0 {
				patternQuery := &query.Query{
					Find:  q.Find,
					In:    q.In,
					Where: []query.Clause{c},
				}
				if len(q.Where) == 1 {
					patternQuery.OrderBy = q.OrderBy
					patternQuery.Limit = q.Limit
				}
				newRel, err := e.executePattern(patternQuery, c, groups)
				if err != nil {
					return nil, fmt.Errorf("clause %d (pattern) failed: %w", i, err)
				}
				if newRel != nil {
					groups = append(groups, newRel)
				}
			}
			groups = Relations(reportCollapse(e.options.Handler, []Relation(groups), func() []Relation {
				return []Relation(groups.Collapse(e.options.Handler))
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
					if len(entities) > prefetchEntityThreshold {
						if e.options.Handler != nil {
							e.options.Handler(annotations.Event{
								Name: annotations.PrefetchTrigger,
								Data: map[string]interface{}{
									"entity_count": len(entities),
									"symbols":      g.Symbols(),
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
			groups = Relations(reportCollapse(e.options.Handler, []Relation(groups), func() []Relation {
				return []Relation(groups.Collapse(e.options.Handler))
			}))

		case query.Predicate:
			var err error
			groups, err = e.executePredicate(ctx, c, groups)
			if err != nil {
				return nil, fmt.Errorf("clause %d (predicate) failed: %w", i, err)
			}

		case *query.SubqueryPattern:
			var err error
			groups, err = e.executeSubquery(ctx, c, groups)
			if err != nil {
				return nil, fmt.Errorf("clause %d (subquery) failed: %w", i, err)
			}
			groups = Relations(reportCollapse(e.options.Handler, []Relation(groups), func() []Relation {
				return []Relation(groups.Collapse(e.options.Handler))
			}))

		case *query.RuleInvocation:
			// An invocation deriving its predicate at the clause over a
			// streaming group binding an argument is read in windows, the
			// derivation seeded per window. An invocation of a predicate the
			// scope holds reads the held relation, and one a group held in
			// memory seeds through its projection. A rename of a held
			// predicate whose set the evaluation keeps joins into the first
			// group sharing a symbol with it, through the set's index, the
			// group on the left as the collapse joins it, and nothing is
			// drained (docs/proposals/A_HELD_RELATION_KEEPS_ITS_JOIN_INDEX_PROOF.md,
			// T3); every other result joins at the collapse.
			if _, held := ctx.Derived(c.Name); !held && streamsThrough(c.Variables(), groups) {
				var err error
				groups, err = e.executeBoundInvocation(ctx, c, groups)
				if err != nil {
					return nil, fmt.Errorf("clause %d (rule invocation) failed: %w", i, err)
				}
			} else {
				newRel, set, err := e.executeRuleInvocation(ctx, c, groups)
				if err != nil {
					return nil, fmt.Errorf("clause %d (rule invocation) failed: %w", i, err)
				}
				target := -1
				for j := 0; set != nil && j < len(groups); j++ {
					if hasSharedSymbols(groups[j], newRel) {
						target = j
						break
					}
				}
				if target < 0 {
					groups = append(groups, newRel)
				} else {
					group := groups[target]
					common := CommonSymbols(group, newRel)
					groups = append(Relations(nil), groups...)
					groups[target] = reportJoin(e.options.Handler, group, newRel, func() Relation {
						return hashJoinWithSet(group, newRel, common, e.options, set)
					})
				}
			}
			groups = Relations(reportCollapse(e.options.Handler, []Relation(groups), func() []Relation {
				return []Relation(groups.Collapse(e.options.Handler))
			}))

		case *query.OrJoinClause:
			var err error
			groups, err = e.executeOrJoinClause(ctx, c, groups)
			if err != nil {
				return nil, fmt.Errorf("clause %d (or-join) failed: %w", i, err)
			}
			groups = Relations(reportCollapse(e.options.Handler, []Relation(groups), func() []Relation {
				return []Relation(groups.Collapse(e.options.Handler))
			}))

		case *query.OrDefaultJoinClause:
			var err error
			groups, err = e.executeOrDefaultJoinClause(ctx, c, groups)
			if err != nil {
				return nil, fmt.Errorf("clause %d (or-default-join) failed: %w", i, err)
			}
			groups = Relations(reportCollapse(e.options.Handler, []Relation(groups), func() []Relation {
				return []Relation(groups.Collapse(e.options.Handler))
			}))

		case *query.NotJoinClause:
			var err error
			groups, err = e.executeNotJoinClause(ctx, c, groups)
			if err != nil {
				return nil, fmt.Errorf("clause %d (not-join) failed: %w", i, err)
			}

		default:
			// A headerless form reaches here only when preparation was
			// skipped: every or, not and or-default is a header form by
			// the time a query is planned.
			return nil, fmt.Errorf("unsupported clause type: %T", clause)
		}

		// Early termination on empty
		if len(groups) == 0 {
			return []Relation{}, nil
		}
	}

	// With no find the groups are returned as they are, the clause-level
	// result: the phase pipeline applies the verdict a fragment finding
	// nothing yields, and the executor's tests read a form's groups here.
	if len(q.Find) == 0 {
		return groups, nil
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
		// Under an aggregating find every group is a factor of the relation
		// the aggregate runs over, the groups the find does not read included
		// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §3): a group with
		// no tuple empties the product, and one with tuples multiplies it.
		if len(groups) == 0 {
			return []Relation{}, nil
		}
		result := ExecuteAggregations(Relations(groups).Product(), q.Find)
		if materialized, ok := result.(*MaterializedRelation); ok && materialized.err != nil {
			return nil, materialized.err
		}
		return []Relation{result}, nil

	} else {
		// Under a projection find the boundary keeps the groups holding the
		// symbols the find consumes, one of them or their product, and takes
		// the existence verdict over every other group: a group with none is
		// the join zero and empties the result (∅ × R = ∅), and one with a
		// tuple is the join identity and gates nothing further (unit × R = R).
		// A zero-symbol verdict, a group of environment symbols the find does
		// not consume, and a pattern's group sharing no symbol with the rest
		// are such groups.
		consumed := consumedFindSymbols(q.Find)
		holders := make(Relations, 0, len(groups))
		var unread []Relation
		for _, group := range groups {
			if containsAny(group.Symbols(), consumed) {
				holders = append(holders, group)
				continue
			}
			unread = append(unread, group)
		}
		verdict, verdictErr := existenceVerdict(unread, holders, e.executor.unit)
		if verdictErr != nil {
			return nil, verdictErr
		}
		if len(verdict) == 0 {
			return []Relation{}, nil
		}
		groups = holders

		// Simple projection to :find symbols. Pull find elements project
		// their entity variable like any other symbol (Identity values):
		// pull rendering is result presentation and happens at the result
		// boundary in ExecuteRealized, after sort/strip/limit — never
		// inside relational flow. See
		// docs/bugs/resolved/BUG_PULL_WITH_ORDER_BY_PANICS.md.
		findSymbols := query.FindSymbols(q.Find)

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

// existenceVerdict is the verdict over groups a find does not read: unit,
// the executor's relation of one tuple over no symbol, when each group has a
// tuple, nothing when one has none, and a group's failure as its error.
// The find boundary takes it over the groups the find does not consume, the
// groups it consumes held, and a phase whose fragment finds nothing takes it
// over every group, the schema a phase providing nothing declares, none held.
//
// Every group is read to its first tuple, its end, or its failure before
// the verdict is decided. A group with no tuple holds at no assignment, so
// the verdict is empty and no group's failure is raised. Otherwise a group's
// failure is at an assignment where every other clause holds, and the first
// is the verdict's error. A group whose reading evaluates a function is read
// past its first tuple only once every group has shown one: then each of its
// tuples meets a tuple of every other group, and the group is read to its
// end to raise a failure on any of them
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, T11).
//
// The held groups are the ones the caller reads after the verdict, so the
// verdict reads them only when it has a failure to raise, and then the
// caller reads none. A held group with no tuple holds at no assignment, so
// the verdict is empty and the failure is not raised.
func existenceVerdict(groups, held []Relation, unit *MaterializedRelation) ([]Relation, error) {
	// The iterators of the groups whose reading evaluates a function, each
	// past its first tuple, and the first failure a group answered in place of
	// a first tuple.
	var evaluating []Iterator
	var failure error
	closeEvaluating := func(from int) error {
		var err error
		for _, it := range evaluating[from:] {
			completeCache(it)
			err = errjoin.Join(err, it.Close())
		}
		return err
	}
	for _, group := range groups {
		var has bool
		var err error
		if group.Properties().Evaluates {
			it := group.Iterator()
			if it.Next() {
				evaluating = append(evaluating, it)
				continue
			}
			err = errjoin.Join(it.Error(), it.Close())
		} else {
			has, err = hasTuple(group)
		}
		if err != nil {
			if failure == nil {
				failure = err
			}
			continue
		}
		if !has {
			if closeErr := closeEvaluating(0); closeErr != nil {
				return nil, closeErr
			}
			return []Relation{}, nil
		}
	}
	if failure != nil {
		failure = errjoin.Join(failure, closeEvaluating(0))
	} else {
		for i, it := range evaluating {
			for it.Next() {
			}
			if err := errjoin.Join(it.Error(), it.Close()); err != nil {
				failure = errjoin.Join(err, closeEvaluating(i+1))
				break
			}
		}
	}
	if failure == nil {
		return []Relation{unit}, nil
	}
	for _, group := range held {
		has, err := hasTuple(group)
		if err == nil && !has {
			return []Relation{}, nil
		}
	}
	return nil, failure
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
	consumed := consumedFindSymbols(q.Find)

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
	return e.matcher.Match(q, bindings)
}

type attributeFetchSpec struct {
	attr         datalog.Keyword
	output       query.Symbol
	expected     interface{}
	isConstraint bool
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

// replaceConsumedGroups puts a form's result in the place of the groups its
// subject was joined from, consumed, so the collapse that follows joins
// nothing the result already carries, and reports the replacement through
// handler. A result whose subject consumed no group, the unit relation's,
// is appended as a new group, with no report.
func replaceConsumedGroups(groups Relations, consumed []int, relation Relation, handler annotations.Handler) Relations {
	if len(consumed) == 0 {
		return append(groups, relation)
	}
	result := replaceGroups(groups, consumed, relation)
	if handler != nil {
		handler(annotations.Event{
			Name: annotations.OrOuterReplaced,
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
	ctx *Context,
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
	// The bundle reads its input whole at the clause, so a group whose reading
	// can fail is left to the pattern's windowed route, which pulls none of it
	// where the pattern alone has no match
	// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, T9).
	if groups[entityGroup].Properties().Evaluates {
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
		if !ok || !fusable.CanFuseAttributeFetch(datalog.SymDollar, attr) {
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
	iterErr = errjoin.Join(iterErr, it.Close())
	if iterErr != nil {
		return nil, 0, iterErr
	}

	if e.options.Handler != nil {
		for i, fetch := range fetches {
			eventName := annotations.PatternFusedFetch
			if fetch.isConstraint {
				eventName = annotations.PatternFusedConstraint
			}
			e.options.Handler(annotations.Event{
				Name: eventName,
				Data: map[string]interface{}{
					"attr": fetch.attr,
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
	// The bundle answers an input tuple only where its entity is an identity.
	if !query.ContainsSymbol(properties.Identities, entityVar.Name) {
		properties.Identities = append(properties.Identities, entityVar.Name)
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
func (e *DefaultQueryExecutor) executeExpression(ctx *Context, expr *query.Expression, groups []Relation) ([]Relation, error) {
	// Find relations with any required symbols
	var relevantRels []Relation
	var otherRels []Relation

	requiredSyms := expr.Function.RequiredSymbols()

	// Symbols the environment binds (single-valued :in parameters) need no
	// relation group: the environment relation binds into the operator.
	env := ctx.Environment()
	envSymbols, _ := environmentTuple(env)
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

	if len(relevantRels) == 0 && len(unresolvedExprSyms) > 0 {
		// Skip expression if no relevant relations and expression needs symbols
		return groups, nil
	}

	// A database function evaluates through the lookup for the source it names.
	var lookup query.EntityLookup
	if dbFunc, ok := expr.Function.(query.DatabaseFunction); ok {
		var lookupErr error
		if lookup, lookupErr = e.entityLookup(dbFunc); lookupErr != nil {
			return nil, lookupErr
		}
	}

	// An expression whose symbols no group carries, a ground expression or
	// one the environment alone binds, extends every group, and stands over
	// the unit when there is none. It evaluates as each result is pulled, so a
	// group with no tuple never evaluates it, and a failure is decided where
	// the result is read, beside the other clauses
	// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, T8 and the failure
	// contract).
	if len(relevantRels) == 0 {
		if len(groups) == 0 {
			return []Relation{evaluateExpressionWithLookup(e.executor.unit, expr, lookup, env)}, nil
		}
		extended := make([]Relation, len(groups))
		for i, rel := range groups {
			extended[i] = evaluateExpressionWithLookup(rel, expr, lookup, env)
		}
		return extended, nil
	}

	var result Relation
	var err error
	if len(relevantRels) == 1 {
		// Single relation — evaluate directly, no join needed
		result = evaluateExpressionWithLookup(relevantRels[0], expr, lookup, env)
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

// entityLookup is the lookup f evaluates through: the lookup of the source f
// names, which the matcher gives. A matcher that cannot look up, and a source
// it cannot give, are errors.
func (e *DefaultQueryExecutor) entityLookup(f query.DatabaseFunction) (query.EntityLookup, error) {
	matcher, ok := e.matcher.(EntityLookupMatcher)
	if !ok {
		return nil, fmt.Errorf("%s requires database lookup but matcher %T doesn't support it", f, e.matcher)
	}
	source, err := matcher.LookupSource(f.ReadsFrom())
	if err != nil {
		return nil, err
	}
	return source, nil
}

// executePredicate filters relations using a predicate
// Predicates TRANSFORM groups - may use Product() for multi-relation predicates
func (e *DefaultQueryExecutor) executePredicate(ctx *Context, pred query.Predicate, groups []Relation) ([]Relation, error) {
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
		// decides the predicate. Filter the zero-symbol unit with the
		// environment bound in — one evaluation, and a surviving tuple is a
		// uniform pass. A verdict that can fail as it is read joins the groups
		// as a group of no symbol, and so does a failure the evaluation met,
		// held: the find boundary reads such a group for its existence, beside
		// every other clause (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, the
		// failure contract).
		var lookup query.EntityLookup
		if dbPred, ok := pred.(*query.DatabaseFunctionPredicate); ok {
			var lookupErr error
			if lookup, lookupErr = e.entityLookup(dbPred.Function); lookupErr != nil {
				return nil, lookupErr
			}
		}
		verdict, err := filterWithPredicateAndLookup(e.executor.unit, pred, lookup, env)
		if err != nil {
			failed := newMaterializedRelationFromSet([]query.Symbol{}, nil, e.options, RelationProperties{})
			failed.err = err
			return append(groups[:len(groups):len(groups)], failed), nil
		}
		if verdict.Properties().Evaluates {
			return append(groups[:len(groups):len(groups)], verdict), nil
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

	// A database-function predicate evaluates through the lookup for the
	// source its function names.
	var lookup query.EntityLookup
	if dbPred, ok := pred.(*query.DatabaseFunctionPredicate); ok {
		var lookupErr error
		if lookup, lookupErr = e.entityLookup(dbPred.Function); lookupErr != nil {
			return nil, lookupErr
		}
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

// executeSubquery runs the subquery composed over the modules: the inner
// query is one body under the call's substitution, framed over its data
// inputs with the inner :find as written, aggregates included, and the
// subject is the groups holding an argument's symbol or a binding symbol,
// joined into one, or the unit relation when none holds one. The result, a
// streaming relation over the subject's symbols and the binding symbols the
// subject lacks, yields each subject tuple joined with the body's answers for
// its combination, and stands in the place of the groups it consumed. A
// constant argument and an argument the environment alone binds ride the
// frames' environment relation under the inner symbol, so every combination
// carries the value; a collection input's whole value rides the required run
// under its holder, query.ScopeInput, and the scope's clauses open with the
// binding of its members from the holder, so the members join the body as a
// group and the inner find aggregates over them per argument value; a
// binding symbol the subject or the environment binds is a carried output,
// joined after the probe and never bound into the inner query. An argument
// nothing binds and a refusal of the body's plan are the clause's errors,
// and a failure of the body's scan is the result's, surfacing when it is
// driven.
func (e *DefaultQueryExecutor) executeSubquery(ctx *Context, subq *query.SubqueryPattern, groups Relations) (Relations, error) {
	bound := subq.Binding.BoundVariables()

	// The call's frame, read from its text through the planner's one reading
	// of it: the inner symbols, the clauses under the call's sources, the
	// substitution and the enclosing variable each inner symbol stands for.
	frame, err := planner.ReadSubqueryFrame(subq)
	if err != nil {
		return nil, err
	}

	// The subject: the groups holding an argument's symbol or a binding
	// symbol.
	var needed []query.Symbol
	for _, sym := range frame.StandsFor {
		if sym != nil && !query.ContainsSymbol(needed, sym) {
			needed = append(needed, sym)
		}
	}
	for _, sym := range bound {
		if !query.ContainsSymbol(needed, sym) {
			needed = append(needed, sym)
		}
	}
	subject, consumed := e.findOuterRelation(needed, groups)
	outerSyms := subject.Symbols()

	// The key: the subject's symbols an argument or the binding names, in
	// the subject's order.
	var key []query.Symbol
	for _, outerSym := range outerSyms {
		if query.ContainsSymbol(needed, outerSym) {
			key = append(key, outerSym)
		}
	}

	// The frames' environment relation: one tuple over the inner symbols the
	// subject lacks, a constant argument's value and the enclosing
	// environment's value of an argument it alone binds.
	var enclosingSyms []query.Symbol
	var enclosing Tuple
	if env := ctx.Environment(); env != nil {
		enclosingSyms, enclosing = environmentTuple(env)
	}
	var environmentSyms []query.Symbol
	var environmentValues Tuple
	for position, sym := range frame.Inner {
		if sym == nil {
			continue
		}
		switch arg := subq.Inputs[position].(type) {
		case query.Constant:
			environmentSyms = append(environmentSyms, sym)
			environmentValues = append(environmentValues, arg.Value)
		case query.Variable:
			if query.ContainsSymbol(outerSyms, arg.Name) {
				continue
			}
			pos := query.SymbolIndex(enclosingSyms, arg.Name)
			if pos < 0 {
				return nil, fmt.Errorf("subquery input %s is not bound in the enclosing scope", arg.Name)
			}
			environmentSyms = append(environmentSyms, sym)
			environmentValues = append(environmentValues, enclosing[pos])
		default:
			return nil, fmt.Errorf("unsupported subquery input element %T", arg)
		}
	}
	var environment Relation
	if len(environmentSyms) > 0 {
		environment = NewMaterializedRelation(environmentSyms, []Tuple{environmentValues})
	}

	// The carried outputs: the binding symbols the subject or the environment
	// binds, joined with the body's answer after the probe.
	var carried []carriedOutput
	for _, sym := range bound {
		outerPos := query.SymbolIndex(outerSyms, sym)
		envPos := query.SymbolIndex(enclosingSyms, sym)
		if outerPos < 0 && envPos < 0 {
			continue
		}
		output := carriedOutput{symbol: sym, outerPos: outerPos}
		if outerPos < 0 {
			output.envValue = enclosing[envPos]
		}
		carried = append(carried, output)
	}

	// The result's symbols: the subject's, then the binding symbols the
	// subject lacks. The subject's keys survive when the body answers at most
	// one tuple per subject tuple, every plain find variable a binding symbol
	// the subject carries and the rest aggregates.
	symbols := append([]query.Symbol(nil), outerSyms...)
	for _, sym := range bound {
		if !query.ContainsSymbol(outerSyms, sym) {
			symbols = append(symbols, sym)
		}
	}
	carriedSet := make(map[query.Symbol]bool, len(outerSyms))
	for _, sym := range outerSyms {
		carriedSet[sym] = true
	}
	atMostOne := subqueryAnswersAtMostOne(subq.Query, bound, carriedSet)
	properties := orProperties(subject.Properties(), bound, nil, symbols, atMostOne)
	// The body and the :find's folds run per window as the result is pulled.
	properties.Evaluates = properties.Evaluates || canFailOnAValue([]query.Clause{subq}, ctx.Rules())

	f := framing{
		form:        "subquery",
		subject:     subject,
		required:    frame.Given,
		find:        frame.Find,
		given:       frame.Given,
		standsFor:   frame.StandsFor,
		key:         key,
		environment: environment,
		subst:       frame.Subst,
	}
	plans, err := frameBranches(e, ctx, [][]query.Clause{frame.Clauses}, f)
	if err != nil {
		return nil, err
	}
	it := &subqueryIterator{
		windowedIterator: newWindowedIterator(e, ctx, f, plans, e.options),
		matchEmit:        matchEmit{outerSyms: outerSyms, outputSyms: symbols, carried: carried},
	}
	it.policy = formPolicy{answer: it.answer}
	result := NewStreamingRelationWithProperties(symbols, it, e.options, properties)
	return replaceConsumedGroups(groups, consumed, result, e.options.Handler), nil
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
func applyFindPulls(
	matcher PatternMatcher,
	entityResolver EntityResolver,
	rel Relation,
	find []query.FindElement,
) (Relation, error) {
	symbols := rel.Symbols()
	type pullRequest struct {
		symbolIndex int
		pull        query.FindPull
	}
	// A find element renders at its own position: the boundary projects one
	// position per element in the find's order, so the k-th element naming a
	// variable reads the k-th position of the result carrying it. A pull
	// whose variable the result lacks is the query's error.
	var requests []pullRequest
	occurrence := make(map[query.Symbol]int, len(find))
	for _, elem := range find {
		var variable query.Symbol
		switch e := elem.(type) {
		case query.FindVariable:
			variable = e.Symbol
		case query.FindPull:
			variable = e.Variable
		default:
			continue
		}
		k := occurrence[variable]
		occurrence[variable]++
		pull, ok := elem.(query.FindPull)
		if !ok {
			continue
		}
		_, positions, err := query.SymbolPositions(symbols, []query.Symbol{variable})
		if err != nil {
			return nil, fmt.Errorf("pull %s: %w", pull, err)
		}
		requests = append(requests, pullRequest{symbolIndex: positions[0][k], pull: pull})
	}
	if len(requests) == 0 {
		return rel, nil
	}

	// NewPullContext(nil) is BasePullContext, so one construction covers both
	// the annotated and the annotations-off case.
	puller := NewPullExecutorWithHandler(matcher, entityResolver, rel.Options().Handler)

	var resultTuples []Tuple
	it := rel.Iterator()
	for it.Next() {
		tuple := it.Tuple()
		newTuple := make(Tuple, len(tuple))
		copy(newTuple, tuple)
		resultTuples = append(resultTuples, newTuple)
	}
	if err := errjoin.Join(it.Error(), it.Close()); err != nil {
		return nil, err
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

// findOuterRelation is a form's subject: the groups holding any of the needed
// symbols joined into one relation, and the unit relation when none does.
// Several groups join by a fold of the hash join under their common symbols,
// none for groups sharing no symbol, whose join is their product. Each later
// group is the build side, held in the join index under its key, and the
// join so far is the probe side streaming through it, whatever the executor's
// join option, so each group is opened once, the subject reports no size, and
// the fold holds the later groups and never the join so far
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.2).
func (e *DefaultQueryExecutor) findOuterRelation(neededSymbols []query.Symbol, groups Relations) (Relation, []int) {
	if len(groups) == 0 || len(neededSymbols) == 0 {
		return e.executor.unit, nil
	}

	var consumed []int
	for i, rel := range groups {
		if containsAny(rel.Symbols(), neededSymbols) {
			consumed = append(consumed, i)
		}
	}
	if len(consumed) == 0 {
		return e.executor.unit, nil
	}
	if len(consumed) == 1 {
		return groups[consumed[0]], consumed
	}
	streaming := e.options
	streaming.EnableStreamingJoins = true
	result := groups[consumed[0]]
	for _, index := range consumed[1:] {
		held := groups[index]
		result = HashJoinWithOptions(held, result, CommonSymbols(held, result), streaming)
	}
	return result, consumed
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
	if scanErr := errjoin.Join(it.Error(), it.Close()); scanErr != nil {
		return nil, scanErr
	}

	return entities, nil
}
