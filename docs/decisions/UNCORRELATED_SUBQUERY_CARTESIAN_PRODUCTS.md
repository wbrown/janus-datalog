# Uncorrelated Subquery Cartesian Products

**Date**: October 2025
**Status**: Implemented (with future review needed)

## Context

During the implementation of QueryExecutor (Stage B), we encountered an architectural tension around uncorrelated subqueries:

```datalog
[:find ?name ?max-age
 :where [?e :person/name ?name]
        [(q [:find (max ?a)
             :where [?p :person/age ?a]]
            $) [[?max-age]]]]
```

This query creates a **disjoint relation scenario**:
- Pattern `[?e :person/name ?name]` produces relation with columns `[?e, ?name]`
- Subquery produces relation with columns `[?max-age]` (after filtering out `$`)
- These relations **share no columns** → cannot be joined
- Projecting `[:find ?name ?max-age]` requires both relations

## The Dilemma

The codebase has a stated policy against Cartesian products:

1. **Philosophy**: "Cartesian products not supported" - prevents accidental result explosions
2. **Safety**: `RelationCollapser` detects disjoint groups and returns error
3. **User guidance**: Forces queries to be explicit about cross-products

However, uncorrelated subqueries are:
- **Semantically valid** in Datalog
- **Useful** for queries like "show all items with the global average"
- **Expected** by users coming from Datomic/other Datalog systems
- **Already working** in the legacy executor

## Decision: Support via Product()

We chose to **support uncorrelated subqueries** by taking the Cartesian product when necessary:

### Implementation

In `query_executor.go`, when projecting across disjoint groups:

```go
// Check if :find symbols span multiple groups
if len(groups) > 1 {
    // ... determine if symbols are spread across groups ...

    // If symbols span multiple groups - need Cartesian product
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
```

### Why This Decision

1. **Compatibility**: Matches legacy executor behavior
2. **User expectations**: Uncorrelated subqueries should work
3. **Explicitness**: The query syntax makes it clear (database `$` is passed, not variables)
4. **Limited scope**: Only applies when projection requires it
5. **Pragmatic**: Uncorrelated subqueries typically produce small results (aggregations)

## Implications

### What This Means

1. **Cartesian products ARE supported** in specific cases:
   - When uncorrelated subqueries create disjoint groups
   - When user explicitly requests Product() via API

2. **Cartesian products are NOT supported** in other cases:
   - Disjoint patterns without bridging expressions → error
   - Prevents accidental explosions from malformed queries

3. **Performance characteristics**:
   - Uncorrelated subqueries usually return 1 row (aggregations like `max`, `min`)
   - Product with 1-row relation is essentially a broadcast
   - Not a performance concern in typical usage

### Edge Cases to Watch

**Large uncorrelated subquery results**:
```datalog
[:find ?name ?other-name
 :where [?e :person/name ?name]
        [(q [:find ?n :where [?p :person/name ?n]] $) [[?other-name]]]]
```

This creates a Cartesian product of all names × all names. Currently allowed but could explode.

**Potential future safeguards**:
- Size limit on Product() operations
- Warning when Product() produces > N tuples
- Query planner hint to detect these cases

## Future Review

This decision should be revisited when:

1. **Statistics available**: If we add cardinality estimation, we could warn about large products
2. **User feedback**: If users hit unexpected performance issues with uncorrelated subqueries
3. **Stage C planner**: AST-oriented planner might have better detection of these patterns

## Related Files

- `datalog/executor/query_executor.go`: Product() logic for projection
- `datalog/executor/subquery.go`: `$` filtering in `applyBindingForm()`
- `datalog/executor/queryexecutor_subquery_projection_test.go`: Test cases

## Decision Rationale Summary

**Chosen**: Support uncorrelated subqueries via Product()
**Alternative considered**: Error on all Cartesian products (too restrictive)
**Trade-off**: Consistency and usability over purity of "no Cartesian products" policy
**Mitigation**: Typical use cases (aggregations) produce small intermediate results
