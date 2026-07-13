# OR Clause Execution Analysis

This document describes the execution lifecycle of OR clauses in the Datalog engine, based on analysis of the bug reported in `datalog/storage/or_clause_bug_test.go`.

## The Bug

**Query without OR** (works correctly):
```clojure
[:find ?t ?type
 :where
 [?t :task/status :status/complete]
 [?t :task/type ?type]
 [?t :task/type :type/a]]
```
Returns 1 result: `[task-1 :type/a]`

**Query with OR** (returns incorrect results):
```clojure
[:find ?t ?type
 :where
 [?t :task/status :status/complete]
 [?t :task/type ?type]
 (or [?t :task/type :type/a]
     [?t :task/type :type/b])]
```
Returns 3 results (should return 2): `[task-1 :type/a]`, `[task-2 :type/b]`, `[task-3 :type/c]`

## Execution Lifecycle

### 1. Query Parsing (`datalog/parser/parser.go`)

The parser transforms EDN into query structures:
- Patterns like `[?t :task/type :type/a]` become `*query.DataPattern`
- OR clauses become `*query.OrClause` with branches containing patterns
- Keywords like `:type/a` are parsed via `datalog.NewKeyword()` which interns them

### 2. Query Planning (`datalog/planner/`)

The planner organizes clauses into phases:

```go
// planner_phases.go:664-677
func (p *Planner) assignOrClausesToPhases(phases []Phase, orClauses []*query.OrClause, ...) {
    // OR clauses don't require prior bindings - they are data sources
    // Assign them to the first phase
    for _, orClause := range orClauses {
        phases[0].OrClauses = append(phases[0].OrClauses, orClause)
    }
}
```

**Key insight**: OR clauses are treated as "data sources" and placed in phase[0] alongside patterns.

### 3. Phase Execution (`datalog/executor/executor_sequential.go`)

Each phase executes in `executePhaseSequentialV2`:

1. **Pattern matching loop** (lines 35-143):
   - Each pattern is matched against storage with available bindings
   - Results are progressively collapsed (joined)
   - After all patterns: `independentGroups` contains collapsed pattern results

2. **Apply expressions and predicates** (line 167):
   ```go
   return e.applyExpressionsAndPredicates(ctx, phase, collapsed)
   ```

### 4. OR Clause Execution (`datalog/executor/expressions_and_predicates.go`)

In `applyExpressionsAndPredicates`, OR clauses execute first:

```go
// lines 32-40
groups := relations  // Pattern results: 3 tuples with symbols [?t, ?type]

for _, orClause := range phase.OrClauses {
    orResult, err := e.executeOrClause(ctx, orClause, groups)
    groups = append(groups, orResult)
    groups = groups.Collapse(ctx)  // Join OR result with pattern results
}
```

### 5. OR Clause Branch Execution (`datalog/executor/not_or.go`)

```go
// executeOrClause (lines 176-223)
func (e *Executor) executeOrClause(ctx Context, clause *query.OrClause, available Relations) (Relation, error) {
    var branchResults []Relation
    var commonCols []query.Symbol

    for i, branch := range clause.Branches {
        // CRITICAL: Branches execute with nil binding (not constrained by available)
        branchResult, err := e.executeInnerClauses(ctx, branch, nil)

        // Track common symbols across branches
        if i == 0 {
            commonCols = branchResult.Symbols()
        } else {
            commonCols = intersect(commonCols, branchResult.Symbols())
        }
        branchResults = append(branchResults, branchResult)
    }

    // Union all branch results, projecting to common symbols
    return unionRelations(branchResults, commonCols, e.options), nil
}
```

**Key insight**: `executeInnerClauses(ctx, branch, nil)` passes `nil` as the binding. Each branch executes independently against the full database.

### 6. Inner Clause Execution (`datalog/executor/not_or.go`)

```go
// executeInnerClauses (lines 257-335)
func (e *Executor) executeInnerClauses(ctx Context, clauses []query.Clause, binding Relation) (Relation, error) {
    var result Relations
    if binding != nil {
        result = Relations{binding}
    }
    // binding is nil for OR branches, so result starts empty

    for _, clause := range clauses {
        switch c := clause.(type) {
        case *query.DataPattern:
            rel, err := e.matcher.Match(c, result)  // result is empty!
            // ...
        }
    }
}
```

### 7. Pattern Matching (`datalog/storage/matcher_relations.go`)

When `bindings` is nil or empty:

```go
// Match (lines 55-58)
if bindings == nil || len(bindings) == 0 {
    return m.matchUnboundAsRelation(pattern, symbols, constraints)
}
```

This scans the database directly:
- Chooses index based on bound pattern elements (AVET for A+V bound)
- Returns all matching datoms as a streaming relation
- Symbols come from the pattern's variable-extraction routine (only variables)

### 8. Relation Collapse/Join (`datalog/executor/relations.go`)

After OR execution, `groups.Collapse(ctx)` joins relations:

```go
// Collapse (lines 130-187)
func (rs Relations) Collapse(ctx Context) Relations {
    for len(remaining) > 0 {
        currentGroup := remaining[0]
        for i := 0; i < len(remaining); i++ {
            if hasSharedSymbols(currentGroup, remaining[i]) {
                currentGroup = ctx.JoinRelations(currentGroup, remaining[i], func() Relation {
                    return currentGroup.Join(remaining[i])
                })
            }
        }
    }
}
```

### 9. Hash Join (`datalog/executor/join.go`)

The join is implemented as a hash join:

1. **Determine common symbols** (line 1181-1195 in relation.go)
2. **Choose build vs probe relation** (lines 205-258):
   - If left is streaming and right is materialized, right becomes build
   - OR result (MaterializedRelation) becomes build side
   - Pattern result (StreamingRelation) becomes probe side
3. **Build hash table** from build relation (lines 260-477)
4. **Probe phase** (lines 565-600):
   - For each probe tuple, lookup key in hash table
   - Only output tuples that have a match

## Expected vs Actual Flow

### Expected for Query with OR:

1. Patterns produce: `[(task1, :type/a), (task2, :type/b), (task3, :type/c)]` with symbols `[?t, ?type]`
2. OR branch 1 `[?t :task/type :type/a]` produces: `[(task1)]` with symbols `[?t]`
3. OR branch 2 `[?t :task/type :type/b]` produces: `[(task2)]` with symbols `[?t]`
4. OR union: `[(task1), (task2)]` with symbols `[?t]`
5. Join on `?t`: `[(task1, :type/a), (task2, :type/b)]` - **2 tuples**

### Actual Result:

All 3 tuples are returned, suggesting the join is not filtering correctly.

## Key Differences: Regular Pattern vs OR Branch

**Regular pattern** `[?t :task/type :type/a]` in the non-OR query:
- Executed via `e.matchPatternWithRelations(ctx, pattern, availableRelations)`
- `availableRelations` contains prior binding tuples
- Matcher uses bindings to constrain the scan
- Result is already filtered

**OR branch** `[?t :task/type :type/a]`:
- Executed via `e.executeInnerClauses(ctx, branch, nil)` with **nil binding**
- Matcher scans full database (no binding constraint)
- Result must be joined later to filter

## Root Cause Found

**The bug had TWO causes** working together to create the failure:

### 1. `realizePhase` Omitted OR/NOT Clauses

The `realizePhase` function in `datalog/planner/types.go` (lines 687-769) builds the `Query.Where` clause from phase components but **NEVER added** OR clauses or NOT clauses:

```go
// realizePhase was adding only:
// 1. Patterns
// 2. Expressions
// 3. Predicates
// 4. Subqueries
// MISSING: OrClauses, NotClauses, OrJoinClauses, NotJoinClauses
```

The Phase struct has separate fields for these (`Phase.OrClauses`, `Phase.NotClauses`, etc.), but `realizePhase` simply didn't include them in the generated Query.

**Why it didn't error**: The OR clause was never in `Query.Where`, so `QueryExecutor.Execute` never saw it.

### 2. `QueryExecutor.Execute` Lacked Cases for OR/NOT

Even if the clauses had been included, `DefaultQueryExecutor.Execute` in `query_executor.go` only handled:
- `*query.DataPattern`
- `*query.Expression`
- `query.Predicate`
- `*query.SubqueryPattern`
- `default` → error

It had no cases for `*query.OrClause`, `*query.NotClause`, `*query.OrJoinClause`, or `*query.NotJoinClause`.

### Evidence

| Configuration | Result |
|---------------|--------|
| `UseQueryExecutor: false` (old executor) | **PASS** - 2 tuples |
| `UseQueryExecutor: true` (new executor, default) | **FAIL** - 3 tuples |

**Why the old executor worked**: It processed `Phase.OrClauses` directly in `applyExpressionsAndPredicates()`, bypassing the `Query.Where` clause entirely.

## The Fix

Two changes were made:

### Fix 1: Update `realizePhase` (types.go)

Added OR/NOT clause inclusion after subqueries:

```go
// 5. Add NOT clauses (anti-join filtering)
for _, nc := range phase.NotClauses {
    where = append(where, nc)
}

// 6. Add NOT-JOIN clauses
for _, njc := range phase.NotJoinClauses {
    where = append(where, njc)
}

// 7. Add OR clauses (union)
for _, oc := range phase.OrClauses {
    where = append(where, oc)
}

// 8. Add OR-JOIN clauses
for _, ojc := range phase.OrJoinClauses {
    where = append(where, ojc)
}
```

### Fix 2: Add Cases to `QueryExecutor.Execute` (query_executor.go)

Added switch cases for all four clause types:
- `*query.OrClause` → execute branches, union results, append and collapse
- `*query.OrJoinClause` → same with explicit join vars
- `*query.NotClause` → anti-join filter on groups
- `*query.NotJoinClause` → anti-join with explicit join vars

Also added supporting methods:
- `executeOrClause()`
- `executeOrJoinClause()`
- `executeNotClause()`
- `executeNotJoinClause()`
- `filterWithNotClause()`
- `filterWithNotJoinClause()`
- `executeInnerClauses()`

## Why This Bug Snuck Through

1. **Missing test coverage**: No test verified that the new QueryExecutor handled OR/NOT clauses
2. **Silent failure mode**: Instead of erroring, the clauses were simply dropped during planning
3. **Correct planner assignment**: The planner correctly assigned OR clauses to `Phase.OrClauses`, but the realization step dropped them
4. **Backward compatibility path**: The old executor worked via a different code path, masking the issue

## Lessons Learned

1. **Test new execution paths explicitly**: When adding a new executor path (QueryExecutor), test ALL clause types
2. **Realize() should be complete**: The realized Query should be semantically equivalent to the original
3. **Use tracing/annotations**: The annotation system helped identify where execution diverged
