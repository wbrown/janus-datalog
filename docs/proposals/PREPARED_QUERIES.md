# Proposal: Prepared Queries

## Summary

Add a `Prepare` API that pre-computes query plans at initialization time, eliminating parsing and planning overhead from query execution. This is analogous to prepared statements in SQL databases.

## Motivation

### Current Flow

Every query execution, even with plan caching:

```
Query String → Parse → Cache Lookup → [Plan if miss] → Execute
                 ↓
            ~microseconds of overhead per query
```

With plan cache hit:
1. Parse query string to AST
2. Compute cache key (SHA256 hash of query structure)
3. Map lookup
4. Execute with cached plan

### Observation

Most applications have **static queries with dynamic inputs**:

```go
// The query structure never changes - only inputs do
db.Query(`[:find ?e :in $ ?name :where [?e :person/name ?name]]`, "Alice")
db.Query(`[:find ?e :in $ ?name :where [?e :person/name ?name]]`, "Bob")
db.Query(`[:find ?e :in $ ?name :where [?e :person/name ?name]]`, "Charlie")
```

Query plans depend only on query structure, not input values. This is already exploited by the plan cache, but there's still overhead:
- Parsing the query string every time
- Computing the cache key
- Hash map lookup
- Cache eviction/TTL checks

### The Opportunity

If queries are known at compile/init time, we can:
1. Parse once
2. Plan once
3. Hold the plan directly (no cache lookup)
4. Catch syntax errors at init, not runtime

## Design

### Core Types

```go
// PreparedQuery holds a pre-computed query plan
type PreparedQuery struct {
    queryStr string              // Original query string (for debugging)
    query    *query.Query        // Parsed query
    plan     *planner.QueryPlan  // Pre-computed plan
    findCols []string            // Symbol names for result mapping
    mu       sync.RWMutex        // Protects plan updates (for Replan)
}

// PreparedQueryOption configures prepared query behavior
type PreparedQueryOption func(*preparedQueryConfig)

type preparedQueryConfig struct {
    plannerOpts planner.PlannerOptions
    panicOnError bool  // true = panic on parse/plan error; false = return error
}
```

### API

```go
// Prepare parses and plans a query, panicking on error.
// Intended for use at package initialization.
func Prepare(queryStr string, opts ...PreparedQueryOption) *PreparedQuery

// TryPrepare parses and plans a query, returning an error instead of panicking.
// Useful for dynamic query preparation.
func TryPrepare(queryStr string, opts ...PreparedQueryOption) (*PreparedQuery, error)

// Execute runs the prepared query against a database
func (pq *PreparedQuery) Execute(db *Database, inputs ...interface{}) ([][]interface{}, error)

// ExecuteInto runs the prepared query and populates typed results
func (pq *PreparedQuery) ExecuteInto(db *Database, dest interface{}, inputs ...interface{}) error

// ExecuteOne runs the prepared query expecting exactly one result
func (pq *PreparedQuery) ExecuteOne(db *Database, inputs ...interface{}) ([]interface{}, error)

// ExecuteOneInto runs the prepared query and populates a single typed result
func (pq *PreparedQuery) ExecuteOneInto(db *Database, dest interface{}, inputs ...interface{}) error

// Replan forces re-planning (useful if planner options change)
func (pq *PreparedQuery) Replan(opts planner.PlannerOptions) error

// String returns the original query string
func (pq *PreparedQuery) String() string
```

### Usage

#### Basic Usage

```go
package myapp

import "github.com/wbrown/janus-datalog/datalog/storage"

// Queries prepared at package initialization
var (
    FindPersonByName = storage.Prepare(`
        [:find ?name ?age
         :in $ ?search-name
         :where [?e :person/name ?search-name]
                [?e :person/name ?name]
                [?e :person/age ?age]]
    `)

    FindTradesBySymbol = storage.Prepare(`
        [:find ?price ?time
         :in $ ?symbol
         :where [?t :trade/symbol ?symbol]
                [?t :trade/price ?price]
                [?t :trade/time ?time]]
    `)
)

func GetPerson(db *storage.Database, name string) ([][]interface{}, error) {
    // No parsing, no planning, no cache lookup
    return FindPersonByName.Execute(db, name)
}
```

#### With Typed Results

```go
type PersonResult struct {
    Name string `datalog:"?name"`
    Age  int64  `datalog:"?age"`
}

var FindPerson = storage.Prepare(`[:find ?name ?age :where ...]`)

func GetPerson(db *storage.Database, name string) ([]PersonResult, error) {
    var results []PersonResult
    err := FindPerson.ExecuteInto(db, &results, name)
    return results, err
}
```

#### Dynamic Preparation

```go
func BuildQuery(filters []string) (*storage.PreparedQuery, error) {
    queryStr := buildQueryString(filters)
    return storage.TryPrepare(queryStr)
}
```

### Implementation

#### Prepare Function

```go
func Prepare(queryStr string, opts ...PreparedQueryOption) *PreparedQuery {
    pq, err := TryPrepare(queryStr, opts...)
    if err != nil {
        panic(fmt.Sprintf("failed to prepare query: %v\nQuery: %s", err, queryStr))
    }
    return pq
}

func TryPrepare(queryStr string, opts ...PreparedQueryOption) (*PreparedQuery, error) {
    cfg := &preparedQueryConfig{
        plannerOpts: planner.DefaultPlannerOptions(),
    }
    for _, opt := range opts {
        opt(cfg)
    }

    // Parse query
    q, err := parser.ParseQuery(queryStr)
    if err != nil {
        return nil, fmt.Errorf("parse error: %w", err)
    }

    // Plan query (with nil matcher - plan is matcher-independent)
    p := planner.NewPlanner(nil, cfg.plannerOpts)
    plan, err := p.Plan(q, nil)
    if err != nil {
        return nil, fmt.Errorf("planning error: %w", err)
    }

    return &PreparedQuery{
        queryStr: queryStr,
        query:    q,
        plan:     plan,
        findSymbols: extractFindSymbolStrings(q.Find),
    }, nil
}
```

#### Execute Method

```go
func (pq *PreparedQuery) Execute(db *Database, inputs ...interface{}) ([][]interface{}, error) {
    pq.mu.RLock()
    plan := pq.plan
    q := pq.query
    pq.mu.RUnlock()

    // Convert inputs to relations
    inputRelations, err := db.convertInputsToRelations(q, inputs)
    if err != nil {
        return nil, err
    }

    // Execute with pre-computed plan (bypass planning entirely)
    exec := db.NewExecutor()
    ctx := executor.NewContext(db.annotationHandler)
    result, err := exec.ExecuteWithPlan(ctx, plan, inputRelations)
    if err != nil {
        return nil, err
    }

    return relationToSlice(result), nil
}
```

#### Executor Integration

Requires adding `ExecuteWithPlan` to the executor:

```go
// In executor/executor.go or executor/query_executor.go

// ExecuteWithPlan executes a pre-computed plan with input relations
func (e *Executor) ExecuteWithPlan(ctx Context, plan *planner.QueryPlan, inputs []Relation) (Relation, error) {
    // Skip planning - go directly to phase execution
    return e.executePhases(ctx, plan.Phases, inputs)
}
```

## Performance Analysis

### Current Overhead (with cache hit)

| Operation | Approximate Cost |
|-----------|------------------|
| Parse query string | 5-50 μs |
| Compute cache key (SHA256) | 1-2 μs |
| Cache map lookup | 0.1-0.5 μs |
| Execute | varies |

### With Prepared Queries

| Operation | Approximate Cost |
|-----------|------------------|
| Dereference PreparedQuery pointer | ~0 |
| Execute | varies |

### When It Matters

For queries where execution is fast (simple lookups, small result sets), planning overhead can be 10-50% of total time. Examples:

- Point lookups by entity ID
- Single-value index scans
- Pre-filtered aggregations
- Cached/warm data access

For complex queries (large joins, full scans), execution dominates and planning overhead is negligible.

## Type-Safe Variant (Future)

With Go generics, could add compile-time type checking:

```go
// Type-safe prepared query
type TypedQuery[T any] struct {
    pq *PreparedQuery
}

func PrepareTyped[T any](queryStr string) *TypedQuery[T] {
    pq := Prepare(queryStr)
    // Could validate T structure matches query at init time
    return &TypedQuery[T]{pq: pq}
}

func (tq *TypedQuery[T]) Execute(db *Database, inputs ...interface{}) ([]T, error) {
    var results []T
    err := tq.pq.ExecuteInto(db, &results, inputs...)
    return results, err
}

// Usage
var FindPerson = storage.PrepareTyped[PersonResult](`[:find ?name ?age :where ...]`)

results, err := FindPerson.Execute(db, "Alice")  // returns []PersonResult directly
```

## Migration Path

1. **Additive** - `Prepare` is a new API, doesn't change existing `Query` behavior
2. **Gradual adoption** - Convert hot-path queries first
3. **Coexistence** - Can mix prepared and ad-hoc queries

## Testing Strategy

1. **Unit tests** for Prepare/TryPrepare (parse errors, plan errors)
2. **Equivalence tests** - PreparedQuery.Execute should match Query for same inputs
3. **Concurrency tests** - Multiple goroutines executing same PreparedQuery
4. **Benchmark** - Compare prepared vs cached vs uncached execution

## Open Questions

1. **Matcher dependency**: Current planner takes a matcher. Can we plan without one?
   - Likely yes - plan structure doesn't depend on data, only query structure
   - May need to defer some optimizations to execution time

2. **Schema changes**: What happens if schema changes after Prepare?
   - Could add `Replan()` method
   - Or treat PreparedQuery as immutable, require re-preparation

3. **Statistics**: Should prepared queries use statistics for planning?
   - Without stats: deterministic plan, always the same
   - With stats: better plan, but need mechanism to update

4. **Plan cache interaction**: Should prepared queries still populate the cache?
   - Probably no - they bypass the cache entirely
   - But could share plans if same query is also used ad-hoc

## Estimated Effort

- Core implementation: 200-300 lines
- Executor integration: 50-100 lines
- Tests: 300-400 lines
- Documentation: 100 lines

**Timeline**: 2-3 days when prioritized

## References

- [PostgreSQL PREPARE](https://www.postgresql.org/docs/current/sql-prepare.html)
- [Go database/sql PreparedStatement](https://pkg.go.dev/database/sql#Stmt)
- [Datomic Query](https://docs.datomic.com/query/query-executing.html) - doesn't have explicit prepare, but peer library caches plans
