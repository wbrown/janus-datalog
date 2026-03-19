# Proposal: Multi-Source Queries

## Summary

Extend janus-datalog to query across multiple data sources in a single query. This enables:

1. **Named database inputs** - Reference multiple databases via `$name` syntax
2. **DataSource interface** - Abstract over persistent databases, in-memory facts, and Go data structures
3. **Heterogeneous joins** - Join data from different sources via shared variables

## Motivation

### The Problem: Rules Trapped in Code

The `pkg/tasks` dependency resolver in the application has:

- **Rules as Go structs** - Data trapped in code, not queryable
- **Custom filter DSL** - `"region = $.region"`, `"spans($key)"` interpreted by Go
- **Imperative resolution** - Subject expressions (`$self`, `$world`, `*.property`) resolved in Go loops

```go
// Current: Rules are Go structs with a custom DSL
DataRule{
    Key: KeyRegionLore,
    DependsOn: []DependencyExpr{
        {Key: KeyWorldLore, SubjectExpr: "$world"},
        {Key: KeyRegion, SubjectExpr: "$self"},
    },
    Filter: "spans($.region)",  // Custom mini-DSL
}

// Resolution requires Go iteration
for _, dep := range rule.DependsOn {
    subject := resolveSubjectExpr(dep.SubjectExpr, entity)  // Go code
    if !matchesFilter(rule.Filter, entity) {                // More Go code
        continue
    }
    // ...
}
```

### The Vision: Rules as Queryable Data

```clojure
;; Rules, entities, and tasks are all queryable sources
[:find ?key ?subject
 :in $db $rules $tasks ?target
 :where
 ;; What does this rule need?
 [$rules ?r :rule/key ?target]
 [$rules ?r :rule/depends-on ?dep]
 [$dep :dep/key ?key]
 [$dep :dep/subject-expr ?expr]

 ;; Resolve subject expression via join (not Go code)
 [$db ?e :entity/code ?subject]
 [$db ?e :entity/region ?expr]  ; Subject expression becomes a join

 ;; Filter to incomplete tasks
 (not-join [$tasks] ?key ?subject
   [$tasks ?t :task/key ?key]
   [$tasks ?t :task/subject ?subject]
   [$tasks ?t :task/status :complete])]
```

**Benefits:**
- Rules become data, queryable and introspectable
- Filter DSL replaced by actual Datalog (no custom parser)
- Subject resolution becomes joins (no imperative Go)
- The query engine handles optimization

### Other Use Cases

Multi-source queries are broadly useful:

- **Join user DB with permissions DB** - Correlate identity with access control
- **Combine live data with snapshots** - Compare current state to historical
- **Query across microservice data** - Unified view of distributed state
- **Mix persistent + cached data** - Join database with in-memory structures

## Design

### Part 1: Named Database Inputs

#### Syntax

```clojure
;; Multiple named databases in :in clause
[:find ?name ?balance
 :in $users $accounts
 :where [$users ?u :user/name ?name]
        [$users ?u :user/id ?id]
        [$accounts ?a :account/user-id ?id]
        [$accounts ?a :account/balance ?balance]]
```

**Key points:**
- `$` remains the default (unnamed) database
- `$name` declares a named database source
- Patterns prefixed with `[$name ...]` query that source
- Unprefixed patterns query `$` (default)

#### Source-Qualified Patterns

```clojure
[$users ?e :user/name ?name]     ; Query $users source
[$cache ?e :cache/value ?val]    ; Query $cache source
[?e :default/attr ?v]            ; Query $ (default) source
```

#### Join Semantics

Cross-source joins work via shared variables:

```clojure
[:find ?name ?role
 :in $hr $perms
 :where [$hr ?emp :employee/name ?name]
        [$hr ?emp :employee/id ?eid]        ; ?eid binds from $hr
        [$perms ?p :permission/employee ?eid]  ; ?eid joins to $perms
        [$perms ?p :permission/role ?role]]
```

The executor:
1. Matches `[$hr ...]` patterns against `$hr` source
2. Matches `[$perms ...]` patterns against `$perms` source
3. Joins results on shared variable `?eid`

Existing join algorithms (hash join, merge join) work unchanged - they operate on relations regardless of source.

### Part 2: DataSource Interface

Generalize from concrete `*Database` to an interface:

```go
// DataSource can answer pattern queries
type DataSource interface {
    // Match returns a relation of tuples matching the pattern
    // with the given input bindings
    Match(pattern *query.DataPattern, bindings Bindings) (Relation, error)
}

// Database implements DataSource (existing behavior)
func (d *Database) Match(p *query.DataPattern, b Bindings) (Relation, error)
```

#### MemorySource: In-Memory Facts

```go
// MemorySource holds facts in memory, queryable like a database
type MemorySource struct {
    facts []Datom  // [entity, attribute, value, tx]
}

func NewMemorySource(facts []Datom) *MemorySource

func (m *MemorySource) Match(p *query.DataPattern, b Bindings) (Relation, error) {
    // Iterate facts, match against pattern, return relation
}

// Usage
cache := datalog.NewMemorySource([]Datom{
    {Entity: 1, Attr: ":cache/key", Value: "foo"},
    {Entity: 1, Attr: ":cache/value", Value: 42},
})
```

#### SliceSource: Go Slices as Queryable Data

This is the key feature for rules-as-data:

```go
// SliceSource wraps a Go slice, making it queryable via a schema
type SliceSource[T any] struct {
    items  []T
    schema AttributeSchema[T]
}

// AttributeSchema maps keywords to accessor functions
type AttributeSchema[T any] map[Keyword]func(T) any

func NewSliceSource[T any](items []T, schema AttributeSchema[T]) *SliceSource[T]

func (s *SliceSource[T]) Match(p *query.DataPattern, b Bindings) (Relation, error) {
    // For each item, check if pattern matches via schema accessors
    // Entity ID is the slice index
}
```

**Example: Rules as queryable data**

```go
// Existing rule definitions (Go structs)
rules := []DataRule{
    {Key: KeyRegionLore, DependsOn: []DependencyExpr{...}},
    {Key: KeyCharacterEval, DependsOn: []DependencyExpr{...}},
}

// Make them queryable
ruleSource := datalog.NewSliceSource(rules, datalog.AttributeSchema[DataRule]{
    Keyword(":rule/key"):        func(r DataRule) any { return r.Key },
    Keyword(":rule/depends-on"): func(r DataRule) any { return r.DependsOn },
    Keyword(":rule/task"):       func(r DataRule) any { return r.Task },
})

// Now query rules alongside the entity database
results, _ := db.ExecuteMultiSource(
    `[:find ?dep-key
      :in $db $rules ?target
      :where [$rules ?r :rule/key ?target]
             [$rules ?r :rule/depends-on ?dep]
             [$dep :dep/key ?dep-key]]`,
    map[string]DataSource{
        "":      entityDB,
        "rules": ruleSource,
    },
    KeyRegionLore,
)
```

### Part 3: Go API

```go
// ExecuteMultiSource runs a query against multiple named sources
func ExecuteMultiSource(
    query string,
    sources map[string]DataSource,
    inputs ...any,
) ([]Tuple, error)

// Sources map:
// - "" (empty string) is the default $ source
// - "name" corresponds to $name in the query

// Example
results, err := datalog.ExecuteMultiSource(
    `[:find ?name ?cached
      :in $ $cache ?id
      :where [?e :entity/id ?id]
             [?e :entity/name ?name]
             [$cache ?c :cache/entity ?e]
             [$cache ?c :cache/value ?cached]]`,
    map[string]DataSource{
        "":      mainDB,
        "cache": cacheSource,
    },
    entityID,
)
```

### Part 4: Query Builder Integration

```go
// Declare sources
users := qb.Source("users")
perms := qb.Source("perms")

// Variables
u, name, role := qb.Var("u"), qb.Var("name"), qb.Var("role")

// Build query with source-qualified patterns
q := qb.Query().
    Find(name, role).
    In(users, perms).
    Where(
        qb.PatFrom(users, u, UserName, name),
        qb.PatFrom(perms, u, PermRole, role),
    ).
    MustBuild()
```

## Implementation Plan

### Phase 1: Parser Extensions

1. Parse `$name` in `:in` clause as named source input
2. Parse `[$name ?e :attr ?v]` as source-qualified pattern
3. Add `SourceName` field to `DataPattern`

**Files:**
- `datalog/parser/parser.go` - Parse `$name` and `[$name ...]`
- `datalog/query/types.go` - Add `Source` field to `DataPattern`

**Estimated effort:** Small (parser is straightforward)

### Phase 2: Executor Integration

1. Thread `map[string]DataSource` through executor
2. Route pattern matching to correct source based on pattern's `Source` field
3. Ensure joins work across sources (they should - same relation interface)

**Files:**
- `datalog/executor/query_executor.go` - Source routing
- `datalog/executor/pattern_matcher.go` - Dispatch to DataSource.Match

**Estimated effort:** Medium (need to thread context through)

### Phase 3: DataSource Interface

1. Extract `DataSource` interface from `*Database`
2. Implement `Database.Match()` delegating to existing pattern matching

**Files:**
- `datalog/storage/datasource.go` - Interface definition
- `datalog/storage/database.go` - Implement interface

**Estimated effort:** Small (interface extraction)

### Phase 4: MemorySource

1. Implement in-memory fact storage
2. Implement `Match()` with simple iteration
3. Optional: Add indexing for larger fact sets

**Files:**
- `datalog/storage/memory_source.go`

**Estimated effort:** Small

### Phase 5: SliceSource

1. Implement generic slice wrapper
2. Use reflection to apply schema accessors
3. Handle nested structures (e.g., `DependsOn` slice)

**Files:**
- `datalog/storage/slice_source.go`

**Estimated effort:** Medium (reflection, nested access)

### Phase 6: Query Builder Extensions

1. Add `qb.Source("name")` for declaring sources
2. Add `qb.PatFrom(source, ...)` for source-qualified patterns
3. Update `In()` to accept sources

**Files:**
- `datalog/qb/sources.go`
- `datalog/qb/builder.go`

**Estimated effort:** Small

## Examples

### Example 1: Rules-as-Data Resolution

```clojure
;; Find all incomplete dependencies for a target rule
[:find ?dep-key ?subject
 :in $db $rules $tasks ?target
 :where
 ;; Get dependencies from rules source
 [$rules ?r :rule/key ?target]
 [$rules ?r :rule/depends-on ?dep]
 [$dep :dep/key ?dep-key]
 [$dep :dep/subject-expr ?expr]

 ;; Resolve subject via entity database
 [$db ?e :entity/type :region]
 [$db ?e :entity/code ?subject]

 ;; Filter to incomplete
 (not-join [$tasks] ?dep-key ?subject
   [$tasks ?t :task/key ?dep-key]
   [$tasks ?t :task/subject ?subject]
   [$tasks ?t :task/status :complete])]
```

### Example 2: Cache Overlay

```clojure
;; Get value from cache if present, otherwise from DB
[:find ?id ?value
 :in $db $cache
 :where [$db ?e :entity/id ?id]
        (or [$cache ?e :cache/value ?value]
            [$db ?e :entity/value ?value])]
```

### Example 3: Cross-Database Join

```clojure
;; Join user data with their permissions
[:find ?username ?resource ?permission
 :in $users $perms
 :where [$users ?u :user/name ?username]
        [$users ?u :user/id ?uid]
        [$perms ?p :perm/user-id ?uid]
        [$perms ?p :perm/resource ?resource]
        [$perms ?p :perm/level ?permission]]
```

### Example 4: Snapshot Comparison

```clojure
;; Find entities that changed since snapshot
[:find ?id ?current ?snapshot
 :in $live $snapshot
 :where [$live ?e :entity/id ?id]
        [$live ?e :entity/value ?current]
        [$snapshot ?e :entity/value ?snapshot]
        [(!= ?current ?snapshot)]]
```

## Performance Considerations

### Query Planning

- **Filter pushdown**: Apply constraints before cross-source joins
- **Source statistics**: Smaller sources should be build side of hash joins
- **Parallel evaluation**: Query independent sources concurrently

### SliceSource Optimization

- **Schema caching**: Compile accessor functions once, not per-query
- **Index building**: For repeated queries, build indexes lazily
- **Reflection avoidance**: Use code generation for hot paths (future)

### Memory Management

- **Streaming where possible**: MemorySource can stream results
- **Lazy materialization**: Don't copy data unnecessarily
- **Reference semantics**: SliceSource doesn't copy the underlying slice

## Relationship to Recursive Rules

Multi-source queries and recursive rules are **orthogonal features**:

- Multi-source: Query across different data locations
- Recursive rules: Derive transitive closure within a single logical dataset

They compose naturally - rules can reference multiple sources:

```clojure
;; Rule that spans sources (requires both features)
[[(authorized ?user ?resource)
  [$users ?u :user/name ?user]
  [$users ?u :user/role ?role]
  [$perms ?p :perm/role ?role]
  [$perms ?p :perm/resource ?resource]]]
```

However, multi-source provides **80% of the value** for the rules-as-data use case without the complexity of fixpoint evaluation. Recursive rules can be added independently later.

## References

1. Datomic Multi-Database Queries - https://docs.datomic.com/pro/query/query.html#multiple-databases
2. "Federated Database Systems" - Sheth & Larson (1990)
3. SQL ATTACH DATABASE (SQLite) - https://sqlite.org/lang_attach.html
