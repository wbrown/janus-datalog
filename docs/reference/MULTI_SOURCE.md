# Multi-Source Queries

Query across multiple data sources in a single query. Sources can be additional databases, in-memory fact sets, or Go slices mapped via a schema. All sources are unified behind the `PatternMatcher` interface.

## Concept

Every Datalog pattern `[?e :attr ?v]` resolves against a data source. By default, that source is the calling database (`$`). Multi-source queries let you:

- **Name additional sources** in the `:in` clause (`$users`, `$perms`, `$cache`)
- **Qualify patterns** to target a specific source (`[$users ?e :user/name ?name]`)
- **Join across sources** via shared variables (`?uid` bound in one source, consumed in another)

Any type implementing `PatternMatcher` is a valid source. The engine doesn't distinguish between a database, an in-memory fact set, or a Go slice — they all match patterns the same way.

## EDN Syntax

### Named Sources in `:in`

Declare named sources in the `:in` clause. Source names start with `$`:

```clojure
[:find ?name ?role
 :in $users $perms
 :where [$users ?u :user/name ?name]
        [$users ?u :user/id ?uid]
        [$perms ?p :perm/user-id ?uid]
        [$perms ?p :perm/role ?role]]
```

The `:in` clause lists source names. Each `$name` must be provided at execution time via `WithSources`.

### Source-Qualified Patterns in `:where`

Prefix a pattern with `$source` to target that source:

```clojure
[$users ?u :user/name ?name]   ; resolves against $users
[$perms ?p :perm/role ?role]   ; resolves against $perms
[?e :thing/name ?name]          ; resolves against default $ (the calling database)
```

### Mixing Default and Named Sources

The default source `$` is the calling database. Use it alongside named sources:

```clojure
[:find ?name ?score
 :in $ $cache
 :where [?e :user/name ?name]          ; default $ (database)
        [?e :user/id ?uid]              ; default $ (database)
        [$cache ?c :cache/user-id ?uid] ; named $cache source
        [$cache ?c :cache/score ?score]]
```

You can omit `$` from `:in` — it's always available. But including it makes the query self-documenting.

### Combining Sources with Scalar/Collection Inputs

Named sources and regular inputs coexist in `:in`:

```clojure
[:find ?dep
 :in $rules ?key
 :where [$rules ?r :rule/key ?key]
        [$rules ?r :rule/depends-on ?dep]]
```

Here `$rules` is a named source and `?key` is a scalar input. Both are passed at execution time.

## Query Builder API

### Source()

Create a named source reference for use in `In()` and `PatFrom()`:

```go
users := qb.Source("$users")
perms := qb.Source("$perms")
```

The name must start with `$`.

### PatFrom()

Create a source-qualified pattern:

```go
// [$users ?e :user/name ?name]
qb.PatFrom(users, e, qb.Kw(":user/name"), name)
```

Arguments after the source follow the same rules as `Pat()`: `*Var`, `Attr`, `Val`, `Blank()`, or raw Go values.

### DB Constant

The `qb.DB` constant represents the default database source (`$`). Use it in `In()` when mixing default and named sources:

```go
cache := qb.Source("$cache")

q := qb.Query().
    Find(name, val).
    In(qb.DB, cache).
    Where(
        qb.Pat(e, qb.Kw(":entity/name"), name),           // default $
        qb.PatFrom(cache, e, qb.Kw(":cache/value"), val),  // $cache
    ).
    MustBuild()
```

### Complete Example

Cross-database join using the query builder:

```go
users := qb.Source("$users")
perms := qb.Source("$perms")
e := qb.NewVar("e")
name := qb.NewVar("name")
uid := qb.NewVar("uid")
role := qb.NewVar("role")

q := qb.Query().
    Find(name, role).
    In(users, perms).
    Where(
        qb.PatFrom(users, e, qb.Kw(":user/name"), name),
        qb.PatFrom(users, e, qb.Kw(":user/id"), uid),
        qb.PatFrom(perms, qb.NewVar("p"), qb.Kw(":perm/user-id"), uid),
        qb.PatFrom(perms, qb.NewVar("p2"), qb.Kw(":perm/role"), role),
    ).
    MustBuild()
```

## Source Types

### Database (`*storage.Database`)

Any `*Database` implements `PatternMatcher`. Use this for cross-database joins:

```go
usersDB, _ := storage.NewDatabase("users.db")
permsDB, _ := storage.NewDatabase("perms.db")

results, err := usersDB.ExecuteQueryWithInputs(query,
    storage.WithSources(map[query.Symbol]executor.PatternMatcher{
        query.Symbol("$users"): usersDB,
        query.Symbol("$perms"): permsDB,
    }),
)
```

### MemoryPatternMatcher

An in-memory set of datoms. Useful for caches, computed facts, or test data:

```go
datoms := []datalog.Datom{
    {E: datalog.NewIdentity("cache:1"), A: datalog.NewKeyword(":cache/user-id"), V: "uid-1"},
    {E: datalog.NewIdentity("cache:1"), A: datalog.NewKeyword(":cache/score"), V: int64(95)},
    {E: datalog.NewIdentity("cache:2"), A: datalog.NewKeyword(":cache/user-id"), V: "uid-2"},
    {E: datalog.NewIdentity("cache:2"), A: datalog.NewKeyword(":cache/score"), V: int64(82)},
}
cache := executor.NewMemoryPatternMatcher(datoms)
```

Pass as a named source:

```go
results, err := db.ExecuteQueryWithInputs(query,
    storage.WithSources(map[query.Symbol]executor.PatternMatcher{
        query.Symbol("$cache"): cache,
    }),
)
```

### SliceSource[T]

Wraps a Go slice, making it queryable via an attribute schema. Each slice item becomes an entity with attributes defined by accessor functions. Multi-valued attributes (slices/arrays) are automatically expanded into multiple datoms.

```go
type Rule struct {
    Key       string
    DependsOn []string
}

rules := []Rule{
    {Key: "region-lore", DependsOn: []string{"world-lore", "region"}},
    {Key: "character-eval", DependsOn: []string{"character"}},
}

ruleSource := executor.NewSliceSource(rules, executor.AttributeSchema[Rule]{
    datalog.NewKeyword(":rule/key"):        func(r Rule) any { return r.Key },
    datalog.NewKeyword(":rule/depends-on"): func(r Rule) any { return r.DependsOn },
})
```

Query it like any other source:

```go
results, err := db.ExecuteQueryWithInputs(
    `[:find ?dep
      :in $rules ?key
      :where [$rules ?r :rule/key ?key]
             [$rules ?r :rule/depends-on ?dep]]`,
    storage.WithSources(map[query.Symbol]executor.PatternMatcher{
        query.Symbol("$rules"): ruleSource,
    }),
    "region-lore",  // scalar input for ?key
)
// results: [["world-lore"], ["region"]]
```

## Execution

### WithSources Option

`WithSources` is a functional option passed to `ExecuteQueryWithInputs`. It adds named sources to the query's execution context:

```go
func WithSources(sources map[query.Symbol]executor.PatternMatcher) QueryOption
```

Sources are mixed into the variadic `inputs` parameter alongside regular query inputs (scalars, collections, relations):

```go
results, err := db.ExecuteQueryWithInputs(query,
    storage.WithSources(map[query.Symbol]executor.PatternMatcher{
        query.Symbol("$rules"): ruleSource,
    }),
    "region-lore",  // regular scalar input
)
```

`WithSources` values are extracted before mapping regular inputs to `:in` parameters. The order of `WithSources` relative to other inputs doesn't matter.

### Source Validation

At execution time, the engine validates that every source declared in `:in` is provided. If a source is missing, you get an error:

```go
// Query declares $users but doesn't provide it
_, err := db.ExecuteQueryWithInputs(
    `[:find ?e :in $users :where [$users ?e :attr ?v]]`,
)
// err: "query declares source $users in :in but no PatternMatcher was provided for it"
```

### Default Source ($)

The default source `$` always resolves to the calling database. You never need to provide it via `WithSources` — it's set up automatically. Patterns without a source prefix resolve against `$`.

## Cross-Source Joins

Cross-source joins work through shared variables. The engine doesn't need special join logic — the standard variable unification handles it:

```clojure
[:find ?name ?score
 :in $ $cache
 :where [?e :user/name ?name]              ; from database: binds ?e, ?name
        [?e :user/id ?uid]                  ; from database: binds ?uid
        [$cache ?c :cache/user-id ?uid]     ; from cache: joins on ?uid
        [$cache ?c :cache/score ?score]]    ; from cache: binds ?score
```

The join on `?uid` bridges the database and cache sources. The planner processes patterns in dependency order, carrying bindings forward across sources.

## Subqueries

Inner queries inherit all sources from the execution context. When an outer query has `$users` and `$perms` available, any subquery can reference them:

```go
users := qb.Source("$users")
dept := qb.NewVar("dept")

innerQ := qb.Query().
    Find(qb.Count(qb.NewVar("e"))).
    In(users, qb.Scalar(dept)).
    Where(
        qb.PatFrom(users, qb.NewVar("e"), qb.Kw(":user/dept"), dept),
    )

q := qb.Query().
    Find(dept, qb.NewVar("headcount")).
    Where(
        qb.Pat(qb.NewVar("d"), qb.Kw(":dept/name"), dept),
        qb.Subquery(innerQ, dept).BindTuple(qb.NewVar("headcount")),
    ).
    MustBuild()
```

The `SubqueryBuilder` automatically collects database source references from the inner query's `:in` clause and prepends them to the subquery's input list.

## Implementing Custom Sources

To create a custom data source, implement `PatternMatcher`:

```go
type PatternMatcher interface {
    Match(pattern *query.DataPattern, bindings Relations) (Relation, error)
}
```

The `Match` method receives a pattern (with entity, attribute, value elements that may be variables or constants) and existing bindings from prior joins. It returns a `Relation` containing all matching tuples.

Optionally implement `PredicateAwareMatcher` for predicate pushdown:

```go
type PredicateAwareMatcher interface {
    PatternMatcher
    MatchWithConstraints(pattern *query.DataPattern, bindings Relations, constraints []StorageConstraint) (Relation, error)
}
```

If your source supports predicate pushdown, the `SourceRouter` will call `MatchWithConstraints` instead of `Match` when constraints are available.

Pass your custom source via `WithSources`:

```go
results, err := db.ExecuteQueryWithInputs(query,
    storage.WithSources(map[query.Symbol]executor.PatternMatcher{
        query.Symbol("$custom"): myCustomSource,
    }),
)
```

## Key Files

| File | Purpose |
|------|---------|
| `executor/source_router.go` | Routes patterns to sources by `pattern.Source` field |
| `executor/slice_source.go` | `SliceSource[T]` — Go slices as queryable sources |
| `executor/pattern_match.go` | `NewMemoryPatternMatcher` — in-memory datom sets |
| `storage/database.go` | `WithSources`, `buildSourceMap`, `validateQuerySources` |
| `qb/source.go` | `Source()`, `PatFrom()` — query builder support |
| `query/types.go` | `DatabaseInput`, `DataPattern.Source` field |
