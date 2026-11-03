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

### Database Functions on a Named Source

A database function's first argument is the source it reads, as a pattern's prefix is:

```clojure
[:find ?run ?failed
 :in $ $build
 :where [$build ?run :run/name _]
        [(get-else $build ?run :run/failed "") ?failed]]
```

`get-else`, `get-some` and `missing?` look the attribute up in the source they name. A source that cannot look an attribute up is an error naming the source.
With the algebra optimizer on, a `get-else` is rewritten into a scan of its source, joined to the query, only where the source answers that its match of the attribute gives, for every entity, the one value its lookup gives (`CanFuseAttributeFetch`). A database answers true for an attribute it resolves cardinality-one in its latest view, declared so or with no definition. On a cardinality-many attribute, a history or as-of handle, or a source without the capability, the `get-else` runs as a lookup per tuple. Both answer alike ([GET_ELSE_SCAN_REWRITE_PROOF.md](../proposals/GET_ELSE_SCAN_REWRITE_PROOF.md)).

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

### GetElseFrom(), MissingFrom(), GetSomeFrom()

Database functions on a named source take the source first:

```go
build := qb.Source("$build")
// [(get-else $build ?run :run/failed "") ?failed]
qb.GetElseFrom(build, run, qb.Kw(":run/failed"), "").As(failed)
```

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

### Database (`*db.DB`)

Any `*db.DB` implements `PatternMatcher`. Use this for cross-database joins:

```go
usersDB, _ := db.Open("users.db")
permsDB, _ := db.Open("perms.db")

results, err := usersDB.Query(query,
    storage.WithSources(map[query.Symbol]executor.PatternMatcher{
        query.Symbol("$users"): usersDB,
        query.Symbol("$perms"): permsDB,
    }),
)
```

### Memory database

Facts held in memory are a database `db.OpenMemory` opens, written through a transaction. Useful for caches, computed facts, or test data:

```go
cache, err := db.OpenMemory()
if err != nil {
    return err
}
defer cache.Close()
tx := cache.NewTransaction()
for _, fact := range []datalog.Datom{
    {E: datalog.NewIdentity("cache:1"), A: datalog.NewKeyword(":cache/user-id"), V: "uid-1"},
    {E: datalog.NewIdentity("cache:1"), A: datalog.NewKeyword(":cache/score"), V: int64(95)},
    {E: datalog.NewIdentity("cache:2"), A: datalog.NewKeyword(":cache/user-id"), V: "uid-2"},
    {E: datalog.NewIdentity("cache:2"), A: datalog.NewKeyword(":cache/score"), V: int64(82)},
} {
    if err := tx.Add(fact.E, fact.A, fact.V); err != nil {
        return err
    }
}
if _, err := tx.Commit(); err != nil {
    return err
}
```

Pass as a named source:

```go
results, err := d.Query(query,
    storage.WithSources(map[query.Symbol]executor.PatternMatcher{
        query.Symbol("$cache"): cache,
    }),
)
```

### Slice source

`db.NewSliceSource` opens a memory database holding a Go slice's facts under an attribute schema. The item at index i is the entity `"slice:i"`, holding at each attribute the value its accessor answers. An accessor that answers a slice or an array gives the item one fact per element, and an attribute at which one item holds two elements is cardinality-many. The caller closes the database.

```go
type Rule struct {
    Key       string
    DependsOn []string
}

rules := []Rule{
    {Key: "region-lore", DependsOn: []string{"world-lore", "region"}},
    {Key: "character-eval", DependsOn: []string{"character"}},
}

ruleSource, err := db.NewSliceSource(rules, db.AttributeSchema[Rule]{
    datalog.NewKeyword(":rule/key"):        func(r Rule) any { return r.Key },
    datalog.NewKeyword(":rule/depends-on"): func(r Rule) any { return r.DependsOn },
})
if err != nil {
    return err
}
defer ruleSource.Close()
```

Query it like any other source:

```go
results, err := d.Query(
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

`WithSources` is a functional option passed to `d.Query()`. It adds named sources to the query's execution context. The consumer imports `storage` for `WithSources` in multi-source scenarios:

```go
func WithSources(sources map[query.Symbol]executor.PatternMatcher) QueryOption
```

Sources are mixed into the variadic `inputs` parameter alongside regular query inputs (scalars, collections, relations):

```go
results, err := d.Query(query,
    storage.WithSources(map[query.Symbol]executor.PatternMatcher{
        query.Symbol("$rules"): ruleSource,
    }),
    "region-lore",  // regular scalar input
)
```

`WithSources` values are extracted before mapping regular inputs to `:in` parameters. The order of `WithSources` relative to other inputs doesn't matter.
`Explain`, `ExplainAlgebra` and `Analyze` take the same option and plan with the query's sources as `Query` does, so a `get-else` on a named source is explained as `Query` plans it.

### Source Validation

At execution time, the engine validates that every source declared in `:in` is provided. If a source is missing, you get an error:

```go
// Query declares $users but doesn't provide it
_, err := d.Query(
    `[:find ?e :in $users :where [$users ?e :attr ?v]]`,
)
// err: "query declares source $users in :in but no PatternMatcher was provided for it"
```

### Default Source ($)

The default source `$` always resolves to the calling database. You never need to provide it via `WithSources` — it's set up automatically. Patterns without a source prefix resolve against `$`.

The unknown-attribute refusal ([SCHEMA.md](SCHEMA.md#schema-as-facts)) asks each source the query names about the attributes the query names on it. A source refuses when it carries the option: a `*db.DB` opened with `RefuseUnknownAttributes`, or the matcher such a database mints, refuses a pattern on it as a named source as it does as `$`. Any other source refuses nothing.

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

Inner queries inherit all sources from the execution context. When an outer query has `$users` and `$perms` available, any subquery can reference them. A source the inner `:in` declares stands for the source the call passes at its place: an inner query written over `$t` reads `$perms` when the call passes `$perms` there, and the calling database when it passes `$`. An inner query declaring no `:in`, or declaring `$`, reads the source the call passes the same way.

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
        qb.Subquery(innerQ, dept).BindRelation(qb.NewVar("headcount")),
    ).
    MustBuild()
```

The `SubqueryBuilder` automatically collects database source references from the inner query's `:in` clause and prepends them to the subquery's input list.

## Implementing Custom Sources

To create a custom data source, implement `PatternMatcher`:

```go
type PatternMatcher interface {
    Match(q *query.Query, bindings Relations) (Relation, error)
}
```

The query fragment contains exactly one data pattern. Extract it with
`q.SingleDataPattern()`. `q.OrderBy` and `q.Limit` may carry physical
requirements after the executor has proven pushdown safe. A custom source may
ignore those requirements, but must not claim properties it does not satisfy.

Optionally implement `PredicateAwareMatcher` for predicate pushdown:

```go
type PredicateAwareMatcher interface {
    PatternMatcher
    MatchWithConstraints(q *query.Query, bindings Relations, constraints []StorageConstraint) (Relation, error)
}
```

If your source supports predicate pushdown, the `SourceRouter` will call `MatchWithConstraints` instead of `Match` when constraints are available.

A source a database function names implements `EntityLookupMatcher`, which looks one entity's attribute up. The `SourceRouter` gives the function that source's lookup through `LookupSource`, and a source without the capability is an error naming it.
A source may implement `AttributeFetchFusable`, answering whether its match of an attribute gives, for every entity, the one value its lookup gives. A true answer lets the executor fuse a same-entity fetch into the lookup and lets the optimizer rewrite a `get-else` on the source into a scan, so a source answers true only where that holds. A source without the capability answers false through the `SourceRouter`.

Pass your custom source via `WithSources`:

```go
results, err := d.Query(query,
    storage.WithSources(map[query.Symbol]executor.PatternMatcher{
        query.Symbol("$custom"): myCustomSource,
    }),
)
```

## Key Files

| File | Purpose |
|------|---------|
| `executor/source_router.go` | Routes patterns to sources by `pattern.Source` field |
| `storage/slice_source.go` | `NewSliceSource` — a Go slice's facts as a memory database |
| `storage/facts.go` | `OpenFacts` — a memory database holding given facts |
| `storage/database.go` | `WithSources`, `buildSourceMap`, `validateQuerySources` |
| `qb/source.go` | `Source()`, `PatFrom()` — query builder support |
| `query/types.go` | `DatabaseInput`, `DataPattern.Source` field |
