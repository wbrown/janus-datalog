# Query Builder Ergonomics Proposal

## Problem Statement

The current query builder (`qb`) works well for simple queries but becomes verbose and error-prone for complex queries with multiple subqueries, OR fallbacks, and many variables.

Pain points observed in real usage:

1. **Variable declaration noise** - Every variable requires explicit `qb.NewVar("name")` declaration
2. **No compile-time safety** - Variable names are strings; typos cause runtime failures
3. **Subquery variable pollution** - Multiple subqueries need unique variable names (`t, t2, t3, t4`)
4. **Clunky OR syntax** - `[]interface{}{}` wrappers are verbose and ugly
5. **Find/struct tag mismatch** - Easy to have variable names that don't match QueryInto struct tags

## Proposal 1: Type-Safe Field References with QueryFor[T]

### Concept

Use the result struct itself to provide type-safe variable references. The struct's `datalog` tags define variable names, and pointer-to-field provides compile-time safety.

### API Design

```go
type QueryFor[T any] struct {
    F        T                    // zero-valued struct for field references
    vars     map[uintptr]*Var     // cache: offset -> *Var
    findVars []*Var               // accumulated Find elements
    findSet  map[uintptr]bool     // deduplication
}

// V returns the *Var for a field (reference only, does not add to Find)
func (q *QueryFor[T]) V(fieldPtr any) *Var

// Find returns the *Var AND adds it to the Find clause
// First call for a field determines its position in Find
// Subsequent calls return same *Var without duplicating
func (q *QueryFor[T]) Find(fieldPtr any) *Var
```

### Usage Examples

**Basic query:**

```go
type PersonResult struct {
    Person datalog.Identity `datalog:"?person"`
    Name   string           `datalog:"?name"`
    Age    int64            `datalog:"?age"`
}

q := qb.QueryFor[PersonResult]()
f := &q.F

query := q.Where(
    qb.Pat(q.Find(&f.Person), attrPersonName, q.Find(&f.Name)),
    qb.Pat(q.V(&f.Person), attrPersonAge, q.Find(&f.Age)),
    qb.Gt(q.V(&f.Age), 21),
).MustBuild()

// Generated Find: [?person, ?name, ?age] (order of first q.Find() calls)
```

**With explicit Find (both modes supported):**

```go
// Mode 1: Explicit Find at root
q.Find(q.V(&f.Person), q.V(&f.Name)).
    Where(...)

// Mode 2: Accumulated from q.Find() calls in Where
q.Where(
    qb.Pat(q.Find(&f.Person), attr, q.Find(&f.Name)),
)

// Mode 3: Mixed - explicit plus accumulated
q.Find(q.V(&f.Person)).
    Where(
        qb.Pat(q.V(&f.Person), attr, q.Find(&f.Name)),
    )
// Find = [?person, ?name]
```

**With OR fallback:**

```go
q.Where(
    qb.Pat(q.Find(&f.Scenario), attrScenarioID, q.Find(&f.ID)),
    qb.Or().
        Branch(
            qb.Subquery(statsQuery, q.V(&f.Scenario)).BindTuple(
                q.Find(&f.TaskCount), q.Find(&f.TotalTokens),
            ),
        ).
        Branch(
            // Fallback uses V() - vars already in Find from first branch
            qb.TupleGround(0, 0).As(
                q.V(&f.TaskCount), q.V(&f.TotalTokens),
            ),
        ),
)
```

### Implementation Notes

The `V()` and `Find()` methods use reflection to:
1. Compute pointer offset from struct base address
2. Find the corresponding struct field
3. Extract the `datalog` tag
4. Create/cache a `*Var` with that name

```go
func (q *QueryFor[T]) V(fieldPtr any) *Var {
    ptr := reflect.ValueOf(fieldPtr).Pointer()
    base := reflect.ValueOf(&q.F).Pointer()
    offset := ptr - base

    if v, ok := q.vars[offset]; ok {
        return v
    }

    // Find field by offset, extract datalog tag
    t := reflect.TypeOf(q.F)
    for i := 0; i < t.NumField(); i++ {
        field := t.Field(i)
        if field.Offset == uintptr(offset) {
            tag := field.Tag.Get("datalog")
            v := NewVar(tag)
            q.vars[offset] = v
            return v
        }
    }
    panic("field not found in struct")
}
```

### What This Solves

- **Type safety**: Rename a struct field and the code won't compile
- **Tag consistency**: Variable names automatically match struct tags
- **QueryInto compatibility**: Guaranteed to work since names come from same source

### What Remains Separate

- Subquery internal variables (not in result struct)
- Intermediate variables used only within the query
- Attribute constants

---

## Proposal 2: Builder Pattern for OR/OrJoin

### Design Principle

**Users should never write `interface{}` or `[]interface{}` in query builder code.**

### Audit of Current API

Most `interface{}` usage is acceptable because it's hidden in variadic parameters:

| Function | Signature | User Writes | OK? |
|----------|-----------|-------------|-----|
| `Find` | `Find(elements ...interface{})` | `Find(name, age)` | Yes |
| `Where` | `Where(clauses ...interface{})` | `Where(c1, c2)` | Yes |
| `In` | `In(specs ...interface{})` | `In(qb.DB, qb.Scalar(x))` | Yes |
| `Not` | `Not(clauses ...interface{})` | `Not(clause)` | Yes |
| `NotJoin` | `NotJoin(vars []*Var, clauses ...interface{})` | `NotJoin(vars, c1)` | Yes |
| `Pat` | `Pat(args ...interface{})` | `Pat(e, attr, v)` | Yes |
| `Lt/Gt/Eq` | `Lt(left, right interface{})` | `Lt(age, 21)` | Yes |
| `Str` | `Str(parts ...interface{})` | `Str(a, " ", b)` | Yes |
| **`Or`** | `Or(branches ...[]interface{})` | `Or([]interface{}{...})` | **No** |
| **`OrJoin`** | `OrJoin(vars, branches ...[]interface{})` | `OrJoin(v, []interface{}{...})` | **No** |

Only `Or` and `OrJoin` require users to write `[]interface{}{}` explicitly.

### Current (Verbose)

```go
qb.Or(
    []interface{}{clause1, clause2},
    []interface{}{clause3, clause4},
)
```

### Proposed: Builder Pattern

Use method chaining consistent with the rest of qb:

```go
qb.Or().
    Branch(clause1, clause2).
    Branch(clause3, clause4)
```

### API Design

```go
// OrBuilder accumulates branches for an OR clause.
type OrBuilder struct {
    branches [][]interface{}
}

// Or starts building an OR clause.
func Or() *OrBuilder {
    return &OrBuilder{}
}

// Branch adds a branch with one or more clauses.
func (o *OrBuilder) Branch(clauses ...interface{}) *OrBuilder {
    o.branches = append(o.branches, clauses)
    return o
}

// toClause implements Clause interface - no explicit Build() needed.
func (o *OrBuilder) toClause() query.Clause { ... }

// OrJoinBuilder for OR with explicit join variables.
type OrJoinBuilder struct {
    joinVars []*Var
    branches [][]interface{}
}

// OrJoin starts building an OR-JOIN clause with join variables.
func OrJoin(joinVars ...*Var) *OrJoinBuilder {
    return &OrJoinBuilder{joinVars: joinVars}
}

// Branch adds a branch with one or more clauses.
func (o *OrJoinBuilder) Branch(clauses ...interface{}) *OrJoinBuilder {
    o.branches = append(o.branches, clauses)
    return o
}
```

### Usage Examples

**Simple OR:**

```go
qb.Or().
    Branch(qb.Eq(status, qb.V("active"))).
    Branch(qb.Eq(status, qb.V("pending")))
```

**OR with fallback (subquery with default):**

```go
qb.Or().
    Branch(qb.Subquery(statsQuery, scenario).BindTuple(taskCount, totalTokens)).
    Branch(qb.TupleGround(0, 0).As(taskCount, totalTokens))
```

**OR-JOIN:**

```go
qb.OrJoin(name).
    Branch(qb.Pat(e, PersonNickname, name)).
    Branch(qb.Pat(e, PersonName, name))
```

**Multi-clause branches:**

```go
qb.Or().
    Branch(
        qb.Pat(user, UserTier, qb.Kw(":tier/premium")),
        qb.Pat(user, UserStatus, qb.Kw(":status/active")),
    ).
    Branch(
        qb.Pat(user, UserRole, qb.Kw(":role/admin")),
    )
```

**Nested OR:**

```go
qb.Or().
    Branch(qb.Pat(user, UserStatus, qb.Kw(":status/active"))).
    Branch(
        qb.Or().
            Branch(qb.Pat(user, UserTier, qb.Kw(":tier/premium"))).
            Branch(qb.Pat(user, UserTier, qb.Kw(":tier/enterprise"))),
        qb.Not(qb.Pat(user, UserStatus, qb.Kw(":status/banned"))),
    )
```

**In a full query:**

```go
qb.Query().
    Find(scenario, taskCount).
    Where(
        qb.Pat(scenario, ScenarioName, name),
        qb.Or().
            Branch(qb.Subquery(statsQuery, scenario).BindTuple(taskCount)).
            Branch(qb.TupleGround(0).As(taskCount)),
    ).
    Build()
```

### Why Builder Pattern

1. **Consistent with qb style** - Everything else uses method chaining
2. **No explicit slice types** - `[]interface{}{}` completely eliminated
3. **Reads naturally** - "Or, branch A, branch B"
4. **Composable** - Nested Or is just another clause in a Branch
5. **No terminal method needed** - OrBuilder implements Clause directly

---

## Proposal 3: Subquery Variable Naming

### Non-Problem

This initially seemed like a problem but isn't. Consider this anti-pattern:

```go
// Anti-pattern: unique Datalog names for each subquery
t, s := qb.NewVar("t"), qb.NewVar("s")
t2, s2 := qb.NewVar("t2"), qb.NewVar("s2")
t3, s3 := qb.NewVar("t3"), qb.NewVar("s3")
```

This is unnecessary. **Datalog subqueries have lexical scoping.** The `?t` inside one subquery is completely independent of `?t` in another subquery.

Compare to the equivalent EDN, which reuses `?t` and `?s` freely:

```edn
(or [(q [:find (count ?t) (sum ?tok)
         :in $ ?s
         :where [?t :task/scenario ?s] ...]
        $ ?scenario) [[?taskCount ?totalTokens]]]
    [(ground [0 0]) [?taskCount ?totalTokens]])

(or [(q [:find (count ?t)
         :in $ ?s
         :where [?t :task/scenario ?s]
                [?t :task/key :scenario/opening] ...]
        $ ?scenario) [[?openingCount]]]
    [(ground 0) ?openingCount])
```

Both subqueries use `?t` and `?s`. No conflict because each subquery is its own scope.

### Correct Pattern

Reuse the same Datalog variable names. Reassign the Go variables between subqueries:

```go
// Task stats subquery
t, s := qb.NewVar("t"), qb.NewVar("s")
tok, dur := qb.NewVar("tok"), qb.NewVar("dur")

taskStatsSubquery := qb.Query().
    Find(qb.Count(t), qb.Sum(tok), qb.Sum(dur)).
    In(qb.DB, qb.Scalar(s)).
    Where(
        qb.Pat(t, attrTaskScenario, s),
        qb.Pat(t, attrTaskStatus, kwStatusComplete),
        qb.GetElse(t, attrTaskTokenCount, 0).As(tok),
        qb.GetElse(t, attrTaskDuration, 0).As(dur),
    )

// Opening count subquery - reassign Go vars, reuse Datalog names
t, s = qb.NewVar("t"), qb.NewVar("s")

openingCountSubquery := qb.Query().
    Find(qb.Count(t)).
    In(qb.DB, qb.Scalar(s)).
    Where(
        qb.Pat(t, attrTaskScenario, s),
        qb.Pat(t, attrTaskKey, kwScenarioOpening),
        qb.Pat(t, attrTaskStatus, kwStatusComplete),
    )
```

Both subqueries generate `?t` and `?s` in their EDN output. Datalog's lexical scoping keeps them separate.

### Key Insight

**Go variable uniqueness and Datalog variable uniqueness are separate concerns.**

- Go variables must be unique within Go scope (function/block)
- Datalog variables are scoped per subquery

You can reassign Go variables (`t, s = ...`) between subqueries while keeping the Datalog names (`?t`, `?s`) the same. This matches how you'd write it in EDN.

### Alternative: Helper Functions

For complex queries, extract subqueries into helper functions:

```go
func buildTaskStatsSubquery() *qb.QueryBuilder {
    t, s := qb.NewVar("t"), qb.NewVar("s")
    tok, dur := qb.NewVar("tok"), qb.NewVar("dur")

    return qb.Query().
        Find(qb.Count(t), qb.Sum(tok), qb.Sum(dur)).
        In(qb.DB, qb.Scalar(s)).
        Where(...)
}

func buildOpeningCountSubquery() *qb.QueryBuilder {
    t, s := qb.NewVar("t"), qb.NewVar("s")

    return qb.Query().
        Find(qb.Count(t)).
        In(qb.DB, qb.Scalar(s)).
        Where(...)
}
```

Each function has its own Go scope, and each subquery uses the natural `?t`, `?s` names.

### Recommendation

No API changes needed. Document the pattern:
1. Reuse Datalog variable names (`?t`, `?s`) across subqueries
2. Reassign Go variables between subquery definitions, or use helper functions
3. Don't create artificial unique names like `?t2`, `?t3`

---

## Summary

| Proposal | Complexity | Value | Recommendation |
|----------|------------|-------|----------------|
| QueryFor[T] with V()/Find() | Medium | High | Implement |
| Builder pattern for OR/OrJoin | Low-Medium | High | Implement |
| Subquery variable naming | None | High | Document the pattern |

### Implementation Order

1. **Builder pattern for OR/OrJoin** - Eliminates `[]interface{}{}`, consistent with qb style
2. **Subquery variable naming** - Documentation only, clarifies a common confusion
3. **QueryFor[T]** - Larger change, high value, implement after OR builder
