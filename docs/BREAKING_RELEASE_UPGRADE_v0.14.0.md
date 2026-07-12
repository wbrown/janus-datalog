# v0.14.0 Breaking Release Upgrade Guide

Version `v0.14.0` intentionally removes retired execution APIs and changes
matcher, relation, arithmetic, and physical-storage contracts. It does not
provide V2 interfaces or compatibility shims.

## Required source migrations

### Pattern matchers

`PatternMatcher.Match` and `PredicateAwareMatcher.MatchWithConstraints` now
receive a one-pattern `*query.Query`, not a `*query.DataPattern`.

```go
func (m *Matcher) Match(q *query.Query, bindings executor.Relations) (executor.Relation, error) {
    pattern, err := q.SingleDataPattern()
    if err != nil {
        return nil, err
    }
    // Match pattern. q.OrderBy and q.Limit may contain safe physical requests.
}
```

Use `query.PatternQuery(pattern)` when adapting an existing call site.

### Entity lookup

Lookup failures are no longer represented as attribute absence:

```go
LookupAttribute(
    entity datalog.Identity,
    attr datalog.Keyword,
) (value interface{}, found bool, err error)
```

Return `(nil, false, nil)` only for a genuinely absent attribute. Propagate
storage, decode, and close failures through `err`.

### Relations

External `executor.Relation` implementations must add:

```go
Properties() executor.RelationProperties
```

Return `executor.RelationProperties{}` when no ordering or candidate-key
guarantee is proven. Do not infer properties from sampled rows.

### Arithmetic

`query.ArithmeticFunction` now stores `Args []query.Term`; `Left` and `Right`
were removed. Arithmetic accepts one or more operands. Unary subtraction
negates, unary division computes a reciprocal, and longer forms reduce
left-to-right. Zero-argument functions remain invalid.

The query builder functions `qb.Add`, `Sub`, `Mul`, and `Div` take a required
first operand followed by variadic operands. Existing two-operand calls remain
valid.

### Planner and executor cleanup

The supported planner interchange is `planner.RealizedPlan` /
`planner.RealizedPhase`, whose phases carry Datalog in `RealizedPhase.Query`.
Legacy `QueryPlan`, `Phase`, planner metadata records, predicate-classifier,
componentized-subquery, worker-pool, and streaming-union APIs were removed.

Removed debug options are replaced by annotation handlers. Removed context
metadata/micro-operation methods have no replacement; execution context now
exposes lifecycle, join/collapse observation, collectors, and scan registries.

### Binary key APIs

`storage.NewBadgerStore` and `storage.DatomFromKey` now take
`*storage.BinaryKeyEncoder`. `KeyEncoder`, encoding strategies, and
`L85KeyEncoder` were removed.

## Legacy L85 physical databases

Normal databases created through `db.Open` or `storage.NewDatabase` already use
binary physical keys and remain byte-compatible.

Databases explicitly written with the retired L85 physical-key encoder are not
safe to open with `v0.14.0`. There is no in-place migration and no reliable
format marker that can distinguish every legacy directory before scanning.

Migrate before upgrading:

1. Open the legacy directory with the old release.
2. Export it to EDN.
3. Create a fresh database directory with `v0.14.0`.
4. Import the EDN into the fresh database.
5. Archive or remove the legacy directory from service; never open it with
   `v0.14.0` or mix binary writes into it.

L85 remains supported in identities and EDN export/import. Only L85 physical
index keys were removed.
