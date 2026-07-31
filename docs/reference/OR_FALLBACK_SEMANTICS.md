# OR Clause Semantics

This document describes the OR clause variants and their semantics.

## Two Clause Types, Two Semantics

| Clause | Semantics | Behavior |
|--------|-----------|----------|
| `(or ...)` / `(or-join ...)` | **Union** (Datomic-compatible) | All branches execute, results merged |
| `(or-default ...)` / `(or-default-join ...)` | **Fallback** (janus extension) | Branches tried in order, first non-empty wins (per correlation group when `or-default-join` declares required vars) |

## Union Semantics: `(or ...)`

Standard Datalog/Datomic union. All branches are evaluated and results are combined.

```clojure
;; Returns ALL users who are active OR premium (both branches contribute)
(or [?e :user/active true]
    [?e :user/premium true])
```

Expression branches work in union mode — both data patterns and expressions contribute:

```clojure
;; Returns rooms sharing the area AND the area entity itself
(or [?related :entity/area ?target]
    [(identity ?target) ?related])
```

When branches reference symbols from the outer query context, the executor uses **correlated union**: each outer tuple evaluates all branches, and results are unioned per tuple.

When branches are independent (no outer symbol references), the executor uses **uncorrelated union**: branches execute independently and results are merged.

### or-join

Explicit join variables control which symbols are exposed:

```clojure
(or-join [?e]
  [?e :user/status "active"]
  [?e :admin/status "enabled"])
```

## Fallback Semantics: `(or-default ...)`

A janus-datalog extension for the "data with default" pattern. Branches are tried in order, and the first non-empty result is returned.

```clojure
;; Return "Unknown" if no name exists
(or-default [?e :user/name ?name]
            [(ground "Unknown") ?name])
```

Evaluation order:
1. If branch 1 returns results, return immediately (short-circuit)
2. If branch 1 is empty, try branch 2
3. Continue until a non-empty result or all branches exhausted

### Common Patterns

**Default value for missing attribute:**

```clojure
[:find ?name ?status
 :where [?e :user/name ?name]
        (or-default [?e :user/status ?status]
                    [(ground :status/unknown) ?status])]
```

**Count with zero default:**

```clojure
[:find ?category ?count
 :where [?c :category/name ?category]
        (or-default [(q [:find (count ?p)
                         :in $ ?c
                         :where [?p :product/category ?c]]
                        $ ?c) [[?count]]]
                    [(ground 0) ?count])]
```

**Multiple fallback levels:**

```clojure
(or-default [?e :user/nickname ?display]
            [?e :user/fullname ?display]
            [?e :user/email ?display]
            [(ground "Anonymous") ?display])
```

### or-default-join

The header declares the clause's complete interface. Its first element may be a nested vector of **required vars** — the per-group correlation keys, which the enclosing query must bind before the clause runs; the remaining symbols are **output vars**, which every branch must bind. Branch variables outside the header are locals: renaming them — even onto a name the outer query binds — does not change results.

```clojure
;; Per-entity fallback: for each ?e, take :source1/value, else :source2/value, else 0
(or-default-join [[?e] ?x]
  [?e :source1/value ?x]
  [?e :source2/value ?x]
  [(ground 0) ?x])
```

A flat header declares outputs only — the fallback decision is then **global**: branch 1 evaluates once, and only when it returns nothing does evaluation fall back.

```clojure
;; Global fallback: all configured limits, or the single default when none exist
(or-default-join [?limit]
  [?c :config/limit ?limit]
  [(ground 100) ?limit])
```

or-default is non-monotone — which variables the fallback decision is grouped by changes the *results*, not just the plan — so the correlation keys are declared syntax, never inferred from branch structure or from what happens to be bound at execution. Validation enforces the declaration loudly: at least one output, required and output sets disjoint, every branch binds every output, and any variable a branch consumes without binding must be a declared required var. A per-group filter with no outputs is `or-join`'s job — with nothing to output, fallback-filter and union-filter are the same relation.

## Algebra IR Representation

| Clause | IR Node | Description |
|--------|---------|-------------|
| `(or ...)` / `(or-join ...)` | `Union` | Branch evaluation, correlated or not |
| `(or-default ...)` / `(or-default-join ...)` | `LateralUnion` | Correlated per-tuple evaluation |
| `(or-default ...)` over a **correlated** subquery + ground | `LateralJoin` carrying `DefaultValues` | |
| `(or-default ...)` over an **uncorrelated** subquery + ground | `Join{Kind: LeftOuterJoin}` carrying `DefaultValues` | Non-matching outer tuples take the defaults |

A union whose branches contain correlated predicates (NOT, `missing?`) still
compiles to `Union`. What differs is the branch context: branches compile against
a `Project` schema placeholder so anti-joins see the outer symbols without
embedding outer scans, and the node's interface comes from the clause's canonical
scope rather than from the placeholder-inflated children.

The fallback machinery is not a valid target for union. Fallback short-circuits
per group and union does not, so encoding union through `LateralUnion` drops
tuples — `BUG_CORRELATED_ORJOIN_GLOBAL_FALLBACK_DROPS_TUPLES`.

## Comparison with Datomic

| Feature | Datomic | janus-datalog |
|---------|---------|---------------|
| `(or ...)` union | Yes | Yes (identical semantics) |
| `(or-join ...)` union | Yes | Yes (identical semantics) |
| Default values in query | No (application-level) | Yes via `(or-default ...)` |
| `get-else` for single attr | Yes | Yes |

Datomic users who need "default if missing" handle it in application code.
janus-datalog's `(or-default ...)` provides this within the query itself.
