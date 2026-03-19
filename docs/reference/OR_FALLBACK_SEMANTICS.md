# OR Clause Semantics

This document describes the OR clause variants and their semantics.

## Two Clause Types, Two Semantics

| Clause | Semantics | Behavior |
|--------|-----------|----------|
| `(or ...)` / `(or-join ...)` | **Union** (Datomic-compatible) | All branches execute, results merged |
| `(or-default ...)` / `(or-default-join ...)` | **Fallback** (janus extension) | Branches tried in order, first non-empty wins |

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

Explicit join variables for fallback:

```clojure
(or-default-join [?x]
  [?e :source1/value ?x]
  [?e :source2/value ?x]
  [(ground 0) ?x])
```

## Algebra IR Representation

| Clause | IR Node | Description |
|--------|---------|-------------|
| `(or ...)` / `(or-join ...)` | `Union` | Independent branch evaluation |
| `(or-default ...)` / `(or-default-join ...)` | `LateralUnion` | Correlated per-tuple evaluation |
| `(or-default ...)` with subquery+ground | `LateralJoin` | Correlated subquery with defaults |

When `(or ...)` contains correlated predicates (NOT, missing?) that require
outer context, the compiler detects this and routes to `LateralUnion` instead
of `Union`, since independent union branches cannot express anti-joins.

## Comparison with Datomic

| Feature | Datomic | janus-datalog |
|---------|---------|---------------|
| `(or ...)` union | Yes | Yes (identical semantics) |
| `(or-join ...)` union | Yes | Yes (identical semantics) |
| Default values in query | No (application-level) | Yes via `(or-default ...)` |
| `get-else` for single attr | Yes | Yes |

Datomic users who need "default if missing" handle it in application code.
janus-datalog's `(or-default ...)` provides this within the query itself.
