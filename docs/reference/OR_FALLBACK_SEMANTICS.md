# OR Clause Fallback Semantics

This document describes the extended OR clause semantics that allow expressions (including `ground` and arithmetic) to be used as fallback values when pattern branches return no results.

## Overview

Standard Datalog OR clauses use **union semantics**: all branches are executed and results are merged. This works well for pattern matching but doesn't support providing default values when queries return empty.

When an OR clause contains expression branches (e.g., `ground`, arithmetic), janus-datalog switches to **fallback semantics**: branches are tried in order, and the first non-empty result is returned.

## Motivation

Consider a scenario where you want to count tasks but return 0 when none exist:

```clojure
;; Standard subquery - returns empty when no tasks match
[(q [:find (count ?t)
     :in $ ?scenario
     :where [?t :task/scenario ?scenario]
            [?t :task/status :status/complete]]
   $ ?scenario) [[?count]]]
```

When no matching tasks exist, this subquery returns empty (no rows), causing the outer query row to be excluded entirely.

With OR fallback semantics, you can provide a default:

```clojure
(or [(q [:find (count ?t) ...] $ ?scenario) [[?count]]]
    [(ground 0) ?count])
```

Now when the subquery returns empty, the ground expression provides `0` as the fallback.

## Semantics

### Pattern-Only OR (Union Semantics)

When all branches contain only patterns, standard Datalog union semantics apply:

```clojure
;; Union: returns ALL users who are either active OR premium
(or [?e :user/active true]
    [?e :user/premium true])
```

Both branches execute, and results are merged (deduplicated on common columns).

### OR with Expressions (Fallback Semantics)

When any branch contains an expression, fallback semantics apply:

```clojure
;; Fallback: returns first non-empty result
(or [?e :user/name ?name]           ;; Try pattern first
    [(ground "Unknown") ?name])      ;; Fallback if no match
```

Branches are tried in order:
1. If branch 1 returns results, return immediately
2. If branch 1 is empty, try branch 2
3. Continue until a non-empty result or all branches exhausted

## Supported Expression Types

The following expressions can be used in OR branches:

### Ground Expressions

Bind a constant value to a variable:

```clojure
(or [?e :config/value ?v]
    [(ground "default") ?v])
```

### Arithmetic Expressions

Compute values when patterns don't match:

```clojure
(or [?e :item/discount ?discount]
    [(* ?price 0) ?discount])  ;; 0 discount if none specified
```

### Subquery Expressions

Use subqueries as branches with fallbacks:

```clojure
(or [(q [:find (sum ?amount) :where ...] $) [[?total]]]
    [(ground 0) ?total])
```

## Examples

### Default Value for Missing Attribute

```clojure
[:find ?name ?status
 :where [?e :user/name ?name]
        (or [?e :user/status ?status]
            [(ground :status/unknown) ?status])]
```

### Count with Zero Default

```clojure
[:find ?category ?count
 :where [?c :category/name ?category]
        (or [(q [:find (count ?p)
                 :in $ ?c
                 :where [?p :product/category ?c]]
               $ ?c) [[?count]]]
            [(ground 0) ?count])]
```

### Multiple Fallback Levels

```clojure
;; Try multiple sources in priority order
(or [?e :user/nickname ?display]
    [?e :user/fullname ?display]
    [?e :user/email ?display]
    [(ground "Anonymous") ?display])
```

## OR-JOIN with Fallback

The same semantics apply to `or-join` when expressions are present:

```clojure
(or-join [?x]
  [?e :source1/value ?x]
  [?e :source2/value ?x]
  [(ground 0) ?x])
```

## Implementation Notes

- Fallback semantics require materializing branch results to check for emptiness
- Pattern-only OR continues to use efficient union/streaming semantics
- Detection is automatic based on branch contents - no explicit syntax needed
- Predicates are also supported within expression branches

## Comparison with Datomic

Datomic's OR clauses use pure union semantics and do not support this fallback pattern directly. The workaround in Datomic is typically application-level code:

```clojure
;; Datomic workaround
(or (seq (d/q [:find ...] db))
    [[0]])  ;; application-level default
```

janus-datalog's fallback semantics provide this capability within the query itself, enabling more declarative queries.
