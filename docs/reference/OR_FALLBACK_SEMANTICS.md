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

The header decides the route. When a group of the enclosing scope binds a header symbol, the form is a **correlated union**: those groups are its subject, each branch runs once per window of the subject over the window's keys, each subject tuple takes every branch's answers for it, and the result stands in the consumed groups' place. When no group binds a header symbol, the subject is the unit relation and the form is an **uncorrelated union**: each branch runs once, and the union joins the enclosing scope as a new group. `or/complete` reports `semantics` as `correlated-union` or `union` accordingly.

### or-join

The header states the form's whole interface. It has two halves:

```clojure
(or-join [[?required ...] ?output ...] branch ...)
(or-join [?output ...] branch ...)
```

The required run, the nested vector, names the variables the enclosing scope binds before the form runs; every branch reads them bound. The outputs name the variables every branch binds and the form provides to the enclosing scope. A flat header is all outputs. Every other variable inside a branch is that branch's own, whatever reads it, and a name it shares with a variable outside the header unifies with nothing. A union of one branch is that branch over the header.

```clojure
;; ?e is an output: both branches bind it, and the enclosing scope reads it
(or-join [?e]
  [?e :user/status "active"]
  [?e :admin/status "enabled"])

;; ?a is required: the enclosing scope binds it, and the first branch reads it
(or-join [[?a] ?q]
  [?q ?a ?v]
  [?q :t/tag :t/none])
```

An output some branch does not bind is refused, naming the form, the branch by its position counted from one, and the variable, with the repair of moving the variable into the required run. A variable a branch reads through a predicate, an expression, a subquery input or a rule's required position, which neither the branch nor the header binds, is refused naming the form, the branch by its position counted from one, and the variable, with the repair of binding the variable in the branch or naming it in the header. Both checks run at every boundary, `ParseQuery`, `qb.Build`, `NewRuleSet` and the executor's entry, with the same message; the text boundaries read them with no rule set, under which an invocation provides every variable argument, and `Prepare` under the rule set the query runs with.

## Fallback Semantics: `(or-default ...)`

A janus-datalog extension for the "data with default" pattern. Branches are tried in order, and the first branch with a result wins, per tuple of the form's required run.

```clojure
;; Return "Unknown" if no name exists
(or-default [?e :user/name ?name]
            [(ground "Unknown") ?name])
```

A headerless `or-default` gets its header from the text, by the rule every headerless form follows. A variable inside the form is shared when it occurs elsewhere in its scope: in another clause of the enclosing list, or in the enclosing query's `:in`, `:find` or `:order-by`. A shared variable every branch binds is an output. Every other shared variable is required. Every other variable is local to the branch that holds it. In the example `?name` is an output, since both branches bind it, and `?e` is required, since the ground does not mention it: the header is `[[?e] ?name]`, and each entity gets its name or the default. An `or-default` whose variables occur nowhere else in its scope is refused naming the form.

Evaluation, per tuple of the required run:
1. If branch 1 has a result for the tuple, that result is the form's (short-circuit)
2. If branch 1 has none, try branch 2
3. Continue until a branch has a result or all branches are exhausted

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

Both branches bind `?count`, so it is an output; the subquery reads `?c` and the ground does not mention it, so `?c` is required. The header is `[[?c] ?count]`, and each category gets its count or zero.

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

or-default is non-monotone — which variables the fallback decision is grouped by changes the *results*, not just the plan. An `or-default-join` declares the run in its header, and an `or-default` gets it by the inference rule above; neither is read from what happens to be bound at execution. The checks on the written form:

- The shape, checked from the text alone: a non-empty header, at least two branches, and required and output sets disjoint.
- Every branch binds every output; a branch that does not is refused naming the form, the branch by its position counted from one and the output, with the repair of moving the output into the required run.
- A variable a branch reads through a predicate, an expression, a subquery input or a rule's required position, which neither the branch nor the header binds, is refused naming the form, the branch by its position counted from one and the variable, with the repair of binding the variable in the branch or naming it in the header.
- A per-group filter with no outputs is `or-join`'s job — with nothing to output, fallback-filter and union-filter are the same relation.

## Algebra IR Representation

| Clause | IR Node | Description |
|--------|---------|-------------|
| `(or ...)` / `(or-join ...)` | `Union` | Branch evaluation, correlated or not |
| `(or-default ...)` / `(or-default-join ...)` | `LateralUnion` | Correlated form, run over windows of its subject |
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
