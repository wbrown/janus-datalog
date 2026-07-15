# Complex Query Computational Complexity

Data-complexity analysis for `BenchmarkComplexQueryCheckpoint`, captured against
v0.14.1. The query is fixed while database cardinalities grow.

## Main Result

Under the fixture's cardinality-one functional dependencies and ordinary hash
behavior, expected time is:

```text
Θ(T + S log L + U)
```

With a fixed limit and one argmax winner per scenario, this simplifies to
`Θ(T)`: a constant number of linear passes over task facts, not one complete task
scan per scenario.

Analysis snapshot:

- Measured latency: approximately 33 ms.
- Application datoms: approximately 45,000.
- Subqueries: 4, each executed once.
- Fallback caches: 5, each built once.

## Query Under Test

`BenchmarkComplexQueryCheckpoint` runs this query with `:limit 25`:

```clojure
[:find ?scenario ?title ?createdAt ?taskCount ?totalTokens ?totalDuration ?complete ?lastKey ?lastUpdatedAt
  :where
  [?scenario :entity/type :entity.type/scenario]
  [?scenario :scenario/title ?title]
  [?scenario :scenario/created-at ?createdAt]
  (or-default [(q [:find (count ?t) (sum ?tok) (sum ?dur)
           :in $ ?s
           :where [?t :task/root ?s]
                  [?t :task/status :status/complete]
                  [(get-else $ ?t :task/token-count 0) ?tok]
                  [(get-else $ ?t :task/duration 0) ?dur]]
          $ ?scenario) [[?taskCount ?totalTokens ?totalDuration]]]
      [(ground [0 0 0]) [[?taskCount ?totalTokens ?totalDuration]]])
  (or-default [(q [:find (count ?t)
           :in $ ?s
           :where [?t :task/root ?s]
                  [?t :task/key :task/opening]
                  [?t :task/status :status/complete]]
          $ ?scenario) [[?openingCount]]]
      [(ground 0) ?openingCount])
  [[(> ?openingCount 0)] ?complete]
  (or-default [(q [:find ?key ?ca
           :in $ ?s
           :where [?t :task/root ?s]
                  [?t :task/status :status/complete]
                  [?t :task/completed-at ?ca]
                  [?t :task/key ?key]
                  [(q [:find (max ?ca)
                       :in $ ?s
                       :where [?t :task/root ?s]
                              [?t :task/status :status/complete]
                              [?t :task/completed-at ?ca]]
                      $ ?s) [[?maxCa]]]
                  [(= ?ca ?maxCa)]]
          $ ?scenario) [[?lastKey ?lastUpdatedAt]]]
      [(ground [:none :none]) [[?lastKey ?lastUpdatedAt]]])
  :order-by [[?lastUpdatedAt :desc]]
  :limit 25]
```

## Logical Rendering After Algebra Optimization

The default algebra passes decorrelate the aggregate subqueries and rewrite
`get-else` expressions as left outer joins. Algebra is then lowered back into
Datalog before physical phase planning. Whitespace and symbol names are
normalized below, but this is the equivalent decompiled shape:

```clojure
[:find ?scenario ?title ?createdAt
       ?taskCount ?totalTokens ?totalDuration
       ?complete ?lastKey ?lastUpdatedAt
 :where
 [?scenario :entity/type :entity.type/scenario]
 [?scenario :scenario/title ?title]
 [?scenario :scenario/created-at ?createdAt]

 (or-default-join [?scenario]
   [(q [:find ?scenarioInput
              (count ?task) (sum ?tokens) (sum ?duration)
        :in $
        :where
        [?task :task/root ?scenarioInput]
        [?task :task/status :status/complete]
        (or-default-join [?task]
          [?task :task/token-count ?tokens]
          [(get-else $ ?task :task/token-count 0) ?tokens])
        (or-default-join [?task]
          [?task :task/duration ?duration]
          [(get-else $ ?task :task/duration 0) ?duration])]
       $)
    [[?scenario ?taskCount ?totalTokens ?totalDuration] ...]]
   [(ground [0 0 0])
    [[?taskCount ?totalTokens ?totalDuration]]])

 (or-default-join [?scenario]
   [(q [:find ?scenarioInput (count ?task)
        :in $
        :where
        [?task :task/root ?scenarioInput]
        [?task :task/key :task/opening]
        [?task :task/status :status/complete]]
       $)
    [[?scenario ?openingCount] ...]]
   [(ground 0) ?openingCount])

 [[(> ?openingCount 0)] ?complete]

 (or-default-join [?scenario]
   [(q [:find ?scenarioInput ?key ?completedAt
        :in $
        :where
        [?task :task/root ?scenarioInput]
        [?task :task/status :status/complete]
        [?task :task/completed-at ?completedAt]
        [?task :task/key ?key]
        [(q [:find ?scenarioInput (max ?candidateCompletedAt)
             :in $
             :where
             [?candidate :task/root ?scenarioInput]
             [?candidate :task/status :status/complete]
             [?candidate :task/completed-at ?candidateCompletedAt]]
            $)
         [[?scenarioInput ?maxCompletedAt] ...]]
        [(= ?completedAt ?maxCompletedAt)]]
       $)
    [[?scenario ?lastKey ?lastUpdatedAt] ...]]
   [(ground [:none :none])
    [[?lastKey ?lastUpdatedAt]]])

 :order-by [[?lastUpdatedAt :desc]]
 :limit 25]
```

The important structural changes are:

1. Each correlation input moves from subquery `:in` to subquery `:find`.
2. Aggregate subqueries group by that input and return a relation containing all
   scenarios in one execution.
3. The outer query joins those relations back on `?scenario`.
4. Defaults become `or-default-join` branches over the explicit join key.
5. The two `get-else` expressions become indexed attribute scans with typed
   fallback branches.
6. The nested maximum becomes a grouped relation joined into the argmax query.

## Variables

| Symbol | Meaning | Fixture |
|:------:|---------|--------:|
| `S` | Number of scenarios | 75 |
| `T` | Number of tasks | 7,500 |
| `K` | Tasks per scenario; `T = S·K` | 100 |
| `D` | Application datoms in fixture | ≈45,225 |
| `H` | Raw historical operation records in relevant index ranges | ≈`D` here |
| `U` | Argmax rows after ties | 75 here; ≤`T` |
| `L` | Requested result limit | 25 |

## Operation-by-Operation Cost

| Stage | Relational work | Expected time | Per-query space |
|-------|-----------------|---------------|-----------------|
| Outer scenarios | Scan scenario type; attach title and creation time | `Θ(S)` | `O(S)` result |
| Grouped task statistics | Count tasks; sum tokens and durations; resolve two defaults | `Θ(T)` expected | `O(S)` aggregate state; joins may build `O(T)` |
| Opening count | AVET lookup for `:task/opening`, then root/status checks | `Θ(S)` in fixture; `O(T)` worst selective match | `O(S)` |
| Maximum completion | Scan completed tasks and group `max(completed-at)` by scenario | `Θ(T)` | `O(S)` aggregate state |
| Argmax join-back | Join each scenario maximum back to task completion/key facts | `Θ(T + S + U)` expected | `O(T + S)` hash/stream state |
| Fallback probes | Build five branch caches once; probe by outer scenario | `O(T + S)` across branches | `O(T + S)`, implementation-dependent |
| Top 25 | Bounded heap over final scenario rows | `O(S log L)` | `O(L)` |

## Expected Bound for This Data Model

The general expected time bound is:

```text
Time = O(H_relevant + T + S log L + U)
```

With one stored version, fixed task attributes, bounded fanout, one argmax
winner, and constant `L`:

```text
Time = Θ(T)
```

The conservative current space bound is:

```text
Space = O(T + S + L)
```

This accounts for current hash joins and fallback caches. Grouped aggregate
state alone is `O(S)`, while Top-N state is `O(L)`.

## What Decorrelation Changes

With an AVET lookup on the bound scenario, correlated execution is already
linear in the balanced fixture:

```text
S · O(K + log H) = O(T + S log H)
```

Janus moves the scenario input into the aggregate grouping key, executes each
subquery once, and joins grouped results back:

```text
O(T + S)
```

The key gain is fewer executions, scans, setup operations, and repeated nested
work—not automatically a better asymptotic bound. If a correlated inner clause
lacks an indexable binding, its alternative can degrade toward `O(S·T)`. The
current test pins four total subquery executions.

## Worst-Case Qualifications

The linear bound depends on the query's functional dependencies. Connected joins
can still fan out even though disjoint Cartesian products are rejected.

| Condition | Effect |
|-----------|--------|
| Many-to-many join fanout | Add output cardinality `R`: expected hash-join time becomes `O(n + m + R)`; `R` can be quadratic. |
| Tied maximum completion times | Argmax output `U` may grow from `S` to `T`. |
| Long append-only history | Broad current-state scans may traverse `H` raw operation records even when fewer current facts survive CRDT resolution. |
| Cold entity/attribute cache | Point attachments include LSM seek costs, approximately `O(log H)` per uncached lookup. |
| Pathological hash collisions | Expected `O(n + m + R)` hash behavior can degrade, though this is not the operational model. |

## Interpreting Benchmark Counters

The captured run measured approximately `52.1 MB/op` and `648.6K allocs/op`.
Go's `B/op` is cumulative heap allocation traffic during one query. It is not
peak live memory, retained database size, or resident-set growth.

Likewise, `allocs/op` counts allocation events, not simultaneously live domain
objects.

## Interpretation

This benchmark primarily tests whether nested correlated semantics can be
converted into a small fixed number of linear relational passes. At
approximately 33 ms, the implementation exhibits the intended linear
data-complexity behavior with substantial but bounded allocation traffic.
