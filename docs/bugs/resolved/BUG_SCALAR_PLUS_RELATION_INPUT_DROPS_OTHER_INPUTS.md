# Bug: Scalar (or other) `:in` Input Dropped When Combined With a Relation Input

**Status**: Resolved (2026-05-25) — see Resolution at the end
**Discovered**: 2026-05-25

## Symptoms

A query that combines a **scalar** `:in` input with a **relation** input
(`[[?a ?b] ...]`), where the scalar's variable is used in a `:where` clause,
returns **zero rows**. The relation-only form and the scalar-only form each work
in isolation — only the combination fails.

```clojure
;; Returns 0 rows (expected 1):
[:find ?e
 :in $ ?grp [[?name ?owner] ...]
 :where [?e :t/grp ?grp]
        [?e :t/name ?name]
        [?e :t/owner ?owner]]
;; args: grp, [][]any{{"A", ownerX}}

;; Works (relation only):
[:find ?e :in $ [[?name ?owner] ...] :where [?e :t/name ?name] [?e :t/owner ?owner]]

;; Works (scalar only):
[:find ?e :in $ ?owner :where [?e :t/owner ?owner]]
```

It is not specific to value type: the dropped scalar can be a string, int,
`datalog.Keyword`, or `datalog.Identity` (ref). It is the *combination* —
scalar-then-relation — that fails, regardless of clause shape (`(not …)`,
wildcard `_`, finding the input vars, etc. all reproduce).

Reproduced by `TestRelationInput_RefAndKeywordColumns` in
`datalog/storage/query_inputs_test.go` (the "scalar+relation" sub-checks).
`TestJoin_NoSharedColumns_CrossProduct` (`datalog/executor`) confirms the cross
product itself is correct — the bug is upstream in input binding, not the join.

## Root Cause

A query containing a `RelationInput` takes a dedicated iteration path —
`Executor.ExecuteRealized` routes to `executeRealizedWithRelationInputIteration`,
which runs the plan once per relation tuple (`executor/executor.go:211`):

```go
if hasRelationInput(plan.Query) && len(inputRelations) > 0 {
    return e.executeRealizedWithRelationInputIteration(ctx, plan, inputRelations)
}
```

### 1. The iteration builds per-tuple inputs from the relation only (executor/executor.go)

In `executeRealizedWithRelationInputIterationSequential` (and its `…Parallel`
twin), the per-tuple input relations are built **solely** from
`relationInput.Symbols`. The other input relations in `inputRelations` — the
scalar `?grp`/`?root`, any collections, etc. — are never forwarded:

```go
for it.Next() {
    tuple := it.Tuple()

    // Create scalar input relations for this tuple
    var tupleInputRelations []Relation
    for i, sym := range relationInput.Symbols {   // <-- ONLY the relation's columns
        if i < len(tuple) {
            tupleInputRelations = append(tupleInputRelations,
                NewMaterializedRelation([]query.Symbol{sym}, []Tuple{{tuple[i]}}))
        }
    }
    result, err := e.executeRealizedNonIterating(ctx, plan, tupleInputRelations, relationInput)
    // ...
}
```

`inputRelations` (which holds `[rel(?grp), rel(?name,?owner)]`) is a parameter to
this function but is otherwise unused — the scalar is silently discarded.

### 2. The per-tuple query keeps the scalar spec, causing positional misalignment (executor/executor.go)

`executeRealizedNonIterating` rebuilds the query, replacing the `RelationInput`
with one `ScalarInput` per relation symbol but **keeping every other input spec**:

```go
for _, input := range modifiedQuery.In {
    if _, isRelInput := input.(query.RelationInput); isRelInput {
        for _, sym := range relationInput.Symbols {
            newIn = append(newIn, query.ScalarInput{Symbol: sym})
        }
    } else {
        newIn = append(newIn, input)   // <-- keeps ScalarInput(?grp)
    }
}
modifiedQuery.In = newIn
boundRelation := BindQueryInputs(&modifiedQuery, scalarInputRelations)
```

So for `:in $ ?grp [[?name ?owner] ...]` the modified `:in` becomes
`[$, ScalarInput(?grp), ScalarInput(?name), ScalarInput(?owner)]`, but
`scalarInputRelations` passed from the iterator is only `[rel(?name), rel(?owner)]`.
`BindQueryInputs` consumes inputs by positional `relationIndex`, so:

- `ScalarInput(?grp)`   ← `scalarInputRelations[0]` = `rel(?name)` → **`?grp` bound to the name value (wrong)**
- `ScalarInput(?name)`  ← `scalarInputRelations[1]` = `rel(?owner)` → **`?name` bound to the owner value (wrong)**
- `ScalarInput(?owner)` ← index out of range → **skipped, `?owner` unbound**

The `:where` clauses then join on garbage/unbound variables and match nothing →
empty result. A scalar placed *after* the relation shifts differently but is
equally broken; the invariant — non-relation inputs must survive the iteration —
is violated either way.

## Fix Direction

Forward the non-iteration input relations through the relation-input iteration so
each per-tuple execution binds them **alongside** the relation tuple's scalarized
values, keeping `modifiedQuery.In` and the passed `scalarInputRelations` aligned
(same order, same count). The cross-product machinery already handles the
no-shared-variable combination correctly once the relations all reach
`BindQueryInputs`.

## Impact

Any query mixing a scalar (or other non-relation) `:in` input with a relation
input silently returns empty — e.g. a root-scoped batch lookup
`:in $ ?root [[?key ?subject] ...]`. Discovered while implementing a batched
task-content read (`BatchGetTaskContents`) in the narrative-generators project,
which returned 0 rows for exactly this query shape.

## Test Coverage (to add with the fix)

- `datalog/storage/query_inputs_test.go` — `TestRelationInput_RefAndKeywordColumns`
  (scalar+relation, identity and keyword columns; already present, currently red).
- Library-level cases for `scalar + relation`, `collection + relation`,
  `tuple + relation`, and relation followed by scalar — each must bind both.
- `datalog/executor` — direct coverage that the iteration path forwards
  non-iteration inputs per tuple.

## Files (to change with the fix)

1. `datalog/executor/executor.go` — `executeRealizedWithRelationInputIterationSequential`,
   `executeRealizedWithRelationInputIterationParallel`, and
   `executeRealizedNonIterating` (thread non-iteration inputs through; align specs).
2. `datalog/storage/query_inputs_test.go` / `datalog/executor/*_test.go` — tests above.

---

## Resolution (2026-05-25)

**Resolved**, exactly as the Fix Direction prescribed: the relation-input
iteration now forwards the non-iteration input relations through each per-tuple
execution, keeping them aligned with `executeRealizedNonIterating`'s in-place `:in`
rewrite.

- `datalog/executor/executor.go`: `executeRealizedWithRelationInputIteration` now
  records the iteration relation's index within `inputRelations` (`iterationIndex`)
  and threads it into the sequential and parallel paths. A new helper,
  `perTupleInputRelations`, builds the per-tuple input list — every original input
  relation is forwarded in its original slot, and only the iteration relation's
  slot is replaced by one single-value relation per `RelationInput` symbol drawn
  from the tuple. Both paths call it instead of building inputs from the relation's
  columns alone.

### Tests

- `datalog/storage/query_inputs_test.go` — `TestRelationInput_RefAndKeywordColumns`:
  scalar+relation (find `?e`, find input vars, wildcard `_`, `(not …)`),
  relation+scalar (order independence), collection+relation, and ref/keyword
  column types. Red before the fix, green after.
- `datalog/executor/multi_tuple_binding_test.go` —
  `TestJoin_NoSharedColumns_CrossProduct`: confirms the no-shared-column cross
  product is correct, isolating the bug to input binding rather than the join.

### Provenance

Diagnosed and fixed in a downstream GOPATH checkout (at PR #75) while building a
batched task-content read in the narrative-generators project, then ported to
`main` on top of the current `executor.go` — preserving the iterator-error
handling that `BUG_ITERATOR_ERRORS_DROPPED_IN_MATERIALIZATION_PATHS` added to the
same parallel function in the interim.

### Verification

`go build ./...`, `go vet ./...`, and `go test -count=1 ./...` all green.
