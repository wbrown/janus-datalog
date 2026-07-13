# Bug: `:order-by` Variables Not in `:find` Are Silently Ignored

**Status**: FIXED (2026-07-04). Parser
validation, planner retention, executor sort-strip finalization, and the
`SortRelation` deferred-error guard are in place; the full coverage matrix
below is green, including cases N/P — pull + order-by is green **by
design** since the pull relocation (`BUG_PULL_WITH_ORDER_BY_PANICS.md`,
also fixed): pulls render at the result boundary after sort/strip/limit,
so entity bindings are `Identity` through every relational operation.
`go test -count=1 ./...` passes.
**Discovered**: 2026-07-04

## Symptoms

A query that orders by a variable bound in `:where` but not projected in
`:find` returns its results in **arbitrary order**, reported as success. No
error or warning surfaces at parse time, plan time, or execution time.

```clojure
;; Intends "tasks by ascending priority". Actually returns arbitrary order:
[:find ?task
 :where [?task :task/status :pending]
        [?task :task/priority ?p]
 :order-by [[?p :asc]]]
```

With a single order-by clause the sort is a complete no-op. With multiple
clauses, resolvable keys still sort but each non-projected key silently drops
out of the comparison, so the result is ordered by a *different* key sequence
than the query states.

The failure mode is nastiest in combination with:

- **`:limit`** — `ExecuteRealized` sorts then limits
  (`datalog/executor/executor.go:231-236`), so "top N by ?p" becomes
  "arbitrary N rows".
- **First-row consumers** (`QueryOneInto`, scalar `.` find specs) — "the row
  with the highest/lowest ?p" becomes "whatever row materialization produced
  first".

Because the order is whatever the underlying iteration happened to produce, it
can look correct on small or freshly-written databases and only visibly
misbehave later — a silent contract violation.

Reproduced by `TestOrderByNonProjectedVariable`,
`TestOrderByMixedProjectedAndNonProjectedKeys`,
`TestOrderByNonProjectedVariableWithLimit`, and
`TestOrderByUnboundVariableIsError` in
`datalog/executor/executor_sorting_test.go` (currently red; they pin the
intended behavior). The observed failures match the root cause exactly: the
asc and desc runs return the *same* arbitrary order (the sort is a complete
no-op, direction has no effect), the mixed-key run sorts the projected `?dept`
key but ignores the non-projected `?age` key, and the unbound-variable query
succeeds without error.

## Root Cause

Three layers each assume another layer handled it; none does.

### 1. The executor projects to `:find` before sorting (`datalog/executor/executor.go`)

`ExecuteRealized` applies `:order-by` at the single outer boundary, on the
fully-assembled result:

```go
result, err := e.executeRealizedPlan(ctx, plan, inputRelations)
...
if len(plan.Query.OrderBy) > 0 {
    result = result.Sort(plan.Query.OrderBy)
}
```

(`executor.go:227-233`). But by that point the last phase's output has already
been projected down to the `:find` symbols (`executor.go:393-395` — "Skip for
last phase - QueryExecutor already projected to :find symbols"). Any order-by
variable bound only in `:where` is structurally gone from the relation before
the sort ever sees it.

### 2. `SortRelation` skips unresolvable sort keys (`datalog/executor/executor_utils.go`)

`SortRelation` resolves each order-by variable against the relation's attributes;
a variable that isn't among them resolves to index `-1` (`:119-129`) and the
comparator silently skips it:

```go
if sortIndices[k] < 0 {
    // Variable not in results, skip
    continue
}
```

(`executor_utils.go:134-137`). With a single order-by clause every comparison
falls through to `return false`, so `sort.Slice` leaves the tuples in
materialization order — unsorted, no error.

### 3. Nothing validates the clause anywhere in the pipeline

- `parseOrderByClause` checks shape only — symbol-or-vector, `:asc`/`:desc`
  (`datalog/parser/parser.go:166-217`).
- The end-of-parse validation checks `:find`, `:where`, and the
  scalar-vs-`:limit` contradiction, but never touches `OrderBy`
  (`parser.go:149-163`).
- `ValidateQuery` (`parser.go:1266-1289`) checks find variables against where
  variables but neither inspects `OrderBy` nor has any production caller.

The behavior is designed-in, not an oversight in a single function: the
archived implementation plan explicitly specifies "Variables not in find
clause (should be ignored)"
(`docs/archive/completed/order-by-implementation-plan.md:241`, with the
skip logic sketched at `:110-120`). The only test touching the case —
"Sort with missing sort symbol" in
`datalog/executor/executor_sorting_test.go:159-167` (runner `:181-204`) —
asserts nothing beyond "doesn't crash", tolerating parse errors and execution
errors alike.

## Fix Direction

Support ordering by any variable bound in `:where` — this matches what
affected call sites already simulate by hand (projecting the sort variable
into `:find` solely so `SortRelation` can see it, then reading only the data
result attributes; the existence of that workaround pattern is the evidence the
capability is wanted):

1. **Retain order-by variables through the final projection.** The last
   phase's projection keeps `:find` symbols ∪ `:order-by` symbols, the sort
   runs, then the extra order-by-only attributes are stripped before results are
   returned. Sorting must still precede `:limit` so global top-N stays
   correct.
2. **Reject unbound order-by variables.** A variable that is bound nowhere in
   the query (not in `:find`, not in `:where`, not an `:in` parameter) should
   be a parse/validation error, not a silent skip. The `sortIndices[k] < 0`
   skip in `SortRelation` should become unreachable for well-formed queries —
   consider making it an error rather than a `continue`.
3. **Update the archived plan doc** — annotate the "should be ignored" line
   (`order-by-implementation-plan.md:241`) as superseded so it doesn't read as
   the current contract.

## Design Decision: Valid Sort Keys (2026-07-04)

The fix direction above says "any variable bound in `:where`". This section
records the full decision for every binding class, derived from Datalog and
relational-algebra first principles rather than implementation convenience.
The question: which variables may an `:order-by` clause reference, and what
happens for each class?

### The formal grounding

Pure Datalog and pure relational algebra have no notion of order — results
are sets. `:order-by` is therefore an extra-relational operator, and the
theoretically clean way to graft it on (the one SQL:1999 standardized after
SQL-92's select-list-only restriction proved too strict) is:

```
π_find ( τ_keys ( satisfying-assignments(body) ) )
```

Sort the relation of satisfying assignments *before* the final projection;
the sort keys may reference any attribute of that pre-projection relation.
This composition is exactly the retain–sort–strip mechanism of the fix, and
it fixes the evaluation order: sort, then strip to `:find`, then `:limit` —
all at the single outer boundary (`ExecuteRealized`), so global top-N stays
correct and the inline and RelationInput-iteration paths behave identically.

Two further pieces of theory decide the `:in` cases:

1. **Inputs are EDB relations.** A parameterized Datalog query is formally a
   query over an extended EDB: `:in $ ?min` is a singleton relation `Min(x)`
   joined into the body; `:in $ [[?a ?b] ...]` is a binary EDB relation.
   Input variables are therefore positively bound body variables.
2. **Safety (range restriction).** Every variable a query references must be
   positively bound in body ∪ inputs. A variable bound nowhere fails safety —
   the same rule that makes an unbound `:find` variable an error.

One distinction matters throughout: **bound *by* `:where`** (a pattern
binding position, an expression output, a subquery binding — the variable
becomes an attribute of the satisfying-assignment relation) versus merely
**mentioned in `:where`** (a predicate argument like `?min` in
`[(> ?age ?min)]` is consumed as a filter and never becomes a relation attribute). This
is the environment-vs-data type system of
`docs/INPUT_PARAMETER_SEMANTICS.md` applied to sort keys. Validation must
use a provides-style extraction, not naive variable mention.

### The decisions

**1. Bound by `:where` → supported (retain–sort–strip).** This includes
variables whose *values* arrive from collection or relation inputs but that
are bound per-row by a `:where` pattern — e.g. `?k` in
`:in $ [?k ...] :where [?e :item/key ?k]` is a genuine per-tuple binding and
sorts normally.

**2. Scalar or tuple `:in` constant → accepted as a well-defined no-op.**
Theory: the input is a singleton attribute; every row carries the same
value, so every comparison is equal and the sort is the identity permutation
(under a stable sort). SQL agrees: `ORDER BY <constant/parameter>` is legal
and a no-op. Rejecting it was considered and discarded — that would be a
lint, not semantics. The executor drops such clauses at the finalization
boundary (they are provably identity) and emits an annotation event so the
drop is observable rather than silent. This holds regardless of whether the
scalar is also pattern-bound (the relation attribute exists but is constant — dropping
is still exact) and regardless of aggregation.

**3. Relation/collection input symbol *not* bound by `:where` → explicit
"not supported" error.** Theoretically this one *is* sortable:
`τ_a(body ⋈ R)` is well-formed, and for a relation input the values vary
across the union of per-tuple executions ("order output blocks by their
driving input"). But janus executes relation inputs by per-tuple iteration
(deliberately, for aggregation scoping), so per-execution results carry no
such relation attribute; supporting it would require tagging each per-tuple result with
its driving input values — new machinery for an exotic case. Deliberately
deferred, not silently skipped: the error message names both escape hatches
(bind the variable in `:where`, or project it in `:find` — input parameters
are projectable, at which point it is ordinary data).

**4. Bound nowhere → parse error.** Safety violation; theory-mandated, not
defensive ergonomics.

**5. Aggregate queries → sort keys must be find variables (the group
keys).** Aggregation produces a *new* relation whose attributes are the
group keys and the aggregate values; a pre-aggregation variable is not an
attribute of that relation, so ordering by it is ill-formed. Ordering by the
aggregate *value* itself is not currently expressible: aggregate output
attributes are named by the expression string (`(max ?age)` —
`aggregation.go:565-573`) and `:order-by` syntax accepts only variables.
Supporting it would require order-by expressions or aggregate aliasing — a
separate feature, out of scope here.

### The set-vs-bag wrinkle

Sort-before-project is unambiguous only under bag semantics. Under set
semantics projection is many-to-one: if "Alice" rows carry ages 20 and 90,
`sort by ?age, project ?name, dedupe` cannot say where Alice goes — which is
exactly why SQL forbids `SELECT DISTINCT ... ORDER BY <non-selected>`.

**janus has set semantics** — established empirically by the coverage audit
(case M returned 2 rows for 3 satisfying assignments), then confirmed in
code: `NewMaterializedRelation`/`NewMaterializedRelationWithOptions`
deduplicate at creation (`relation.go:367-388` →
`deduplicateTuples`, `:421-439`). An earlier draft of this section claimed
`Project` preserves row count with no deduplication; that was wrong —
`Project`'s row loop copies all rows, but it constructs its result through
the deduplicating constructor. Two consequences, both resolved by decisions
already latent in this design:

1. **"First occurrence in sorted order wins."** `deduplicateTuples` keeps
   the first occurrence in input order, so projecting *sorted* tuples
   through the standard constructor produces exactly the decided resolution
   — sorted rows `(Alice,10) (Bob,20) (Alice,30)` strip to `[Alice, Bob]`.
   No new machinery.
2. **Sort-time deduplication panics on pulled tuples — open, pending its
   own design decision.** Pull results contain `map[string]interface{}`
   values, which `executePulls` deliberately exempts from deduplication
   (maps are not comparable — it bypasses the constructor,
   `query_executor.go:1378-1381`), and `SortRelation`'s deduplicating
   construction reintroduces the comparison and panics. Discovered by
   coverage case N: **any query combining `(pull ...)` with `:order-by`
   panics**, even with fully projected sort keys — a second live bug, one
   site of a wider crash class, filed as
   `BUG_PULL_WITH_ORDER_BY_PANICS.md`.

   One approach — blanket non-deduplicating construction in `SortRelation`
   — was **considered and rejected** (2026-07-04): it changes an exported
   primitive's contract to work around the panic rather than fixing the
   partial comparison underneath it, and the risk asymmetry is wrong — a
   loud panic traded for latent silent duplicates — with a safety proof
   that would require whole-codebase rigor out of scope here. Two
   artifacts from evaluating it are retained because they remain useful
   regardless of the eventual class decision:
   *analysis* — every current route into `SortRelation` passes a
   deduplicating boundary first (every `Relation.Sort` wrapper
   materializes via the deduplicating constructor or an inline dedup:
   `DedupIterator` in streaming `Project`, `UnionIterator`'s seen-set; the
   only `NoDedupe`-constructed inputs are storage scans, `executePulls`
   output, and sort output itself) — necessary but not sufficient for any
   future no-dedup argument, since it covers today's callers only; and
   *guard* — `TestOrderByProjectedKeyDeduplicates` permanently pins that
   projected-key order-by membership is identical to the same query
   without `:order-by`.

## Impact

Every query ordering by a non-projected variable silently returns arbitrary
order. Consumers that take the first row or apply `:limit` on top of such an
ordering select arbitrary rows while appearing to work. Downstream code has
had to adopt a hand-projection workaround (add the sort variable to `:find`,
ignore that tuple position when reading results), which pollutes result shapes and
scatters knowledge of this defect through call sites.

## Test Coverage Matrix

One test (or test group) per design-decision case. "Pre-fix" is the expected
status before the fix lands — red tests pin the intended behavior. Audited
2026-07-04: cases F–N had **no coverage anywhere in the repo** (all existing
order-by tests — `executor_sorting_test.go`, `executor/limit_test.go`,
`db/limit_test.go`, qb tests — use projected sort keys only, and no
aggregate+order-by test existed at all).

| # | Case | Decided behavior | Test | Pre-fix |
|---|------|------------------|------|---------|
| A | `:where`-bound, non-projected (asc/desc) + result shape | sorts; sort attribute stripped | `TestOrderByNonProjectedVariable` | red |
| B | projected + non-projected keys mixed | both honored in precedence | `TestOrderByMixedProjectedAndNonProjectedKeys` | red |
| C | non-projected key + `:limit` | global top-N | `TestOrderByNonProjectedVariableWithLimit` | red |
| D | bound nowhere | parse/validation error | `TestOrderByUnboundVariableIsError` (executor), `TestOrderByValidation` (parser) | red |
| E | projected keys (regression guard) | unchanged | `TestQueryWithOrderBy`, `TestQueryLimitWithOrderByTopN`, `TestOrderByAndLimitWithRelationInputIsGlobal`, `db/limit_test.go` | green |
| F | scalar `:in` constant key | accepted no-op | `TestOrderByScalarInputConstantKeyIsNoOp` | green* |
| G | scalar `:in` constant + real key | constant identity, real key honored | `TestOrderByScalarConstantKeyThenRealKey` | flaky† |
| H | collection input var bound via `:where`, non-projected | sorts | `TestOrderByCollectionInputBoundVariable` | red |
| I | relation-input symbol bound via `:where`, non-projected, union path | sorts globally | relation-input-across-union test | red |
| J | relation input col NOT bound by `:where` | explicit error | `TestOrderByValidation` (parser) | red |
| K | aggregate + group-key order | sorts | `TestOrderByAggregateGroupKey` | green (verified 2026-07-04) |
| L | aggregate + non-find variable | error | `TestOrderByAggregateNonFindVariableIsError` | red |
| M | duplicate find values through sort+strip | set semantics: dedup keeps first occurrence in sorted order → `[Alice, Bob]` | `TestOrderByNonProjectedPreservesDuplicates` | red/flaky† |
| N | `(pull ...)` in find + non-projected key | pulls in sorted order | `TestOrderByNonProjectedWithPull` | green by design (pull relocation); tie case green via `TestOrderByPullWithTiedSortKeys` |
| P | `(pull ...)` in find + *projected* key | pulls in sorted order | `TestOrderByProjectedKeyWithPull` | green by design (pull relocation) |
| O | order-by shape errors (existing) | unchanged | `TestOrderByParsing` | green |

\* Case F is green *accidentally* today — the silent skip happens to produce
the decided no-op behavior. The test pins it so the fix's constant-drop path
preserves it deliberately.

† Cases G and M assert a specific order that the pre-fix engine returns
arbitrarily; the arbitrary order can coincide with the expected one on a
given run (join order varies with Go map iteration), so these may
intermittently pass before the fix. They are deterministic once the fix
lands.

The audit run (2026-07-04) matched this matrix, with two discoveries beyond
the original defect: janus find results are **set semantics** (case M
returned 2 rows, leading to the corrected wrinkle section above), and
**`(pull ...)` + `:order-by` panics today** (cases N/P — filed as
`BUG_PULL_WITH_ORDER_BY_PANICS.md`).

The tolerance-only "Sort with missing sort symbol" subtest in
`TestSortingEdgeCases` is superseded by case D and gets removed with the
fix; the remaining edge cases (empty result, single tuple) get strict
assertions instead of error tolerance.

## Files (to change with the fix)

1. `datalog/parser/parser.go` — validate order-by variables per the design
   decision (safety error for unbound; unsupported error for
   relation/collection input symbols not bound by `:where`; find-variable
   restriction under aggregation; accept `:where`-bound and scalar/tuple
   `:in` constants). Depends on `ExtractVariables` covering every binding
   form — becoming this validation's first production consumer exposed
   gaps in it, fixed separately:
   `BUG_EXTRACT_VARIABLES_MISSES_BINDING_FORMS.md`.
2. `datalog/planner/planner_clause_based.go` — add retained order-by
   variables to `findSymbols` (threads them through phase Keep computation)
   and augment `buildFindClause`'s last-phase find with them.
3. `datalog/executor/executor.go` — at the finalization boundary: drop
   provably-constant sort keys (scalar/tuple `:in`) with an annotation
   event, sort, project back to the original `:find` symbols, then
   apply `:limit`; `emptyRelationForQuery` symbols must match the augmented
   shape.
4. `datalog/executor/executor_utils.go` — `SortRelation`: unresolvable sort
   key becomes a deferred relation error, not a skip (unreachable for parsed
   queries post-fix; guards API-constructed queries).
5. `datalog/executor/executor_sorting_test.go`,
   `datalog/parser/order_by_test.go` — coverage matrix above.
6. `docs/archive/completed/order-by-implementation-plan.md` — supersession
   note on the "should be ignored" behavior.
