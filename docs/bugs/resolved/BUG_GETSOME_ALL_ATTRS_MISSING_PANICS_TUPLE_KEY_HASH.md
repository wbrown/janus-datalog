# BUG: get-some with every listed attribute missing panics tuple-key hashing on an `:in`-bound entity

**Status**: ✅ RESOLVED (2026-07-22). The sentinel consumption moved into the one boundary every evaluation site already crosses: `admitExpressionResult` (datalog/executor/iterator_composition.go) now resolves the get-some result — consuming the absence sentinel and validating the value against the datalog domain — and returns `(value, found, err)`. The signature change made site-completeness compiler-enforced; the six call sites each apply their own absence semantics (per-tuple paths skip the tuple; the no-groups and literal-entity arms return an empty result; the with-groups environment arm empties every group **schema-preservingly**, keeping the aligned symbols the phase contract expects). The investigation widened the bug beyond this report: **both** `Found` states leaked through the with-groups arm (`Found=true` bound the raw sentinel and panicked identically — this report's "value ✓" row below did not hold for the `:in`-bound shape), and a sixth, never-reported site leaked the same way (the zero-required-symbols arm, reachable by a get-some over a tagged-literal entity — probe-confirmed panic on both modes). Pinned by six mode-matrixed regression tests in `datalog/storage/get_some_input_test.go` (with-groups all-missing and attr-present, relation-input mixed-`Found`, literal-entity both legs, collection-input contrast control), all red-first.

**Original report follows.**

**Status at filing**: Open (2026-07-22). Found by a downstream application's suite at `70474ea`: a get-some fallback-read test panics inside the executor, killing the whole package test binary (~540 tests with no verdict recorded). Turnkey CLI reproduction below. Both prior pins tested (`331ba214`, `ba6ee242`) return the documented empty relation for the same query; the panic is new in the `ba6ee242` → `70474ea` range.

## Symptom

`get-some` over an `:in`-bound entity, where **none** of the listed attributes exist on that entity:

```clojure
[:find ?t ?v
 :in $ ?e
 :where
 [?e :entity/type ?t]
 [(get-some $ ?e :entity/code :db/id) ?v]]
```

panics instead of dropping the row:

```
panic: hashValue: *query.GetSomeResult is not a datalog value type

executor.hashValue            (datalog/executor/tuple_key.go:208)
executor.hashValues           (tuple_key.go:61)
executor.NewTupleKeyFull      (tuple_key.go:48)
executor.deduplicateTuples    (datalog/executor/relation.go:500)
executor.NewMaterializedRelationWithOptions (relation.go:411)
(*DefaultQueryExecutor).executeExpression   (datalog/executor/query_executor.go:987)
```

The no-match sentinel (`*query.GetSomeResult`) escapes expression evaluation unresolved, lands in the expression's output relation, and the materialization dedup hashes it. Expected behavior per the absence semantics (and per both prior pins): the row is dropped — empty relation.

## Reproduction

Against a downstream module fixture; `?e` is an entity that carries `:entity/type` but neither of the listed attributes:

```bash
datalog -db <fixture>.jdzl \
  -query '[:find ?t ?v :in $ ?e :where [?e :entity/type ?t] [(get-some $ ?e :entity/code :db/id) ?v]]' \
  -in '#identity "<entity-hash>"'
```

| Shape | 331ba214 | ba6ee242 | 70474ea |
|---|---|---|---|
| `:in`-bound entity, all listed attrs missing | empty relation ✓ | empty relation ✓ | **panic ✗** |
| clause-bound entity (`[?e :entity/name ?ref]` binds `?e`), all listed attrs missing | empty relation ✓ | | empty relation ✓ |
| some listed attr exists (either binding shape) | value ✓ | | value ✓ |

(Blank cells not measured.)

## What the discriminators say

- **The trigger needs both conditions**: the entity arriving through `:in` (the expression evaluating against the unit input relation) AND no listed attribute present. A clause-bound entity with the same all-missing attribute list returns the correct empty relation at `70474ea`, and any present attribute resolves correctly in both shapes — so the sentinel is normally consumed on those paths and only the `:in`-bound no-match path lets it reach materialization.
- The panic site is the tuple-key hash introduced/reworked in this range ("Tuple keys probe positionally; key map inlines single-entry buckets" `5717e99`; "Hash-join build rows group into spans; probes materialize no keys" `0d39463`) — but the defect is upstream of the hash: an unresolved `*query.GetSomeResult` should never be a tuple value entering `NewMaterializedRelationWithOptions`. The hash's fail-loud panic is doing its job; the leak is in `executeExpression`'s handling of get-some's no-match result on the `:in`-bound path.
- Downstream severity is process-death, not wrong results: in the downstream application the panic killed an entire package test binary; any production Go call site that passes a bound entity via `:in` and uses `get-some` over possibly-absent attributes takes down the process.

## Impact

Downstream at `70474ea`: the get-some test panics; the package death leaves ~540 tests with no verdict, masking that package's real state (including the or-join `:in`-bound-correlate recoveries this pin otherwise delivers — see `BUG_ORJOIN_IN_BOUND_CORRELATE_TREATED_AS_BRANCH_LOCAL.md`, whose narrow-header repro is verified fixed at `70474ea`).
