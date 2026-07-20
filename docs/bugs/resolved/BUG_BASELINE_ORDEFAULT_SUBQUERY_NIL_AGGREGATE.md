# BUG: or-default aggregate subquery yields nil sums for get-else-defaulted absent attributes

**Status**: RESOLVED (2026-07-20). All three fix layers ruled and landed the same day; full gate (native + wasm) green. Found by the optimizer mode matrix migration (OR-family batch). The original read ("baseline path only") was wrong in an important way: **both paths computed the same nil sums; only the baseline path detected them.** The algebra path silently emitted the nil values in result rows.

## Resolution

Three layers, each with red-first pins:

1. **qb boundary normalization** — `normalizeConstant` (`datalog/qb/pattern.go`, delegating to `datalog.NormalizeValue`, elementwise for `[]interface{}`) applied at every raw-value entry point: pattern constants and predicate/expression operands (`toPatternElement`/`toTerm`), `Ground`, `TupleGround`, `GetElse` defaults, pull `Default`, and `V()`. Pinned by `TestRawConstantsNormalizeToInt64` (`datalog/qb/value_normalization_test.go`).
2. **Loud aggregate fold** — `aggregateOps.update` returns an error: `sum`/`avg` reject any non-`int64`/`float64` input (nil included), `min`/`max` reject nil; both batch folds and the streaming path propagate. Pinned by `TestAggregateFoldRejectsNonDomainValues` (`datalog/executor/aggregation_canonical_test.go`). `count` keeps its documented SQL semantics (counts non-nil).
3. **Emission guard** — all three aggregation paths (single, grouped, streaming) error when an aggregate folded no values while a sibling folded some; the streaming path additionally gained batch parity for all-empty groups (dropped, previously emitted as a tuple of nils). Pinned by `TestConditionalAggregateEmptyBesideNonEmptyIsError` and `TestStreamingAggregationDropsAllEmptyGroups` (`datalog/executor/conditional_aggregate_internal_test.go`).

`TestNotClauseComplexQuery_E2E` passes both optimizer modes and now pins the aggregate columns exactly, as `int64` — including the previously-nil `?totalVolume`/`?totalUnits` and both projects' fallback tuples. One legacy assertion updated to the ruled contract: `TestTupleGroundMixedTypes` expected the raw Go `int` the builder used to store.

## Symptom

`TestNotClauseComplexQuery_E2E` (`datalog/storage/not_clause_regression_test.go`) — the GitHub #58 reproducer, built with the `qb` builder: a query with NOT, six get-else expressions, three `or-default` clauses whose primary branches are correlated aggregate subqueries, a comparison binding, and order-by.

- `algebra_off`: loud error —
  ```
  query execution failed: phase 1 failed: clause 13 (not) failed: NOT input combinations failed: binding form application failed: subquery result contains nil value at position 3 - this violates datalog semantics
  ```
- `algebra_on`: "passes" the error check, but the result rows are corrupt — the project that has items carries `<nil>` for `?totalVolume` and `?totalUnits`:
  ```
  Result: [... Alpha ... 2 300 8000 <nil> <nil> true :tag/secondary ...]
  ```
  Nil is not a datalog value; these rows reach the caller with no signal, and would panic `hashValue`/`ValuesEqual` if they entered a join or dedup.

Reproduction is deterministic: `go test ./datalog/storage -run 'TestNotClauseComplexQuery_E2E'` (the `algebra_off` leg stands red in-tree as the regression guard; the `algebra_on` leg's corruption is currently invisible to the test's assertions).

## Root cause (verified)

Three layers, each confirmed by reading the code and the annotation stream of the failing run:

**1. Origin — the qb boundary does not normalize integer widths.**
`qb.GetElse(entity, attr, 0)` (`datalog/qb/database_function.go`) stores the Go literal `int(0)` verbatim as `GetElseFunction.Default`. The value-domain contract says integer widths normalize to `int64` at the boundary; the EDN parser honors this (integer literals parse as `int64`), the qb builder does not. Every EDN-based get-else test passes both modes for exactly this reason — the campaign's `algebra_getelse_product_test.go` runs the same shapes with `int64` defaults and is green on both legs. The test data's `:item/volume` and `:item/units` have no datoms on any entity, so only those two columns exercise the default path: `GetElseFunction.EvalWithLookup` finds nothing, `TypeDefault` passes the default through unchanged (schemaless database), and `int(0)` — not `int64(0)` — is bound into `?vol`/`?units` for every row. `:item/cost`/`:item/weight` exist in storage, decode as `int64`, and behave.

**2. Amplifier — the aggregate fold's silent taxonomy switch.**
`updateSum` (`datalog/executor/aggregation.go`) type-switches on `int64`/`float64` only and **silently skips** anything else; `resultSum` returns `(nil, nil)` when nothing was folded. So `(sum ?vol)` over two rows of `int(0)` collects the values, folds none of them, and manufactures nil — while `(count ?i)` and the sums over stored attributes look healthy beside it. `executeSingleAggregation`'s `hasAnyValues` guard only catches the all-empty case, so the mixed tuple `(2, 300, 8000, nil, nil)` is emitted. The annotation stream pins this exactly: `aggregation/strategy.selected ... input_size:2` followed by the nil at position 3 = `(sum ?vol)`. This is the "silent default in a taxonomy switch" pattern.

**3. Detector asymmetry — only one path checks the invariant.**
The baseline per-combination path applies the tuple binding through `applyExactlyOneBinding` (`datalog/executor/subquery.go`), which enforces "subquery tuples contain no nil values" and errors loudly. The algebra path's decorrelated route has no equivalent check downstream of the aggregate, so the same nils flow into the final result relation. The `clause 13 (not)` label in the baseline error is only the clause that forces the lazy or-fallback stream; the nil is manufactured in the aggregation.

The mode divergence is therefore not "baseline rotted behind the optimizer" — the baseline's guard caught a value-domain violation that the optimizer path silently ships. The matrix still did its job: the red leg is what exposed all of this.

## Fix directions (owner ruling needed; layers, not alternatives)

1. **Normalize at the qb boundary** (the origin, Pillar 2): every value entering query ASTs through qb — get-else defaults, `Ground`/`TupleGround` values, pattern constants, comparison operands — normalizes integer widths to `int64`, the same contract the EDN parser enforces. Needs an audit of qb's value entry points; note the passing leg's fallback rows (`TupleGround(0,0,0,0,0)`) very likely place raw `int`s in user-visible result rows today.
2. **Make the aggregate fold loud** (the amplifier): `updateSum`/`updateMin`/`updateMax` error on non-domain values instead of skipping — this alone would have converted both legs' failures into the same loud type error at the aggregation site.
3. **Close the detector asymmetry**: an aggregate output tuple containing nil in an aggregate position is an error at emission (`executeSingleAggregation`/grouped/streaming), not a row — so no path, optimizer or baseline, can ship nil into relational flow.

When fixed, the red `algebra_off` leg of `TestNotClauseComplexQuery_E2E` goes green as the regression guard; a structural pin should also assert the algebra leg's rows contain no nils (today's assertions don't look at the aggregate columns).
