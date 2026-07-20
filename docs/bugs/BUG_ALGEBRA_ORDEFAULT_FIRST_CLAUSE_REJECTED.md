# BUG: algebra bridge rejects or-default as the first clause — algebra path only

**Status**: Open (2026-07-20). Found by the optimizer mode matrix migration of `datalog/executor` (phase 5), which put that package's query tests on the algebra path for the first time. Loud error at planning, no wrong data; the baseline path executes the same queries correctly.

## Symptom

`TestOrFallbackFirstBranchMatches` and `TestOrFallbackPatternWithStreamingRelation` (`datalog/executor/not_or_test.go`): an uncorrelated `(or-default ...)` as the first and only clause in `:where` — the global-fallback shape, where the primary branch evaluates against the whole database and the ground default applies only if it is empty. With the algebra optimizer off, both tests pass. With it on:

```
execution failed: query planning failed: algebra optimization failed: algebra compile: OR fallback requires prior relation
```

Reproduction is deterministic: `go test ./datalog/executor -run 'TestOrFallbackFirstBranchMatches|TestOrFallbackPatternWithStreamingRelation'` (the `algebra_on` legs stand red in-tree as regression guards).

## Reading

The bridge's or-fallback lowering assumes a prior relation to correlate against and errors when the clause opens the query (`compileOrFallback`'s nil-current guard). But the global-fallback shape is defined by the language — plain `or-default` with a flat header and no outer correlation is precisely "first non-empty branch, evaluated once, globally" — and the baseline executor supports it, with these two tests pinning the semantics since before the matrix. The bridge must extend to the uncorrelated case (a fallback whose outer group is the unit relation), not refuse it; per the extend-don't-avoid discipline, "requires prior relation" is a lowering limitation, not a language rule.

Related family: `BUG_ALGEBRA_BRIDGE_COMPILES_IN_SOURCE_ORDER.md`, `BUG_NOTJOIN_HEADER_VALIDATION_ONLY_ON_ALGEBRA_PATH.md`, `BUG_ALGEBRA_NOT_REJECTS_SINGLE_BRANCH_ORJOIN.md` — algebra-path-only rejections of baseline-legal shapes.

## Protocol

Fix direction needs an owner ruling. When fixed, the red `algebra_on` legs of both tests go green as regression guards; no assertions were weakened and the queries are unchanged from their pre-migration form.
