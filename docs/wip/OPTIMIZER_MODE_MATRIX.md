# Optimizer Mode Matrix — every query test runs both paths

**Status**: Ruled 2026-07-19; `datalog/storage` migrated 2026-07-20 (~294 tests, committed `44edd06`). Remaining packages per the migration plan below. This document is the plan of record.

## Storage migration outcome (2026-07-20)

The sweep surfaced three real divergences, each ledgered in `docs/bugs/`:

- `BUG_NOTJOIN_HEADER_VALIDATION_ONLY_ON_ALGEBRA_PATH.md` — algebra-path-only validation.
- `BUG_DECORRELATION_PREDICATE_ONLY_INPUT_SYMBOLS.md` — algebra-path-only error.
- `BUG_BASELINE_ORDEFAULT_SUBQUERY_NIL_AGGREGATE.md` — baseline-path-only error (the "optimizer makes it work" class); its regression guard is the red `algebra_off` leg of `TestNotClauseComplexQuery_E2E`, which stands red in-tree until fixed.

Three tests were ruled plan-structure pins under exemption 4 below (ruled 2026-07-20): `TestComplexQuerySubqueryExecutionCounts` (decorrelated execution counts and optimizer event counters), `TestComplexQueryRetainsScenarioKeyThroughFallbacks` (or-fallback derived key metadata), and `TestGetElseMultiEntityScanNarrowed` (branch-narrowing annotation and index choice). The last was split: its row semantics run on the matrix as `TestGetElseMultiEntityStoredOrDefault`, its narrowing structure declares `EnableAlgebraOptimizer` explicitly.

Benchmarks are outside the ruling's scope (tests were the ruling); they keep their explicit options.

## The ruling

Every test that executes a query must exercise both the non-optimizer and optimizer paths. The optimizer must never change results, and must never make things work: a query that fails without the algebra optimizer must fail with it, and vice versa. The two paths are observationally equivalent — same result set, same error/no-error outcome — for every query the engine accepts.

Two wrong-results bugs motivated the ruling, both instances of the same gap: the correlated or-join fallback encoding dropped rows only on the optimizer path (`docs/bugs/resolved/BUG_CORRELATED_ORJOIN_GLOBAL_FALLBACK_DROPS_ROWS.md`), and a NOT written before its binding clause plans on the baseline path but errors on the optimizer path (`docs/bugs/BUG_ALGEBRA_BRIDGE_COMPILES_IN_SOURCE_ORDER.md`). The differential net that catches this class existed only where hand-built — a handful of baseline-vs-optimized pins — while the rest of the suite exercised a single path.

## The mechanism — the existing matrix-test convention, nothing new

The repo already has the idiom and the plumbing:

- **The idiom**: `cacheTestModes` (`datalog/storage/crdt_cache_matrix_test.go`) — a mode struct plus slice, `t.Run(mode.name, ...)` per mode, a mode-aware constructor — applied across the CacheMatrix family, with the constitutional statement in its header: the cache is an optimization, not a correctness requirement; if a test passes only with cache enabled, the fix is incomplete. This ruling is that statement with "optimizer" substituted.
- **The axis**: `TestAlgebraMatrix_ComparisonBindingOr` (`tests/algebra_matrix_test.go`) already loops `EnableAlgebraOptimizer` over `{false, true}` for one query shape.
- **The plumbing**: `DatabaseOptions.PlannerOptions` is an existing optional override; no constructor changes needed.

No environment variables, no build tags, no gate changes: the matrix runs inside ordinary `go test`, so the native and wasm legs inherit it unchanged, and every future test written to the convention carries the invariant automatically.

## Design

1. `optimizerModes` defined next to `cacheTestModes` in `datalog/storage`, same shape:

   ```go
   type optimizerMode struct {
       name    string
       algebra bool
   }

   var optimizerModes = []optimizerMode{
       {"algebra_on", true},
       {"algebra_off", false},
   }
   ```

   with a mode-aware database constructor threading `DatabaseOptions.PlannerOptions`. Packages that cannot import storage's test files (`tests/`, `db`, `qb`) carry their own copy of the six-line axis — the Go-testing reality for cross-package test code.

2. Query-executing tests wrap their query-and-assert body in `for _, mode := range optimizerModes { t.Run(mode.name, ...) }`. Setup (schema, writes) stays outside the loop where the database can be shared; where the test mutates state, the whole body moves inside.

3. Where a test already matrixes another axis (cache modes, backend cases), orthogonal axes cross — the honest matrix is the product.

4. Tests that pin one mode's behavior (plan structure, EXPLAIN output, the algebra package's own unit tests, the existing differential pins) declare `EnableAlgebraOptimizer` explicitly and are exempt from the loop — an explicit declaration is the correct statement of "this test is about one mode."

## Migration plan

Package by package, largest query corpus first:

1. `datalog/storage` — also where both motivating bugs lived.
2. `tests/`
3. `datalog/db`
4. `datalog/qb`
5. `datalog/executor` (tests that run full queries; pure relation-algebra unit tests have no optimizer axis)

The migration is itself a sweep: every test that fails one mode is either pinning optimizer-specific structure (it declares its mode explicitly) or it has found a real divergence — a new bug, ledgered on the spot. A failure only on `algebra_off` means the baseline path rotted behind the optimizer; a failure only on `algebra_on` is the bridge-divergence class.

Cost: query-heavy suite time roughly doubles, inside the existing gate structure.

## Interaction with the source-order bug

The first red pin on the new axis is the NOT-before-binder shape (`BUG_ALGEBRA_BRIDGE_COMPILES_IN_SOURCE_ORDER.md`): green on `algebra_off`, red on `algebra_on` until the bridge fix lands. The fix direction (pre-order clauses entering `Compile` by `ScopeOf` readiness, or teach `compileClausesFrom` to defer unplaceable clauses) is an open owner ruling.
