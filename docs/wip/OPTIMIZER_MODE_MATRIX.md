# Optimizer Mode Matrix — every query test runs the legs its reach needs

**Status**: Ruled 2026-07-19; migration COMPLETE 2026-07-20. The five planned packages — `datalog/storage` (~294 tests, committed `44edd06`), `tests/` (40), `datalog/db` (23), `datalog/qb` (33), `datalog/executor` (117 across three batches) — plus three the plan's list missed, found by a repository-wide sweep for query-executing tests: `datalog/reflect` (32), `cmd/datalog` (8), `datalog/wasmtest` (1). Every other package is exempt (planner/algebra pin plan structure by nature) or executes no queries. The one structural exemption class is CLOSED (owner ruling 2026-07-20): the CLI gained `-optimize` (default true, matching the engine default; `-optimize=false` runs the baseline planner), riding the database's planner options so every CLI query path — `db.Query` with inputs, single-query, interactive — honors it. The six subprocess query tests (`TestCLI_QueryFromEDNDump`, `TestCLI_QueryFromJDZLDump`, `TestCLI_QueryWithScalarInput`, `TestCLI_QueryWithCollectionInput`, `TestCLI_QueryWithStringInput`, `TestCLI_QueryWithMultipleInputs`) now loop the axis via `mode.cliFlag()`; the in-process `openDatabaseOrEDN` tests open with the mode's options and query through `db.NewExecutor()`, the same funnel `main()` uses. This document is the plan of record; the outcome sections below record what each phase surfaced.

## Backend axis outcome (2026-08-01) — the matrix gains its second dimension

**Status**: COMPLETE for `datalog/storage`, `tests/`, `db`, `qb` and `reflect`; the open items below are named rather than pending discovery. The optimizer axis crossed with the storage backend, and a test's subject decides which backends it crosses with. A test of the engine (planning, execution, resolution, the cache, the language) runs on `optimizerModes`: the tree store alone, crossed with the two optimizer paths, since the backend supplies a scan bound's datoms in index order and nothing else, and the store contract pins that at the seam for every backend. A test of the store, what it persists or how it reads (persistence and reopen, dumps, snapshots, truncation, forks, read sessions, scan intake, index order, the key layout, the blob tier), runs on `backendModes`: every backend the build has, six legs natively (badger, badger-emulator, memory-trees × two optimizer paths) and four under wasm, since Badger needs a filesystem. `TestBadgerAnswersWhatTheTreeStoreAnswersAndAgainAfterReopen` runs the engine's query shapes on Badger once, against the tree store's answers, before and after a close and reopen. Benchmarks and profiling instruments keep the default backend — tests were the ruling, the same carve-out the optimizer campaign made.

The sweep ran twice. The first pass enumerated one constructor and reported completeness off it; the second found a second constructor, a whole package outside the wasm leg, and three tests whose converted outer loop sat over an inner one that still supplied the mode. What follows records both passes, because the misses are the more useful half.

What the sweep surfaced:

- **"Needs Badger" was false in four places.** `BadgerEmulatorStore` has a complete Tier-3 blob tier: `assertMemoryDatom` writes the compressed bytes under a `[blobKeyPrefix][hash]` key, `memoryBlobReader` reads them back, `Scan` skips 21-byte blob keys so they never surface as datoms, and `retractMemoryDatom` decodes through a `BlobReader`. `MemoryTreeStore` has none, because a store holding whole datoms has no fixed-width key for a large value to escape from — the same partition `byteKeyBackends` already encoded for the index key, one step upstream, and that `deleteStoreBlobs` had been classifying on all along. Four pieces of prose asserted otherwise: the comment on `writeTier3ValueThenCorruptBlob` ("it has to be the backend that has a Tier 3"), `appendNativeBlobFaultCase`'s doc, the wasm stub beside it, and `query_boundary_fault_test.go`'s header. Eight Tier-3 tests in `blob_store_test.go`, both blob-corruption openers, and `TestKeyOnlyIteratorRetainsBlobErrorAfterRepeatedNext` now run `byteKeyBackends`.
- **A build tag fell out of it.** Replacing the two hand-rolled Badger blob walks with `deleteStoreBlobs` left `query_boundary_error_test.go` with no Badger dependency, so its `!(js && wasm)` tag came off and the Tier-3 boundary reproduction now runs under wasm on the Badger emulator. `deleteStoreBlobs` itself split into `blobKeys` plus a per-backend delete so the same walk serves both counting and corruption.
- **Mode loops that were theater.** A `t.Run(mode.name, ...)` around a database built outside the loop — or built by `Path:`, which is `DefaultBackend()` — runs one backend under N mode names. Found and fixed in `hash_join_scan_range`, `ohlc_realistic`, `production_query`, `crdt_orderedset`, `attribute_truncation_collision`, `eachFaultAndMode`, and `db`'s `TestIssue61_ExampleUseCases`.
- **A case table needed a per-entry axis.** `queryBoundaryFaultCase` gained `modes`: the injected-fault case wraps any store and runs the full axis, the Tier-3 blob case runs `byteKeyBackends`. Inheriting the global axis had the blob case claiming memory legs it was executing on Badger.
- **`storage.NewDatabase(path)` is a second construction entry point, and the first sweep missed it.** The sweep enumerated `NewDatabaseWithOptions` and treated that as "construction"; `NewDatabase` reaches it internally, so its own callers never appeared in any list. Nine files in `tests/` were building databases through it on `DefaultBackend()` while the package's `eachBackendAndMode` harness sat unused beside them — including `algebra_matrix_test.go`, the file this document cites as the original axis idiom. All nine are converted. The lesson is the enumeration, not the sites: a public constructor that delegates to another is invisible to a reference search on the delegate.
- **`tests/` was outside the wasm leg.** Not by build tag — `make test-wasm` scoped to `./datalog/...`, so 40 integration tests on the full matrix never ran under Node despite compiling for it cleanly. The target now covers `./tests/...` as well, which is what its own comment already claimed: a misclassified file should fail loudly in that leg rather than be silently excluded.
- **A doubled axis is invisible to the compiler.** Converting a file's outer loop while leaving its inner `for _, mode := range optimizerModes` yields `badger-emulator/algebra_on/algebra_on` — the outer mode dead, the leg named for a path it never took. `go vet` is silent because an unused function parameter is legal. The check is `gopls references` on `optimizerModes`: every remaining direct loop must be either the harness itself or a deliberate one-backend instrument.

Where one backend is the test's *instrument* rather than a parameter, it is named and the reason recorded: `iterator_leak_test.go` observes the leak as a Badger WAL file surviving `Close` and is tagged `!(js && wasm)`, because `BadgerEmulatorStore` holds no refcounted resource a leaked iterator would pin and the assertion would pass vacuously; `TestMemoryBackendReportsIntakeNatively` injects a `BadgerEmulatorStore` because that store's `Scanned()` counter is what it pins; `TestBadgerParallelProfile` and `TestProfileBinaryDecoding` write CPU profiles and assert nothing, so a fixed backend is what makes successive profiles comparable.

Three selectors sit on the engine axis in `tests/`: `eachMode`, `eachModeOpts` for a test that needs a schema or the cache disabled, and `eachModeWith` for one that builds its own executor or matcher options and therefore needs the mode itself — without it that test takes the mode's name while running the default optimizer path. `eachStoreBackendAndModeOpts` is the store axis there, every backend the build has, for the compressed-value tests.

Open:

- **`cmd/datalog` exposes no backend selector.** `openDatabaseOrEDN` opens by path and dump files load into a temporary database on `DefaultBackend()`, so the CLI's query tests run one backend. The mechanism it lacked now exists — `db.WithBackend(name)` and `DatabaseOptions.BackendName` resolve through `BackendNamed`, so a `-backend` flag would be a pass-through of the flag string — but whether the CLI should have one is an owner ruling; its axis copy carries no backend field until then.
- **Key-only decode across the value domain is asserted on Badger alone.** `TestKeyOnlyScanningAllTypes` covers nine value types through `ScanKeysOnly`; `TestStoreBackendContract` exercises that method on all three backends with two. Either the nine-type case belongs in the contract or it is a Badger statement — not both.
- **Nothing measures whether a passing leg asserts anything.** Every finding above was found by reading, and one of them (the blob tier) only because it was asked about directly. The mechanism that would answer it is bounded: break each backend deliberately — `MemoryTreeStore.Scan` returns empty, then `BadgerEmulatorStore`, then the blob tier — and every test that still passes is vacuous on that backend. Three mutants, three runs, and the output is a list rather than a belief.

## Decorrelation-fix outcome (2026-07-20) — the matrix red set goes to zero

The campaign's entire standing red set is resolved and the full gate (native + wasm) is green both legs; the branch's first fully green sweep since the matrix landed. The fixes, in dependency order:

- **`resolved/BUG_ALGEBRA_NOT_REJECTS_SINGLE_BRANCH_ORJOIN.md`** and **`resolved/BUG_ALGEBRA_ORDEFAULT_FIRST_CLAUSE_REJECTED.md`** — Union IR accepts arity ≥ 1; uncorrelated or-default-first compiles against the unit relation. (Low-hanging batch, with the temporal-handle inheritance fix and the boundary-validation pair from `resolved/BUG_SUBQUERY_BINDING_ARITY_VALIDATED_AT_DIFFERENT_LAYERS.md`.)
- **`resolved/BUG_DECORRELATION_PREDICATE_ONLY_INPUT_SYMBOLS.md`** — correlation parameters are classified before the rewrite fires (data-bound / equality-bound / decline with an `algebra/decorrelate-skip` annotation); equality-bound parameters translate the correlation predicate into the join condition; `LateralJoin` carries the call site's argument list verbatim so declined subqueries round-trip losslessly.
- The corrected rewrite exposed two independent engine defects, each with isolated red-first reproducers that stay red unless their own defect is fixed: **`resolved/BUG_UNCORRELATED_SUBQUERY_SCHEDULES_BEFORE_BINDING_PROVIDERS.md`** (planner: readiness-restricted deferral gate, `subqueryDependsOnPendingProvider`) and **`resolved/BUG_EXPRESSION_BINDING_OVERWRITES_BOUND_VARIABLE.md`** (executor: expression binding application unified into one home, `bindingAlignment` — bound positions unify, never overwrite).
- `TestParallelDecorrelationSymbolOrderBadger` migrated onto the matrix per its ledger protocol, closing the campaign's last deferred migration.

Remaining open on the algebra path: `BUG_ALGEBRA_BRIDGE_COMPILES_IN_SOURCE_ORDER.md` (fix direction pending ruling; see "Interaction with the source-order bug" below).

## Phase 2–5 outcome (2026-07-20)

`tests/`, `db`, and `qb` migrated divergence-free (the qb and tests builder/AST test bulk is no-axis by the plan's own carve-out; one `tests/` case crosses the cache-mode axis for the honest product). The `executor` phase was the high-yield one: bare `NewExecutor`'s default profile leaves `EnableAlgebraOptimizer` false, so that package's ~120 query-planning tests had never run the algebra path. Putting them on the matrix surfaced:

- **Seven more reproducers of `BUG_DECORRELATION_PREDICATE_ONLY_INPUT_SYMBOLS`** (five OHLC-shaped grouped-decorrelation tests, plus a second symptom — `cannot resolve right term` — for subqueries with an extra scalar `:in` input consumed only by predicates).
- **`BUG_ALGEBRA_NOT_REJECTS_SINGLE_BRANCH_ORJOIN`** (new): the bridge's NOT-inner analysis rejects a legal single-branch or-join as an invalid union.
- **`BUG_ALGEBRA_ORDEFAULT_FIRST_CLAUSE_REJECTED`** (new): the bridge's or-fallback lowering refuses the uncorrelated global-fallback shape (`or-default` opening the `:where`).
- **`BUG_TEMPORAL_HANDLES_DROP_PLANNER_OPTIONS`** (new, found twice independently in the `db` and `tests/` phases): `AsOf()`/`History()` construct child handles without the parent's `plannerOptions`, silently reverting temporal queries to defaults and blinding the matrix's `algebra_off` legs on those tests.

Ten test functions stood red on their `algebra_on` legs in-tree as regression guards for the three open algebra-path bugs; every other migrated test passed both modes. **All ten are green as of 2026-07-20 — see the decorrelation-fix outcome below.** Where both modes reject an invalid query but phrase the error differently (planning-time vs execution-time), each mode pins its own message per the `correlated_not_join_test.go` convention. The executor axis lives in `datalog/executor/optimizer_modes_test.go` with two base profiles (NewExecutor's default via `defaultPlannerOptions()` — extracted from the constructor so the axis cannot drift — and the bare zero-value profile), preserved per test rather than normalized.

## Storage migration outcome (2026-07-20)

The sweep surfaced three real divergences, each ledgered in `docs/bugs/`:

- `BUG_NOTJOIN_HEADER_VALIDATION_ONLY_ON_ALGEBRA_PATH.md` — algebra-path-only validation.
- `BUG_DECORRELATION_PREDICATE_ONLY_INPUT_SYMBOLS.md` — algebra-path-only error.
- `resolved/BUG_BASELINE_ORDEFAULT_SUBQUERY_NIL_AGGREGATE.md` — surfaced as a baseline-only error (the "optimizer makes it work" class); root cause was a qb boundary violation both paths shared — the baseline's nil guard caught what the algebra path silently emitted. RESOLVED 2026-07-20 with a three-layer fix (qb int64 normalization, loud aggregate fold, emission nil guard); `TestNotClauseComplexQuery_E2E` passes both modes with exact row pins.

Three tests were ruled plan-structure pins under exemption 4 below (ruled 2026-07-20): `TestComplexQuerySubqueryExecutionCounts` (decorrelated execution counts and optimizer event counters), `TestComplexQueryRetainsScenarioKeyThroughFallbacks` (or-fallback derived key metadata), and `TestGetElseMultiEntityScanNarrowed` (branch-narrowing annotation and index choice). The last was split: its row semantics run on the matrix as `TestGetElseMultiEntityStoredOrDefault`, its narrowing structure declares `EnableAlgebraOptimizer` explicitly.

Benchmarks are outside the ruling's scope (tests were the ruling); they keep their explicit options.

## The ruling

Every test that executes a query must exercise both the non-optimizer and optimizer paths. The optimizer must never change results, and must never make things work: a query that fails without the algebra optimizer must fail with it, and vice versa. The two paths are observationally equivalent — same result set, same error/no-error outcome — for every query the engine accepts.

Two wrong-results bugs motivated the ruling, both instances of the same gap: the correlated or-join fallback encoding dropped tuples only on the optimizer path (`BUG_CORRELATED_ORJOIN_GLOBAL_FALLBACK_DROPS_TUPLES`), and a NOT written before its binding clause plans on the baseline path but errors on the optimizer path (`BUG_ALGEBRA_BRIDGE_COMPILES_IN_SOURCE_ORDER`). The differential net that catches this class existed only where hand-built — a handful of baseline-vs-optimized pins — while the rest of the suite exercised a single path.

## The mechanism — the existing matrix-test convention, nothing new

The repo already has the idiom and the plumbing:

- **The idiom**: `cacheTestModes` (`datalog/storage/crdt_cache_matrix_test.go`) — a mode struct plus slice, `t.Run(mode.name, ...)` per mode, a mode-aware constructor — applied across the CacheMatrix family, with the constitutional statement in its header: the cache is an optimization, not a correctness requirement; if a test passes only with cache enabled, the fix is incomplete. This ruling is that statement with "optimizer" substituted.
- **The axis**: `TestAlgebraMatrix_ComparisonBindingOr` (`tests/algebra_matrix_test.go`) already loops `EnableAlgebraOptimizer` over `{false, true}` for one query shape.
- **The plumbing**: `DatabaseOptions.PlannerOptions` is an existing optional override; no constructor changes needed.

No environment variables, no build tags, no gate changes: the matrix runs inside ordinary `go test`, so the native and wasm legs inherit it unchanged, and every future test written to the convention carries the invariant automatically.

## Design

1. `optimizerMode` in `datalog/storage`, defined next to `cacheTestModes`, is one leg of the matrix — a storage backend crossed with an option leg, a setting of every planner option a test can turn on:

   ```go
   type optionLeg struct {
       name        string
       algebra     bool
       sharing     bool
       prefetch    bool
       symmetric   bool
       fusion      bool
       aggregation bool
   }

   type optimizerMode struct {
       name string
       optionLeg
       backend Backend
   }
   ```

   The legs are the pairwise covering design `optionLegs` states: the strength-2 orthogonal array on three bits (a, b, c), with the algebra optimizer at a, scan sharing at c, prefetch at a⊕c, the symmetric hash join at a⊕b⊕c, attribute-fetch fusion at b⊕c and streaming aggregation at a⊕b: eight legs, on which every pair of the six options takes all four settings. The first leg is the reference, every option off, and one leg is `DefaultPlannerOptions`' profile. A leg's `plannerOptions` is the default options with its settings applied, and a mode carries its leg's fields and that method by promotion.

   `referenceMode` is the reference leg on the engine backend: a test comparing the legs' answers computes the reference's once and compares each leg's with it inside that leg, so a leg run alone compares against the reference.

   `optimizerModes` is the engine axis, the tree store crossed with the legs, and `backendModes` is the store axis, `AvailableBackends()` crossed with them.

   A test loops the legs `legsFor` answers over its axis for the options its queries reach. `legsFor(modes, reach...)` answers one leg per setting of the reach the pairwise design needs: two legs for a reach of one option, four for two or three, every leg for four or more, and the default profile alone for a reach of none. The reference leg is always among them, and the default profile wherever a sub-design of that size holds both.

   A test of the engine passes `optimizerModes` and its measured reach. A test of the store, what it persists or how it reads, passes `backendModes` and no reach, one leg per backend, since no optimizer option changes what the store persists or how it reads. A test that fixes its own store loops `optionLegs`.

   Two constructors sit on the axes:

   - `createOptimizerModeDB(t, mode, opts)` — the mode's backend and the mode's planner options, threaded through `DatabaseOptions`. This is what a test on either axis uses.
   - `openBackendDB(t, backend, opts)` — a named backend with the caller's `PlannerOptions` left intact, for a test whose own options are the subject of its assertion. The option half of the mode has nothing left to vary there, so it takes a `Backend` rather than a mode; reaching for it drops that half of the axis.

   A test's reach is measured, never read off its text. Under `-reach`, `legsFor` answers every leg, and a recording handler reads each query's events for the option each reaches:

   - `PatternFusedFetch` and `PatternFusedConstraint`: attribute-fetch fusion.
   - `ScanSharingCacheHit` and `ScanSharingCacheMiss`: scan sharing.
   - `PrefetchTrigger`: entity prefetch.
   - `AlgebraDecorrelateApply` and `AlgebraGetElseScanApply`: the algebra optimizer.
   - `JoinStrategy` with `mode` `symmetric`: the symmetric hash join.
   - `AggregationStrategy` with `strategy` `streaming`: streaming aggregation.

   The reach is the union over every leg. One all-on leg would hide the symmetric join: a shared scan is a `LazySeqRelation`, so under scan sharing no join takes the symmetric path.

   Where the recorder rides:

   - In `datalog/storage` and `tests/` it wraps the annotation handler of every database the package's constructor opens, so every test opening through the constructor is in the record.
   - In the other packages the mode carries `recordsReach`, the test's name, which `legsFor` sets under `-reach`, and the mode's planner options carry the recorder as their handler. A database opened under them, or an executor built under them, carries it on.

   Each package's `reach_report_test.go` holds the flag, the record and the report written at the end of the run, and the loops take their reaches from it. A test absent from the report, one that opens its database outside the constructor or builds its executor under a handler of its own, keeps every option.

   Two narrowings, each naming what it gives up:

   - `legsWhere(modes, keep)` — the legs of the given set a predicate admits. A test pinning one option's mechanism names the option in its reach, so a leg carrying it is among `legsFor`'s legs, and keeps those legs through `legsWhere(legsFor(modes, reach...), keep)` in place of setting the option. That is still a leg per backend and per setting of the other options the reach names, because pinning one option pins neither a store nor another option.
   - `byteKeyBackends(t)` — badger and badger-emulator out of the store axis, not memory-trees. The tree store hands out the datom it already holds, so a property of the encoded index key, or of anything downstream of it such as the Tier-3 blob tier, is not a property it has. The switch has no silent default: a new backend fails there until someone says which side of the line it falls on.

   Packages that cannot import storage's test files carry their own copy of the eight legs and of `legsFor`, each pinned to the design by its own copies of the pair-coverage, reference-and-default-profile, options-carry and legs-by-reach tests — the Go-testing reality for cross-package test code:

   - `executor` carries the eight legs and a ninth, fusion with streaming aggregation, the profile `NewExecutor` constructs with, in `datalog/executor/optimizer_modes_test.go`. Its `legsFor(t, reach...)` answers the ninth leg beside the array's whenever its setting of the reach is one no array leg holds, and `export_test.go` exports `LegsFor`, `Option`, `AllOptions` and the option constants for the external test package.
   - `db` carries both axes, with `referenceMode`, `legsFor(t, modes, reach...)` and `legsWhere`.
   - `qb` and `reflect` carry the engine axis alone, `legsFor(t, optimizerModes, reach...)`, threading `mode.backend` into their own constructors.
   - `tests/` has `createBackendModeDB`, with `eachMode`, `eachModeOpts` and `eachModeWith` on the engine axis, each taking a trailing reach its callers pass and looping `legsFor(reach...)`, `eachStoreBackendAndModeOpts` on the store axis looping `legsFor()`, the default profile per backend, and `legsWhere` over the legs a test is given. Its mode carries no backend; the selectors cross it with one.
   - `wasmtest` carries the legs with no backend, `legsFor(t, reach...)`.
   - `cmd/datalog` carries the legs with no backend for its in-process tests, `legsFor(t, reach...)`. A test that runs a query through the built binary loops `cliOptimizeFlags`, the two values of `-optimize`, the one planner option the CLI exposes: a leg of the eight would name options the binary never set.

2. Query-executing tests wrap their query-and-assert body in `for _, mode := range legsFor(optimizerModes, <reach>...) { t.Run(mode.name, ...) }`, the reach the report names for the test, **with the database built inside the loop**. A mode loop around a database constructed outside it — or constructed from `Path:`, which is `DefaultBackend()` — runs one backend under N mode names and reads as coverage it does not have. Schema values and other backend-independent fixtures can still be built once above. A loop that builds its planner options from one field of the mode runs every leg under the other options' zero values, the same coverage it does not have; the options a leg runs under are `mode.plannerOptions()`.

3. Where a test already matrixes another axis (cache modes, schemaless vs schema-aware), orthogonal axes cross — the honest matrix is the product. The storage backend is not one of those: it lives inside `optimizerMode`, so a test on the axis gets it without crossing anything.

   The cache axis crosses in two shapes, and the backend nests differently in each. `cacheTestModes` loops, so the product is taken and every leg meets the same written-down expectation. `assertCacheModesAgree(t, omode, build, probe)` — and `assertVBoundEquivalent`, the same shape for V-bound validation — instead builds both cache modes **on one backend leg** and requires them to agree, needing no expectation at all. Cache varies inside a backend leg and never across it: cache-on-badger against cache-off-memory confounds two variables and makes a disagreement unattributable.

   The two cache legs are not worth the same under this axis. `DisableCache: true` sends reads to storage, and storage is now three implementations — so the cache-off legs are where the backends genuinely differ. The EA cache is a `Database` structure rather than a store one, so the cache-on legs run the same cache code with different fill paths and can mask a storage-path divergence on their own. That asymmetry is what the differential shape exists to catch, and the backend axis is what makes it name the store responsible.

4. A test that pins one option's mechanism (a plan shape under the algebra optimizer, a shared scan's completion, a fallback's prefetch) names the option in its reach and runs on the legs of `legsFor`'s answer that carry it, kept through `legsWhere`. It sets no option itself: an option set inside a test is absent from the matrix, and a test that needs an option for its answer has let a performance option stand in for correctness. The planner and algebra packages' own unit tests, which plan without executing, declare `EnableAlgebraOptimizer` as the path they pin.

5. A test whose *subject* is one backend names it and says why: the assertion reads a property only that store has, so on the others it would hold vacuously. A table of such tests carries the axis per entry rather than inheriting the global one — `queryBoundaryFaultCase.modes` is the instance, with the injected-fault case on the engine axis and the Tier-3 blob case on `byteKeyBackends`.

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
