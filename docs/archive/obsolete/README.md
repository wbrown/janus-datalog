# Obsolete — superseded documentation

Outdated status files and documents whose premise no longer holds. Historical reference only; do not read for current state.

This category is named in `docs/archive/README.md` but had no contents until 2026-07-31.

## `QUERY_EXECUTOR_FEATURE_GAPS.md`

An audit dated 2026-01-16 comparing "the legacy phase-based executor" against "the new QueryExecutor," with two hand-maintained ✅/❌ status tables, per-gap severity labels, and a timeline.

**Why it is obsolete.** Its premise is gone: that migration resolved, and `CLAUDE.md` describes the RealizedPlan/QueryExecutor architecture as current rather than as a candidate. Its status tables then drifted the way a hand-maintained view over code always does. The clearest instance is the row *"Input Parameter Projection | ❌ Missing | LOW — input params only in `:find` may fail"* — that was fixed by `renderConstantFindSymbols` and later rewritten as `group.Join(env.Project(missing))` when `:in` parameters became an environment Relation. Two further rows, "Storage Constraints" and "Time Range Optimization", describe infrastructure that has since been replaced rather than added.

**Where current information lives.** `ARCHITECTURE.md` and `CLAUDE.md` for the execution architecture; `docs/wip/PHASE_AS_QUERY_ARCHITECTURE.md` for the phase-as-query design; `docs/reference/PLANNER_OPTIONS.md` for what the planner does and does not do. Executor behaviour is specified by its tests.

## `TEST_COVERAGE_PLAN.md`

A revised coverage plan opening with "Current State" figures — executor 40.9%, planner 57.6% — and a prioritised work list of executor functions needing tests.

**Why it is obsolete.** The figures carry no date, so they assert a present that was already past. More decisively, **none of the functions its Priority 1 list names still exists**: `sortRelation`, `compareTupleValues`, `applyExpression`, `evaluateFunction` and the `extractYear`/`extractMonth`/`extractDay`/`extractHour` family all return zero matches in the tree. The dead unexported value functions were deleted by ruling during the function-registry consolidation, and the expression and sort paths were rewritten. A work list against deleted code cannot be executed or corrected — only rewritten from a fresh measurement.

**Where current information lives.** Run `go test -cover` for coverage. `docs/wip/CORRECTNESS_MEASUREMENT.md` holds the proposed oracle hierarchy — a different and more useful framing of the same concern — and `docs/wip/OPTIMIZER_MODE_MATRIX.md` holds the ruling that every query test runs both planner paths.

## `PHASE5_STATUS.md`

A status file for the Phase 5 CRDT-vector work, dated 2026-01-31. It lived at `datalog/storage/PHASE5_STATUS.md` — **inside the Go package it described** — which is why every documentation sweep missed it.

**Why it is obsolete.** Its headline is *"Bug #5 solution designed (implementation pending)"* and it carries a five-step implementation plan; Bug #5 shipped long ago — `AfterRef` is in the key and `OpRGAInsert`/`OpRGATombstone` exist. Worse, the key layout it specifies is **wrong in two ways**, sitting a few files from the encoder that contradicts it:

- It gives `[Lookup components][Tx][Op][AfterRef?]` — **Op before AfterRef**. The real layout is `[…][Tx↓][AfterRef?][Op]`, and Op must be last so the tail arithmetic can read it from the final byte to learn whether an AfterRef block precedes it. See `OP_POSITION_PROOF.md`.
- Its index table lists **six** indices. There are eight; `AETV` and `ATEV` are absent.

It also freezes a January test run as a PASS list, and reports E-unbound vector queries as returning raw RGA bytes.

**One section of it was still accurate** and is noted here so it is not lost: the priority-based selectivity scoring (constants weighted 100, available variables 10) is live at `datalog/planner/clause_utils.go` in `scoreClause` and `countSelectivityFactors`, and the reasoning traces to `docs/papers/PAPER_PROPOSAL_1_GREEDY_JOINS.md`.

**Where current information lives.** `datalog/storage/key_encoder_binary.go` is authoritative for key layout; `docs/reference/KEY_ENCODING_AND_CRDT.md` and `docs/reference/OP_POSITION_PROOF.md` document it. Vector semantics are in `docs/reference/CRDT.md`.

## `READ_PATH.md`

A walkthrough of the read path for a bound-entity query, formerly at `docs/dev-notes/READ_PATH.md`. It opens *"Based on my thorough investigation…"* — a generated investigation transcript rather than a maintained document.

**Why it is obsolete.** PR #114 rewrote the seam it traces. The document shows `chooseIndex` returning `(IndexType, start, end)` and cites `matcher.go lines 489-677`; the function returns a typed `ScanBound`, takes no encoder, and lives at line 359. Re-tracing the path is a fresh piece of work, not an edit to this one.

**Where current information lives.** `datalog/storage/matcher.go` (`chooseIndex`), `docs/reference/INDEX_SELECTION_PROOF.md` for the selection matrix and state machine, and `docs/reference/CRDT_STREAMING_RESOLUTION.md` for what the scan is then resolved by.
