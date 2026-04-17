# Resolved Bugs

This directory contains detailed post-mortem documentation of significant bugs that have been fixed in the Janus Datalog engine.

## Purpose

These documents serve as:
1. **Historical record** - Understanding what went wrong and why
2. **Learning resource** - Architectural lessons and debugging techniques
3. **Regression prevention** - Context for why certain code exists and tests are structured the way they are
4. **Onboarding material** - Examples of complex debugging scenarios

## Document Structure

Each bug report follows a consistent format:
- **Status** - Current state (all are RESOLVED ✅ in this directory)
- **Summary** - Quick overview of the problem
- **Symptom** - How the bug manifested (error messages, test failures)
- **Root Cause Analysis** - Deep dive into what went wrong
- **The Actual Fix** - Code changes with before/after comparison
- **Verification** - How we confirmed the fix works
- **Why This Bug Was Subtle** - What made it hard to find
- **Architectural Lesson** - Key takeaways for future development

## Bugs Documented

### [CONDITIONAL_AGGREGATE_REWRITING_BUG.md](./CONDITIONAL_AGGREGATE_REWRITING_BUG.md)
**Fixed:** October 12, 2025

**Problem:** Multi-phase queries with conditional aggregate rewriting were dropping aggregate input variables during phase symbol propagation.

**Key Lesson:** Phase metadata must be treated as transitive - when a phase creates metadata that affects execution, that metadata must propagate to ALL subsequent phases.

**Test:** `TestConditionalAggregateRewritingE2E`

---

### [INPUT_PARAMETER_KEEP_BUG.md](./INPUT_PARAMETER_KEEP_BUG.md)
**Fixed:** October 12, 2025

**Problem:** Phase symbol update logic was incorrectly adding input parameters to Keep even when those parameters weren't in the relation produced by the phase patterns.

**Key Lesson:** Input parameters are metadata, not data. `Keep ⊆ Provides ∩ Available` - you can only keep symbols that actually exist in the relation.

**Test:** `TestExecuteQueryWithTimeInput`

---

### [PHASE_REORDERING_CONDITIONAL_AGGREGATE_BUG.md](./PHASE_REORDERING_CONDITIONAL_AGGREGATE_BUG.md)
**Fixed:** October 14-17, 2025 (during fix-buffered-iterator-architecture branch)

**Problem:** Phase reordering alone was breaking all subquery execution, returning 0 tuples when it should return multiple tuples. Initially thought to be interaction with conditional aggregates, but turned out to be core phase reordering issue.

**Key Lesson:** Test optimization interactions - bugs can be masked by other optimizations that happen to work around them.

**Test:** `TestOptimizationComposition`, `TestConditionalAggregateRewritingE2E`

---

### [IDENTITY_ZERO_BUG_WITH_REORDERING.md](./IDENTITY_ZERO_BUG_WITH_REORDERING.md)
**Fixed:** October 14-17, 2025 (during fix-buffered-iterator-architecture branch)

**Problem:** Entity Identity values were becoming empty Identity objects (zero values) during query execution when phase reordering was enabled, breaking subqueries that depend on entity values.

**Key Lesson:** Identity propagation through phase boundaries requires careful handling - empty Identities are not the same as nil and can cause silent data corruption.

**Test:** `TestConditionalAggregateRewritingE2E`

---

### [SUBQUERY_PARAMETER_BUG_INVESTIGATION.md](./SUBQUERY_PARAMETER_BUG_INVESTIGATION.md)
**Fixed:** October 14-17, 2025 (during fix-buffered-iterator-architecture branch)

**Problem:** After fixing `extractSubqueryParameters()` to use correct parameter names, subqueries returned wrong results (day numbers instead of event values) when phase reordering was enabled. Mapping between subquery arguments and parameters was broken.

**Key Lesson:** Parameter name vs argument name distinction matters - planner and executor must use consistent symbol naming for subquery parameter passing.

**Test:** `TestConditionalAggregateRewritingE2E` (with reordering enabled)

---

### [BUG_ENTITY_JOIN_LOSES_FIRST_TUPLE.md](./BUG_ENTITY_JOIN_LOSES_FIRST_TUPLE.md)
**Fixed:** October 16, 2025

**Problem:** Joining two patterns on the same entity variable lost the first tuple. `StreamingRelation.IsEmpty()` called `Next()` to peek, consuming the first tuple from the iterator. Later materialization only captured remaining tuples.

**Key Lesson:** `IsEmpty()` on streaming iterators is dangerous - must consume data to check. Type-based dispatch (MaterializedRelation vs StreamingRelation) prevents such bugs.

**Test:** `TestEntityJoinBug`, `TestMultipleAggregateSubqueriesNilBug`

---

### [INVESTIGATION_JOIN_RETURNS_ZERO.md](./INVESTIGATION_JOIN_RETURNS_ZERO.md)
**Fixed:** October 15, 2025

**Problem:** After implementing lazy materialization, basic join queries returned 0 results. Root cause: `ProjectIterator` accessed raw storage iterator directly instead of calling `Relation.Iterator()` which respects caching. This consumed the iterator before join could use it.

**Key Lesson:** Iterator composition must respect relation-level abstractions. Operations should wrap Relations, not raw Iterators.

**Test:** `TestDebugBasicQuery`

---

### [EXTERNAL_REVIEW_2026_04.md](./EXTERNAL_REVIEW_2026_04.md)
**Fixed:** April 17, 2026

**Problem:** A full-codebase external review identified 24 items: five correctness bugs (CRDT tombstone handling, Op-at-last key decoding, pre-expansion index enum use, false-positive debug panic), one feature gap (EDN parser rejecting `:db.cardinality/vector`), one self-rule violation (`SubqueryWorkerCount` global), and 17 cleanup/dead-code/doc-drift items.

**Key Lessons:**
1. **Globals survive refactors quietly.** Three of four streaming flags were threaded through `ExecutorOptions`; a fourth (`SubqueryWorkerCount`) was missed and drifted from the rule for months.
2. **Dead code accumulates its own bugs.** `batch_iterator.go` (zero callers) had the same pre-expansion enum bug as its live counterpart, plus natural-order Tx encoding.
3. **Optimizations need empirical validation before wiring.** The `RepeatOffsets` ring was expected to give 2–5% compression on structured data; measured result on the actual Datom-scale corpus was a net regression for most inputs. The algorithm had sat unwired for exactly this reason.
4. **Version-gated format evolution is a design commitment.** A codec change that loses backward-read compatibility breaks every existing BadgerDB database at once.

**Commits:** `03205b0`, `f56aef8`, `cffa61b`, `b8eb8c2`, `2ca8f7d`, `c791358`

---

## Contributing

When documenting a new resolved bug:

1. **Create a new .md file** with a descriptive name
2. **Follow the existing format** for consistency
3. **Include concrete examples** from the actual failing test
4. **Show the fix** with before/after code
5. **Extract the lesson** - what does this teach us about the system?
6. **Link related tests** so future developers understand the coverage

## Related Directories

- `docs/wip/` - Active bug investigations and in-progress work
- `docs/archive/` - Completed documentation and feature designs
- `tests/` - Test files that verify these bugs stay fixed
