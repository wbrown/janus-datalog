# Bug Fixes and Learnings - REQUIRED READING

**This file is PART OF CLAUDE.md and must be read before making any code changes.**

This document catalogs critical bugs that have been fixed and the patterns that lead to them. Understanding these patterns prevents repeating the same mistakes.

---

## Storage Layer Integration (2025-06-21)

1. **Attribute Encoding Bug**: Pattern matcher was passing raw keyword bytes (10 bytes) to EncodePrefix which expected 20-byte arrays. Fixed by converting to storage format first.

2. **Identity Decoding Bug**: Entity IDs were being decoded with `NewIdentity(sd.E.String())` which created new identities with different hashes. Fixed by using `NewIdentityFromHash(sd.E)`.

3. **Value Type Preservation**: Identity values stored as references must decode back to the same Identity for joins to work. Fixed ValueFromBytes to properly reconstruct Identity from hash.

**Pattern**: Type mismatches between storage and query layers. Always verify type conversions preserve semantics, especially for identity/hashing.

---

## Expression Clauses (2025-06-21)

1. **Smart Joining**: Expression clauses that need variables from multiple relations now automatically join those relations first.

2. **Type Handling**: Proper type conversions for arithmetic operations between int64, int, and float64.

3. **Evaluation Order**: Fixed critical bug where predicates were evaluated before expressions in phases. Expressions must be evaluated first so predicates can use their output symbols.

**Pattern**: Execution order matters. If operation B depends on output of operation A, A must execute first.

---

## Query Optimization (2025-06-23)

1. **Predicate Pushdown**: Intra-phase predicates are now applied immediately after their required symbols are available, reducing intermediate result sizes.

2. **Equi-Join Optimization**: Equality predicates between phases are detected and pushed into join conditions, avoiding massive cross-products.

3. **Performance Annotations**: Fixed 150x slowdown caused by unnecessary unique value counting in annotation code.

**Pattern**: Premature optimization is real. Profile first, optimize second. The annotation code seemed harmless but was doing expensive counting on every call.

---

## Aggregation Scoping (2025-06-23)

1. **Issue Identified**: Current aggregation model (following Datomic) creates Cartesian products when mixing aggregated and non-aggregated values from different scopes.

2. **Solution Designed**: Datomic-style subqueries provide clean scoping for aggregations without breaking compatibility.

3. **Implementation Plan**: Created detailed plan in `docs/archive/completed/subquery-implementation-plan.md` for 3-phase implementation approach.

**Pattern**: Design before implementation. Complex features need detailed planning to avoid architectural mistakes.

---

## Subquery Implementation (2025-06-23)

1. **Completed**: Full implementation of Datomic-style subqueries with TupleBinding and RelationBinding

2. **Bug Fixed**: Input variables are now properly available during predicate evaluation in subqueries

3. **Solves Aggregation Bug**: Subqueries properly scope aggregations, solving the Cartesian product issue

4. **Demo Added**: `examples/subquery_proper_demo.go` demonstrates multi-day OHLC aggregation without Cartesian products

**Pattern**: Test with real-world use cases. The OHLC demo proved the implementation works for production scenarios.

---

## Result/Relation Unification (2025-06-23)

1. **Design Fix**: Unified redundant Result and Relation types

2. **Result as Alias**: Result is now a type alias for MaterializedRelation

3. **Consistent API**: All query execution returns Relation interface

4. **Table Formatter**: Added table formatting utilities for debugging Relations

**Pattern**: Eliminate redundancy. Two types doing the same thing creates confusion and maintenance burden.

---

## Order-By Implementation (2025-06-24)

1. **Parser Support**: Added parsing for `:order-by` clause with `[?var :asc/:desc]` syntax

2. **Executor Implementation**: Added sorting after query execution with type-aware comparison

3. **Multi-symbol Sorting**: Supports multiple sort keys with independent directions

4. **Type-aware Comparison**: Properly handles all value types including time.Time

**Pattern**: Type-aware operations throughout. Don't assume all values are strings or numbers.

---

## Time Comparison Fix (2025-06-24)

1. **Bug Identified**: `compareValues` function didn't handle time.Time, causing string comparison

2. **Root Cause**: Times were being compared lexicographically ("2025-06-17" > "2025-06-20")

3. **Fix Applied**: Added time.Time case to compareValues using Before()/After() methods

4. **Impact**: Fixed `min`/`max` aggregations and all comparison predicates for time values

**Pattern**: Missing type case in switch. Always have a default case that errors on unknown types rather than falling through to wrong behavior.

---

## Table Formatter Enhancement (2025-06-24)

1. **Markdown Output**: Replaced ASCII tables with clean markdown using tablewriter library

2. **Header Preservation**: Disabled auto-formatting to preserve exact variable names (e.g., ?var)

3. **Relation Methods**: Added String() and Table() methods to Relation interface

4. **Colored Output**: String() method includes ANSI colors matching annotation format

**Pattern**: Debug output quality matters. Good formatting makes debugging 10x faster.

---

## RelationInput and Subquery Iteration (2025-08-26)

**Problem**: Subqueries were executing sequentially for each input combination (870 times for OHLC queries)

**Solution Implemented**: Proper RelationInput iteration semantics
- `:in $ [[?x ?y] ...]` now iterates over each tuple
- Query executes once per tuple with correct aggregation scoping
- Semantically correct (each tuple processed independently)

**Current Status**:
- ✅ Correct semantics - aggregations compute per-tuple not globally
- ⏳ Still sequential execution (performance optimization needed)
- See `docs/archive/2025-10/SUBQUERY_PERFORMANCE_ANALYSIS.md` for full details

**Key Insight**: Datalog is not SQL - no implicit GROUP BY. Getting the semantics right is more important than speed.

**Pattern**: Correctness first, performance second. Don't optimize prematurely if it breaks semantics.

---

## Bindings to Relations Migration (2025-06-26)

1. **Motivation**: Simple Bindings (map[Symbol]interface{}) couldn't support multi-value variable bindings needed for batch operations

2. **Migration**: Replaced Bindings with Relations throughout the codebase - this was the RIGHT decision!

3. **Performance Work**: Iterator reuse and batch scanning optimizations explored
   - Batch scanning implemented with threshold-based activation (>100 tuples)
   - SimpleBatchScanner used for large binding sets
   - Benchmarks show code clarity benefits, modest performance impact
   - See `PERFORMANCE_STATUS.md` for current state

**Pattern**: When existing abstractions can't handle new requirements, replace them entirely rather than patching.

---

## Decorrelation Pure Aggregation Bug (2025-10-10)

**Critical Correctness Bug**: Multiple pure aggregation subqueries returned `nil` values instead of correct aggregates.

**Root Cause**: The decorrelation optimization made a **category error** - it treated all aggregations the same:
- Pure aggregations: `[:find (max ?x)]` → Single global aggregate
- Grouped aggregations: `[:find ?group (max ?x)]` → Aggregate per group

Adding input parameters to pure aggregations **changed their type** from single to grouped, breaking semantics.

**The Fix**: Modified `extractCorrelationSignature()` to distinguish pure vs grouped aggregations. Only grouped aggregations are decorrelated.

**Why Tests Missed It**:
1. **Tested outcomes, not structure** - Verified result values but not find clause structure
2. **Simple data masked the problem** - Small test data (2-5 tuples) still produced correct-looking results by accident
3. **No structural invariants** - Didn't verify aggregation type preservation
4. **Missing annotations** - Couldn't observe internal transformations

**Lessons Learned**:
1. **Optimizations must preserve semantics** - Test that transformations don't change query meaning
2. **Use realistic data sizes** - Test with 100s-1000s of tuples, not just 2-5
3. **Test internal structure** - Use annotations to verify intermediate transformations
4. **Category distinctions matter** - Pure vs grouped aggregations are fundamentally different
5. **Annotations catch root causes** - They revealed wrong find clause structure, not just nil symptoms

**See**: `docs/bugs/resolved/DECORRELATION_BUG_FIX.md` for full details.

**Pattern**: Optimizations must preserve query semantics. Test transformations at the structural level, not just outcomes.

---

## Input Parameter Semantics (2025-10-13)

**Three Related Bugs** revealed the importance of correctly handling input parameters from `:in` clauses.

**Key Insight**: Input parameters are "environment" symbols (Available) not "data" symbols (Provides). They're metadata ABOUT query execution, not data IN the result.

**The Three-Level Type System**:
1. **Input Parameters**: Environment symbols available in ALL phases for filtering/correlation
2. **Pattern Variables**: Computation symbols that flow between phases via joins
3. **Relation Symbols**: Actual data in phase output relations

**Critical Invariants**:
```
Available = Environment symbols (inputs + previous outputs)
Provides = Relation symbols (what this phase produces)
Keep ⊆ Provides ∩ Available  (can only keep what's in the relation)
```

**Bugs Fixed**:

1. **INPUT_PARAMETER_KEEP_BUG** (Oct 12): Phase symbol calculation incorrectly added input parameters to Keep even though they weren't in the relation, causing projection errors. Fixed by checking Keep ⊆ Provides ∩ Available.

2. **BUG_PARAMETERIZED_QUERY_CARTESIAN_PRODUCT** (Oct 13): Selectivity scoring treated input parameters as unbound variables (+5 score) instead of bound like constants (-500 score), causing wrong phase ordering. Fixed by passing availableSymbols to scorePattern().

3. **BUG_STRING_PREDICATES_CANT_USE_PARAMETERS** (Oct 13): Predicate assignment only made input parameters available in phase 0, not subsequent phases, causing "predicates could not be assigned" panic. Fixed by using phases[i].Available which includes inputs for all phases.

**Analogy**: Input parameters are like SQL prepared statement parameters - they filter data but don't appear as result symbols:
```sql
-- ?symbol filters but isn't in output
SELECT time, close FROM prices WHERE symbol = ?
```

**See**: `docs/INPUT_PARAMETER_SEMANTICS.md` for comprehensive guide with examples and testing patterns.

**Pattern**: Understand the type system. Input parameters, pattern variables, and relation symbols are fundamentally different types with different semantics.

---

## Expression-Only Phases Bug (2025-10-14)

**Critical Bug**: Phases containing only expressions (no patterns) received empty relations instead of previous phase's results, causing zero-tuple outputs.

**Root Cause**: In `executor_sequential.go`, the phase execution logic built up `independentGroups` through pattern matching. When a phase had zero patterns, the pattern loop never executed, leaving `independentGroups` empty. This empty slice was then passed to `applyExpressionsAndPredicates()` instead of the previous phase's results.

**Symptoms**:
- Phase completes with 0 tuples when it should have N tuples
- No `expression/begin` or `expression/complete` annotations in logs
- Conditional aggregate rewriting returns empty results

**The Fix**:
```go
// If phase has no patterns, use availableRelations (results from previous phase)
collapsed := independentGroups
if len(phase.Patterns) == 0 && len(collapsed) == 0 {
    collapsed = availableRelations
}
```

**Detection Method**: Added expression annotations and searched for `grep "expression/"`. Finding zero annotations revealed expressions weren't executing at all.

**Key Lesson**: **Absence of expected annotations is as important as presence of error annotations.** If you expect certain events but see none, investigate immediately.

**General Pattern - "Pure-Type Phases"**: Any phase containing only ONE type of operation (patterns, expressions, predicates, subqueries) is vulnerable to this bug class. Always test:
- Pure pattern phases (no expressions/predicates)
- Pure expression phases (no patterns) ← **This bug**
- Pure predicate phases (no patterns/expressions)
- Empty phases (should probably error)

**Phase Execution Invariants**:
1. **Input Invariant**: Every phase receives either `nil` (first phase, no inputs) or previous phase's `Keep` symbols
2. **Output Invariant**: Every phase produces a Relation with symbols matching `phase.Provides` (or subset in `Keep`)
3. **Data Flow Invariant**: If Phase N produces K tuples with symbols S, and Phase N+1 needs S' ⊆ S, then Phase N+1 receives K tuples with S' available
4. **Composition Invariant**: Patterns, expressions, predicates, subqueries can appear in any combination (including zero), and phase execution MUST handle all combinations

**See**: `docs/bugs/resolved/BUG_EXPRESSION_ONLY_PHASES.md` for detailed analysis and debugging guide.

**Pattern**: Don't assume phases always have certain components. Test all combinations including edge cases.

---

## Decorrelation Inside Union Branches (2026-03-21)

**Critical Semantic Bug**: The algebra bridge's decorrelation pass transformed correlated subqueries inside Union branches into uncorrelated ones, producing Cartesian products.

**Root Cause**: Decorrelation moves the correlation variable from `:in` (input) to `:find` (output). Inside a Union, the other branch (ground default) doesn't have this variable. The Union produces branches with incompatible schemas. When joined with the outer relation, the ground branch rows cross-product because they lack the join key.

**The Fix**: The decorrelation transform checks `ctx.Parent.Rule == RuleUnion` using the EBNF transform framework's `TransformContext.Parent`. LateralJoins inside Union nodes are not decorrelated — their per-tuple correlation is semantically load-bearing.

**Key Insight**: Algebraic equivalence rules have structural preconditions. The decorrelation rule `R ⋈_L S(r.x) → R ⋈ (S GROUP BY x)` requires that the LateralJoin result is consumed directly by a Join. Inside a Union, the result is unioned with other branches first — a different structure the rule doesn't cover.

**Pattern**: Optimization rules are not universally applicable. Always verify the structural context matches the rule's preconditions. A rule that's valid at the top level may be invalid inside a compound operator.

---

## Meta-Patterns Across All Bugs

### 1. Type Mismatches Kill
- Storage vs query types (Attribute Encoding Bug)
- Input parameters vs pattern variables (Input Parameter Bugs)
- Pure vs grouped aggregations (Decorrelation Bug)

**Rule**: Make types explicit and enforce distinctions.

### 2. Execution Order Matters
- Expressions before predicates (Expression Clauses)
- Predicates as soon as symbols available (Predicate Pushdown)
- Pattern execution before expressions (Expression-Only Phases)

**Rule**: Define and enforce dependency ordering.

### 3. Test Structure, Not Just Outcomes
- Decorrelation bug passed value tests, failed structure tests
- Annotations reveal what's happening, not just results
- Small test data masks category errors

**Rule**: Use annotations to verify internal transformations.

### 4. Edge Cases Are Real Cases
- Expression-only phases (Expression-Only Phases Bug)
- Empty relations (multiple bugs)
- Single tuple inputs (Decorrelation Bug)

**Rule**: Test all combinations, especially the weird ones.

### 5. Correctness Before Performance
- RelationInput semantics over speed
- Proper aggregation scoping over optimization
- Type preservation over clever encoding

**Rule**: Make it right, then make it fast.

---

## Streaming Architecture Violation (2026-02-05)

**Critical Architecture Bug**: Created a buffering "CRDT resolution" layer that defeated the entire streaming architecture.

**What I Did Wrong**:
1. Created `CRDTResolvingIterator` that buffers ALL datoms for an (E, A) group
2. Added `copyDatom()` function to copy iterator results into a buffer
3. Built complex "resolution" logic (resolveLWW, resolveAddWins, resolveRGA)
4. Completely ignored that the storage layer already solves this problem

**Why It Was Wrong**:
The EATV index stores Tx with **bitwise NOT for descending order**. This means:
- First entry for each (E, A) IS the LWW winner
- No resolution logic needed for CardinalityOne
- Just skip subsequent entries with same (E, A)

The comments in `key_encoder_binary.go` say it explicitly:
```go
// EATV: [prefix][E][A][Tx↓][type][value][Op][AfterRef?] - first entry is current
// Tx is encoded with bitwise NOT for descending sort order (highest Tx first)
```

**Red Flags I Should Have Noticed**:
1. Creating a `copy*` function for iterator results → buffering smell
2. Accumulating datoms in a slice → materialization smell
3. Building "resolution" logic → the index already does this
4. Adding complexity to "solve" something → should have asked why it's needed

**The Correct Approach for CardinalityOne**:
```go
// Track current (E, A), skip duplicates
// First entry wins because EATV orders Tx descending
// Pure filtering, zero buffering
```

**The Correct Approach for CardinalityMany**:
```go
// Track state per value: map[valueKey]{highestAdd, highestRemove}
// When (E, A) changes, emit values where add >= remove
// Buffer state, not datoms
```

**Pattern**: The storage layer index ordering IS the CRDT resolution. If you think you need resolution logic, you don't understand the storage layer. READ THE INDEX COMMENTS.

**Meta-Pattern**: If you're buffering iterator results, you're breaking streaming. STOP and ask.

---

## Cache Path CardinalityOne Tombstone Gap (2026-02-08)

**Critical Correctness Bug**: `ResolveLWW` (cache path) does not check `datom.Op`, so Remove() tombstones on CardinalityOne attributes are invisible to PullInto and multi-clause join-bound queries. The streaming path (`CRDTResolvingIterator`) handles it correctly.

**What I Did Wrong**:
1. Wrote `ResolveLWW` without checking `datom.Op` — returns first datom's V blindly
2. Wrote 13 Remove tests that ALL bind E via `:in` parameters — streaming path only
3. Never tested Remove() through PullInto or multi-clause join-bound E — cache path
4. Reported "all tests pass" with zero coverage of the buggy code path

**Why Tests Missed It**:
The tests looked comprehensive: round-trip, overwrite, re-add, V-irrelevant, multi-entity, bound query, V-bound query, unbound query. But every test used `[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]`, which goes through `CRDTResolvingIterator` (streaming), never through `ResolveLWW` (cache). 13 passing tests, zero coverage of the bug.

**The Trust Problem**:
Claude wrote the buggy code, wrote tests that don't cover it, and shipped it as complete. The user cannot trust "all tests pass" as evidence of correctness. This bug was only found because the user built a real application on top and hit it in production use.

**See**: `docs/bugs/BUG_CACHE_CARDINALIY_ONE_TOMBSTONE.md` for full analysis and reproducer.

**Root Cause Mental Model Error**: Claude thinks of datoms as values that replace each other — "Set name to Bob overwrites Alice." This is wrong. Storage is append-only. Both datoms exist. Resolution reads the first entry (highest Tx) and interprets the **operation**. If the operation is a tombstone, the attribute doesn't exist. The word "overwrite" encodes a mutable-storage mental model that directly caused this bug: ResolveLWW returns the first entry's V without checking its Op, because in the "overwrite" model there's nothing to check.

**Correct model**: Datoms are operation records. Resolution interprets operations. Every code path that reads a datom must check Op. No exceptions.

**Pattern**: When multiple code paths resolve the same semantic operation, tests must cover ALL paths. Test quantity and scenario variety mean nothing if they all exercise the same code path.

**Meta-Pattern**: Claude does not reliably catch its own coverage gaps. Apparent test thoroughness (many tests, many scenarios) creates false confidence when all tests go through the same path.

---

### 6. Understand Before Implementing
- Read code AND understand what it means
- Index ordering has semantic meaning (descending Tx = first wins)
- Don't treat features as problems to solve without understanding existing design

**Rule**: If you read comments explaining WHY something works a certain way, actually think about the implications.