# Datalog Performance Investigation

## Overview

This document captures our investigation into performance issues with janus-datalog when running complex queries against the gopher-street database.

## Test Environment

- **Test Database**: `/Users/wbrown/go/src/github.com/wbrown/gopher-street/datalog-db`
- **Query Reference**: `/Users/wbrown/go/src/github.com/wbrown/gopher-street/datalog_queries_reference.md`
- **Test Tool**: Enhanced datalog CLI with `-verbose` flag for performance analysis

## Key Findings

### 1. Cross-Phase Binding Issue (FIXED)

**Problem**: When a variable was bound in one phase, patterns in subsequent phases weren't using that binding for index selection.

**Example**:
```datalog
[:find (max ?time)
 :where
 [?s :symbol/ticker "CRWV"]      ; Phase 1: binds ?s
 [?p :price/symbol ?s]            ; Phase 2: should use ?s binding
 [?p :price/time ?time]]
```

**Before Fix**:
```
Phase 2:
  Index: AEVT for [?p :price/symbol ?s] (bound: E=false A=true V=false T=false)
  Scan: 12238 datoms in 10.575ms
```

**After Fix**:
```
Phase 2:
  Index: AVET for [?p :price/symbol ?s] (bound: E=false A=true V=true T=false)
  Scan: 2924 datoms in 8.838ms
```

**Impact**: 4x performance improvement for patterns with bound values from previous phases.

### 2. Intra-Phase Pattern Execution (NOT FIXED)

**Problem**: Patterns within the same phase are executed independently and joined at the end, missing optimization opportunities.

**Example**:
```datalog
[:find ?date
 :where
 [?s :symbol/ticker "CRWV"]
 [?morning-bar :price/symbol ?s]          ; Could bind ?morning-bar
 [?morning-bar :price/minute-of-day 570]  ; Could use ?morning-bar binding
 [?morning-bar :price/time ?t]            ; Currently scans ALL times
 ...]
```

**Current Behavior**:
1. All patterns in Phase 2 execute independently
2. `[?morning-bar :price/time ?t]` scans 12,238 datoms
3. Results are joined after all patterns complete

**Optimal Behavior**:
1. Execute patterns sequentially
2. Each pattern provides bindings for the next
3. `[?morning-bar :price/time ?t]` would only lookup times for ~15 morning bars

### 3. Complex Query Performance

The OHLC aggregation queries in gopher-street are particularly affected:

```datalog
[:find ?date ?open-price ?daily-high ?daily-low ?close-price ?total-volume
 :where
 ; Main query finds dates
 ; 4 subqueries for aggregations
 ; Each subquery repeats similar pattern matching
]
```

Each subquery suffers from the same issues, compounding the performance problem.

## Root Cause Analysis

The executor's phase execution follows this pattern:

```go
// Current approach (simplified)
for each pattern in phase {
    datoms = matcher.Match(pattern)  // No bindings from other patterns
    relations = append(relations, PatternToRelation(datoms))
}
result = JoinAllRelations(relations)
```

This architecture prevents patterns from benefiting from bindings produced by earlier patterns in the same phase.

## Proposed Solution

Refactor the executor to use sequential pattern execution within phases:

```go
// Proposed approach
bindings = previousPhaseBindings
for each pattern in phase {
    datoms = matcher.MatchWithBindings(pattern, bindings)
    newBindings = ExtractBindings(datoms, pattern)
    bindings = MergeBindings(bindings, newBindings)
}
```

This would allow:
1. Each pattern to benefit from previous patterns' bindings
2. Optimal index selection throughout the phase
3. Significantly reduced datom scanning

## Performance Testing Commands

### Simple Symbol Lookup
```bash
./datalog -verbose -query '[:find ?e :where [?e :symbol/ticker "CRWV"]]' $DB
```
Result: 1 datom scanned (optimal)

### Count Query
```bash
./datalog -verbose -query '[:find (count ?p) :where [?s :symbol/ticker "CRWV"] [?p :price/symbol ?s]]' $DB
```
Result: After fix, scans 2,924 instead of 12,238 datoms

### Complex Date Extraction
```bash
./datalog -verbose -query '[:find ?date
 :where
        [?s :symbol/ticker "CRWV"]
        [?morning-bar :price/symbol ?s]
        [?morning-bar :price/minute-of-day 570]
        [?morning-bar :price/time ?t]
        [(year ?t) ?year]
        [(month ?t) ?month]
        [(day ?t) ?day]
        [(str ?year "-" ?month "-" ?day) ?date]]' $DB
```
Result: Still suboptimal due to intra-phase issue

## Sequential Pattern Execution (FIXED)

**Status**: Successfully implemented sequential pattern execution within phases with full subquery support.

**Results**:
- `[?p :price/symbol ?s]` with bound ?s: 2,924 datoms (was 12,238) - 4x improvement
- `[?morning-bar :price/time ?t]` with bound entity: 15x 8-datom scans (was 12,238) - 100x improvement  
- Complex OHLC query with 4 subqueries: Works correctly, producing accurate daily aggregations
- Total improvement: ~8x fewer datom scans overall

**Key Implementation Details**:
1. Patterns execute sequentially, propagating bindings to subsequent patterns
2. Bindings from previous phases are preserved and used
3. Subqueries are properly executed after pattern matching
4. Expressions and predicates are applied in correct order

## Subquery Input Parameter Binding (NOT FIXED)

**Problem**: Subqueries don't use input parameters for index selection, causing massive performance degradation.

**Example**:
```datalog
; Main query has ?s bound to CRWV entity
[(q [:find (max ?h)
     :in $ ?sym ?y ?m ?d  ; ?sym is passed as input
     :where 
     [?b :price/symbol ?sym]  ; Should use AVET with bound ?sym
     ...]
    ?s ?year ?month ?day) [[?high]]]
```

**Current Behavior**:
```
[0ms]   Index: AEVT for [?b :price/symbol ?sym] (bound: E=false A=true V=false T=false)
[10ms]   Scan: 12238  datoms in 10.439291ms
```

**Expected Behavior**:
```
[0ms]   Index: AVET for [?b :price/symbol ?sym] (bound: E=false A=true V=true T=false)
[3ms]   Scan: 2924  datoms in 3.224375ms
```

**Impact**: 
- Each subquery scans 12,238 datoms instead of 2,924 (4x overhead)
- Daily OHLC query: 4 subqueries × 15 days × 4x overhead = 60x more datoms scanned
- Causes queries to timeout in production

**Root Cause Analysis**:
1. The planner's `Plan` method doesn't accept initial bindings
2. When `assignSubqueriesToPhases` calls `p.Plan(subq.Query)`, it can't pass the input parameters
3. The subquery is planned in isolation, treating all `:in` variables as unbound
4. This leads to suboptimal index selection for patterns using input parameters

**Code Location**:
- `datalog/planner/planner.go:1820` - `nestedPlan, err := p.Plan(subq.Query)`
- `datalog/planner/planner.go:32` - `func (p *Planner) Plan(q *query.Query) (*QueryPlan, error)`

**Proposed Fix**: 
1. **Option A**: Add `PlanWithBindings` method:
   ```go
   func (p *Planner) PlanWithBindings(q *query.Query, bindings map[query.Symbol]bool) (*QueryPlan, error)
   ```
   
2. **Option B**: Modify existing `Plan` to accept optional bindings:
   ```go
   func (p *Planner) Plan(q *query.Query, initialBindings ...map[query.Symbol]bool) (*QueryPlan, error)
   ```

3. **Option C**: Remove artificial query/subquery distinction:
   - Queries should accept Relations as input (not just map of bindings)
   - Subqueries are just queries with input Relations
   - Regular queries would accept an empty Relations array
   - This aligns with the relational model throughout the system

When planning a subquery, the `:in` clause variables should be treated as already bound, allowing optimal index selection.

**Remaining Optimization Opportunities**:
1. Storage matcher could support E+V index selection for even better performance
2. Pattern reordering within phases based on selectivity estimates  
3. Subquery result caching for repeated input values

## Performance Summary

| Optimization | Before | After | Improvement |
|--------------|--------|-------|-------------|
| Cross-phase binding | 12,238 datoms | 2,924 datoms | 4x |
| Sequential execution | 12,238 datoms | 120 datoms (15x8) | 100x |
| Combined | 24,476 datoms | 3,044 datoms | 8x |

## Next Steps

1. **Immediate**: ✅ Cross-phase binding fix (DONE)
2. **Short-term**: ✅ Sequential pattern execution (DONE)
3. **CRITICAL**: 🚨 Subquery input parameter binding (NOT DONE)
   - Subqueries must use input parameters for index selection
   - Currently causing 4x performance degradation per subquery
   - Simple fix: treat input parameters as initial bindings in query planner
4. **Future Improvements**:
   - Storage matcher enhancement for E+V index selection
   - Pattern reordering based on selectivity estimates
   - Query planner improvements for better phase splitting

## Related Files

- Query executor: `datalog/executor/executor.go`
- Storage matcher: `datalog/storage/matcher.go`
- Pattern matching: `datalog/executor/pattern_match.go`
- Query planner: `datalog/planner/planner.go`