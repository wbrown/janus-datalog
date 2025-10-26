# In-Memory Pattern Matcher Optimization Plan

**Date**: 2025-10-08
**Status**: 📋 Planning → Implementation
**Goal**: Optimize MemoryPatternMatcher from O(N) linear scans to O(1) indexed lookups

---

## Problem Statement

### Current Performance
- **In-memory OHLC (260 hours)**: 1.77s, 709k allocations
- **In-memory OHLC (22 days)**: 1.28s, 515k allocations
- **Bottleneck**: Pattern matching takes 58% of CPU time

### Root Cause
`MemoryPatternMatcher` uses linear scans:
```go
func (m *MemoryPatternMatcher) matchWithoutBindings(...) {
    for _, datom := range m.datoms {  // O(N) scan for EVERY pattern
        if matchesDatomWithPattern(datom, pattern) {
            results = append(results, datom)
        }
    }
}
```

### Why BadgerDB is Fast
BadgerDB uses **hash indices** (EAVT, AEVT, AVET, VAET, TAEV) for O(1) lookups:
- Query: `[?e :price/time ?t]` → use AEVT index, lookup by `:price/time`
- Query: `[#person1 ?a ?v]` → use EAVT index, lookup by `#person1`
- Query: `[?e ?a "value"]` → use VAET index, lookup by `"value"`

---

## Solution: IndexedMemoryMatcher

Build hash indices for in-memory datoms, matching BadgerDB's approach.

### Data Structure
```go
type IndexedMemoryMatcher struct {
    datoms []datalog.Datom

    // Lazy-initialized indices
    built          bool
    entityIndex    map[string][]int      // E.L85() → datom positions
    attributeIndex map[string][]int      // A.String() → datom positions
    valueIndex     map[uint64][]int      // hash(V) → datom positions
    eavIndex       map[string]int        // E.L85()+A.String() → position
}
```

### Index Building (Lazy)
```go
func (m *IndexedMemoryMatcher) buildIndices() {
    if m.built {
        return
    }

    m.entityIndex = make(map[string][]int, len(m.datoms)/4)
    m.attributeIndex = make(map[string][]int, len(m.datoms)/4)
    m.valueIndex = make(map[uint64][]int, len(m.datoms)/4)
    m.eavIndex = make(map[string]int, len(m.datoms))

    for i, datom := range m.datoms {
        // Entity index
        eKey := datom.E.L85()
        m.entityIndex[eKey] = append(m.entityIndex[eKey], i)

        // Attribute index
        aKey := datom.A.String()
        m.attributeIndex[aKey] = append(m.attributeIndex[aKey], i)

        // Value index (hash-based for interface{})
        vHash := hashValue(datom.V)
        m.valueIndex[vHash] = append(m.valueIndex[vHash], i)

        // EA index (unique per entity+attribute)
        eaKey := eKey + "|" + aKey
        m.eavIndex[eaKey] = i  // Keep latest
    }

    m.built = true
}
```

### Index Selection Logic (Port from BadgerDB)
```go
func (m *IndexedMemoryMatcher) chooseStrategy(pattern *query.DataPattern) matchStrategy {
    e := extractValue(pattern.GetE())
    a := extractValue(pattern.GetA())
    v := extractValue(pattern.GetV())

    // Priority order (same as BadgerDB):
    // 1. EA bound → use eavIndex for O(1) lookup
    // 2. E bound → use entityIndex
    // 3. A bound → use attributeIndex
    // 4. V bound → use valueIndex
    // 5. Nothing bound → linear scan (unavoidable)

    if e != nil && a != nil {
        return useEAIndex{e: e.(datalog.Identity), a: a.(datalog.Keyword)}
    }
    if e != nil {
        return useEntityIndex{e: e.(datalog.Identity)}
    }
    if a != nil {
        return useAttributeIndex{a: a.(datalog.Keyword)}
    }
    if v != nil {
        return useValueIndex{v: v}
    }
    return useLinearScan{}
}
```

### Indexed Matching
```go
func (m *IndexedMemoryMatcher) matchWithIndex(pattern *query.DataPattern) []datalog.Datom {
    m.buildIndices()  // Lazy build on first use

    strategy := m.chooseStrategy(pattern)

    var candidates []int  // Datom positions to check

    switch s := strategy.(type) {
    case useEAIndex:
        // O(1) lookup
        key := s.e.L85() + "|" + s.a.String()
        if pos, ok := m.eavIndex[key]; ok {
            candidates = []int{pos}
        }

    case useEntityIndex:
        // O(1) lookup, returns multiple datoms
        key := s.e.L85()
        candidates = m.entityIndex[key]

    case useAttributeIndex:
        // O(1) lookup, returns multiple datoms
        key := s.a.String()
        candidates = m.attributeIndex[key]

    case useValueIndex:
        // O(1) lookup by hash
        hash := hashValue(s.v)
        candidates = m.valueIndex[hash]

    case useLinearScan:
        // Fallback: scan all datoms
        candidates = make([]int, len(m.datoms))
        for i := range m.datoms {
            candidates[i] = i
        }
    }

    // Filter candidates by full pattern match
    var results []datalog.Datom
    for _, pos := range candidates {
        datom := m.datoms[pos]
        if matchesDatomWithPattern(datom, pattern) {
            results = append(results, datom)
        }
    }

    return results
}
```

---

## Implementation Phases

### Phase 1.1: Core Infrastructure (Day 1)
**Files to create**:
- `datalog/executor/indexed_memory_matcher.go` - Main implementation
- `datalog/executor/indexed_memory_matcher_test.go` - Unit tests

**Tasks**:
1. ✅ Define `IndexedMemoryMatcher` struct
2. ✅ Implement `buildIndices()` with lazy initialization
3. ✅ Add `hashValue()` helper for value hashing
4. ✅ Write tests for index building correctness

**Validation**: All datoms findable via indices

---

### Phase 1.2: Index Selection (Day 2)
**Files to modify**:
- `datalog/executor/indexed_memory_matcher.go`

**Tasks**:
1. ✅ Define `matchStrategy` interface
2. ✅ Implement `chooseStrategy()` (port from BadgerDB logic)
3. ✅ Add strategy types (useEAIndex, useEntityIndex, etc.)
4. ✅ Write tests for strategy selection

**Validation**: Correct strategy chosen for each pattern type

---

### Phase 1.3: Indexed Matching (Day 3)
**Files to modify**:
- `datalog/executor/indexed_memory_matcher.go`

**Tasks**:
1. ✅ Implement `matchWithIndex()` using strategy pattern
2. ✅ Implement `MatchWithConstraints()` interface method
3. ✅ Handle edge cases (empty indices, hash collisions)
4. ✅ Write comprehensive pattern matching tests

**Validation**: Same results as linear scan, but faster

---

### Phase 1.4: Integration (Day 4)
**Files to modify**:
- `datalog/executor/pattern_match.go` - Update or replace
- `datalog/executor/executor.go` - Use new matcher

**Tasks**:
1. ✅ Make `IndexedMemoryMatcher` implement `PatternMatcher` interface
2. ✅ Add `NewIndexedMemoryMatcher()` constructor
3. ✅ Update `NewMemoryPatternMatcher()` to return indexed version
4. ✅ Run all existing tests to ensure no regressions

**Validation**: All existing tests pass with new implementation

---

### Phase 1.5: Performance Validation (Day 5)
**Files to create**:
- `datalog/executor/indexed_matcher_bench_test.go`

**Tasks**:
1. ✅ Create benchmarks comparing linear vs. indexed
2. ✅ Test various pattern types (E-bound, A-bound, EA-bound, etc.)
3. ✅ Test various dataset sizes (100, 1k, 10k, 100k datoms)
4. ✅ Profile and verify speedup (target: 5-10×)

**Validation**:
- Indexed matcher 5-10× faster for large datasets
- No regression for small datasets (<100 datoms)

---

## Expected Performance Gains

### Theoretical Analysis

| Pattern Type | Linear Scan | Indexed | Speedup |
|--------------|-------------|---------|---------|
| `[?e :price/time ?v]` | O(N) | O(K) where K = datoms with `:price/time` | 10-100× |
| `[#entity1 ?a ?v]` | O(N) | O(K) where K = datoms with `#entity1` | 10-100× |
| `[#entity1 :price/time ?v]` | O(N) | O(1) | 100-1000× |
| `[?e ?a "value"]` | O(N) | O(K) where K = datoms with `"value"` | 10-100× |
| `[?e ?a ?v]` (full scan) | O(N) | O(N) | 1× (no improvement) |

### Real-World Benchmarks (Predicted)

| Query | Dataset | Before | After | Speedup |
|-------|---------|--------|-------|---------|
| **OHLC Hourly** | 260 hours, 3,120 datoms | 1.77s | 250-350ms | **5-7×** |
| **OHLC Daily** | 22 days, 264 datoms | 1.28s | 180-250ms | **5-7×** |
| **Single entity lookup** | 10k datoms | 150ms | 2-5ms | **30-75×** |
| **Attribute scan** | 10k datoms | 150ms | 10-20ms | **7-15×** |

### Memory Overhead

**Index Size Estimation**:
- Entity index: ~20 bytes/entry × N datoms = 60KB for 3k datoms
- Attribute index: ~40 bytes/entry × N datoms = 120KB for 3k datoms
- Value index: ~20 bytes/entry × N datoms = 60KB for 3k datoms
- EA index: ~50 bytes/entry × N datoms = 150KB for 3k datoms

**Total overhead**: ~400KB for 3k datoms (~130 bytes/datom)

**Trade-off**: Acceptable overhead for 5-7× speedup

---

## Testing Strategy

### Unit Tests (Correctness)
```go
func TestIndexedMatcher_Correctness(t *testing.T) {
    // Create test dataset
    datoms := createTestDatoms(1000)

    linear := NewMemoryPatternMatcher(datoms)      // Old
    indexed := NewIndexedMemoryMatcher(datoms)      // New

    // Test various patterns
    patterns := []string{
        "[?e :price/time ?v]",
        "[#entity1 ?a ?v]",
        "[#entity1 :price/time ?v]",
        "[?e ?a 100]",
        "[?e ?a ?v]",
    }

    for _, patternStr := range patterns {
        pattern := parsePattern(patternStr)

        linearResult := linear.Match(pattern, nil)
        indexedResult := indexed.Match(pattern, nil)

        // Results should be identical (order may differ)
        assert.Equal(t,
            sortRelation(linearResult),
            sortRelation(indexedResult),
            "Mismatch for pattern: %s", patternStr)
    }
}
```

### Performance Tests (Speedup)
```go
func BenchmarkPatternMatch_LinearVsIndexed(b *testing.B) {
    sizes := []int{100, 1000, 10000, 100000}
    patterns := []string{"[?e :price/time ?v]", "[#entity1 ?a ?v]"}

    for _, size := range sizes {
        datoms := createTestDatoms(size)

        for _, patternStr := range patterns {
            pattern := parsePattern(patternStr)

            b.Run(fmt.Sprintf("Linear_%d_%s", size, patternStr), func(b *testing.B) {
                m := NewMemoryPatternMatcher(datoms)
                b.ResetTimer()
                for i := 0; i < b.N; i++ {
                    m.Match(pattern, nil)
                }
            })

            b.Run(fmt.Sprintf("Indexed_%d_%s", size, patternStr), func(b *testing.B) {
                m := NewIndexedMemoryMatcher(datoms)
                b.ResetTimer()
                for i := 0; i < b.N; i++ {
                    m.Match(pattern, nil)
                }
            })
        }
    }
}
```

### Integration Tests (No Regression)
```go
func TestIndexedMatcher_AllExistingTests(t *testing.T) {
    // Run all existing executor tests with IndexedMemoryMatcher
    // to ensure no regressions
}
```

---

## Risk Mitigation

### Risk 1: Hash Collisions in Value Index
**Problem**: Different values could hash to same uint64
**Mitigation**: Always verify with `datalog.ValuesEqual()` after hash match
**Impact**: Minimal (false positives filtered, no false negatives)

### Risk 2: Index Overhead for Small Datasets
**Problem**: Building indices costs time for small datasets
**Mitigation**: Use threshold (e.g., >1000 datoms) before enabling indices
**Code**:
```go
func NewMemoryPatternMatcher(datoms []datalog.Datom) PatternMatcher {
    if len(datoms) < 1000 {
        return &LinearMemoryMatcher{datoms: datoms}  // Old implementation
    }
    return &IndexedMemoryMatcher{datoms: datoms}     // New implementation
}
```

### Risk 3: Memory Usage Increase
**Problem**: Indices use ~130 bytes/datom extra memory
**Mitigation**: Lazy initialization, only build when first query runs
**Impact**: Acceptable (400KB for 3k datoms)

### Risk 4: Complexity Increase
**Problem**: More complex code to maintain
**Mitigation**:
- Keep both implementations (linear and indexed)
- Extensive tests for correctness
- Clear documentation

---

## Success Criteria

### Must Have ✅
1. All existing tests pass with IndexedMemoryMatcher
2. Correctness verified against LinearMemoryMatcher
3. 5× speedup on OHLC queries (1.77s → 350ms target)
4. No regression on small datasets (<100 datoms)

### Nice to Have 🎯
1. 10× speedup on single-entity lookups
2. Memory overhead < 200 bytes/datom
3. Index build time < 10ms for 10k datoms

### Documentation 📚
1. Update PERFORMANCE_STATUS.md with results
2. Add implementation notes to CLAUDE.md
3. Benchmark results in commit message

---

## Rollout Plan

### Step 1: Development Branch
```bash
git checkout -b feature/indexed-memory-matcher
```

### Step 2: Implementation (5 days)
- Day 1: Core infrastructure
- Day 2: Index selection
- Day 3: Indexed matching
- Day 4: Integration
- Day 5: Performance validation

### Step 3: Testing
```bash
go test ./datalog/executor/... -v
go test ./datalog/executor/... -bench=. -benchmem
```

### Step 4: Review & Commit
- Code review
- Update documentation
- Commit with detailed benchmarks

### Step 5: Merge to Main
```bash
git checkout main
git merge feature/indexed-memory-matcher
```

---

## Future Work (Phase 2+)

### Phase 2: Pattern Match Fast Paths
- Inline common type comparisons
- Compile patterns to specialized functions
- Target: Additional 20% speedup

### Phase 3: Tuple Optimization
- Implement tuple pooling
- Add copy-on-write semantics
- Target: 15% less memory

### Phase 4: Streaming Joins
- Streaming hash join operator
- Iterator fusion
- Target: 25% less memory for large results

---

## References

- Profiling results: `PROFILING_SUMMARY.md`
- Hash join optimization: `HASH_JOIN_PRESIZING_SUMMARY.md`
- BadgerDB matcher: `datalog/storage/matcher.go`
- Current implementation: `datalog/executor/pattern_match.go`
