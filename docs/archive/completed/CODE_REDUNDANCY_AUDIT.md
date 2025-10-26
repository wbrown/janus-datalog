# Code Redundancy Audit

## Summary
The codebase has accumulated various V2 implementations and duplicate functions, mostly from the Relations migration and recent performance work.

## Major Redundancies Found

### 1. Executor Phase Functions
**Files**: executor.go, executor_sequential_v2.go
- `executePhaseSequentialV2` - Only version, but named V2 (should be renamed)
- `executePhasesWithInputs` - Main version
- `executePhasesWithInputsNonIterating` - Created for RelationInput workaround
- **Action**: Merge these or make clear when each is used

### 2. Storage Matcher Functions  
**Files**: matcher.go, matcher_v2.go
- `MatchOld` - Deprecated, returns []Datom
- `MatchWithRelationOld` - Deprecated but still used in tests
- `Match` - Current version in matcher_v2.go returning Relation
- **Action**: Remove Old versions, update tests

### 3. Subquery Execution Functions
**File**: subquery.go (28 functions!)
- `executeSubquery` - Method on Executor
- `ExecuteSubquery` - Standalone function
- `ExecuteSubqueryWithExecutor` - Another variant
- `executePhasesWithInputs` - Duplicate of executor version
- `canBatchSubquery` / `executeBatchedSubquery` - Disabled batching attempt
- **Action**: Consolidate to single implementation

### 4. Pattern Matching Interfaces
- `PatternMatcher` interface has multiple implementations
- `MemoryPatternMatcher` for tests
- `BadgerMatcher` for production
- Both have different method signatures for same operations
- **Action**: Unify interface

### 5. Result vs Relation Types
- `Result` type aliased to `MaterializedRelation`
- Still referenced in many places
- **Action**: Complete migration to Relation

## File-Specific Issues

### executor_sequential_v2.go (242 lines)
- Only contains `executePhaseSequentialV2` 
- Should be merged into executor.go
- Remove V2 suffix

### matcher_v2.go (1000+ lines)
- Is the "current" version but named V2
- Should be renamed to matcher.go
- Old matcher.go should be archived

### subquery.go (700+ lines)
- Too many functions (28!)
- Multiple ways to do same thing
- Lots of helper functions that could be private

## Naming Issues
- Functions with "V2" that have no V1
- Functions with "Old" still in use
- Functions with "NonIterating" (implementation detail in name)

## Test Redundancy
- executor_subquery_test.go (217 lines)
- executor_subquery_complete_test.go (221 lines)
- executor_subquery_comprehensive_test.go (584 lines)
- executor_subquery_datomic_test.go (173 lines)
- Multiple test files testing same functionality

## Recommendations

### Immediate Actions (High Priority)
1. **Remove V2 suffixes** where there's no V1
2. **Delete Old functions** and update tests
3. **Merge executor_sequential_v2.go** into executor.go
4. **Consolidate subquery execution** to one path

### Medium Priority
1. **Rename matcher_v2.go** to matcher.go
2. **Unify PatternMatcher interface**
3. **Consolidate test files**
4. **Make helper functions private**

### Low Priority
1. **Complete Result→Relation migration**
2. **Remove disabled batching code**
3. **Clean up function names** (remove implementation details)

## Impact Assessment
- **Code size reduction**: ~20-30% (estimate 1000+ lines)
- **Clarity improvement**: Single path for each operation
- **Maintenance**: Easier to understand and modify
- **Performance**: No impact (removing duplicates)

## Migration Strategy
1. Start with removing "Old" functions
2. Then remove V2 suffixes
3. Consolidate files
4. Update tests once
5. Document the single correct path