# Test Coverage Improvement Plan (REVISED)

## Current State
- **Executor**: 40.9% coverage (needs 70%+)
- **Planner**: 57.6% coverage (needs 70%+)
- **Existing tests**: ~60% behavioral (good!), ~40% implementation-focused

## Testing Strategy
1. **Remove dead code first** - Immediate coverage boost
2. **Add integration tests** for uncovered user-facing features
3. **Accept some test breakage** during refactoring (implementation tests)

## Critical Executor Functions Needing Coverage

### Priority 1: Core Query Execution (Will Survive Refactoring)
These test the external API and core behaviors that must remain stable:

1. **Sorting functionality** (`sortRelation`, `compareTupleValues`)
   - **What to test**: Order-by clauses with different data types
   - **Test approach**: Integration tests with full queries containing `:order-by`
   - **Why critical**: User-facing feature that must work correctly

2. **Expression evaluation** (`evaluateExpression`, `evaluateFunction`, `applyExpression`)
   - **What to test**: Arithmetic (`+`, `-`, `*`, `/`), string operations (`str`), comparisons
   - **Test approach**: Integration tests with expression patterns in queries
   - **Why critical**: Core query functionality used in many queries

3. **Time extraction functions** (`extractYear`, `extractMonth`, `extractDay`, `extractHour`)
   - **What to test**: Time-based queries with temporal functions
   - **Test approach**: Integration tests with time extraction in WHERE clauses
   - **Why critical**: Essential for time-series queries (OHLC, etc.)

### Priority 2: Query Variants (API Surface)
These test different ways to execute queries:

4. **ExecuteWithRelations** 
   - **What to test**: Queries with pre-bound input relations
   - **Test approach**: Subquery-like tests with input data
   - **Why critical**: Needed for subqueries and parameterized queries

5. **Subquery context handling** (`createSubqueryContext`, `augmentWithInputValues`)
   - **What to test**: Nested subqueries with input bindings
   - **Test approach**: Integration tests with complex subqueries
   - **Why critical**: Already implemented feature that needs coverage

### Priority 3: Can Be Removed/Refactored
These are implementation details or deprecated code:

6. ~~**executePhaseOld**~~ - Deprecated, should be removed
7. ~~**executeWithoutPlan**~~ - Legacy path, should be removed
8. ~~**Annotation methods**~~ - Implementation detail, will change during refactor
9. ~~**findOrBuildRelationWithSymbols**~~ - Internal helper, will be refactored

## Critical Planner Functions Needing Coverage

### Priority 1: Core Planning Logic
1. **Fine-grained phase creation** (`createFineGrainedPhases`)
   - **What to test**: Queries that would create cross-products
   - **Test approach**: Complex queries with disjoint patterns
   - **Why critical**: Prevents memory explosions

2. **Pattern selection** (`selectPatternsForPhase`, `estimatePatternSelectivity`)
   - **What to test**: Pattern ordering and selectivity
   - **Test approach**: Queries with multiple patterns, verify execution order
   - **Why critical**: Performance optimization

### Priority 2: Helper Functions
3. **Symbol tracking** (`addPatternSymbols`, `extractPatternVariables`)
   - **What to test**: Symbol resolution across patterns
   - **Test approach**: Unit tests with various pattern types
   - **Why critical**: Correctness of query execution

## Test Implementation Plan (REVISED)

### Phase 0: Quick Wins - Remove Dead Code (TODAY)
**Immediate coverage boost by removing unused code:**
```bash
# Functions with 0% coverage that should be removed:
- executePhaseOld (deprecated)
- executeWithoutPlan (legacy path)
- Annotation/collector methods (if unused)
- createSimplePlan (if unused)
```
**Expected impact**: Executor coverage 40.9% → ~45-50%

### Phase 1: Integration Tests for Missing Features (Days 1-3)
Write tests for critical uncovered functionality:

```go
// executor_integration_test.go
func TestSortingQueries(t *testing.T) {
    // MISSING: sortRelation has 0% coverage
    // Test ORDER BY with different types
    // Test multi-column sorting
}

func TestExpressionQueries(t *testing.T) {
    // MISSING: evaluateExpression, evaluateFunction have 0% coverage
    // Test arithmetic in WHERE clauses
    // Test string operations
}

func TestTimeExtractionQueries(t *testing.T) {
    // MISSING: extractYear/Month/Day/Hour have 0% coverage
    // Test temporal queries
}
```
**Expected impact**: Executor coverage ~50% → ~65%

### Phase 2: Fill Planner Gaps (Days 3-4)
```go
// planner_integration_test.go
func TestFineGrainedPhaseCreation(t *testing.T) {
    // MISSING: createFineGrainedPhases has 0% coverage
    // Test queries that would create cross-products
}
```
**Expected impact**: Planner coverage 57.6% → ~70%

### Phase 3: Start Incremental Refactoring (Day 5+)
**With ~65-70% coverage, we can safely refactor:**

#### 3A: Extract Expression Evaluation (Safe - has tests now)
- Move expression evaluation to `datalog/expressions` package
- Keep same interface initially
- Update imports

#### 3B: Extract Time Functions (Safe - has tests now)
- Move time extraction to `datalog/temporal` package
- Or add as methods on time.Time

#### 3C: Break up Executor (Gradual)
1. First: Extract stateless functions (evaluateFunction, compareValues)
2. Then: Create focused components (AggregationEngine, SortEngine)
3. Finally: Slim down Executor to coordination only

#### 3D: Simplify Planner (Gradual)
1. First: Extract pattern analysis functions
2. Then: Separate phase creation logic
3. Finally: Clean up the main Plan() method

## Success Metrics
- [ ] Dead code removed (Phase 0)
- [ ] Executor coverage > 65% (Phase 1)
- [ ] Planner coverage > 70% (Phase 2)
- [ ] All integration tests pass
- [ ] Begin incremental refactoring (Phase 3)

## Key Insight from Test Analysis
The existing tests are better than expected - mostly behavioral! This means:
1. We can be more aggressive about removing dead code
2. We don't need to rewrite existing tests
3. We can start refactoring sooner (with just 65% coverage)
4. Some implementation tests will break during refactoring - that's OK

## Anti-Patterns to Avoid
- **DON'T** test private methods directly
- **DON'T** test implementation details
- **DON'T** mock everything - use real components where possible
- **DON'T** write tests that break on refactoring

## Patterns to Follow  
- **DO** test behavior not implementation
- **DO** use table-driven tests
- **DO** test error cases
- **DO** use integration tests for complex features
- **DO** keep tests simple and readable