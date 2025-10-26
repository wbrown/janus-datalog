# Executor Refactoring Plan: Go-Idiomatic Standalone Functions

## Goal
Transform Executor methods into standalone functions following Go idioms:
- Prefer functions over methods when possible
- Make dependencies explicit
- Avoid unnecessary abstraction layers
- Ensure tests pass after EVERY change

## Refactoring Rules
1. **Tests Must Pass**: After each function extraction, run tests before proceeding
2. **Backward Compatibility**: Keep wrapper methods during transition
3. **Simple Extraction First**: Use cut-and-paste, don't change logic
4. **Document Dependencies**: Clearly state what each function needs
5. **Incremental Commits**: Commit after each successful extraction

## Phase 1: Pure Data Transformation Functions
These methods don't use any Executor fields - they're pure data transformations.

### 1.1 ProjectToFind
**Current**: `func (e *Executor) projectToFind(rel Relation, find []query.Symbol) Relation`
**Target**: `func ProjectToFind(rel Relation, find []query.Symbol) Relation`
**Steps**:
1. Create standalone function `ProjectToFind`
2. Update method to call standalone function
3. Run tests: `go test ./datalog/executor/...`
4. Commit if tests pass

### 1.2 MaterializeResult  
**Current**: `func (e *Executor) materializeResult(rel Relation, find []query.Symbol) Relation`
**Target**: `func MaterializeResult(rel Relation, columns []query.Symbol) Relation`
**Steps**:
1. Create standalone function `MaterializeResult`
2. Update method to call standalone function
3. Run tests: `go test ./datalog/executor/...`
4. Commit if tests pass

### 1.3 SortRelation
**Current**: `func (e *Executor) sortRelation(rel Relation, orderBy []query.OrderByClause) Relation`
**Target**: `func SortRelation(rel Relation, orderBy []query.OrderByClause) Relation`
**Steps**:
1. Create standalone function `SortRelation`
2. Update method to call standalone function
3. Run tests: `go test ./datalog/executor/...`
4. Commit if tests pass

### 1.4 EvaluateExpressionFunction
**Current**: Already delegates to `expression.EvaluateTuple`
**Action**: Remove this trivial wrapper method entirely
**Steps**:
1. Find all callers of `evaluateExpressionFunction`
2. Update them to call `expression.EvaluateTuple` directly
3. Remove the method
4. Run tests: `go test ./datalog/executor/...`
5. Commit if tests pass

### 1.5 ExecuteAggregations
**Current**: Already delegates to `ExecuteAggregations`
**Action**: Remove this trivial wrapper method
**Steps**:
1. Find all callers of `executeAggregations`
2. Update them to call `ExecuteAggregations` directly
3. Remove the method
4. Run tests: `go test ./datalog/executor/...`
5. Commit if tests pass

## Phase 2: Functions That Need PatternMatcher
These methods only use `e.matcher` from the Executor.

### 2.1 MatchPatternWithRelations
**Current**: `func (e *Executor) matchPatternWithRelations(ctx Context, pattern *query.DataPattern, relations []Relation) ([]datalog.Datom, error)`
**Target**: `func MatchPatternWithRelations(ctx Context, matcher PatternMatcher, pattern *query.DataPattern, relations []Relation) ([]datalog.Datom, error)`
**Steps**:
1. Create standalone function with `matcher` parameter
2. Update method to call standalone function passing `e.matcher`
3. Run tests: `go test ./datalog/executor/...`
4. Commit if tests pass

## Phase 3: Core Execution Functions
These are more complex but still good candidates for extraction.

### 3.1 ApplyExpressionsAndPredicates
**Current**: `func (e *Executor) applyExpressionsAndPredicates(ctx Context, phase *planner.Phase, groups Relations) (Relations, error)`
**Target**: `func ApplyExpressionsAndPredicates(ctx Context, phase *planner.Phase, groups Relations) (Relations, error)`
**Note**: This doesn't actually use Executor fields!
**Steps**:
1. Create standalone function
2. Update method to delegate
3. Run tests: `go test ./datalog/executor/...`
4. Commit if tests pass

### 3.2 ExecutePhaseSequentialV2
**Current**: `func (e *Executor) executePhaseSequentialV2(ctx Context, phase *planner.Phase, phaseIndex int, previousResult Relation) (Relation, error)`
**Target**: `func ExecutePhase(ctx Context, matcher PatternMatcher, phase *planner.Phase, phaseIndex int, previousResult Relation) (Relation, error)`
**Steps**:
1. Create standalone function with `matcher` parameter
2. Update method to delegate
3. Run tests: `go test ./datalog/executor/...`
4. Commit if tests pass

### 3.3 BindInputRelations
**Current**: `func (e *Executor) bindInputRelations(ctx Context, inputs []query.InputSpec, bindings map[query.Symbol]interface{}) []Relation`
**Target**: `func BindInputRelations(ctx Context, inputs []query.InputSpec, bindings map[query.Symbol]interface{}) []Relation`
**Steps**:
1. Create standalone function
2. Update method to delegate
3. Run tests: `go test ./datalog/executor/...`
4. Commit if tests pass

## Phase 4: Evaluation Functions
These handle expression evaluation contexts.

### 4.1 EvaluateExpression & EvaluateExpressionWithContext
**Current**: Two related methods for expression evaluation
**Target**: Merge into single context-aware function
**Steps**:
1. Analyze usage patterns
2. Create unified standalone function
3. Update both methods to delegate
4. Run tests: `go test ./datalog/executor/...`
5. Commit if tests pass

### 4.2 ApplyExpression & ApplyExpressionWithContext  
**Current**: Two related methods for applying expressions
**Target**: Merge into single context-aware function
**Steps**:
1. Analyze usage patterns
2. Create unified standalone function
3. Update both methods to delegate
4. Run tests: `go test ./datalog/executor/...`
5. Commit if tests pass

## Phase 5: High-Level Orchestration
Keep these as methods but make them thin.

### 5.1 ExecutePhasesWithInputs
**Current**: Orchestrates phase execution with inputs
**Action**: Keep as method but simplify by using extracted functions
**Steps**:
1. After Phase 3 is done, refactor to use new functions
2. Run tests: `go test ./datalog/executor/...`
3. Commit if tests pass

### 5.2 Execute, ExecuteWithContext, ExecuteWithRelations
**Current**: Entry points
**Action**: Keep as methods, ensure they're thin wrappers
**Steps**:
1. After all phases done, review and simplify
2. Run tests: `go test ./datalog/executor/...`
3. Commit if tests pass

## Testing Strategy

### After Each Extraction
```bash
# Run executor tests
go test ./datalog/executor/... -v

# Run integration tests
go test ./datalog/... -v

# Check coverage hasn't dropped
go test -cover ./datalog/executor/...
```

### Regression Tests to Add
Before starting, add tests for:
1. Complex queries that use multiple features
2. Edge cases in data transformation
3. Performance benchmarks to ensure no regression

## Success Criteria
- [ ] All tests pass after each extraction
- [ ] No performance regression
- [ ] Code coverage maintained or improved
- [ ] Executor struct has fewer methods
- [ ] Functions have clear, explicit dependencies
- [ ] Documentation updated for new functions

## Risk Mitigation
1. **Backup Point**: Commit before starting
2. **Incremental Changes**: One function at a time
3. **Test Coverage**: Ensure good coverage before refactoring
4. **Performance Monitoring**: Run benchmarks after each phase
5. **Revert Strategy**: Each commit should be revertable

## Order of Execution
1. Start with Phase 1 (pure functions) - lowest risk
2. Then Phase 2 (PatternMatcher functions) - medium risk
3. Then Phase 3 (core execution) - higher risk
4. Then Phase 4 (evaluation) - medium risk
5. Finally Phase 5 (orchestration) - cleanup

## Notes
- This plan is iterative - we can stop at any phase if issues arise
- Each phase builds on the previous one
- Keep wrapper methods until we're confident in the refactoring
- Document any surprises or complications discovered during refactoring