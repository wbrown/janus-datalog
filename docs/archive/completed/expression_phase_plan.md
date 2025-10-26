# Plan: Move Expression Evaluation into Query Phases

## Current Architecture Problems

1. **Expression evaluation happens AFTER all phases complete**
   - Forces us to keep intermediate symbols (like `?time-open`, `?time-close`) through all phases
   - Creates massive intermediate results (540,000 tuples instead of 600)
   - Equality constraints can't filter early

2. **Equality constraints are treated as predicates, not filters**
   - `[(= ?year ?year-open)]` should filter tuples where the years don't match
   - Currently just checked after all data is materialized

## Proposed Architecture

### Phase 1: Extend Phase Structure

Add expression evaluation capability to phases:

```go
type Phase struct {
    Patterns      []PatternPlan
    Predicates    []PredicatePlan
    Expressions   []ExpressionPlan  // NEW: expressions to evaluate
    Available     []query.Symbol
    Provides      []query.Symbol
    Keep          []query.Symbol
    Find          []query.Symbol
}

type ExpressionPlan struct {
    Expression *query.ExpressionPattern
    Inputs     []query.Symbol  // Which symbols this expression needs
    Output     query.Symbol    // The binding it produces
}
```

### Phase 2: Planner Changes

Update the planner to assign expressions to phases:

1. **Expression Assignment Logic**:
   ```
   For each expression:
   - Find the earliest phase where ALL input symbols are available
   - Assign expression to that phase
   - Add the expression's binding to that phase's Provides
   ```

2. **Equality Constraint Handling**:
   - Equality expressions (no binding) become filters in the phase where both arguments are available
   - Convert `[(= ?year ?year-open)]` to a predicate filter

### Phase 3: Executor Changes

1. **executePhase() modifications**:
   ```go
   func (e *Executor) executePhase(...) {
       // 1. Execute patterns (existing)
       
       // 2. Apply predicates (existing)
       
       // 3. NEW: Evaluate expressions
       for _, exprPlan := range phase.Expressions {
           phaseResult = e.evaluateExpressionInPhase(phaseResult, exprPlan)
       }
       
       // 4. NEW: Apply equality filters
       for _, equalityFilter := range phase.EqualityFilters {
           phaseResult = e.applyEqualityFilter(phaseResult, equalityFilter)
       }
       
       // 5. Join with previous result (existing)
       
       // 6. Project to Keep (existing)
   }
   ```

2. **Expression evaluation within phase**:
   - Only evaluate expressions whose inputs are available
   - Add new columns to the relation for expression bindings
   - Can project out intermediate columns immediately after use

### Phase 4: Optimization Opportunities

1. **Immediate Projection**:
   - After evaluating `[(year ?time-open) ?year-open]`, we can drop `?time-open`
   - Only keep symbols needed for: future joins, future expressions, or final result

2. **Early Filtering**:
   - Apply equality constraints as soon as both arguments are available
   - Reduces data volume before expensive joins

3. **Expression Ordering**:
   - Within a phase, order expressions to minimize intermediate columns
   - Evaluate expressions that enable projections first

## Implementation Steps

### Step 1: Extend Phase and Planner Types
- [ ] Add ExpressionPlan to Phase struct
- [ ] Add EqualityFilter to Phase struct (or reuse PredicatePlan)
- [ ] Update planner types.go

### Step 2: Update separatePatterns
- [ ] Keep expressions separate but track them for phase assignment
- [ ] Identify equality expressions vs binding expressions

### Step 3: Implement Expression Phase Assignment
- [ ] Create `assignExpressionsToPhases()` method
- [ ] For each expression, find phase where inputs are available
- [ ] Update phase.Provides with expression bindings

### Step 4: Update determinePhaseKeepSymbols
- [ ] Consider expression outputs when determining what to keep
- [ ] Can be more aggressive about dropping symbols used only for expressions

### Step 5: Implement Phase Expression Evaluation
- [ ] Add `evaluateExpressionInPhase()` to executor
- [ ] Modify executePhase to call expression evaluation
- [ ] Ensure proper column tracking

### Step 6: Implement Equality Filtering
- [ ] Convert equality expressions to filters
- [ ] Apply filters after expressions are evaluated
- [ ] Track filter statistics for debugging

### Step 7: Testing and Validation
- [ ] Update annotation_simple_demo to show improved performance
- [ ] Create test cases for various expression patterns
- [ ] Verify correctness with complex queries

## Expected Benefits

1. **Performance**: 
   - Reduce 540,000 tuple intermediate result to ~600 tuples
   - Earlier filtering through equality constraints
   - Less memory usage

2. **Correctness**:
   - Same results as current implementation
   - Better error messages (can report which phase failed)

3. **Debuggability**:
   - Can see expression evaluation in phase annotations
   - Track how data flows through expressions

## Example: How the Demo Query Would Execute

```
Phase 1: [?s :symbol/ticker "CRWV"]
  Keep: [?s]

Phase 2: [?p-close ...] patterns
  Keep: [?close ?s ?time-close]
  
Phase 3: [?p-open ...] patterns  
  Expressions:
    - [(year ?time-open) ?year-open]
    - [(month ?time-open) ?month-open]  
    - [(day ?time-open) ?day-open]
  Keep: [?close ?open ?s ?year-open ?month-open ?day-open]
  (Note: ?time-open dropped after expression evaluation!)

Phase 4: [?p :price/...] patterns
  Expressions:
    - [(year ?time) ?year]
    - [(month ?time) ?month]
    - [(day ?time) ?day]
  Equality Filters:
    - ?year = ?year-open
    - ?month = ?month-open  
    - ?day = ?day-open
  Keep: [?close ?high ?low ?open ?year ?month ?day]
  (Note: ?time dropped, equality vars dropped)
```

## Risks and Mitigation

1. **Risk**: More complex phase execution logic
   - **Mitigation**: Clear separation of concerns, good tests

2. **Risk**: Breaking existing queries
   - **Mitigation**: Extensive testing, gradual rollout

3. **Risk**: Expression evaluation order dependencies
   - **Mitigation**: Topological sort of expressions within phase

## Alternative Approaches Considered

1. **Keep current architecture, optimize differently**
   - Rejected: Fundamental limitation of late expression evaluation

2. **Evaluate all expressions in final phase**
   - Rejected: Still requires keeping intermediate symbols

3. **Create separate expression phases**
   - Rejected: Adds complexity without clear benefit