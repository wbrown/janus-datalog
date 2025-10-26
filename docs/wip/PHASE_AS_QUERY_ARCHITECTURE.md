# Architectural Proposal: Optimize-First Phase Planning

**Status**: PROPOSAL
**Author**: Inspired by distributed Datalog implementation patterns
**Date**: 2025-10-14

## Executive Summary

This proposal advocates for **two complementary architectural shifts**:

1. **Optimize-First Planning**: Optimize the query FIRST (on `[]Clause`), then divide into phases ONCE
2. **Query as Interface Boundary**: Phases ARE Datalog query fragments, execution is `Query + Relations → Relations`

The current architecture divides queries into phases early (creating 7 operation types), then applies optimizations that change dependencies, requiring re-phasing. Some implementations demonstrate a simpler approach: optimize the clause list, phase it once, and execute phases as queries.

**Key insights**:
1. **Clause is the atomic unit** - `Query.Where` is `[]Clause`, already in our codebase
2. **Optimizations are pure transformations** - `[]Clause → []Clause` composition pipeline
3. **Phasing happens ONCE** - After all optimizations, using greedy symbol dependency algorithm
4. **Datalog is the universal interface** - Every layer speaks Query + Relations → Relations

## Quick Comparison

| Aspect | Current (Phase-First) | Proposed (Optimize-First) |
|--------|----------------------|--------------------------|
| **Flow** | Parse → Phase → Optimize → Re-phase | Parse → Optimize → Phase |
| **Phase structure** | 7 operation types (Patterns, Expressions, ...) | Single Query fragment |
| **Execution interface** | Phase internals → Relations | **Query + Relations → Relations** |
| **Optimizers** | Mutate Phase internals | Transform `[]Clause` |
| **Symbol tracking** | Recalculated after each optimization | Calculated once during phasing |
| **Composition** | Complex interdependencies | Clean functional pipeline |
| **Clause ordering** | Fixed (patterns → exprs → preds) | Context-dependent scoring |
| **Edge cases** | Expression-only phases, etc. | Eliminated by query structure |
| **Phasing count** | Multiple (initial + after reordering) | Once |

## Current Architecture

### Current Flow

```
Parse Query
    ↓
Separate Clauses (patterns, predicates, expressions, subqueries)
    ↓
Create Phases (group by symbol dependencies)
    ↓
Optimize Phases:
    - Predicate pushdown (modifies Phase.Predicates → PatternPlan.Filters)
    - Conditional aggregate rewriting (transforms Phase.Subqueries → Phase.Expressions)
    - Semantic rewriting (modifies Phase.Predicates in place)
    - Phase reordering (moves entire phases, re-assigns subqueries)
    - updatePhaseSymbols() (recalculates Available/Provides/Keep)
    ↓
Execute Phases
```

**Problem**: Optimizations change dependencies, requiring re-phasing and symbol recalculation.

### Phase Structure

```go
type Phase struct {
    Patterns               []PatternPlan              // Data retrieval
    Predicates             []PredicatePlan            // Filters
    JoinPredicates         []JoinPredicate            // Equality filters
    Expressions            []ExpressionPlan           // Computations
    Subqueries             []SubqueryPlan             // Nested queries
    DecorrelatedSubqueries []DecorrelatedSubqueryPlan // Optimized subqueries
    Available              []query.Symbol             // Symbol tracking
    Provides               []query.Symbol
    Keep                   []query.Symbol
    Find                   []query.Symbol
    Metadata               map[string]interface{}
}
```

### Execution Flow

```go
func executePhase(phase *Phase) Relation {
    // 1. Execute patterns
    for _, pattern := range phase.Patterns {
        rel := matcher.Match(pattern, bindings)
        // Progressive joining...
    }

    // 2. Handle expression-only phases edge case
    if len(phase.Patterns) == 0 {
        // Special handling!
    }

    // 3. Execute expressions (fixed order)
    for _, expr := range phase.Expressions {
        rel = evaluateExpression(expr, rel)
    }

    // 4. Execute predicates (fixed order)
    for _, pred := range phase.Predicates {
        rel = filterWithPredicate(pred, rel)
    }

    // 5. Execute subqueries (fixed order)
    for _, subq := range phase.Subqueries {
        rel = executeSubquery(subq, rel)
    }

    // 6. Project to Keep columns
    return rel.Project(phase.Keep)
}
```

### Problems

1. **Optimizations change dependencies**:
   - Conditional aggregate rewriting adds new expressions (changes Provides)
   - CSE adds new bindings (changes symbol dependencies)
   - Phase reordering breaks subquery inputs (requires re-assignment)
   - Must call `updatePhaseSymbols()` after each optimization

2. **Phase structure is complex**:
   - 7 operation types (patterns, expressions, predicates, join predicates, subqueries, decorrelated subqueries, projection)
   - Must track Available/Provides/Keep/Find symbols
   - Metadata for special cases (aggregate_required_columns, conditional_aggregates)

3. **Edge cases proliferate**:
   - "What if phase has zero patterns?" (just fixed!)
   - "What if expressions need symbols from disjoint groups?"
   - "What if Keep includes symbols not in Provides?" (just fixed!)

4. **Fixed execution order limits optimization**:
   ```datalog
   [?e :event/value ?value]      ; 1M rows
   [(* ?value 2) ?doubled]       ; Expression computes on 1M rows
   [(> ?value 100)]              ; Predicate filters to 1K rows
   ```
   We compute on 1M rows, then filter. Optimal: filter first, compute on 1K.

## Clojure Reference Architecture

### Phase Structure (Simplified)

```clojure
;; A phase in Clojure
{:patterns   [...]     ; Flat list - patterns AND predicates mixed!
 :symbols    #{...}    ; Symbols involved
 :referred   #{...}    ; Symbols referenced
 :query      <parsed>} ; Generated Datalog query
```

### Key Functions

```clojure
(defn associated->query
  "Generate a Datalog query from the phase patterns"
  [[var {:keys [patterns] :as phase}]]
  (let [where (mapv datalog->repr patterns)  ; Convert patterns to :where clause
        dq [:find ...vars... :where ...where...]]
    (assoc phase :query (parse-query dq))))  ; Parse and store query!
```

**The phase IS a Datalog query!**

### Execution Model

```clojure
;; Elasticsearch integration (from CLAUDE.md context)
;; Accepts: Relations + Datalog query
;; Produces: Relations

(defn execute-phase [input-relations phase]
  ;; Just execute the query!
  (execute-query (:query phase) input-relations))
```

**Universal interface**: `Relations + Query → Relations`

## Proposed Architecture

### Proposed Flow

```
Parse Query → query.Query{Find, In, Where: []Clause}
    ↓
Optimize Clauses (pure transformations on []Clause):
    clauses = query.Where
    clauses = cseOptimizer(clauses, ctx)
    clauses = decorrelationOptimizer(clauses, ctx)
    clauses = semanticRewriter(clauses, ctx)
    ↓
Phase Planning (greedy algorithm on optimized clauses):
    available = inputSymbols
    for each clause in clauses:
        score = scoreClause(clause, available)  // context-dependent!
        if clause can execute now: add to currentPhase
        else: start new phase
    generate Phase.Query from clause groups
    ↓
Execute Phases (uniform interface):
    for each phase:
        result = executor.Execute(phase.Query, previousResults)
```

**Benefits**:
- Optimizations don't trigger re-phasing (they run first)
- Phasing happens once with final dependencies
- No symbol recalculation needed

### Clause as the Atomic Unit

Our codebase already has this! `Query.Where` is `[]Clause`:

```go
// From datalog/query/clause.go
type Clause interface {
    Pattern  // String() method
    clause() // Private marker
}

// Implementations:
// - *DataPattern:        [?e :person/name ?name]
// - *Comparison:         [(> ?x 10)]
// - *Expression:         [(+ ?x ?y) ?z]
// - *Subquery:           [(q [...] $) [[?max]]]
// (and more predicate types)
```

Each clause has **context-dependent** inputs/outputs:

```go
// NOT static methods on Clause, but functions that operate on Clause + context
func extractSymbols(clause Clause) []Symbol {
    // All symbols mentioned in clause
}

func scoreClause(clause Clause, available map[Symbol]bool) int {
    symbols := extractSymbols(clause)
    bound := intersection(symbols, available)
    unbound := difference(symbols, available)

    // Reward bound symbols (selective), penalize new symbols
    return len(bound)*10 - len(unbound)*5
}
```

**Example**: `[?e :person/name ?name]`
- If `?e` available: bound=1, unbound=1, score=5 (lookup by entity)
- If `?e` NOT available: bound=0, unbound=2, score=-10 (full scan)

Same clause, different score based on context!

### Optimizers as Pure Transformations

```go
// All optimizers have same signature
type Optimizer func(clauses []Clause, ctx OptimizerContext) []Clause

type OptimizerContext struct {
    FindSymbols  []Symbol
    InputSymbols []Symbol
    // ... other context
}

// Example: CSE
func cseOptimizer(clauses []Clause, ctx OptimizerContext) []Clause {
    var result []Clause
    seen := make(map[string]Symbol) // expression → binding

    for _, clause := range clauses {
        if expr, ok := clause.(*Expression); ok {
            key := expr.Function.String()
            if prev, exists := seen[key]; exists {
                // Replace duplicate with equality
                result = append(result, &GroundPredicate{
                    Var: expr.Binding,
                    Val: prev,
                })
            } else {
                seen[key] = expr.Binding
                result = append(result, clause)
            }
        } else {
            result = append(result, clause)
        }
    }
    return result
}

// Example: Conditional aggregate rewriting
func decorrelationOptimizer(clauses []Clause, ctx OptimizerContext) []Clause {
    var result []Clause
    for _, clause := range clauses {
        if subq, ok := clause.(*Subquery); ok && isCorrelatedAggregate(subq) {
            // Transform: [(q [:find (max ?v) ...] ?p) [[?max]]]
            // Into: [(max-if ?v ?pred) ?max] + predicate expression
            newExprs := transformToConditionalAggregate(subq)
            result = append(result, newExprs...)
        } else {
            result = append(result, clause)
        }
    }
    return result
}

// Compose them!
clauses := query.Where
clauses = cseOptimizer(clauses, ctx)
clauses = decorrelationOptimizer(clauses, ctx)
clauses = semanticRewriter(clauses, ctx)
```

**Key insight**: All optimizations are `[]Clause → []Clause` transformations. Clean, composable, testable.

### Phasing Algorithm (Greedy Selection)

After optimization, phase planning is simple:

```go
func createPhases(clauses []Clause, findSymbols, inputSymbols []Symbol) []Phase {
    available := symbolSet(inputSymbols)
    remaining := clauses
    var phases []Phase

    for len(remaining) > 0 {
        currentPhase := []Clause{}
        newRemaining := []Clause{}

        // Greedy: pick best clause that can execute now
        for len(remaining) > 0 {
            best := -1
            bestScore := -9999

            for i, clause := range remaining {
                syms := extractSymbols(clause)
                if canExecute(syms, available) {
                    score := scoreClause(clause, available)
                    if score > bestScore {
                        bestScore = score
                        best = i
                    }
                }
            }

            if best == -1 {
                // No executable clause - start new phase
                break
            }

            // Add best clause to current phase
            clause := remaining[best]
            currentPhase = append(currentPhase, clause)
            available = union(available, extractSymbols(clause))
            remaining = append(remaining[:best], remaining[best+1:]...)
        }

        if len(currentPhase) == 0 {
            // No more progress possible
            return nil, fmt.Errorf("cannot satisfy dependencies")
        }

        // Generate query for this phase
        phase := generatePhaseQuery(currentPhase, available, findSymbols)
        phases = append(phases, phase)
    }

    return phases
}

func canExecute(clauseSymbols []Symbol, available map[Symbol]bool) bool {
    // All unbound symbols in clause must come from available set
    for _, sym := range clauseSymbols {
        if sym.IsVariable() && !available[sym] {
            return false  // Requires symbol not yet available
        }
    }
    return true
}
```

**This is the Clojure algorithm!** Simple greedy selection based on what's available.

### Phase Structure (Simplified)

```go
type Phase struct {
    // The phase IS a Datalog query fragment
    Query     *query.Query

    // Symbol flow metadata (unchanged)
    Available []query.Symbol  // What's available from previous phases
    Provides  []query.Symbol  // What this query produces
    Keep      []query.Symbol  // What to project for next phase

    // Optional metadata for optimizations
    Metadata  map[string]interface{}
}
```

**No more**: Patterns, Expressions, Predicates, JoinPredicates, Subqueries, DecorrelatedSubqueries

**Just**: A Datalog query fragment!

### Universal Interface: Query + Relations → Relations

This is the **key architectural insight**: Datalog becomes the interface boundary at every layer.

```go
// Every component speaks the same language
type QueryExecutor interface {
    Execute(ctx Context, query *query.Query, inputs []Relation) (Relation, error)
}
```

**Phase execution** becomes trivial:
```go
func (e *Executor) executePhase(ctx Context, phase *Phase, input Relation) (Relation, error) {
    // Universal interface!
    result, err := e.queryExecutor.Execute(ctx, phase.Query, []Relation{input})
    if err != nil {
        return nil, err
    }

    // Project to Keep columns
    if len(phase.Keep) > 0 {
        return result.Project(phase.Keep)
    }

    return result, nil
}
```

**From ~200 lines of complex logic to ~10 lines.**

**Pattern matching** (storage layer):
```go
// PatternMatcher stays low-level (just data retrieval)
type PatternMatcher interface {
    Match(pattern *DataPattern, bindings Relation) (Relation, error)
}

// QueryExecutor orchestrates (high-level)
type DefaultQueryExecutor struct {
    matcher PatternMatcher
}

func (e *DefaultQueryExecutor) Execute(ctx Context, q *query.Query, inputs []Relation) (Relation, error) {
    // For each clause in q.Where:
    //   - DataPattern → use matcher.Match()
    //   - Expression → evaluate function
    //   - Predicate → filter relation
    //   - Subquery → recursively Execute()

    // Returns: unified Relation
}
```

**Benefits of universal interface**:
1. **Storage layer stays simple** - Just pattern matching, no query logic
2. **Query logic centralized** - One place handles all clause types
3. **Easy to test** - Mock QueryExecutor for unit tests
4. **Easy to optimize** - Optimize at query level, not operation level
5. **Matches Clojure architecture** - Proven approach from production system

### Query Executor Interface

```go
// Universal interface for query execution
type QueryExecutor interface {
    // Execute a Datalog query with input relations
    Execute(ctx Context, q *query.Query, inputs []Relation) (Relation, error)
}
```

**Question**: Should storage layer implement this, or keep PatternMatcher separate?

**Option A - Storage implements QueryExecutor**:
```go
// Storage executes full queries
type BadgerQueryExecutor struct {
    store *Store
}

func (e *BadgerQueryExecutor) Execute(ctx Context, q *query.Query, inputs []Relation) (Relation, error) {
    // Decompose query into storage operations
    // Handle patterns, expressions, predicates internally
    // Return final relation
}
```

**Option B - Keep PatternMatcher, QueryExecutor wraps it**:
```go
// Storage stays low-level (just patterns)
type PatternMatcher interface {
    Match(pattern *DataPattern, bindings Relations) (Relation, error)
}

// QueryExecutor orchestrates
type DefaultQueryExecutor struct {
    matcher PatternMatcher
}

func (e *DefaultQueryExecutor) Execute(ctx Context, q *query.Query, inputs []Relation) (Relation, error) {
    // Use matcher for patterns
    // Evaluate expressions
    // Apply predicates
    // Return relation
}
```

**Recommendation**: Option B - Keep PatternMatcher for storage abstraction, introduce QueryExecutor as orchestration layer.

### Layered Architecture

```
┌─────────────────────────────────────────┐
│  Query (User-facing)                    │
│  [:find ?x ?y :where ...]              │
└─────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│  Planner                                │
│  - Groups patterns/expressions/predicates│
│  - Generates Phase queries              │
│  - Determines symbol flow               │
└─────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│  Phase (Query Fragment)                 │
│  Query: [:find ?p ?day                  │
│          :in [[?e ?time]]               │
│          :where [?e :event/person ?p]   │
│                 [(day ?time) ?day]]     │
│  Keep: [?p ?day]                        │
└─────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│  QueryExecutor                          │
│  Execute(query, inputs) → Relation      │
└─────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│  PatternMatcher (Storage)               │
│  Match(pattern, bindings) → Relation    │
└─────────────────────────────────────────┘
```

### Phase Execution (Simplified!)

```go
func (e *Executor) executePhase(ctx Context, phase *Phase, input Relation) (Relation, error) {
    // Convert input to the format expected by phase query
    inputs := []Relation{}
    if input != nil && !input.IsEmpty() {
        inputs = append(inputs, input)
    }

    // Execute the phase query
    result, err := e.queryExecutor.Execute(ctx, phase.Query, inputs)
    if err != nil {
        return nil, err
    }

    // Project to Keep columns (if specified)
    if len(phase.Keep) > 0 {
        return result.Project(phase.Keep)
    }

    return result, nil
}
```

**No more**:
- "What if phase has zero patterns?"
- "What if expressions are in wrong order?"
- Fixed execution ordering
- Complex edge case handling

**Just**: Execute the query!

### Planner Changes

The planner generates Datalog query fragments for each phase:

```go
func (p *Planner) createPhases(...) []Phase {
    // 1. Group patterns/expressions/predicates by dependencies
    groups := p.groupBySymbols(dataPatterns, expressions, predicates)

    // 2. Order groups (same as current logic)
    orderedGroups := p.orderGroups(groups)

    // 3. For each group, generate a Datalog query fragment
    var phases []Phase
    for _, group := range orderedGroups {
        phase := Phase{
            Query: p.generateQueryForGroup(group),
            // ... symbol tracking
        }
        phases = append(phases, phase)
    }

    return phases
}

func (p *Planner) generateQueryForGroup(group PatternGroup) *query.Query {
    // Build :where clause from patterns, expressions, predicates
    var where []query.Clause

    // Add patterns
    for _, pattern := range group.Patterns {
        where = append(where, pattern)
    }

    // Add expressions
    for _, expr := range group.Expressions {
        where = append(where, expr)
    }

    // Add predicates
    for _, pred := range group.Predicates {
        where = append(where, pred)
    }

    // Build :find clause from Provides
    var find []query.FindElement
    for _, sym := range group.Provides {
        find = append(find, query.FindVariable{Symbol: sym})
    }

    // Generate :in clause if needed (for non-first phases)
    var in []query.Input
    if len(group.Available) > 0 {
        // Input is a relation with Available columns
        in = append(in, query.DatabaseInput{})
        in = append(in, query.RelationInput{Symbols: group.Available})
    }

    return &query.Query{
        Find:  find,
        In:    in,
        Where: where,
    }
}
```

## Example Transformation

### Current Query Plan

```
Phase 1:
  Patterns: [[?e :event/person ?p], [?e :event/time ?time]]
  Expressions: []
  Available: []
  Provides: [?e ?p ?time]
  Keep: [?p ?time]

Phase 2:
  Patterns: [[?p :person/name ?name]]
  Expressions: [[(day ?time) ?day]]
  Subqueries: [...]
  Available: [?p ?time]
  Provides: [?p ?name ?day ?max]
  Keep: [?name ?day ?max]
```

### Proposed Query Plan

```
Phase 1:
  Query: [:find ?e ?p ?time
          :where [?e :event/person ?p]
                 [?e :event/time ?time]]
  Available: []
  Provides: [?e ?p ?time]
  Keep: [?p ?time]

Phase 2:
  Query: [:find ?name ?day ?max
          :in $ [[?p ?time]]
          :where [?p :person/name ?name]
                 [(day ?time) ?day]
                 [(q [:find (max ?v) :in $ ?person ?d ...]) ?max]]
  Available: [?p ?time]
  Provides: [?name ?day ?max]
  Keep: [?name ?day ?max]
```

## Benefits

### 1. Single Phasing Pass

**Before**: Phase → Optimize → Re-phase → Recalculate symbols
**After**: Optimize → Phase (once)

No more:
- `updatePhaseSymbols()` after each optimization
- Re-assigning subqueries after phase reordering
- Fixing Keep/Provides mismatches

### 2. Clean Optimizer Composition

**Before**: Each optimizer reaches into Phase internals
```go
phase.Predicates = modified        // Semantic rewriting
phase.Subqueries → phase.Expressions  // Decorrelation
phase.Patterns[i].Filters = pushed   // Predicate pushdown
```

**After**: Pure functional transformations
```go
clauses = cse(clauses)
clauses = decorrelate(clauses)
clauses = semanticRewrite(clauses)
```

### 3. Context-Dependent Scoring

**Before**: Pattern selectivity is static
**After**: Scoring adapts to what's available

Example: `[?e :person/name ?name]`
- Available={?e}: High score (entity lookup)
- Available={}: Low score (full scan)

Same pattern, different scores based on context!

### 4. Clause Reordering Within Phases

**Current**: Fixed order (patterns → expressions → predicates → subqueries)
```datalog
[?e :event/value ?value]      ; 1M rows
[(* ?value 2) ?doubled]       ; Compute on 1M
[(> ?value 100)]              ; Filter to 1K
```

**Proposed**: Optimizer can reorder based on selectivity
```datalog
[?e :event/value ?value]      ; 1M rows
[(> ?value 100)]              ; Filter to 1K first!
[(* ?value 2) ?doubled]       ; Compute on 1K
```

### 5. Proven Architecture

**Some implementations use optimize-first phasing!** We're adopting this proven architecture.

### 6. Simpler Testing

**Before**: Test optimizations + phasing + symbol tracking interactions
**After**: Test optimizers independently, then test phasing on optimized clauses

## Migration Path

This is a **major architectural change**, but can be done **incrementally and safely** by treating the two shifts as separate stages:

### The Key Insight

**Separate concerns**:
- **Shift 1 (Query Interface)** - Can happen NOW without changing planner
- **Shift 2 (Optimize-First)** - Can happen LATER after executor is simplified

**Strategy**: Add a "realization layer" that converts internal planner structure → clean Query-based interchange format.

---

### Stage A: Add Realization Layer (Non-Breaking)

**Goal**: Planner outputs clean Query-based format, but internals unchanged.

**Step 1**: Define realized format (output of planner):
```go
// New interchange format between planner and executor
type RealizedPhase struct {
    Query     *query.Query      // Generated from internal Phase structure
    Available []query.Symbol
    Provides  []query.Symbol
    Keep      []query.Symbol
    Metadata  map[string]interface{}
}

type RealizedPlan struct {
    Query  *query.Query        // Original user query
    Phases []RealizedPhase
}
```

**Step 2**: Add `Realize()` method to existing QueryPlan:
```go
func (qp *QueryPlan) Realize() *RealizedPlan {
    realizedPhases := make([]RealizedPhase, len(qp.Phases))
    for i, phase := range qp.Phases {
        realizedPhases[i] = realizePhase(phase)
    }
    return &RealizedPlan{Query: qp.Query, Phases: realizedPhases}
}

func realizePhase(phase Phase) RealizedPhase {
    // Build Query.Where from phase components
    // CRITICAL: Preserve EXACT execution order from current executor!
    // This ensures identical results for validation.
    //
    // Current executor executes in this order:
    //   1. Patterns (pattern matching)
    //   2. Expressions (function evaluation)
    //   3. Predicates (filtering)
    //   4. Subqueries (nested query execution)
    //
    // We must lay out clauses in the SAME order.
    var where []query.Clause

    // 1. Add patterns (in order)
    for _, pp := range phase.Patterns {
        where = append(where, pp.Pattern)
    }

    // 2. Add expressions (in order)
    for _, ep := range phase.Expressions {
        where = append(where, ep.Expression)
    }

    // 3. Add predicates (in order)
    for _, pred := range phase.Predicates {
        where = append(where, pred.Predicate)
    }

    // 4. Add subqueries (in order)
    for _, sq := range phase.Subqueries {
        where = append(where, sq.Subquery)
    }

    // Build :find from Provides
    var find []query.FindElement
    for _, sym := range phase.Provides {
        find = append(find, query.FindVariable{Symbol: sym})
    }

    // Build :in from Available (if any)
    var in []query.InputSpec
    if len(phase.Available) > 0 {
        in = append(in, query.DatabaseInput{})
        in = append(in, query.RelationInput{Symbols: phase.Available})
    }

    return RealizedPhase{
        Query:     &query.Query{Find: find, In: in, Where: where},
        Available: phase.Available,
        Provides:  phase.Provides,
        Keep:      phase.Keep,
        Metadata:  phase.Metadata,
    }
}
```

**Step 3**: Validate Realize() produces correct queries:
- Tests verify generated queries are valid
- Tests verify execution equivalence (old path vs realized path)
- Can run both in parallel for validation
- **Crucially**: Because clause order is preserved, results should be IDENTICAL

**Key insights**:
1. **The complex Phase structure becomes internal-only to planner** - The output is clean!
2. **Exact ordering preservation enables validation** - New executor produces same results
3. **Clause reordering happens later** - Stage C can optimize ordering, but not now

---

### Stage B: Simplify Executor (Major Cleanup) ✅ COMPLETE

**Goal**: Executor accepts RealizedPlan and executes Query fragments.

**Status**: Fully implemented with comprehensive tests (Oct 2025)

**Step 1**: Create QueryExecutor interface:
```go
type QueryExecutor interface {
    // Execute a Datalog query with input relations and return relation groups
    // Key semantic: Returns []Relation (potentially multiple disjoint groups)
    // Relations are collapsed progressively but may remain disjoint if they share no symbols.
    Execute(ctx Context, query *query.Query, inputs []Relation) ([]Relation, error)
}

type DefaultQueryExecutor struct {
    matcher PatternMatcher
    options ExecutorOptions
}

func (e *DefaultQueryExecutor) Execute(ctx Context, q *query.Query, inputs []Relation) ([]Relation, error) {
    // Start with input relation groups (may be multiple disjoint groups)
    groups := Relations(inputs)

    // Execute each clause in the :where section
    // After each clause: append new relation and collapse
    for i, clause := range q.Where {
        newRel, err := e.executeClause(ctx, clause, []Relation(groups))
        if err != nil {
            return nil, fmt.Errorf("clause %d failed: %w", i, err)
        }

        // Append new relation to groups
        if newRel != nil && !newRel.IsEmpty() {
            groups = append(groups, newRel)
        }

        // Progressive collapse: joins relations sharing symbols, preserves disjoint ones
        groups = Relations(ctx.CollapseRelations([]Relation(groups), func() []Relation {
            return []Relation(groups.Collapse(ctx))
        }))

        // Early termination on empty
        if len(groups) == 0 {
            return []Relation(groups), nil
        }
    }

    // Apply :find projection to each group
    if len(q.Find) > 0 {
        findSymbols := extractFindSymbols(q.Find)
        for i, group := range groups {
            projected, err := group.Project(findSymbols)
            if err != nil {
                return nil, fmt.Errorf("find projection on group %d failed: %w", i, err)
            }
            groups[i] = projected
        }
    }

    return []Relation(groups), nil
}
```

**Step 2**: Add ExecuteRealized to Executor:
```go
func (e *Executor) ExecuteRealized(ctx Context, plan *planner.RealizedPlan) (Relation, error) {
    // Create QueryExecutor
    queryExecutor := NewQueryExecutor(e.matcher, e.options)

    var currentGroups []Relation

    // Execute each phase as an independent query
    for i, phase := range plan.Phases {
        phaseIndex := i
        isLastPhase := (i == len(plan.Phases)-1)

        // Execute phase query
        groups, err := queryExecutor.Execute(ctx, phase.Query, currentGroups)
        if err != nil {
            return nil, fmt.Errorf("phase %d failed: %w", phaseIndex+1, err)
        }

        // Project each group to Keep columns (what passes to next phase)
        if len(phase.Keep) > 0 {
            for i, group := range groups {
                projected, err := group.Project(phase.Keep)
                if err != nil {
                    return nil, fmt.Errorf("phase %d projection of group %d failed: %w", phaseIndex+1, i, err)
                }
                groups[i] = projected
            }
        }

        // Early termination on empty
        if len(groups) == 0 {
            return nil, nil
        }

        // For last phase, must collapse to single relation (error on Cartesian product)
        if isLastPhase && len(groups) > 1 {
            return nil, fmt.Errorf("phase %d resulted in %d disjoint relation groups - Cartesian products not supported", phaseIndex+1, len(groups))
        }

        currentGroups = groups
    }

    // Return the final single relation
    if len(currentGroups) == 0 {
        return nil, nil
    }
    return currentGroups[0], nil
}
```

**Key Design Decisions**:

1. **Multi-Relation Semantics**: QueryExecutor returns `[]Relation` to handle disjoint relation groups
2. **Progressive Collapse**: After each clause, append new relation and collapse (joins sharing symbols, preserves disjoint)
3. **Phase Boundaries**: Multiple groups can flow between phases, projected to Keep columns
4. **Final Phase Validation**: Error if final phase has multiple disjoint groups (prevents Cartesian products)
5. **Clause Execution**: Stubbed for future implementation (pattern, expression, predicate, subquery handlers)

**Implementation Complete**:
- ✅ QueryExecutor interface defined in `datalog/executor/query_executor.go`
- ✅ ExecuteRealized method added to `datalog/executor/executor.go`
- ✅ Multi-relation flow architecture implemented
- ✅ All clause execution methods implemented:
  - `executePattern()` - delegates to PatternMatcher
  - `executeExpression()` - with streaming Product() support for multi-relation expressions
  - `executePredicate()` - with streaming Product() support for multi-relation predicates
  - `executeSubquery()` - stubbed with error (deferred to Stage C as planned)
- ✅ Aggregate handling complete - detects aggregates in :find clause, errors on disjoint groups
- ✅ Relations.Product() - streaming Cartesian product via ProductRelation/ProductIterator
- ✅ Comprehensive test coverage in `datalog/executor/query_executor_test.go` (all tests pass)

**Key Implementation Details**:

1. **Relations.Product()** - Streaming Cartesian product for multi-relation expressions/predicates
   - Single relation: passthrough (no product needed)
   - Multiple relations: ProductRelation with nested-loop iterator
   - Zero materialization - fully streaming

2. **Execution Loop** - Different semantics for clause types:
   - Patterns/Subqueries → produce NEW relations (append + collapse)
   - Expressions/Predicates → TRANSFORM relations (replace groups + collapse)

3. **executePattern** - Simple delegation to PatternMatcher

4. **executeExpression** - Smart multi-relation handling:
   - Identifies relations with required symbols
   - Creates streaming Product() if multiple relations needed
   - Evaluates expression on combined relation
   - Returns result + unchanged relations

5. **executePredicate** - Same pattern as expressions with filtering

6. **Aggregate Detection** - Validates :find clause:
   - Errors if multiple disjoint groups (Cartesian product)
   - Uses ExecuteAggregationsWithContext() for aggregation

---

### Stage C: Rewrite Planner (Datalog AST Manipulation)

**Goal**: Rewrite planner to manipulate Datalog ASTs instead of Phase structures.

This is a **fundamental paradigm shift** in how the planner works:
- **Old planner**: Creates Phase structures with 7 operation types, then optimizes them
- **New planner**: Manipulates `[]query.Clause` AST, then groups into phases

Now that executor only understands RealizedPlan, we can **completely rewrite the planner** without affecting the executor!

**Step 1**: Extract clause utilities (AST introspection):
```go
package optimizer

// Functions that inspect Clause AST nodes
func extractSymbols(clause query.Clause) []query.Symbol {
    // Pattern matching on Clause type
    switch c := clause.(type) {
    case *query.DataPattern:
        return extractPatternSymbols(c)
    case *query.Expression:
        return extractExpressionSymbols(c)
    case query.Predicate:
        return extractPredicateSymbols(c)
    // ... etc
    }
}

func scoreClause(clause query.Clause, available map[query.Symbol]bool) int {
    // Context-dependent scoring based on AST structure
    symbols := extractSymbols(clause)
    bound := intersection(symbols, available)
    unbound := difference(symbols, available)
    return len(bound)*10 - len(unbound)*5
}

func canExecute(clause query.Clause, available map[query.Symbol]bool) bool {
    // Check if all required symbols are available
    required := extractSymbols(clause)
    for _, sym := range required {
        if sym.IsVariable() && !available[sym] {
            return false
        }
    }
    return true
}
```

**Step 2**: Implement optimizers as pure AST transformations:
```go
// Optimizers are pure functions: []Clause → []Clause
type Optimizer func([]query.Clause, OptimizerContext) []query.Clause

// CSE: Find duplicate expressions, replace with bindings
func cseOptimizer(clauses []query.Clause, ctx OptimizerContext) []query.Clause {
    var result []query.Clause
    seen := make(map[string]query.Symbol) // expr AST → binding

    for _, clause := range clauses {
        if expr, ok := clause.(*query.Expression); ok {
            key := expr.Function.String()
            if prev, exists := seen[key]; exists {
                // Replace with ground predicate (AST transformation)
                result = append(result, &query.GroundPredicate{
                    Var: expr.Binding,
                    Val: prev,
                })
            } else {
                seen[key] = expr.Binding
                result = append(result, clause)
            }
        } else {
            result = append(result, clause)
        }
    }
    return result
}

// Decorrelation: Transform subquery AST nodes into expressions
func decorrelationOptimizer(clauses []query.Clause, ctx OptimizerContext) []query.Clause {
    var result []query.Clause
    for _, clause := range clauses {
        if subq, ok := clause.(*query.Subquery); ok && isCorrelatedAggregate(subq) {
            // AST transformation: Subquery → Expression nodes
            newExprs := transformToConditionalAggregate(subq)
            result = append(result, newExprs...)
        } else {
            result = append(result, clause)
        }
    }
    return result
}

// Semantic rewriting: Transform expensive predicates into cheaper ones
func semanticRewriter(clauses []query.Clause, ctx OptimizerContext) []query.Clause {
    var result []query.Clause
    for _, clause := range clauses {
        // AST pattern matching and transformation
        if rewritten := tryRewriteTimePredicate(clause); rewritten != nil {
            result = append(result, rewritten)
        } else {
            result = append(result, clause)
        }
    }
    return result
}
```

**Step 3**: Rewrite planner to optimize-first (AST pipeline):
```go
func (p *Planner) Plan(q *query.Query) (*RealizedPlan, error) {
    // Extract input symbols from :in clause
    inputSymbols := extractInputSymbols(q.In)

    // 1. Optimize Clause AST (pure transformations on []query.Clause)
    clauses := q.Where
    clauses = cseOptimizer(clauses, ctx)
    clauses = decorrelationOptimizer(clauses, ctx)
    clauses = semanticRewriter(clauses, ctx)

    // 2. Phase planning (greedy algorithm on optimized Clause AST, runs ONCE)
    phases := createPhasesGreedy(clauses, q.Find, inputSymbols)

    // 3. Already in realized format!
    return &RealizedPlan{Query: q, Phases: phases}, nil
}

// Greedy phasing algorithm - operates on Clause AST nodes
func createPhasesGreedy(clauses []query.Clause, find []query.FindElement, inputs []query.Symbol) []RealizedPhase {
    available := symbolSet(inputs)
    remaining := clauses  // AST nodes to phase
    var phases []RealizedPhase

    for len(remaining) > 0 {
        currentPhase := []query.Clause{}  // Collect Clause AST nodes

        // Greedy: pick best Clause based on context-dependent scoring
        for {
            best := -1
            bestScore := -9999

            for i, clause := range remaining {
                syms := extractSymbols(clause)  // AST introspection
                if canExecute(clause, available) {
                    score := scoreClause(clause, available)  // Context-dependent!
                    if score > bestScore {
                        bestScore = score
                        best = i
                    }
                }
            }

            if best == -1 {
                break  // Start new phase
            }

            // Add Clause AST node to current phase
            clause := remaining[best]
            currentPhase = append(currentPhase, clause)
            available = union(available, extractSymbols(clause))
            remaining = append(remaining[:best], remaining[best+1:]...)
        }

        if len(currentPhase) == 0 {
            return nil, fmt.Errorf("cannot satisfy dependencies")
        }

        // Generate RealizedPhase from collected Clause AST nodes
        phase := buildRealizedPhase(currentPhase, available, find)
        phases = append(phases, phase)
    }

    return phases
}

// Build RealizedPhase from Clause AST nodes
func buildRealizedPhase(clauses []query.Clause, available symbolSet, find []query.FindElement) RealizedPhase {
    // Extract all symbols from clause AST nodes
    provides := extractAllSymbols(clauses)

    // Build Query from Clause AST nodes
    query := &query.Query{
        Find:  buildFindClause(provides, find),
        In:    buildInClause(available),
        Where: clauses,  // Just use the Clause AST nodes directly!
    }

    return RealizedPhase{
        Query:     query,
        Available: available.toSlice(),
        Provides:  provides,
        Keep:      calculateKeep(provides, find),
    }
}
```

**Step 4**: Delete old planner code (complete paradigm shift):
- **Delete**: Phase struct with 7 operation types (Patterns, Predicates, Expressions, etc.)
- **Delete**: `separatePatterns()` - no longer separating by operation type
- **Delete**: `updatePhaseSymbols()` - no longer needed!
- **Delete**: Phase reordering code - greedy algorithm handles ordering
- **Delete**: `assignSubqueriesToPhases()` - subqueries are just Clause AST nodes
- **Delete**: Complex symbol tracking in planner
- **Delete**: All optimization code that mutates Phase structures

**What remains**:
- **AST utilities**: `extractSymbols()`, `scoreClause()`, `canExecute()`
- **AST transformers**: `cseOptimizer()`, `decorrelationOptimizer()`, `semanticRewriter()`
- **Greedy phasing**: `createPhasesGreedy()` operating on `[]query.Clause`
- **Phase builder**: `buildRealizedPhase()` creating Query from Clause AST nodes

**The new mental model**:
- **Old**: "Build phases with typed operations, then optimize phases"
- **New**: "Transform Clause AST, then group Clauses into phases"

**Benefits**:
- **Optimizations are AST transformations** - Pure, composable, testable
- **Phasing happens once** - After all transformations complete
- **No re-phasing** - Dependencies are final after optimization
- **No symbol recalculation** - Symbols extracted from AST on demand
- **Context-dependent scoring** - Same Clause scores differently based on available symbols
- **Much simpler code** - Working with Clause lists instead of complex Phase structures

---

### Summary of Incremental Approach

| Stage | What Changes | What Stays Same | Risk Level |
|-------|-------------|-----------------|------------|
| **A: Realization** | Add Realize() method | Planner internals unchanged | Low |
| **B: Executor** | Executor uses RealizedPlan | Planner still works | Medium |
| **C: Planner** | **Rewrite to manipulate Datalog ASTs** | Executor already simplified | Low |

**Key advantage**: Each stage is independently testable and provides value:
- **After A**: Clean interchange format, can validate queries
- **After B**: Simplified executor (~200 lines → ~15 lines)
- **After C**: Planner manipulates Clause ASTs, not Phase structures

**The paradigm shift in Stage C**:
- **From**: Phase-oriented programming (7 operation types, complex structure)
- **To**: AST-oriented programming (`[]query.Clause` transformations)
- Planner becomes a Datalog AST compiler, not a Phase builder

**Validation strategy**:
- Stage A: Run both old executor and new executor (via Realize()), verify same results
- Stage B: All existing tests pass with new executor
- Stage C: All existing tests pass with new planner

This approach **minimizes risk** while **maximizing incremental value**.

## Open Questions

### 1. Storage Layer Integration

**Question**: Should `PatternMatcher` stay as-is, or should storage implement `QueryExecutor`?

**Trade-offs**:
- **Keep PatternMatcher**: Storage stays simple, QueryExecutor orchestrates
- **Storage as QueryExecutor**: Storage can optimize full query, but more complex

**Recommendation**: Keep PatternMatcher separate. Storage is about data retrieval, QueryExecutor is about orchestration.

### 2. Query Caching

If phases are queries, should we cache:
- Parsed queries?
- Compiled query plans?
- Query results (memoization)?

### 3. Subquery Handling

How do subqueries work in this model?

**Current**: Subqueries are special operation types

**Proposed**: Subqueries are just nested queries in the WHERE clause:
```datalog
:where [?p :person/name ?name]
       [(q [:find (max ?v) :in $ ?p ...]) ?max]
```

QueryExecutor recognizes `(q ...)` expressions and recursively executes them.

### 4. Metadata and Optimizations

Some optimizations (conditional aggregates, decorrelation) add metadata to phases. How does this work with query-based phases?

**Proposed**: Metadata stays on Phase, but affects query generation or execution, not phase structure.

## Concerns and Mitigation

### Concern 1: Performance

**Worry**: Adding an abstraction layer (QueryExecutor) might slow execution.

**Mitigation**:
- QueryExecutor can be optimized for specific query patterns
- No extra allocations if queries are generated at planning time
- Benchmark against current implementation

### Concern 2: Complexity of Query Generation

**Worry**: Generating correct Datalog queries from groups might be complex.

**Mitigation**:
- Clojure does this successfully (see `associated->query`)
- Query structure mirrors current Phase structure
- Can validate generated queries against existing Phase fields during migration

### Concern 3: Loss of Fine-Grained Control

**Worry**: Current code can optimize pattern matching, expression evaluation separately.

**Mitigation**:
- QueryExecutor can decompose queries internally
- Still uses PatternMatcher for patterns
- Can still apply operation-specific optimizations

### Concern 4: Breaking Change for Users?

**Worry**: This changes internal architecture significantly.

**Mitigation**:
- User-facing query API unchanged
- This is internal refactoring
- Can be done incrementally (migration path above)

## Conclusion

This proposal advocates for **two complementary architectural shifts** that work together:

### 1. Query as Universal Interface

**Datalog becomes the interface boundary**: Every layer speaks `Query + Relations → Relations`.

Benefits:
- Phases ARE query fragments (not 7 operation types)
- Execution is trivial (~200 lines → ~10 lines)
- Storage layer stays simple (just pattern matching)
- Easy to test and optimize

**Implementation**: Stages A & B (Realization + Executor)

### 2. AST-Oriented Planning

**Planner manipulates Datalog ASTs**: Transform `[]query.Clause`, then group into phases.

Benefits:
- Optimize once, phase once (no re-phasing)
- Clean optimizer composition (AST transformations)
- Context-dependent planning (scoring adapts to available symbols)
- Enables clause reordering (filter before compute)

**Implementation**: Stage C (Planner rewrite)

### Key Architectural Insights

1. **Clause is the atomic unit** - Already in our codebase (`Query.Where` is `[]query.Clause`)
2. **Optimizations are AST transformations** - Pure functions on Clause nodes, not Phase mutations
3. **Phasing happens once** - After all AST transformations, using greedy algorithm
4. **Query + Relations is universal** - Same interface for phases, subqueries, storage
5. **Planner is a Datalog compiler** - Transforms and groups Clause AST nodes

### Why This Works

Both shifts reinforce each other:
- **AST-oriented planning** produces clean `[]Clause` for phasing
- **Query interface** makes phases simple to execute (just Query fragments)
- **Greedy phasing** generates optimal Query fragments from Clause AST nodes
- **Universal interface** eliminates special cases
- **AST transformations** are pure, composable, testable

The key enabler: `Query.Where` is already `[]query.Clause` - we're just changing how we manipulate it!

### Recommendation

**Proceed with this architectural change via the phased migration path.**

This is not inventing something new - it's adopting proven architectural patterns from distributed Datalog implementations that have handled billions of facts in production.

### The Meta-Lesson

**"Maybe the distributed approach is usually the right way to do it..."**

After all this analysis, we've essentially rediscovered why certain architectural patterns work:
- Flat pattern lists (just Clause AST nodes, not subdivided types)
- Optimize-first planning (transform clauses, then phase once)
- Query fragments as phases (universal interface)
- AST-oriented compilation (transforming Datalog, not building Phase structures)

These patterns emerged from production experience at scale. We're not improving on them - we're learning from them!

**Key insight**: When architectural patterns have handled billions of facts in production, it's worth understanding WHY they're designed that way before changing them.

This proposal is really: "Here's what we learned by studying query planning patterns, and how to migrate our codebase to match proven approaches."

## References

- Current Phase definition: `datalog/planner/types.go`
- Current executor: `datalog/executor/executor_sequential.go`
- Recent bugs fixed: `docs/bugs/resolved/BUG_EXPRESSION_ONLY_PHASES.md`

## Appendix: Code Samples

### A. Current Phase Execution Complexity

```go
// From executor_sequential.go (simplified)
func (e *Executor) executePhase(phase *Phase) (Relation, error) {
    var independentGroups Relations

    // 1. Pattern execution (35-147 lines)
    for _, pattern := range phase.Patterns {
        rel := e.matcher.Match(pattern, availableRelations)
        independentGroups = append(independentGroups, rel)
        independentGroups = independentGroups.Collapse(ctx)
        // Check for empty, update availableRelations, etc.
    }

    // 2. Expression-only phase edge case (149-168)
    collapsed := independentGroups
    if len(phase.Patterns) == 0 && len(collapsed) == 0 {
        collapsed = availableRelations
    }

    // 3. Expressions and predicates (separate function, 300+ lines)
    return e.applyExpressionsAndPredicates(ctx, phase, collapsed)
}
```

### B. Proposed Phase Execution Simplicity

```go
func (e *Executor) executePhase(ctx Context, phase *Phase, input Relation) (Relation, error) {
    inputs := []Relation{input}
    result, err := e.queryExecutor.Execute(ctx, phase.Query, inputs)
    if err != nil {
        return nil, err
    }

    if len(phase.Keep) > 0 {
        return result.Project(phase.Keep)
    }

    return result, nil
}
```

**From ~200 lines to ~10 lines.**
