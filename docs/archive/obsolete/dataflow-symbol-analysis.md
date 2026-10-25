# Proposal: Dataflow-Centric Symbol Analysis

## Problem

The current planner analyzes clause dependencies using a simple Requires/Provides model:
- Each clause declares what symbols it requires (inputs) and provides (outputs)
- Clauses are ordered so that required symbols are provided by earlier clauses

This model breaks down for OR clauses with fallback semantics because:
1. DataPatterns "provide" all their variables but don't "require" any
2. The planner can't distinguish between internal join variables and correlation variables
3. OR clauses are analyzed in isolation, without context of the surrounding query

**Result**: The planner incorrectly reorders OR clauses before patterns that should bind their correlation variables, breaking per-tuple fallback semantics.

## Current Workaround

A heuristic was added to `extractOrClauseSymbols`: for OR clauses with expressions (fallback semantics), require the entity variable (first element) of the first pattern in pattern branches.

This works for common cases but is fragile and doesn't capture the actual semantic requirement.

## Proposed Solution: Dataflow Analysis

Instead of analyzing clauses in isolation, perform dataflow analysis across the entire AST:

### 1. Symbol Tracing

For each symbol in the query, trace its usage through the AST:
- Where is it first bound? (pattern match, expression output, input parameter)
- Where is it used? (pattern element, expression input, predicate, find clause)
- What is its scope? (global, within OR branch, within subquery)

### 2. Correlation Detection

For OR clauses, identify correlation variables by analyzing symbol sharing:
```
Query:
  [?scenario :scenario/name ?name]
  (or [?scenario :scenario/task ?task] [(ground 0) ?count])

Symbol Analysis:
  ?scenario: bound by pattern#1, used by OR/branch#1/pattern#1
  ?name: bound by pattern#1, used in :find
  ?task: bound by OR/branch#1/pattern#1, internal to OR
  ?count: bound by OR/branch#2/expression, output of OR

Correlation: ?scenario is shared between outer context and OR
```

### 3. Dependency Derivation

Derive clause dependencies from dataflow:
- OR clause requires ?scenario because branch#1 uses it and it's bound outside the OR
- This is computed from the AST, not from a heuristic

### 4. Implementation Sketch

```go
type SymbolInfo struct {
    Name       query.Symbol
    BoundBy    ClauseRef      // Where first bound
    UsedBy     []ClauseRef    // All usage sites
    Scope      Scope          // Global, OR-branch, subquery
}

type DataflowAnalyzer struct {
    symbols map[query.Symbol]*SymbolInfo
}

func (a *DataflowAnalyzer) Analyze(q *query.Query) {
    // Pass 1: Collect all symbol bindings
    for i, clause := range q.Where {
        a.collectBindings(clause, ClauseRef{Index: i})
    }

    // Pass 2: Collect all symbol usages
    for i, clause := range q.Where {
        a.collectUsages(clause, ClauseRef{Index: i})
    }

    // Pass 3: Determine scopes
    a.computeScopes(q)
}

func (a *DataflowAnalyzer) GetOrRequires(orClause *query.OrClause, orIndex int) []query.Symbol {
    var requires []query.Symbol
    for _, sym := range a.symbolsUsedInOr(orClause) {
        info := a.symbols[sym]
        // If bound outside the OR and used inside, it's a correlation variable
        if info.BoundBy.Index < orIndex {
            requires = append(requires, sym)
        }
    }
    return requires
}
```

## Benefits

1. **Principled**: Dependencies derived from actual dataflow, not heuristics
2. **Correct**: Handles complex cases (nested ORs, subqueries, multiple correlation variables)
3. **Maintainable**: Single analysis pass, clear semantics
4. **Extensible**: Foundation for other optimizations (dead symbol elimination, predicate pushdown)

## Migration Path

1. Keep current heuristic as short-term fix
2. Implement DataflowAnalyzer alongside existing planner
3. Validate that derived dependencies match expected behavior
4. Replace heuristic-based analysis with dataflow-based analysis
5. Remove legacy code

## Related Work

- Datomic's query planner uses similar dataflow analysis
- Standard compiler dataflow analysis (reaching definitions, live variables)
- Information-flow approaches in distributed Datalog (see `docs/ideas/planner-improvements.md`)
