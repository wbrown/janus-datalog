# Planner Symbolic Manipulation Analysis

**Date:** January 2025
**Reviewer:** Claude (AI assistant)
**Scope:** Query planner symbolic manipulation, phase creation, pattern scoring

## Executive Summary

The query planner performs sophisticated **symbolic manipulation** to create optimal execution plans. It reasons about query structure using only symbolic relationships (which variables connect which patterns) without touching actual data. This is essentially **abstract interpretation** - deriving execution order from data dependencies.

**Key Finding:** The planner is a mini theorem prover that:
- Tracks symbol binding state (Available → Provides → Keep)
- Scores patterns by bound/unbound positions for selectivity
- Groups patterns by entity symbols for efficient execution
- Selects indices based on abstract BoundMask patterns
- Creates phases ensuring data dependencies are satisfied

---

## 1. The Three Symbol States

The planner tracks symbols through three states as query execution progresses:

### Available Symbols
**Definition:** Symbols already bound from previous phases or input parameters

**Sources:**
```go
// From input parameters
for _, input := range q.In {
    switch inp := input.(type) {
    case query.ScalarInput:
        inputSymbols[inp.Symbol] = true
    case query.TupleInput:
        for _, sym := range inp.Symbols {
            inputSymbols[sym] = true
        }
    }
}

// From previous phases
for _, phase := range previousPhases {
    for _, sym := range phase.Provides {
        available[sym] = true
    }
}
```

**Purpose:** Determines which patterns can execute (patterns need at least one bound element)

### Provides Symbols
**Definition:** New symbols bound by this phase's patterns

```go
// Extract provides from patterns
provides := make(map[query.Symbol]bool)
for _, pattern := range phase.Patterns {
    for _, elem := range pattern.Elements {
        if elem != nil && elem.IsVariable() {
            if v, ok := elem.(query.Variable); ok {
                provides[v.Name] = true
            }
        }
    }
}
phase.Provides = getResolvedSymbols(provides)
```

**Purpose:** Tells next phase what symbols it can use

### Keep Symbols
**Definition:** Symbols to pass forward (needed by find clause or future phases)

```go
keep := make(map[query.Symbol]bool)

// Keep all find variables
for _, sym := range findVars {
    keep[sym] = true
}

// Keep variables needed by remaining patterns
for _, pattern := range remainingPatterns {
    for _, elem := range pattern.Elements {
        if elem.IsVariable() {
            if v, ok := elem.(query.Variable); ok {
                keep[v.Name] = true
            }
        }
    }
}
```

**Purpose:** Prunes unnecessary symbols to reduce memory and join cost

**Example Flow:**
```
Query: [:find ?name :where [?p :person/name ?name] [?p :person/age ?age] [?age >= 18]]

Phase 1:
  Available: []
  Provides: [?p, ?name]
  Keep: [?p, ?name]  // ?p needed for phase 2, ?name needed for :find

Phase 2:
  Available: [?p, ?name]
  Provides: [?age]
  Keep: [?name]  // Only ?name needed for :find
```

---

## 2. Pattern Scoring: Estimating Selectivity

The planner scores patterns based on their **symbolic structure** to estimate selectivity **without seeing data**.

### Scoring Algorithm

**Location:** `planner_patterns.go:288-371`

```go
func (p *Planner) scorePattern(pattern *query.DataPattern, resolved map[query.Symbol]bool) int {
    score := 0
    boundCount := 0

    // Entity position (most selective)
    if elem := pattern.GetE(); elem != nil {
        if elem.IsVariable() {
            if v, ok := elem.(query.Variable); ok && resolved[v.Name] {
                boundCount++
            } else {
                score += 1000 // Unbound entity
            }
        } else {
            boundCount++
            score -= 800  // Constant entity (HUGE bonus)
        }
    }

    // Attribute position (moderately selective)
    if elem := pattern.GetA(); elem != nil {
        if elem.IsVariable() {
            if v, ok := elem.(query.Variable); ok && resolved[v.Name] {
                boundCount++
                score += 10
            } else {
                score += 100 // Unbound attribute
            }
        } else {
            boundCount++
            // Use cardinality statistics if available
            if attr, ok := constant.Value.(datalog.Keyword); ok {
                if card, exists := p.stats.AttributeCardinality[attr.String()]; exists {
                    score += card / 100
                }
            }
        }
    }

    // Value position (least selective)
    if elem := pattern.GetV(); elem != nil {
        if elem.IsVariable() {
            if v, ok := elem.(query.Variable); ok && resolved[v.Name] {
                boundCount++
                score -= 500  // Bound value (as selective as constant!)
            } else {
                score += 500  // Unbound value
            }
        } else {
            boundCount++
            score -= 500  // Constant value (big bonus)
        }
    }

    // Patterns with no bound elements can't execute yet
    if boundCount == 0 && len(resolved) > 0 {
        return -1  // Can't execute
    }

    // Bonus for binding new variables
    newBindings := 0
    for _, elem := range pattern.Elements {
        if elem.IsVariable() {
            if v, ok := elem.(query.Variable); ok && !resolved[v.Name] {
                newBindings++
            }
        }
    }
    score -= newBindings * 10

    return score
}
```

### Selectivity Hierarchy

**Most Selective (lowest score):**
1. Constant entity + constant value: score ≈ −1300
2. Constant entity + bound variable: score ≈ −800
3. Bound entity variable + constant value: score ≈ −500

**Least Selective (highest score):**
1. All unbound: score ≈ +1600
2. Unbound entity + unbound value: score ≈ +1500
3. Only attribute bound: score ≈ +100

**Special Cases:**
- **Input parameters treated as bound**: Patterns with input params get same bonus as constants (−500)
- **Can't execute**: Patterns with no bound elements return -1 (must wait for future phase)

### Why This Works

**Key insight:** Even without statistics, **structural constraints** predict selectivity:

1. **Constant entity** is extremely selective (points to one entity)
2. **Constant attribute** is moderately selective (all entities with that attribute)
3. **Constant value** is selective (entities where attribute = value)
4. **Bound variables** are as selective as constants (already filtered by previous patterns)

**Example:**
```datalog
:in $ ?ticker
:where [?s :symbol/ticker ?ticker]  // ?ticker is input param (bound!)
       [?e :price/symbol ?s]        // ?s bound from previous pattern
       [?e :price/open ?o]          // ?e bound from previous pattern
```

**Scores:**
- Pattern 1: −500 (constant attribute + bound value ?ticker) → Very selective!
- Pattern 2: +10 (constant attribute + bound entity ?s) → Selective
- Pattern 3: +510 (constant attribute + unbound value) → Less selective

**Result:** Execute in order 1 → 2 → 3 (most to least selective)

---

## 3. Index Selection: BoundMask Logic

The planner selects indices based on which pattern positions are bound, represented as a **BoundMask**.

### BoundMask Structure

**Location:** `types.go:375-381`

```go
type BoundMask struct {
    E bool // Entity bound
    A bool // Attribute bound
    V bool // Value bound
    T bool // Transaction/time bound
}
```

### Index Selection Algorithm

**Location:** `planner_patterns.go:413-432`

```go
func (p *Planner) selectIndex(mask BoundMask) IndexType {
    switch {
    case mask.E && mask.A && mask.V:
        return EAVT // All bound - most selective (direct lookup)
    case mask.E && mask.A:
        return EAVT // Entity + attribute (entity's attribute values)
    case mask.A && mask.V:
        return AVET // Attribute + value (reverse lookup: entities with attribute=value)
    case mask.A && mask.E:
        return AEVT // Attribute + entity (same as E+A but different index order)
    case mask.E:
        return EAVT // Entity only (all entity's attributes)
    case mask.A:
        return AEVT // Attribute only (all entities with this attribute)
    case mask.V:
        return VAET // Value only (ref lookups: entities referencing this value)
    default:
        return EAVT // Full scan - try to avoid!
    }
}
```

### Index Semantics

**EAVT (Entity-Attribute-Value-Tx):**
- Best when: E is bound
- Scans: Entity's attributes efficiently
- Use case: "Get all facts about entity E"

**AEVT (Attribute-Entity-Value-Tx):**
- Best when: A is bound (but not E+A together)
- Scans: All entities with this attribute
- Use case: "Find all entities with attribute :person/name"

**AVET (Attribute-Value-Entity-Tx):**
- Best when: A and V are bound
- Scans: Entities where attribute = value
- Use case: "Find entities where :person/age = 25"

**VAET (Value-Attribute-Entity-Tx):**
- Best when: V is bound (value is an entity reference)
- Scans: Entities referencing this value
- Use case: "Find all entities that reference entity E"

**TAEV (Tx-Attribute-Entity-Value):**
- Best when: T is bound
- Scans: Facts created in transaction T
- Use case: "What changed in transaction T?"

### Example Index Selection

**Query:**
```datalog
:in $ ?ticker
:where [?s :symbol/ticker ?ticker]
       [?e :price/symbol ?s]
       [?e :price/open ?o]
```

**Pattern 1:** `[?s :symbol/ticker ?ticker]`
- E: unbound → false
- A: `:symbol/ticker` (constant) → true
- V: `?ticker` (input param, bound) → true
- **BoundMask:** `{E: false, A: true, V: true, T: false}`
- **Index:** AVET (attribute + value lookup)

**Pattern 2:** `[?e :price/symbol ?s]`
- E: `?e` (unbound) → false
- A: `:price/symbol` (constant) → true
- V: `?s` (bound from Pattern 1) → true
- **BoundMask:** `{E: false, A: true, V: true, T: false}`
- **Index:** AVET

**Pattern 3:** `[?e :price/open ?o]`
- E: `?e` (bound from Pattern 2) → true
- A: `:price/open` (constant) → true
- V: `?o` (unbound) → false
- **BoundMask:** `{E: true, A: true, V: false, T: false}`
- **Index:** EAVT (entity's attribute values)

---

## 4. Phase Creation: Grouping by Symbols

The planner groups patterns into phases based on **symbol dependencies**.

### Default Strategy: Group by Entity

**Location:** `planner_phases.go:29-32`

```go
// Group patterns by their primary entity symbol (like Clojure planner)
patternGroups := p.groupPatternsByEntity(dataPatterns)

// Then order the groups based on symbol relationships and selectivity
orderedGroups := p.orderPatternGroups(patternGroups, findVars)
```

**Algorithm:**
```go
func (p *Planner) groupPatternsByEntity(patterns []*query.DataPattern) []patternGroup {
    groups := make(map[query.Symbol]*patternGroup)

    for _, pattern := range patterns {
        elem := pattern.GetE()  // Get entity element
        if elem != nil && elem.IsVariable() {
            v := elem.(query.Variable)
            entitySym := v.Name

            if groups[entitySym] == nil {
                groups[entitySym] = &patternGroup{
                    entitySym: entitySym,
                    patterns:  []*query.DataPattern{},
                    symbols:   make(map[query.Symbol]bool),
                }
            }

            groups[entitySym].patterns = append(groups[entitySym].patterns, pattern)

            // Track all symbols in this group
            for _, e := range pattern.Elements {
                if e != nil && e.IsVariable() {
                    if v, ok := e.(query.Variable); ok {
                        groups[entitySym].symbols[v.Name] = true
                    }
                }
            }
        }
    }

    return groups
}
```

**Why Group by Entity?**
- Patterns sharing entity variable often join efficiently
- Entity lookups are fast (EAVT index)
- Reduces intermediate result sizes (join on entity ID)

**Example:**
```datalog
:where [?p :person/name ?name]
       [?p :person/age ?age]
       [?p :person/email ?email]
```
All three patterns share `?p` → grouped together in one phase

### Group Ordering Algorithm

**Location:** `planner_patterns.go:98-173`

```go
func (p *Planner) orderPatternGroups(groups []patternGroup, findVars []query.Symbol) []patternGroup {
    var ordered []patternGroup
    remaining := append([]patternGroup{}, groups...)
    resolvedSymbols := make(map[query.Symbol]bool)

    // 1. Find best starting group (most selective)
    var startGroup *patternGroup
    bestScore := 999999
    for i, group := range remaining {
        score := p.scorePatternGroup(group, resolvedSymbols)
        if score < bestScore {
            bestScore = score
            startGroup = &remaining[i]
        }
    }

    ordered = append(ordered, *startGroup)
    for sym := range startGroup.symbols {
        resolvedSymbols[sym] = true
    }

    // 2. Order remaining groups by connectivity
    for len(remaining) > 0 {
        bestScore := -1
        bestIdx := -1

        for i, group := range remaining {
            score := 0
            // Count intersections with resolved symbols
            for sym := range group.symbols {
                if resolvedSymbols[sym] {
                    score += 10  // Shares symbols with previous phases
                }
            }
            // Bonus for groups that bind find variables
            for _, findVar := range findVars {
                if group.symbols[findVar] {
                    score += 5  // Provides symbols needed by :find
                }
            }

            if score > bestScore {
                bestScore = score
                bestIdx = i
            }
        }

        // If no group has connections, just take first one
        if bestIdx < 0 {
            bestIdx = 0
        }

        ordered = append(ordered, remaining[bestIdx])
        for sym := range remaining[bestIdx].symbols {
            resolvedSymbols[sym] = true
        }

        remaining = append(remaining[:bestIdx], remaining[bestIdx+1:]...)
    }

    return ordered
}
```

**Scoring Heuristics:**
1. Start with most selective group (best pattern in group)
2. Then prefer groups that:
   - Share symbols with already-ordered groups (+10 per shared symbol)
   - Provide find variables (+5 bonus)
3. If no connections, take first remaining group

**This creates a dependency chain** where each phase builds on previous phases' symbols.

### Fine-Grained Phase Strategy

**Location:** `planner_phases.go:169-400`

**When:** `EnableFineGrainedPhases: true`

**Purpose:** Create smaller phases to avoid large cross-products

**Algorithm:**
1. **Process constant patterns first** (most selective)
   - Patterns with at least one constant element
   - Sort by selectivity
   - Group by entity symbol

2. **Process variable-only patterns second**
   - Only execute after some symbols are bound
   - Still group by entity symbol

3. **Create separate phase for each entity group**
   - Smaller phases = smaller intermediate results
   - Reduces risk of Cartesian products

**Trade-off:**
- **More phases:** More planning overhead, more intermediate materializations
- **Smaller relations:** Lower memory, fewer accidental cross-products

**When to use:**
- Complex queries with many patterns
- Queries with risk of Cartesian products
- When debugging cross-product issues

---

## 5. Symbolic Reasoning Examples

### Example 1: Simple Join Chain

**Query:**
```datalog
[:find ?name
 :where [?p :person/name ?name]
        [?p :person/age ?age]
        [(>= ?age 18)]]
```

**Symbolic Analysis:**

**Phase 1: Patterns**
```
Available: []
Pattern 1: [?p :person/name ?name]
  - E: unbound
  - A: constant (:person/name)
  - V: unbound
  - Score: ~110 (moderate)
  - Provides: {?p, ?name}

Pattern 2: [?p :person/age ?age]
  - E: unbound
  - A: constant (:person/age)
  - V: unbound
  - Score: ~110 (moderate)
  - Provides: {?p, ?age}
```

**Grouping:** Both patterns share `?p` → group together

**Phase 1 Final:**
```
Patterns: [Pattern 1, Pattern 2]
Provides: [?p, ?name, ?age]
Keep: [?name]  // Only ?name needed for :find
```

**Phase 1: Predicates**
```
Predicate: [(>= ?age 18)]
  - Requires: [?age]
  - Available after patterns: [?p, ?name, ?age]
  - Can execute: ✓
```

**Result:** Single-phase query (patterns + predicate)

### Example 2: Multi-Phase with Input Parameters

**Query:**
```datalog
[:find ?high
 :in $ ?ticker
 :where [?s :symbol/ticker ?ticker]
        [?e :price/symbol ?s]
        [?e :price/high ?high]]
```

**Symbolic Analysis:**

**Input Symbols:** `{?ticker: true}` (bound from :in clause)

**Phase 1:**
```
Available: [?ticker]
Pattern: [?s :symbol/ticker ?ticker]
  - E: unbound (?s)
  - A: constant (:symbol/ticker)
  - V: bound (?ticker is input param!)
  - Score: −500 (very selective!)
  - BoundMask: {E: false, A: true, V: true}
  - Index: AVET (attribute + value lookup)
  - Provides: [?s]
Keep: [?s]  // Needed for next phase
```

**Phase 2:**
```
Available: [?ticker, ?s]
Pattern: [?e :price/symbol ?s]
  - E: unbound (?e)
  - A: constant (:price/symbol)
  - V: bound (?s from Phase 1)
  - Score: −500 (very selective!)
  - BoundMask: {E: false, A: true, V: true}
  - Index: AVET
  - Provides: [?e]

Pattern: [?e :price/high ?high]
  - E: bound (?e from previous pattern in this phase)
  - A: constant (:price/high)
  - V: unbound (?high)
  - Score: +10 (selective, entity bound)
  - BoundMask: {E: true, A: true, V: false}
  - Index: EAVT
  - Provides: [?high]
Keep: [?high]  // For :find
```

**Key Observation:** Input parameters (`?ticker`) are treated as bound, making Pattern 1 highly selective even though it has no other patterns to join with.

### Example 3: Cross-Product Detection

**Query (problematic):**
```datalog
[:find ?person-name ?product-name
 :where [?person :person/name ?person-name]
        [?product :product/name ?product-name]]
```

**Symbolic Analysis:**

**Pattern Grouping:**
```
Group 1: entity ?person
  - [?person :person/name ?person-name]

Group 2: entity ?product
  - [?product :product/name ?product-name]
```

**Symbol Connectivity:**
```
Group 1 symbols: {?person, ?person-name}
Group 2 symbols: {?product, ?product-name}
Shared symbols: {} (NONE!)
```

**Phase Creation:**
```
Phase 1:
  Patterns: [[?person :person/name ?person-name]]
  Provides: [?person, ?person-name]
  Keep: [?person-name]

Phase 2:
  Patterns: [[?product :product/name ?product-name]]
  Provides: [?product, ?product-name]
  Keep: [?product-name]
```

**Execution:**
```
Phase 1 result: 1000 tuples
Phase 2 result: 500 tuples
Collapse: 1000 × 500 = 500,000 tuples (Cartesian product!)
```

**Detection:** Executor's `Collapse()` produces multiple disjoint relation groups → error

**Fix:** Add connecting pattern:
```datalog
[?person :person/purchased ?order]
[?order :order/product ?product]
```

---

## 6. Why This is Masterful Design

### 1. Pure Symbolic Reasoning

**No statistics required!** The planner reasons about selectivity using only:
- Constant vs variable (constants are selective)
- Bound vs unbound (bound variables are selective)
- Structural patterns (entity lookups faster than full scans)

**This aligns with the "statistics-free" research thesis:**
- Avoids cost of maintaining statistics
- No staleness issues
- Pattern structure often predicts selectivity well enough

### 2. Index Selection Without Database Access

The planner chooses indices **before seeing any data**:
- BoundMask abstracts the "shape" of the pattern
- Index selection maps shape → optimal index
- Works for any database state

**This enables:**
- Fast planning (15µs)
- Query plan caching
- Deterministic behavior

### 3. Phase Creation as Dependency Ordering

Phases are created by analyzing **symbol flow**:
- Which symbols are available when
- Which patterns can execute with available symbols
- Which symbols must be kept for later phases

**This is essentially:**
- Topological sort of data dependencies
- Incremental binding of variables
- Progressive constraint satisfaction

### 4. Entity Grouping as Join Optimization

Grouping patterns by entity symbol is a **heuristic join optimization**:
- Patterns sharing entity often join efficiently
- Entity-based joins reduce intermediate sizes
- Aligns with index structure (EAVT optimized for entity lookups)

**Without cost-based optimization**, this heuristic works surprisingly well.

### 5. Input Parameters as Symbolic Constants

Treating input parameters as "bound" is clever:
- Captures the selectivity they provide
- Allows queries to start with selective lookups
- Enables index-backed parameter joins

**Example:**
```datalog
:in $ ?ticker
:where [?s :symbol/ticker ?ticker]  // AVET index lookup (fast!)
```

### 6. Fine-Grained Phases as Safety Mechanism

`EnableFineGrainedPhases` is a **safety valve**:
- Breaks query into smaller pieces
- Prevents runaway cross-products
- Trades planning complexity for execution safety

**This shows defensive engineering** - provide escape hatch for pathological queries.

---

## 7. Comparison to Other Systems

### vs. PostgreSQL (Cost-Based Optimizer)

**PostgreSQL:**
- Uses statistics (histograms, distinct counts)
- Estimates cardinalities for all join orders
- Chooses optimal plan via dynamic programming
- Planning: ~15ms

**Janus Datalog:**
- Uses symbolic structure only
- Greedy phase creation + entity grouping
- No statistics needed
- Planning: ~15µs (1000× faster!)

**Trade-off:**
- PostgreSQL finds theoretically optimal plan
- Janus finds "good enough" plan instantly
- For pattern-based queries, heuristics work well

### vs. Datomic (Peer Model)

**Datomic:**
- Symbolic rule-based planner
- Uses qvar/pattern analysis
- Also statistics-free

**Janus Datalog:**
- Similar philosophy
- Adds entity grouping heuristic
- More explicit phase boundaries

**Both avoid cost-based optimization** for simplicity and speed.

### vs. DataScript (Client-Side)

**DataScript:**
- JavaScript implementation
- Even simpler planner (mostly pattern order)
- Works well for small databases

**Janus Datalog:**
- More sophisticated (phase creation, index selection)
- Handles larger datasets
- Production-oriented (BadgerDB backend)

---

## 8. Limitations and Future Work

### Current Limitations

**1. No Cardinality Estimation**
- Can't predict join result sizes
- May choose suboptimal join order for unusual data distributions

**2. Greedy Phase Creation**
- Locally optimal, not globally optimal
- Doesn't consider all possible phase groupings

**3. Entity Grouping Heuristic**
- Assumes patterns sharing entity will join efficiently
- Not always true (e.g., many-to-many relationships)

**4. No Join Order Optimization Within Phases**
- Executor uses greedy collapse
- Doesn't try alternative join orders

### Potential Improvements (Stage C)

**From `PHASE_AS_QUERY_ARCHITECTURE.md`:**

> **Stage C: AST-Oriented Planner**
> - Explicit join trees instead of pattern lists
> - Pre-planned join strategies (hash vs nested loop)
> - Cost estimates using lightweight statistics
> - AST rewriting for algebraic equivalences

**What this would add:**
- Join trees: Explicit left/right/strategy specification
- Cost estimates: Optional statistics for better decisions
- AST rewriting: Algebraic simplifications

**Why not done yet:**
- Stage B (QueryExecutor) must mature first
- Current approach works well for pattern-based queries
- Complexity vs benefit trade-off

---

## 9. The Symbolic Manipulation Toolkit

The planner has a sophisticated toolkit for symbolic manipulation:

### Symbol Extraction

```go
// Extract all variables from a pattern
func (p *Planner) extractPatternVariables(pattern *query.DataPattern) map[query.Symbol]bool {
    vars := make(map[query.Symbol]bool)
    for _, elem := range pattern.Elements {
        if elem.IsVariable() {
            if v, ok := elem.(query.Variable); ok {
                vars[v.Name] = true
            }
        }
    }
    return vars
}
```

### Symbol Dependency Checking

```go
// Check if pattern shares variables with a set
func (p *Planner) sharesVariables(pattern *query.DataPattern, vars map[query.Symbol]bool) bool {
    for _, elem := range pattern.Elements {
        if elem.IsVariable() {
            if v, ok := elem.(query.Variable); ok && vars[v.Name] {
                return true
            }
        }
    }
    return false
}
```

### Bound Element Detection

```go
// Check if a pattern element is bound (constant or bound variable)
func (p *Planner) isElementBound(elem query.PatternElement, resolved map[query.Symbol]bool) bool {
    if elem == nil {
        return false
    }
    if elem.IsVariable() {
        if v, ok := elem.(query.Variable); ok {
            return resolved[v.Name]  // Check if variable is bound
        }
        return false
    }
    return !elem.IsBlank()  // Constants and non-blank are bound
}
```

### Predicate Evaluation Checking

```go
// Check if a predicate can be evaluated with available symbols
func (p *Planner) canEvaluatePredicate(pred query.Predicate, available map[query.Symbol]bool) bool {
    required := pred.RequiredSymbols()
    for _, sym := range required {
        if !available[sym] {
            return false  // Missing required symbol
        }
    }
    return true
}
```

### Symbol Set Operations

```go
// Convert symbol map to sorted slice (for deterministic output)
func (p *Planner) getResolvedSymbols(resolved map[query.Symbol]bool) []query.Symbol {
    var symbols []query.Symbol
    for sym := range resolved {
        symbols = append(symbols, sym)
    }
    sort.Slice(symbols, func(i, j int) bool {
        return symbols[i] < symbols[j]
    })
    return symbols
}
```

**These primitives enable:**
- Dependency analysis (which patterns can execute when)
- Grouping decisions (which patterns belong together)
- Predicate placement (which phase should evaluate predicates)
- Symbol tracking (what's available vs needed)

---

## 10. Lessons Learned

### For This Codebase

**1. Symbolic Reasoning is Powerful**
- Pattern structure alone gives good selectivity estimates
- Entity grouping heuristic works well in practice
- Statistics-free design reduces complexity

**2. Phase Creation is Key**
- Proper dependency ordering prevents errors
- Symbol tracking ensures correctness
- Keep calculation optimizes memory usage

**3. Index Selection as Pattern Matching**
- BoundMask abstracts pattern structure
- Mapping to indices is declarative
- Easy to understand and maintain

**4. Input Parameters as First-Class**
- Treating params as bound enables selective lookups
- Makes parameterized queries efficient
- Aligns with Datomic's `:in` semantics

### For Query Planners

**1. Heuristics Can Be Enough**
- Don't need full cost-based optimization
- Simple rules work well for common cases
- 1000× faster planning may be worth 10% suboptimal execution

**2. Symbolic Structure Carries Information**
- Bound vs unbound predicts selectivity
- Constants are almost always selective
- Entity joins are usually efficient

**3. Phase Boundaries Enforce Correctness**
- Explicit phases prevent dependency violations
- Symbol tracking catches missing bindings at plan time
- Progressive execution enables early termination

**4. Greedy Algorithms Can Be Good Enough**
- Greedy phase creation works for most queries
- Local optimality often sufficient
- Simplicity enables fast planning

### For AI Assistance

**1. Symbolic Manipulation is Underrated**
- Not as flashy as ML-based optimizers
- But reliable, debuggable, and fast
- Pattern matching on AST structure is powerful

**2. Design Principles Matter**
- Statistics-free is a deliberate choice
- Entity grouping is a researched heuristic
- Fine-grained phases are a safety mechanism

**3. Understand the Whole System**
- Planner decisions affect executor performance
- Index structure constrains planner choices
- Symbol flow connects all components

---

## 11. Conclusion

The planner's symbolic manipulation is a masterclass in **reasoning about programs without executing them**. By tracking symbol dependencies, scoring patterns by structure, and grouping by entity, it creates efficient execution plans using only abstract analysis.

**Key Strengths:**
- **Fast:** 15µs planning (vs PostgreSQL's 15ms = 1000× faster)
- **Simple:** No statistics collection or maintenance
- **Correct:** Symbol tracking ensures all variables are bound
- **Practical:** Entity grouping works well for pattern-based queries

**The "Symbolic Manipulation" Here Is:**
1. **Dependency analysis:** Which patterns can execute when
2. **Selectivity estimation:** Which patterns are most selective
3. **Index selection:** Which index best matches the pattern structure
4. **Phase creation:** How to group patterns by symbol dependencies
5. **Symbol flow tracking:** What's available, provides, and kept

This is **abstract interpretation of query structure** - deriving execution plan from symbolic properties alone, without touching data. It's elegant, fast, and effective.

---

## 12. References

### Code Locations

**Core Planner:**
- `datalog/planner/planner.go` - Entry point, Plan() method
- `datalog/planner/types.go` - Phase, PatternPlan, BoundMask types
- `datalog/planner/planner_phases.go` - Phase creation logic
- `datalog/planner/planner_patterns.go` - Pattern scoring and index selection
- `datalog/planner/planner_utils.go` - Symbol manipulation utilities

**Pattern Grouping:**
- `planner_patterns.go:26-95` - Group by entity algorithm
- `planner_patterns.go:98-173` - Group ordering algorithm

**Pattern Scoring:**
- `planner_patterns.go:288-371` - Selectivity estimation

**Index Selection:**
- `planner_patterns.go:413-432` - Index selection from BoundMask

**Phase Creation:**
- `planner_phases.go:11-167` - Default phase creation
- `planner_phases.go:169-400` - Fine-grained phase creation

### Related Documents
- `RELATIONAL_ALGEBRA_REVIEW_2025_01.md` - Executor review
- `PREDICATE_PUSHDOWN_SCAN_BOUNDS_2025_01.md` - Predicate pushdown review
- `docs/papers/PAPER_PROPOSAL_1_GREEDY_JOINS.md` - Research thesis
- `docs/papers/STATISTICS_UNNECESSARY_PAPER_OUTLINE.md` - Statistics-free argument
- `docs/wip/PHASE_AS_QUERY_ARCHITECTURE.md` - Future planner improvements (Stage C)

### External References
- **Datomic Query Engine:** Rule-based symbolic planner inspiration
- **Selinger et al. (1979):** System R cost-based optimizer (what we're NOT doing)
- **Abstract Interpretation:** Static analysis via symbolic execution
- **Datalog Evaluation:** Bottom-up vs top-down, magic sets

---

## Appendix: Concrete Planning Example

### Query
```datalog
[:find ?year ?month (min ?low) (max ?high)
 :in $ ?ticker
 :where [?s :symbol/ticker ?ticker]
        [?e :price/symbol ?s]
        [?e :price/time ?t]
        [(year ?t) ?year]
        [(month ?t) ?month]
        [?e :price/low ?low]
        [?e :price/high ?high]]
```

### Symbolic Analysis

**Input Symbols:** `{?ticker: true}`

**Pattern Analysis:**
```
P1: [?s :symbol/ticker ?ticker]
    Vars: {?s, ?ticker}
    Bound: {?ticker}
    Score: −500 (const attr + bound value)
    Index: AVET
    Provides: {?s}

P2: [?e :price/symbol ?s]
    Vars: {?e, ?s}
    Bound: {} (when P1 not executed yet)
    Score: +110
    After P1: Bound: {?s}, Score: −500, Index: AVET
    Provides: {?e}

P3: [?e :price/time ?t]
    Vars: {?e, ?t}
    Bound: {} (when P1-P2 not executed)
    After P2: Bound: {?e}, Score: +10, Index: EAVT
    Provides: {?t}

P4: [?e :price/low ?low]
    Vars: {?e, ?low}
    After P2: Bound: {?e}, Score: +510, Index: EAVT
    Provides: {?low}

P5: [?e :price/high ?high]
    Vars: {?e, ?high}
    After P2: Bound: {?e}, Score: +510, Index: EAVT
    Provides: {?high}
```

**Expression Analysis:**
```
E1: [(year ?t) ?year]
    Requires: {?t}
    Provides: {?year}
    Can execute: After P3

E2: [(month ?t) ?month]
    Requires: {?t}
    Provides: {?month}
    Can execute: After P3
```

### Phase Creation

**Entity Grouping:**
```
Group 1: entity ?s
  - P1: [?s :symbol/ticker ?ticker]

Group 2: entity ?e
  - P2: [?e :price/symbol ?s]
  - P3: [?e :price/time ?t]
  - P4: [?e :price/low ?low]
  - P5: [?e :price/high ?high]
```

**Group Ordering:**
```
Group 1 connectivity:
  - Has bound value (?ticker is input)
  - Score: −500 (most selective)
  - Choose first

Group 2 connectivity:
  - Needs ?s from Group 1
  - Score: +10 (after ?s is bound)
  - Choose second
```

**Final Plan:**

**Phase 1:**
```
Available: [?ticker]
Patterns:
  - [?s :symbol/ticker ?ticker] (AVET index)
Provides: [?s]
Keep: [?s]
```

**Phase 2:**
```
Available: [?ticker, ?s]
Patterns:
  - [?e :price/symbol ?s] (AVET index)
  - [?e :price/time ?t] (EAVT index)
  - [?e :price/low ?low] (EAVT index)
  - [?e :price/high ?high] (EAVT index)
Expressions:
  - [(year ?t) ?year]
  - [(month ?t) ?month]
Provides: [?e, ?t, ?low, ?high, ?year, ?month]
Keep: [?year, ?month, ?low, ?high]  // For aggregation
```

**Aggregation:** (final step, not a phase)
```
Group by: [?year, ?month]
Aggregates:
  - (min ?low)
  - (max ?high)
```

### Why This Plan is Good

1. **Phase 1 is highly selective:** AVET index on `:symbol/ticker` with bound ?ticker → ~1 result
2. **Phase 2 uses bound entity:** AVET on `:price/symbol` with ?s → ~400 results
3. **Entity lookups in Phase 2:** EAVT index for bound entity ?e → very fast
4. **Expressions evaluated immediately:** As soon as ?t is available
5. **Minimal Keep set:** Only keeps symbols needed for aggregation

**This is what 15µs of symbolic manipulation buys you.**
