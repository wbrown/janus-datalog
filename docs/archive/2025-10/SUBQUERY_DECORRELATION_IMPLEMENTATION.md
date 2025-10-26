# Subquery Decorrelation Implementation Guide

## Executive Summary

Transform the hourly OHLC query from **1,040 sequential subquery executions (72.8s)** into **3 grouped scans + 2 joins (10-15s)** for a **5-7× speedup**.

This implements **Selinger's subquery decorrelation** from "Access Path Selection in a Relational Database Management System" (Selinger et al. 1979).

## Problem Analysis

### Current Query Structure

File: `gopher-street/extract_datalog.go:953-1051`

```datalog
[:find ?datetime ?open-price ?hour-high ?hour-low ?close-price ?total-volume
 :where
    [?s :symbol/ticker "NVDA"]
    [?first-bar :price/symbol ?s]
    [?first-bar :price/time ?t]
    [(year ?t) ?year]
    [(month ?t) ?month]
    [(day ?t) ?day]
    [(hour ?t) ?hour]
    ; ... anchoring logic ...

    ; SubQ1: High/Low for hour (lines 981-995)
    [(q [:find (max ?h) (min ?l)
         :in $ ?sym ?y ?m ?d ?hr
         :where [?b :price/symbol ?sym]
                [?b :price/time ?time]
                [(year ?time) ?py] [(= ?py ?y)]
                [(month ?time) ?pm] [(= ?pm ?m)]
                [(day ?time) ?pd] [(= ?pd ?d)]
                [(hour ?time) ?ph] [(= ?ph ?hr)]
                [?b :price/high ?h]
                [?b :price/low ?l]]
        $ ?s ?year ?month ?day ?hour) [[?hour-high ?hour-low]]]

    ; SubQ2: Open from first 5 minutes (lines 998-1014)
    [(q [:find (min ?o)
         :in $ ?sym ?y ?m ?d ?hr ?smod ?emod
         :where [?b :price/symbol ?sym]
                [?b :price/time ?time]
                [(year ?time) ?py] [(= ?py ?y)]
                [(month ?time) ?pm] [(= ?pm ?m)]
                [(day ?time) ?pd] [(= ?pd ?d)]
                [(hour ?time) ?ph] [(= ?ph ?hr)]
                [?b :price/minute-of-day ?mod]
                [(>= ?mod ?smod)]
                [(<= ?mod ?emod)]
                [?b :price/open ?o]]
        $ ?s ?year ?month ?day ?hour ?hour-start ?open-end) [[?open-price]]]

    ; SubQ3: Close from last 5 minutes (lines 1017-1033)
    [(q [:find (max ?c)
         :in $ ?sym ?y ?m ?d ?hr ?smod ?emod
         :where [?b :price/symbol ?sym]
                [?b :price/time ?time]
                [(year ?time) ?py] [(= ?py ?y)]
                [(month ?time) ?pm] [(= ?pm ?m)]
                [(day ?time) ?pd] [(= ?pd ?d)]
                [(hour ?time) ?ph] [(= ?ph ?hr)]
                [?b :price/minute-of-day ?mod]
                [(>= ?mod ?smod)]
                [(<= ?mod ?emod)]
                [?b :price/close ?c]]
        $ ?s ?year ?month ?day ?hour ?close-start ?close-end) [[?close-price]]]

    ; SubQ4: Volume for hour (lines 1036-1049)
    [(q [:find (sum ?v)
         :in $ ?sym ?y ?m ?d ?hr
         :where [?b :price/symbol ?sym]
                [?b :price/time ?time]
                [(year ?time) ?py] [(= ?py ?y)]
                [(month ?time) ?pm] [(= ?pm ?m)]
                [(day ?time) ?pd] [(= ?pd ?d)]
                [(hour ?time) ?ph] [(= ?ph ?hr)]
                [?b :price/volume ?v]]
        $ ?s ?year ?month ?day ?hour) [[?total-volume]]]]
```

### Performance Analysis

**Correlation Signature**:
- All 4 subqueries use identical inputs: `?year ?month ?day ?hour`
- All scan same base relation: `:price` entities
- All are aggregate queries

**Filter Groups**:
- **Group A**: SubQ1 + SubQ4 (no mod filter - scan all bars in hour)
- **Group B**: SubQ2 (mod ∈ [hour_start, open_end] - first 5 minutes)
- **Group C**: SubQ3 (mod ∈ [close_start, close_end] - last 5 minutes)

**Current Cost**:
- 260 hours × 4 subqueries = **1,040 executions**
- Per execution: 20ms overhead + 2ms work = 22ms
- Total: **72.8 seconds**
- Overhead: 66% of total time

### Selinger's Solution

Transform correlated aggregate subqueries into:
1. Single GROUP BY over all groups
2. Hash join with outer relation

**Decorrelated Queries**:
```datalog
; Query 1: High/Low/Volume for ALL hours (merge SubQ1 + SubQ4)
[:find ?y ?m ?d ?hr (max ?h) (min ?l) (sum ?v)
 :where
    [?b :price/symbol ?sym]
    [?b :price/time ?time]
    [(year ?time) ?y]
    [(month ?time) ?m]
    [(day ?time) ?d]
    [(hour ?time) ?hr]
    [?b :price/high ?h]
    [?b :price/low ?l]
    [?b :price/volume ?v]]

; Query 2: Open from first 5 minutes for ALL hours (SubQ2)
[:find ?y ?m ?d ?hr (min ?o)
 :where
    [?b :price/symbol ?sym]
    [?b :price/time ?time]
    [(year ?time) ?y]
    [(month ?time) ?m]
    [(day ?time) ?d]
    [(hour ?time) ?hr]
    [?b :price/minute-of-day ?mod]
    ; Compute hour start/end for each hour dynamically
    [(* ?hr 60) ?hour-start]
    [(+ ?hour-start 4) ?open-end]
    [(>= ?mod ?hour-start)]
    [(<= ?mod ?open-end)]
    [?b :price/open ?o]]

; Query 3: Close from last 5 minutes for ALL hours (SubQ3)
[:find ?y ?m ?d ?hr (max ?c)
 :where
    [?b :price/symbol ?sym]
    [?b :price/time ?time]
    [(year ?time) ?y]
    [(month ?time) ?m]
    [(day ?time) ?d]
    [(hour ?time) ?hr]
    [?b :price/minute-of-day ?mod]
    ; Compute hour end for each hour dynamically
    [(+ ?hr 1) ?next-hour]
    [(* ?next-hour 60) ?hour-end-mod]
    [(- ?hour-end-mod 5) ?close-start]
    [(- ?hour-end-mod 1) ?close-end]
    [(>= ?mod ?close-start)]
    [(<= ?mod ?close-end)]
    [?b :price/close ?c]]

; Join Results
Result = Query1 ⨝(y,m,d,hr) Query2 ⨝(y,m,d,hr) Query3
```

**Expected Cost**:
- 3 scans + 2 hash joins = **10-15 seconds**
- Speedup: **5-7×**

## Architecture

### New Files

1. **`datalog/planner/decorrelation.go`** (~400 lines)
   - Correlation signature extraction
   - Filter group detection
   - Decorrelation opportunity detection
   - Merged query plan construction

2. **`datalog/executor/subquery_decorrelation.go`** (~200 lines)
   - Decorrelated execution logic
   - Multi-result join logic
   - Result distribution to original bindings

3. **`datalog/planner/decorrelation_test.go`** (~300 lines)
   - Unit tests for detection logic
   - Test correlation signature extraction
   - Test filter grouping

4. **`datalog/executor/subquery_decorrelation_test.go`** (~300 lines)
   - Integration tests for hourly OHLC
   - Correctness tests (compare with sequential)
   - Performance benchmarks

### Modified Files

1. **`datalog/planner/types.go`**
   - Add `DecorrelatedSubqueryPlan` struct
   - Add `Decorrelated bool` flag to `SubqueryPlan`
   - Add `DecorrelatedSubqueries []DecorrelatedSubqueryPlan` to `Phase`

2. **`datalog/planner/planner.go`**
   - Integrate decorrelation detection into planning
   - Call detection after subquery assignment

3. **`datalog/executor/executor.go`**
   - Integrate decorrelated execution into phase execution
   - Execute decorrelated subqueries before sequential ones

## Data Structures

### CorrelationSignature

```go
// CorrelationSignature identifies subqueries that can be decorrelated together
type CorrelationSignature struct {
    BasePatterns    []PatternFingerprint // Simplified pattern structure
    CorrelationVars []query.Symbol       // Input variables from :in clause
    IsAggregate     bool                 // Must have aggregation functions
}

// PatternFingerprint is a simplified representation of patterns for matching
type PatternFingerprint struct {
    Attributes []string       // Attributes accessed (e.g., ":price/high")
    Bound      []query.Symbol // Which variables are bound
}

func (cs CorrelationSignature) Hash() string {
    // Create unique hash for matching
    return fmt.Sprintf("%v|%v|%v", cs.BasePatterns, cs.CorrelationVars, cs.IsAggregate)
}
```

**Purpose**: Identify groups of subqueries with identical correlation structure.

**Example** (from hourly OHLC):
```go
sig := CorrelationSignature{
    BasePatterns: []PatternFingerprint{
        {Attributes: []string{":price/symbol", ":price/time", ":price/high", ":price/low"}},
    },
    CorrelationVars: []query.Symbol{"?year", "?month", "?day", "?hour"},
    IsAggregate: true,
}
// All 4 subqueries match this signature
```

### FilterGroup

```go
// FilterGroup represents subqueries with the same filters
type FilterGroup struct {
    CommonPatterns   []query.Pattern    // Patterns shared by all subqueries
    FilterPredicates []query.Predicate  // Distinguishing filter predicates
    Subqueries       []int              // Indices of subqueries in this group
    AggFunctions     map[int][]string   // SubqIdx -> aggregate functions
}

func (fg FilterGroup) Hash() string {
    // Create unique hash based on predicates
    var predStrs []string
    for _, pred := range fg.FilterPredicates {
        predStrs = append(predStrs, pred.String())
    }
    sort.Strings(predStrs)
    return strings.Join(predStrs, "|")
}
```

**Purpose**: Group subqueries by filter patterns for merging.

**Example** (from hourly OHLC):
```go
groups := []FilterGroup{
    {
        // Group A: No mod filter
        CommonPatterns: [patterns for :price/high, :price/low, :price/volume],
        FilterPredicates: nil, // No additional filters
        Subqueries: []int{0, 3}, // SubQ1, SubQ4
        AggFunctions: map[int][]string{
            0: []string{"max ?h", "min ?l"},
            3: []string{"sum ?v"},
        },
    },
    {
        // Group B: First 5 minutes
        CommonPatterns: [patterns for :price/open],
        FilterPredicates: [(>= ?mod ?hour-start), (<= ?mod ?open-end)],
        Subqueries: []int{1}, // SubQ2
        AggFunctions: map[int][]string{
            1: []string{"min ?o"},
        },
    },
    {
        // Group C: Last 5 minutes
        CommonPatterns: [patterns for :price/close],
        FilterPredicates: [(>= ?mod ?close-start), (<= ?mod ?close-end)],
        Subqueries: []int{2}, // SubQ3
        AggFunctions: map[int][]string{
            2: []string{"max ?c"},
        },
    },
}
```

### DecorrelatedSubqueryPlan

```go
// DecorrelatedSubqueryPlan represents a group of subqueries optimized together
type DecorrelatedSubqueryPlan struct {
    OriginalSubqueries []int              // Indices in Phase.Subqueries
    FilterGroups       []FilterGroup      // Groups of subqueries by filter
    MergedPlans        []*QueryPlan       // One plan per filter group
    CorrelationKeys    []query.Symbol     // Keys to join on (e.g., ?year, ?month, ?day, ?hour)
    ColumnMapping      map[int]ResultMap  // Original subquery -> result columns
}

// ResultMap maps original subquery to columns in merged result
type ResultMap struct {
    FilterGroupIdx int   // Which merged query produced this result
    ColumnIndices  []int // Which columns in that result
}
```

**Purpose**: Store decorrelation plan for execution.

**Example** (from hourly OHLC):
```go
plan := DecorrelatedSubqueryPlan{
    OriginalSubqueries: []int{0, 1, 2, 3},
    FilterGroups: [3 groups as above],
    MergedPlans: [3 query plans],
    CorrelationKeys: []query.Symbol{"?year", "?month", "?day", "?hour"},
    ColumnMapping: map[int]ResultMap{
        0: {FilterGroupIdx: 0, ColumnIndices: []int{4, 5}},    // SubQ1 -> columns 4,5 of Group A (high, low)
        1: {FilterGroupIdx: 1, ColumnIndices: []int{4}},       // SubQ2 -> column 4 of Group B (open)
        2: {FilterGroupIdx: 2, ColumnIndices: []int{4}},       // SubQ3 -> column 4 of Group C (close)
        3: {FilterGroupIdx: 0, ColumnIndices: []int{6}},       // SubQ4 -> column 6 of Group A (volume)
    },
}
```

## Implementation Phases

### Phase 1: Detection Infrastructure

**File**: `datalog/planner/decorrelation.go`

#### 1.1 Pattern Fingerprinting

```go
// extractPatternFingerprint creates a simplified representation for matching
func extractPatternFingerprint(pattern query.Pattern) PatternFingerprint {
    dp, ok := pattern.(*query.DataPattern)
    if !ok {
        return PatternFingerprint{}
    }

    var attributes []string
    var bound []query.Symbol

    // Extract attribute (Element[1])
    if len(dp.Elements) > 1 {
        if c, ok := dp.Elements[1].(query.Constant); ok {
            attributes = append(attributes, fmt.Sprintf("%v", c.Value))
        }
    }

    // Extract bound variables
    for i, elem := range dp.Elements {
        if v, ok := elem.(query.Variable); ok {
            bound = append(bound, query.Symbol(v.Name))
        }
    }

    return PatternFingerprint{
        Attributes: attributes,
        Bound:      bound,
    }
}
```

#### 1.2 Correlation Signature Extraction

```go
// extractCorrelationSignature analyzes a subquery to create its signature
func extractCorrelationSignature(subqPlan *SubqueryPlan) CorrelationSignature {
    var basePatterns []PatternFingerprint

    // Analyze patterns in nested query
    for _, phase := range subqPlan.NestedPlan.Phases {
        for _, patPlan := range phase.Patterns {
            fp := extractPatternFingerprint(patPlan.Pattern)
            basePatterns = append(basePatterns, fp)
        }
    }

    // Check if query is aggregate
    isAggregate := false
    for _, findElem := range subqPlan.NestedPlan.Query.Find {
        if _, ok := findElem.(query.AggregateExpression); ok {
            isAggregate = true
            break
        }
    }

    return CorrelationSignature{
        BasePatterns:    basePatterns,
        CorrelationVars: subqPlan.Inputs,
        IsAggregate:     isAggregate,
    }
}
```

#### 1.3 Filter Group Detection

```go
// groupSubqueriesByFilters groups subqueries with same correlation signature by filters
func groupSubqueriesByFilters(subqueries []*SubqueryPlan, indices []int) []FilterGroup {
    // Map filter hash -> FilterGroup
    groups := make(map[string]*FilterGroup)

    for _, idx := range indices {
        subq := subqueries[idx]

        // Extract filter predicates from subquery
        var filterPreds []query.Predicate
        for _, phase := range subq.NestedPlan.Phases {
            for _, predPlan := range phase.Predicates {
                // Skip correlation predicates (e.g., [(= ?py ?y)])
                if !isCorrelationPredicate(predPlan) {
                    filterPreds = append(filterPreds, predPlan.Predicate)
                }
            }
        }

        // Create filter group
        fg := FilterGroup{
            FilterPredicates: filterPreds,
            Subqueries:       []int{idx},
        }

        hash := fg.Hash()
        if existing, found := groups[hash]; found {
            existing.Subqueries = append(existing.Subqueries, idx)
        } else {
            groups[hash] = &fg
        }
    }

    // Convert map to slice
    var result []FilterGroup
    for _, fg := range groups {
        result = append(result, *fg)
    }

    return result
}

// isCorrelationPredicate checks if predicate is a correlation constraint
// Example: [(= ?py ?y)] where ?y is an input variable
func isCorrelationPredicate(predPlan PredicatePlan) bool {
    // Check if this is an equality predicate between two variables
    if predPlan.Type != PredicateEquality {
        return false
    }

    // Check if both sides are variables (not constants)
    comp, ok := predPlan.Predicate.(query.Comparison)
    if !ok {
        return false
    }

    _, leftIsVar := comp.Left.(query.VariableTerm)
    _, rightIsVar := comp.Right.(query.VariableTerm)

    return leftIsVar && rightIsVar
}
```

#### 1.4 Decorrelation Detection

```go
// detectDecorrelationOpportunities finds groups of subqueries that can be optimized
func detectDecorrelationOpportunities(phase *Phase) []DecorrelationGroup {
    // Map signature hash -> subquery indices
    signatureGroups := make(map[string][]int)

    for i, subqPlan := range phase.Subqueries {
        sig := extractCorrelationSignature(&subqPlan)

        // Only aggregate queries can be decorrelated
        if !sig.IsAggregate {
            continue
        }

        hash := sig.Hash()
        signatureGroups[hash] = append(signatureGroups[hash], i)
    }

    // Find groups with 2+ subqueries (worth decorrelating)
    var opportunities []DecorrelationGroup
    for hash, indices := range signatureGroups {
        if len(indices) >= 2 {
            // Get signature from first subquery
            sig := extractCorrelationSignature(&phase.Subqueries[indices[0]])

            opportunities = append(opportunities, DecorrelationGroup{
                Signature:  sig,
                Subqueries: indices,
            })
        }
    }

    return opportunities
}

// DecorrelationGroup represents a group of subqueries that can be optimized together
type DecorrelationGroup struct {
    Signature  CorrelationSignature
    Subqueries []int // Indices in Phase.Subqueries
}
```

### Phase 2: Query Merging

**File**: `datalog/planner/decorrelation.go` (continued)

#### 2.1 Merged Query Construction

```go
// createDecorrelatedPlan creates an optimized plan for a group of subqueries
func createDecorrelatedPlan(phase *Phase, group DecorrelationGroup) (*DecorrelatedSubqueryPlan, error) {
    subqueries := make([]*SubqueryPlan, len(group.Subqueries))
    for i, idx := range group.Subqueries {
        subqueries[i] = &phase.Subqueries[idx]
    }

    // Group subqueries by filter patterns
    filterGroups := groupSubqueriesByFilters(subqueries, group.Subqueries)

    // Create merged query plan for each filter group
    var mergedPlans []*QueryPlan
    columnMapping := make(map[int]ResultMap)

    for groupIdx, fg := range filterGroups {
        // Merge subqueries in this filter group
        mergedQuery, colMap, err := mergeSubqueriesInGroup(subqueries, fg, group.Signature)
        if err != nil {
            return nil, err
        }

        // Plan the merged query
        planner := NewPlanner(PlannerOptions{})
        mergedPlan, err := planner.Plan(mergedQuery)
        if err != nil {
            return nil, err
        }

        mergedPlans = append(mergedPlans, mergedPlan)

        // Update column mapping
        for subqIdx, cols := range colMap {
            columnMapping[subqIdx] = ResultMap{
                FilterGroupIdx: groupIdx,
                ColumnIndices:  cols,
            }
        }
    }

    return &DecorrelatedSubqueryPlan{
        OriginalSubqueries: group.Subqueries,
        FilterGroups:       filterGroups,
        MergedPlans:        mergedPlans,
        CorrelationKeys:    group.Signature.CorrelationVars,
        ColumnMapping:      columnMapping,
    }, nil
}
```

#### 2.2 Subquery Merging

```go
// mergeSubqueriesInGroup merges subqueries in a filter group into single query
func mergeSubqueriesInGroup(subqueries []*SubqueryPlan, fg FilterGroup,
                            sig CorrelationSignature) (*query.Query, map[int][]int, error) {

    // Start with first subquery as base
    baseSubq := subqueries[fg.Subqueries[0]]
    baseQuery := baseSubq.NestedPlan.Query

    // Collect all aggregate expressions
    var allAggregates []query.FindElement
    columnMapping := make(map[int][]int)
    nextColIdx := len(sig.CorrelationVars) // After grouping keys

    // Add grouping keys to :find first
    for _, key := range sig.CorrelationVars {
        allAggregates = append(allAggregates, query.Variable{Name: string(key)})
    }

    // Add aggregate expressions from each subquery
    for i, subqIdx := range fg.Subqueries {
        subq := subqueries[subqIdx]

        var colIndices []int
        for _, findElem := range subq.NestedPlan.Query.Find {
            if agg, ok := findElem.(query.AggregateExpression); ok {
                allAggregates = append(allAggregates, agg)
                colIndices = append(colIndices, nextColIdx)
                nextColIdx++
            }
        }

        columnMapping[subqIdx] = colIndices
    }

    // Create merged query
    mergedQuery := &query.Query{
        Find:  allAggregates,
        In:    []query.InputSpec{query.DatabaseInput{}}, // Just database, no inputs
        Where: baseQuery.Where, // Start with base WHERE
    }

    // Merge patterns from all subqueries
    // (In practice, they should be very similar)
    // Keep common patterns, union specific patterns

    return mergedQuery, columnMapping, nil
}
```

#### 2.3 Integration into Planning

**File**: `datalog/planner/planner.go`

```go
// In assignSubqueriesToPhases, after assigning subqueries:
func (p *Planner) assignSubqueriesToPhases(phases []Phase, subqueries []*query.SubqueryPattern) ([]Phase, error) {
    // ... existing assignment logic ...

    // Detect decorrelation opportunities
    for i := range phases {
        if err := p.detectAndPlanDecorrelation(&phases[i]); err != nil {
            return nil, err
        }
    }

    return phases, nil
}

// detectAndPlanDecorrelation detects and plans decorrelated subqueries
func (p *Planner) detectAndPlanDecorrelation(phase *Phase) error {
    opportunities := detectDecorrelationOpportunities(phase)

    for _, opp := range opportunities {
        decorPlan, err := createDecorrelatedPlan(phase, opp)
        if err != nil {
            return err
        }

        phase.DecorrelatedSubqueries = append(phase.DecorrelatedSubqueries, *decorPlan)

        // Mark original subqueries as decorrelated (skip in execution)
        for _, idx := range opp.Subqueries {
            phase.Subqueries[idx].Decorrelated = true
        }
    }

    return nil
}
```

### Phase 3: Execution

**File**: `datalog/executor/subquery_decorrelation.go`

#### 3.1 Decorrelated Execution

```go
package executor

import (
    "fmt"
    "github.com/wbrown/janus-datalog/datalog/planner"
    "github.com/wbrown/janus-datalog/datalog/query"
)

// executeDecorrelatedSubqueries executes a group of decorrelated subqueries
func executeDecorrelatedSubqueries(ctx Context,
                                    exec *Executor,
                                    decorPlan *planner.DecorrelatedSubqueryPlan,
                                    inputRelation Relation) (Relation, error) {

    // Execute each merged query ONCE (computes ALL groups)
    var groupResults []Relation

    for i, mergedPlan := range decorPlan.MergedPlans {
        // Execute with no inputs - it computes all groups via GROUP BY
        result, err := executePhasesWithInputs(ctx, exec, mergedPlan, []Relation{})
        if err != nil {
            return nil, fmt.Errorf("merged query %d failed: %w", i, err)
        }
        groupResults = append(groupResults, result)
    }

    // Join all results on correlation keys
    combinedResult, err := joinDecorrelatedResults(groupResults, decorPlan.CorrelationKeys)
    if err != nil {
        return nil, err
    }

    // Join with input relation (hour anchors from outer query)
    finalResult := hashJoinOnKeys(inputRelation, combinedResult, decorPlan.CorrelationKeys)

    return finalResult, nil
}

// joinDecorrelatedResults joins multiple decorrelated query results
func joinDecorrelatedResults(results []Relation, keys []query.Symbol) (Relation, error) {
    if len(results) == 0 {
        return nil, fmt.Errorf("no results to join")
    }

    if len(results) == 1 {
        return results[0], nil
    }

    // Join first two
    combined := hashJoinOnKeys(results[0], results[1], keys)

    // Join remaining
    for i := 2; i < len(results); i++ {
        combined = hashJoinOnKeys(combined, results[i], keys)
    }

    return combined, nil
}

// hashJoinOnKeys performs hash join on specified key columns
func hashJoinOnKeys(left, right Relation, keys []query.Symbol) Relation {
    // Find key column indices in both relations
    leftIndices := make([]int, len(keys))
    rightIndices := make([]int, len(keys))

    for i, key := range keys {
        leftIndices[i] = ColumnIndex(left, key)
        rightIndices[i] = ColumnIndex(right, key)
    }

    // Build hash table from smaller relation
    var buildRel, probeRel Relation
    var buildIndices, probeIndices []int

    if left.Size() <= right.Size() {
        buildRel = left
        probeRel = right
        buildIndices = leftIndices
        probeIndices = rightIndices
    } else {
        buildRel = right
        probeRel = left
        buildIndices = rightIndices
        probeIndices = leftIndices
    }

    // Build hash table: key -> tuples
    hashTable := make(map[string][]Tuple)

    it := buildRel.Iterator()
    for it.Next() {
        tuple := it.Tuple()
        key := makeJoinKey(tuple, buildIndices)
        hashTable[key] = append(hashTable[key], tuple)
    }
    it.Close()

    // Probe and build result
    var resultTuples []Tuple
    resultColumns := append(left.Columns(), filterColumns(right.Columns(), keys)...)

    it = probeRel.Iterator()
    for it.Next() {
        probeTuple := it.Tuple()
        key := makeJoinKey(probeTuple, probeIndices)

        if buildTuples, found := hashTable[key]; found {
            for _, buildTuple := range buildTuples {
                // Combine tuples
                var combined Tuple
                if buildRel == left {
                    combined = append(buildTuple, filterTupleValues(probeTuple, probeIndices)...)
                } else {
                    combined = append(probeTuple, filterTupleValues(buildTuple, buildIndices)...)
                }
                resultTuples = append(resultTuples, combined)
            }
        }
    }
    it.Close()

    return NewMaterializedRelation(resultColumns, resultTuples)
}

// makeJoinKey creates a string key from tuple values at specified indices
func makeJoinKey(tuple Tuple, indices []int) string {
    var parts []string
    for _, idx := range indices {
        if idx < len(tuple) {
            parts = append(parts, fmt.Sprintf("%v", tuple[idx]))
        }
    }
    return strings.Join(parts, "|")
}

// filterColumns removes key columns from column list
func filterColumns(columns []query.Symbol, keys []query.Symbol) []query.Symbol {
    keySet := make(map[query.Symbol]bool)
    for _, key := range keys {
        keySet[key] = true
    }

    var result []query.Symbol
    for _, col := range columns {
        if !keySet[col] {
            result = append(result, col)
        }
    }
    return result
}

// filterTupleValues removes values at key indices from tuple
func filterTupleValues(tuple Tuple, indices []int) Tuple {
    indexSet := make(map[int]bool)
    for _, idx := range indices {
        indexSet[idx] = true
    }

    var result Tuple
    for i, val := range tuple {
        if !indexSet[i] {
            result = append(result, val)
        }
    }
    return result
}
```

#### 3.2 Integration into Executor

**File**: `datalog/executor/executor.go`

```go
// In executePhaseSequential, after expressions and before subqueries:
func (e *Executor) executePhaseSequential(ctx Context, phase *planner.Phase,
                                          inputRelations []Relation) (Relation, error) {

    // ... pattern matching, expressions, predicates ...

    var currentRel Relation = /* result from patterns/expressions */

    // Execute decorrelated subqueries first (if any)
    if len(phase.DecorrelatedSubqueries) > 0 {
        for _, decorPlan := range phase.DecorrelatedSubqueries {
            result, err := executeDecorrelatedSubqueries(ctx, e, &decorPlan, currentRel)
            if err != nil {
                return nil, fmt.Errorf("decorrelated subquery failed: %w", err)
            }
            currentRel = result
        }
    }

    // Execute remaining non-decorrelated subqueries (if any)
    for _, subqPlan := range phase.Subqueries {
        if subqPlan.Decorrelated {
            continue // Skip - already handled above
        }

        // ... original sequential subquery execution ...
    }

    return currentRel, nil
}
```

### Phase 4: Testing

#### 4.1 Unit Tests

**File**: `datalog/planner/decorrelation_test.go`

```go
package planner

import (
    "testing"
    "github.com/wbrown/janus-datalog/datalog/query"
)

func TestExtractCorrelationSignature(t *testing.T) {
    tests := []struct {
        name     string
        subquery *SubqueryPlan
        want     CorrelationSignature
    }{
        {
            name: "hourly OHLC high/low",
            subquery: /* SubQ1 from hourly OHLC */,
            want: CorrelationSignature{
                BasePatterns: []PatternFingerprint{
                    {Attributes: []string{":price/symbol", ":price/time", ":price/high", ":price/low"}},
                },
                CorrelationVars: []query.Symbol{"?year", "?month", "?day", "?hour"},
                IsAggregate: true,
            },
        },
        // More test cases...
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got := extractCorrelationSignature(tt.subquery)
            // Compare signatures
        })
    }
}

func TestGroupSubqueriesByFilters(t *testing.T) {
    // Test filter grouping logic
}

func TestDetectDecorrelationOpportunities(t *testing.T) {
    // Test opportunity detection
}
```

#### 4.2 Integration Tests

**File**: `datalog/executor/subquery_decorrelation_test.go`

```go
package executor

import (
    "testing"
    "time"
)

func TestHourlyOHLCDecorrelation(t *testing.T) {
    // Set up test database with sample data
    db := setupTestDB(t)
    defer db.Close()

    // Load sample price data (1 day, multiple hours)
    loadSamplePriceData(t, db)

    // Execute hourly OHLC query with decorrelation enabled
    decorrelatedResults, err := db.ExecuteQuery(hourlyOHLCQuery)
    if err != nil {
        t.Fatalf("decorrelated query failed: %v", err)
    }

    // Execute with decorrelation disabled (sequential)
    sequentialResults, err := db.ExecuteQueryWithOptions(hourlyOHLCQuery,
        Options{DisableDecorrelation: true})
    if err != nil {
        t.Fatalf("sequential query failed: %v", err)
    }

    // Compare results (must be identical)
    if !resultsEqual(decorrelatedResults, sequentialResults) {
        t.Errorf("results differ:\nDecorrelated: %v\nSequential: %v",
            decorrelatedResults, sequentialResults)
    }
}

func TestDecorrelationPerformance(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping performance test in short mode")
    }

    db := setupTestDB(t)
    defer db.Close()

    // Load realistic data volume (260 hours, ~16 bars/hour)
    loadRealisticPriceData(t, db)

    // Measure decorrelated execution
    startDecor := time.Now()
    decorResults, err := db.ExecuteQuery(hourlyOHLCQuery)
    decorTime := time.Since(startDecor)
    if err != nil {
        t.Fatalf("decorrelated query failed: %v", err)
    }

    // Measure sequential execution
    startSeq := time.Now()
    seqResults, err := db.ExecuteQueryWithOptions(hourlyOHLCQuery,
        Options{DisableDecorrelation: true})
    seqTime := time.Since(startSeq)
    if err != nil {
        t.Fatalf("sequential query failed: %v", err)
    }

    // Calculate speedup
    speedup := float64(seqTime) / float64(decorTime)

    t.Logf("Sequential: %v", seqTime)
    t.Logf("Decorrelated: %v", decorTime)
    t.Logf("Speedup: %.2fx", speedup)

    // Verify speedup is at least 3x (conservative)
    if speedup < 3.0 {
        t.Errorf("insufficient speedup: %.2fx (expected >= 3x)", speedup)
    }

    // Verify correctness
    if !resultsEqual(decorResults, seqResults) {
        t.Errorf("results differ between decorrelated and sequential execution")
    }
}

func TestNoDecorrelationFallback(t *testing.T) {
    // Test cases where decorrelation doesn't apply
    tests := []struct {
        name  string
        query string
    }{
        {"single subquery", /* query with 1 subquery */},
        {"different correlation keys", /* query with different keys */},
        {"non-aggregate", /* query without aggregates */},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Should execute normally without decorrelation
        })
    }
}
```

## Rollout Strategy

### Feature Flag

**File**: `datalog/planner/types.go`

```go
type PlannerOptions struct {
    // ... existing options ...
    EnableSubqueryDecorrelation bool // Default: false initially
}
```

### Phased Rollout

1. **Phase 1**: Implement with flag disabled
   - Write all code
   - Run all tests with flag enabled
   - Fix any issues

2. **Phase 2**: Enable for testing
   - Enable flag in test environment
   - Run comprehensive test suite
   - Monitor for regressions

3. **Phase 3**: Enable by default
   - After validation, enable by default
   - Add documentation
   - Update performance benchmarks

### Monitoring

Add annotations for decorrelation:

```go
// In decorrelation execution:
ctx.Collector().AddAnnotation(annotations.Annotation{
    Type: "decorrelation",
    Message: fmt.Sprintf("Decorrelated %d subqueries into %d filter groups (%d → %d executions)",
        len(decorPlan.OriginalSubqueries),
        len(decorPlan.FilterGroups),
        originalExecs,
        len(decorPlan.FilterGroups)),
})

for i, filterGroup := range decorPlan.FilterGroups {
    ctx.Collector().AddAnnotation(annotations.Annotation{
        Type: "decorrelation_group",
        Message: fmt.Sprintf("  Group %d: %d subqueries, %d groups, %v",
            i+1,
            len(filterGroup.Subqueries),
            result.Size(),
            duration),
    })
}
```

Expected output:
```
Decorrelated 4 subqueries into 3 filter groups (1,040 → 3 executions)
  Group 1: 2 subqueries, 260 groups, 1.2s
  Group 2: 1 subquery, 260 groups, 0.8s
  Group 3: 1 subquery, 260 groups, 0.7s
Join: 3 results, 0.3s
Total: 3.0s (vs 72.8s sequential, 24× speedup)
```

## Success Criteria

### Correctness
- [ ] Hourly OHLC results match sequential execution exactly
- [ ] All existing tests pass
- [ ] Edge cases handled correctly

### Performance
- [ ] 5-7× speedup on hourly OHLC (72.8s → 10-15s)
- [ ] No regression on non-decorrelatable queries
- [ ] Memory usage reasonable

### Code Quality
- [ ] Clean, modular implementation
- [ ] Comprehensive test coverage (>80%)
- [ ] Well-documented code
- [ ] No compiler warnings

### Generality
- [ ] Works for any query with multiple correlated subqueries
- [ ] Not hardcoded for OHLC queries
- [ ] Graceful degradation when decorrelation not applicable

## Implementation Checklist

### Phase 1: Detection Infrastructure
- [ ] Create `datalog/planner/decorrelation.go`
- [ ] Implement `PatternFingerprint` and `extractPatternFingerprint()`
- [ ] Implement `CorrelationSignature` and `extractCorrelationSignature()`
- [ ] Implement `FilterGroup` and `groupSubqueriesByFilters()`
- [ ] Implement `detectDecorrelationOpportunities()`
- [ ] Add unit tests for detection logic

### Phase 2: Query Merging
- [ ] Implement `DecorrelatedSubqueryPlan` type
- [ ] Implement `mergeSubqueriesInGroup()`
- [ ] Implement `createDecorrelatedPlan()`
- [ ] Add `DecorrelatedSubqueries []DecorrelatedSubqueryPlan` to `Phase` struct
- [ ] Add `Decorrelated bool` flag to `SubqueryPlan`
- [ ] Integrate `detectAndPlanDecorrelation()` into planner
- [ ] Add unit tests for merging logic

### Phase 3: Execution
- [ ] Create `datalog/executor/subquery_decorrelation.go`
- [ ] Implement `executeDecorrelatedSubqueries()`
- [ ] Implement `joinDecorrelatedResults()`
- [ ] Implement `hashJoinOnKeys()`
- [ ] Implement helper functions (makeJoinKey, filterColumns, etc.)
- [ ] Integrate into `executePhaseSequential()` in executor.go
- [ ] Add execution tests

### Phase 4: Testing & Validation
- [ ] Create `datalog/planner/decorrelation_test.go`
- [ ] Create `datalog/executor/subquery_decorrelation_test.go`
- [ ] Write unit tests (detection, grouping, merging)
- [ ] Write integration tests (hourly OHLC correctness)
- [ ] Write performance benchmarks
- [ ] Test edge cases

### Phase 5: Documentation & Rollout
- [ ] Add feature flag `EnableSubqueryDecorrelation`
- [ ] Add annotations for monitoring
- [ ] Update SUBQUERY_DECORRELATION_PLAN.md
- [ ] Add code documentation
- [ ] Validate on real queries
- [ ] Enable by default

## Timeline Estimate

- **Phase 1** (Detection): 2-3 hours
- **Phase 2** (Merging): 2-3 hours
- **Phase 3** (Execution): 2-3 hours
- **Phase 4** (Testing): 2-3 hours
- **Phase 5** (Documentation): 1 hour

**Total**: ~10-13 hours of focused implementation time

## References

- Selinger et al. (1979). "Access Path Selection in a Relational Database Management System"
- Graefe & McKenna (1993). "The Volcano Optimizer Generator: Extensibility and Efficient Search"
- Pirahesh et al. (1992). "Extensible/Rule Based Query Rewrite Optimization in Starburst"

## Notes

- This is a **classic database optimization** applied to Datalog
- The implementation is **general** and will benefit any query with multiple correlated subqueries
- The optimization is **transparent** - no query syntax changes needed
- Performance improvement is **significant** for the target use case (5-7× speedup)
- The code is **modular** and can be easily disabled or extended
