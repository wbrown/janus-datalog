# Expunge Bindings and Replace with Relations

## Core Insight

Bindings are a flawed abstraction. They're just Relations with a single row, but by converting them to maps, we lose critical capabilities:
- Tuple relationships are destroyed
- Ordering information is lost  
- We need constant conversion between data structures
- The code is more complex than necessary

**The Solution: Use Relations everywhere. A Binding is just a single-row Relation.**

## Why Bindings Must Go

### Current Problems with Bindings
1. **Type Mismatch**: Bindings are `map[Symbol]interface{}` but patterns need ordered data
2. **Information Loss**: Converting Relations to Bindings destroys tuple relationships
3. **Complexity**: Constant conversion between Relations and Bindings
4. **MultiMatch Bug**: The root cause - can't efficiently iterate through unordered maps

### What Relations Provide
1. **Uniform Data Structure**: Everything is a Relation, from single values to large result sets
2. **Preserved Relationships**: Values from the same tuple stay together
3. **Natural Ordering**: Can be sorted for efficient iteration
4. **Projection**: Can adapt to any pattern's requirements
5. **Set Semantics**: Automatic deduplication, immutable once created
6. **Simple Interface**: No channels, no complex iterators, just []Tuple

## The New Architecture

### Current (Broken) Flow
```
Relation → Extract Bindings → Individual Maps → MatchWithBindings → Merge Results
   ↓                ↓                  ↓                ↓               ↓
[?e ?a ?v]    {?e: 1},{?e: 2}   Process each    Multiple calls    Combine
```

### New (Clean) Flow  
```
Relation → Project → Sort → MatchWithRelation → Results
   ↓          ↓        ↓            ↓              ↓
[?e ?a ?v]  [?e ?v]  Ordered   Single call    Direct results
```

## Implementation Plan

### Phase 1: Extend Interfaces

#### Relation Interface
```go
type Relation interface {
    // ... existing methods ...
    
    // Project creates a new Relation with only the symbols from the pattern
    // that exist in this Relation, in the order they appear in the pattern
    ProjectFromPattern(pattern *query.DataPattern) Relation
    
    // Sorted returns tuples sorted by the relation's symbols
    // First symbol is primary sort key, second is secondary, etc.
    Sorted() []Tuple
    
    // Size returns the number of unique tuples
    Size() int
    
    // Note: Relations are IMMUTABLE and DEDUPLICATED at creation
    // All operations return NEW Relations
}
```

#### PatternMatcher Interface
```go
type PatternMatcher interface {
    Match(pattern *query.DataPattern) ([]datalog.Datom, error)
    
    // REMOVE: MatchWithBindings - no longer needed!
    // MatchWithBindings(pattern *query.DataPattern, bindings Bindings) ([]datalog.Datom, error)
    
    // NEW: All matching goes through Relations
    MatchWithRelation(pattern *query.DataPattern, bindingRel Relation) ([]datalog.Datom, error)
}
```

### Phase 2: Replace Binding Usage

#### Before (with Bindings)
```go
// Creating a binding for a constant
binding := Bindings{"?x": 42}
results := matcher.MatchWithBindings(pattern, binding)

// Processing multiple bindings
for _, entity := range entities {
    binding := Bindings{"?e": entity}
    results = append(results, matcher.MatchWithBindings(pattern, binding)...)
}
```

#### After (with Relations)
```go
// Creating a single-row relation for a constant
rel := NewRelation([]Symbol{"?x"}, [][]interface{}{{42}})
results := matcher.MatchWithRelation(pattern, rel)

// Processing multiple values as a Relation
entities := [][]interface{}{{entity1}, {entity2}, {entity3}}
rel := NewRelation([]Symbol{"?e"}, entities)
projected := rel.ProjectFromPattern(pattern)
results := matcher.MatchWithRelation(pattern, projected)
```

### Phase 3: Update Executor

Remove all Binding-related code:
```go
// REMOVE: This entire pattern
func collectBindings(rel Relation, symbol Symbol) []Bindings {
    var bindings []Bindings
    for _, tuple := range rel.Tuples() {
        bindings = append(bindings, Bindings{symbol: tuple[0]})
    }
    return bindings
}

// REPLACE WITH: Direct relation usage
func (e *Executor) matchPattern(pattern *query.DataPattern, inputRel Relation) (Relation, error) {
    projected := inputRel.ProjectFromPattern(pattern)
    datoms := e.matcher.MatchWithRelation(pattern, projected)
    return e.datomsToRelation(datoms, pattern)
}
```

### Phase 4: Simplify Join Operations

Joins become natural Relation operations:
```go
// Instead of extracting bindings and rejoining
// Just project and match directly
leftProjected := leftRel.ProjectFromPattern(rightPattern)
rightResults := matcher.MatchWithRelation(rightPattern, leftProjected)
joined := leftRel.NaturalJoin(rightResults)
```

## Benefits of Expunging Bindings

### 1. Conceptual Clarity
- One data structure for all data: Relations
- No mental overhead of converting between representations
- Clear data flow from input to output

### 2. Performance
- No overhead of creating map objects
- Natural support for batch operations
- Efficient sorting and iteration

### 3. Correctness
- Tuple relationships always preserved
- MultiMatch optimization works correctly
- No edge cases from binding extraction

### 4. Code Simplification
- Remove entire Bindings type and all conversion code
- Fewer code paths to test and maintain
- More functional, less imperative

## Migration Strategy

### Step 1: Add Relation-based Methods
- Implement ProjectFromPattern
- Implement Sorted (returns []Tuple)
- Add MatchWithRelation to PatternMatcher
- Ensure Relation constructor deduplicates tuples

### Step 2: Parallel Implementation
- Keep MatchWithBindings temporarily
- Implement MatchWithRelation alongside it
- Route new code through Relation path

### Step 3: Migrate Existing Code
- Update executor to use Relations
- Convert binding creation to single-row Relations
- Replace binding extraction with projection

### Step 4: Remove Binding Code
- Delete Bindings type definition
- Remove MatchWithBindings method
- Clean up all binding-related utilities

## Example: Complete Query Flow

```go
// Query: Find all people who like the same things
// [:find ?person1 ?person2 ?thing
//  :where [?person1 :likes ?thing]
//         [?person2 :likes ?thing]
//         [(not= ?person1 ?person2)]]

// Step 1: First pattern matches
rel1 := matcher.MatchWithRelation(pattern1, emptyRelation)
// Returns: [?person1 ?thing]

// Step 2: Project for second pattern  
projected := rel1.ProjectFromPattern(pattern2) // Needs [?thing]
// Returns: [?thing] (projected from rel1)

// Step 3: Second pattern matches with constraint
rel2 := matcher.MatchWithRelation(pattern2, projected)
// Returns: [?person2 ?thing]

// Step 4: Join and filter
result := rel1.NaturalJoin(rel2).Filter(notEqualPredicate)
// Returns: [?person1 ?person2 ?thing]
```

## Success Criteria

1. **No Bindings type** anywhere in the codebase
2. **All tests pass** with the new architecture
3. **MultiMatch optimization works** correctly
4. **Performance improves** due to fewer allocations
5. **Code is simpler** and easier to understand

## Conclusion

By expunging Bindings and using Relations everywhere, we:
- Solve the MultiMatch bug at its root
- Simplify the entire query execution pipeline
- Create a more elegant and performant system
- Align better with relational/Datalog semantics

The key insight: **A Binding is just a single-row Relation. Use Relations everywhere.**