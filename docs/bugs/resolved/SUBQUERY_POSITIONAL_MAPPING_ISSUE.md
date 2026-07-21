# ⚠️ CRITICAL ISSUE: Subquery Variable Mapping is Positional, Not Named

**Status**: RESOLVED (2026-07-20 staleness triage) — the "long term" proposal below was implemented.

> **Triage (2026-07-20)**: the current mapping is true positional *including* `$` — exactly proposed solutions 1/3. `createInputRelationsFromValuesWithOptions` (`datalog/executor/subquery.go`) counts every `DatabaseInput` as a required call-site argument ("Database REQUIRES explicit $"), validates arity loudly (mismatch → "subquery input arity mismatch"), and errors when a `$` position does not receive a source marker. The `continue`-skips-`$` code this entry quotes no longer exists. Each "BREAKS" example is now either correct or a loud error: reordered `:in ?a $ ?b` demands the marker at position 2; no-database `:in ?x ?y` works (and an omitted `:in` defaults to `[$]`); argument-count mismatch is a loud error. The requested tests exist (`subquery_input_arity_test.go`, `subquery_input_validation_test.go`), and multiple database sources landed with the multi-source arc (`$name`, `SourceRouter`). The original report is retained below.

The current subquery implementation uses **positional mapping** between outer query variables and inner query `:in` clause variables. This is confusing, error-prone, and not what users expect.

## Current Behavior

When you write:
```clojure
[(q [:find (max ?price)
     :in $ ?sym
     :where [?p :price/symbol ?sym]
            [?p :price/value ?price]]
    ?s) [[?max-price]]]
```

You might think: "Pass `?s` as `?sym`"

But what actually happens:
1. The subquery pattern arguments after the query (`?s`) are collected in order
2. The `:in` clause is processed, **skipping** `$` (database) inputs
3. Arguments are mapped by position to non-database `:in` variables

So `?s` (1st argument) maps to `?sym` (1st non-database `:in` variable).

## Why This Is Wrong

### 1. Fragile to Reordering
```clojure
; Original - works:
:in $ ?a ?b
; Arguments: ?x ?y
; Mapping: ?x→?a, ?y→?b

; Reordered - breaks silently:
:in ?a $ ?b  
; Arguments: ?x ?y
; Mapping: ?x→?a, ?y→?b (WRONG! $ is skipped)
```

### 2. Confusing with Multiple Databases
```clojure
:in $1 $2 ?var
; Where does ?var map to? Position 3? Or position 1 after skipping databases?
```

### 3. Not What Users Expect
Users expect named variables to map by name, or at least by absolute position including `$`.

## Code Evidence

From `executor.go:createInputRelations`:
```go
// Create ordered list of values from outer query inputs
orderedValues := make([]interface{}, len(outerInputs))
for i, sym := range outerInputs {
    orderedValues[i] = inputValues[sym]
}

// Process :in clause to create appropriate relations
valueIndex := 0
for _, input := range q.In {
    switch inp := input.(type) {
    case query.DatabaseInput:
        // Skip database input
        continue  // <-- This breaks position mapping!
        
    case query.ScalarInput:
        // Create a single-value relation
        if valueIndex < len(orderedValues) {
            rel := NewMaterializedRelation(
                []query.Symbol{inp.Symbol},
                []Tuple{{orderedValues[valueIndex]}},
            )
            relations = append(relations, rel)
            valueIndex++
        }
```

## Examples of Confusion

### Example 1: Simple Case (Works by Accident)
```clojure
[(q [:find ?result :in $ ?x] ?a) ...]
```
- Arguments: `[?a]`
- `:in` after skipping `$`: `[?x]`
- Mapping: `?a → ?x` ✓

### Example 2: Multiple Arguments
```clojure
[(q [:find ?result :in $ ?x ?y] ?a ?b) ...]
```
- Arguments: `[?a, ?b]`
- `:in` after skipping `$`: `[?x, ?y]`
- Mapping: `?a → ?x`, `?b → ?y` ✓

### Example 3: Reordered :in (BREAKS)
```clojure
[(q [:find ?result :in ?x $ ?y] ?a ?b) ...]
```
- Arguments: `[?a, ?b]`
- What user expects: `?a → ?x`, `?b → ?y`
- What happens: Algorithm skips `$`, maps `?a → ?x`, `?b → ?y` (works by accident!)
- But semantically wrong - position 2 should be database!

### Example 4: No Database (BREAKS)
```clojure
[(q [:find ?result :in ?x ?y] ?a ?b) ...]
```
- Arguments: `[?a, ?b]`
- No `$` to skip
- Currently might fail or behave unexpectedly

## Proposed Solution

### Short Term (Document)
Add clear warnings in documentation that:
1. Subquery arguments map to `:in` variables **by position**
2. Database inputs (`$`) are skipped in position counting
3. Always put `$` first in `:in` clause

### Long Term (Fix)
Change to one of:

1. **Explicit Named Mapping**:
   ```clojure
   [(q [:find ... :in $ ?x ?y] $ ?a ?b) ...]  ; Include $ in args
   ```

2. **Map Syntax**:
   ```clojure
   [(q [:find ... :in $ ?x ?y] {:x ?a, :y ?b}) ...]
   ```

3. **True Positional** (including `$`):
   ```clojure
   [(q [:find ... :in $ ?x ?y] ?a ?b) ...]  ; ?a maps to position 2 (?x)
   ```

## Impact

This affects:
- Every subquery with arguments
- Particularly confusing for new users
- Silent failures when `:in` clause is reordered
- Makes multiple database support nearly impossible

## Testing

Current tests work because they all use the pattern:
- `:in $ ?var ...` (database first)
- Arguments in expected order

We need tests for:
- [ ] Reordered `:in` clauses
- [ ] Multiple database inputs
- [ ] No database input
- [ ] Mismatch between argument count and `:in` variables