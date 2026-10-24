# Tuple Ground Proposal

## Problem

When using OR clauses with subqueries that return multiple values, the fallback branch requires verbose repetition:

```datalog
(or [(q [:find (count ?t) (sum ?tok) (sum ?dur) (sum ?cc) (sum ?cr)
         :in $ ?s
         :where [?t :task/scenario ?s]
                [?t :task/status :status/complete]
                [(get-else $ ?t :task/token-count 0) ?tok]
                [(get-else $ ?t :task/duration 0) ?dur]
                [(get-else $ ?t :task/cache-creation-tokens 0) ?cc]
                [(get-else $ ?t :task/cache-read-tokens 0) ?cr]]
       $ ?scenario) [[?taskCount ?totalTokens ?totalDuration ?cacheCreation ?cacheRead]]]
    (and [(ground 0) ?taskCount]
         [(ground 0) ?totalTokens]
         [(ground 0) ?totalDuration]
         [(ground 0) ?cacheCreation]
         [(ground 0) ?cacheRead]))
```

The fallback branch requires N separate `ground` expressions for N return values.

## Proposed Solution

Support tuple destructuring with `ground`:

```datalog
[(ground [0 0 0 0 0]) [?taskCount ?totalTokens ?totalDuration ?cacheCreation ?cacheRead]]
```

This binds multiple values from a vector literal to multiple variables in a single expression.

## Syntax

```
[(ground <vector>) <tuple-binding>]
```

Where:
- `<vector>` is an EDN vector literal: `[val1 val2 ... valN]`
- `<tuple-binding>` is a tuple binding form: `[?var1 ?var2 ... ?varN]`
- The vector length must match the number of variables

## Examples

### Basic usage
```datalog
[(ground [0 0]) [?x ?y]]
;; Binds ?x = 0, ?y = 0
```

### Mixed types
```datalog
[(ground ["default" 0 false]) [?name ?count ?active]]
;; Binds ?name = "default", ?count = 0, ?active = false
```

### OR fallback (primary use case)
```datalog
(or [(q [:find ?a ?b ?c :where ...] $) [[?a ?b ?c]]]
    [(ground [0 0 0]) [?a ?b ?c]])
```

### With keywords
```datalog
[(ground [:none 0]) [?status ?count]]
```

## Implementation

1. **Parser**: Detect when `ground` argument is a vector and binding is a tuple
2. **Query types**: Add `TupleGroundFunction` or extend `GroundFunction` to handle vectors
3. **Executor**: When evaluating, zip the vector values with the binding variables

### Parser changes

In `parser/query_parser.go`, when parsing expression clauses:
- If function is `ground` and argument is a vector
- And binding form is a tuple `[?var1 ?var2 ...]`
- Create appropriate function type

### Executor changes

In `executor/query_executor.go`:
- `TupleGroundFunction.Eval()` returns the vector
- Binding application destructures vector into tuple variables

## Alternatives Considered

### 1. Default values in subquery binding
```datalog
[(q [...] $) [[?a ?b ?c] :default [0 0 0]]]
```
More invasive syntax change, unclear semantics.

### 2. or-default clause
```datalog
(or-default [0 0 0] [(q [...] $) [[?a ?b ?c]]])
```
New clause type, more parser work.

### 3. Status quo
Keep using multiple `ground` expressions. Verbose but works.

## Recommendation

Implement tuple ground as proposed. It:
- Follows existing patterns (`ground` already exists)
- Mirrors the tuple binding syntax naturally
- Requires minimal parser changes
- Provides immediate ergonomic benefit
