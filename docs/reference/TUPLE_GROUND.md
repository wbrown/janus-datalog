# Tuple Ground

Bind multiple constant values to multiple variables in a single expression.

## Syntax

```datalog
[(ground [val1 val2 ... valN]) [?var1 ?var2 ... ?varN]]
```

The vector length must match the number of binding variables.

## Examples

### Basic
```datalog
[(ground [0 0 0]) [?a ?b ?c]]
;; Binds ?a=0, ?b=0, ?c=0
```

### Mixed types
```datalog
[(ground ["unknown" 0 false]) [?name ?count ?active]]
```

### With keywords
```datalog
[(ground [:none 0]) [?status ?count]]
```

## Primary Use Case: OR Fallback

When a subquery returns multiple values, provide defaults in the fallback branch:

```datalog
[:find ?scenario ?taskCount ?totalTokens ?totalDuration
 :where [?scenario :scenario/name ?name]
        (or [(q [:find (count ?t) (sum ?tok) (sum ?dur)
                 :in $ ?s
                 :where [?t :task/scenario ?s]
                        [?t :task/tokens ?tok]
                        [?t :task/duration ?dur]]
               $ ?scenario) [[?taskCount ?totalTokens ?totalDuration]]]
            [(ground [0 0 0]) [?taskCount ?totalTokens ?totalDuration]])]
```

Without tuple ground, the fallback requires N separate expressions:
```datalog
;; Verbose alternative
(and [(ground 0) ?taskCount]
     [(ground 0) ?totalTokens]
     [(ground 0) ?totalDuration])
```

## Query Builder (qb)

```go
count, tokens, duration := qb.NewVar("count"), qb.NewVar("tokens"), qb.NewVar("duration")

qb.Or(
    []interface{}{/* subquery branch */},
    []interface{}{
        qb.TupleGround(0, 0, 0).As(count, tokens, duration),
    },
)
```

## Scalar Ground (unchanged)

Single-value ground still works as before:
```datalog
[(ground 0) ?x]
```
