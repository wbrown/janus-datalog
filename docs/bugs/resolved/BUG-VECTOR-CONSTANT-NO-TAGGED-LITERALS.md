# Vector constants don't support tagged literals (#inst, #db/id)

## Trigger

A `(ground [...])` vector constant containing a tagged literal like `#inst` fails at parse time:

```
error parsing expression argument 1: unsupported type in vector constant: 12
```

(Type 12 = `edn.NodeTagged`)

## Reproduction

```clojure
(ground [:none #inst "0001-01-01T00:00:00Z"])
```

This is the EDN output of `FormatValueEDN` for a `time.Time` value inside a `VectorConstant`. The formatter correctly emits `#inst "..."`, but the parser can't parse it back — breaking round-trippability.

## Root cause

**File**: `datalog/parser/parser.go` — vector constant parsing (around line 730)

The vector element switch handles `NodeInt`, `NodeFloat`, `NodeString`, `NodeBool`, `NodeKeyword`, `NodeSymbol` but not `NodeTagged`. Tagged literals (`#inst`, `#db/id`, etc.) hit the `default` case and error.

```go
case edn.NodeVector:
    for i, elem := range node.Nodes {
        switch elem.Type {
        case edn.NodeInt: ...
        case edn.NodeFloat: ...
        case edn.NodeString: ...
        case edn.NodeBool: ...
        case edn.NodeKeyword: ...
        case edn.NodeSymbol: ...
        default:
            return nil, fmt.Errorf("unsupported type in vector constant: %v", elem.Type)
        }
    }
```

## Fix

Add a `case edn.NodeTagged:` that delegates to `parseTaggedLiteral(elem)`. The tagged literal parser already handles `#inst` (→ `time.Time`) and `#db/id` (→ `datalog.Identity`). The vector constant parser just needs to call it.

```go
case edn.NodeTagged:
    val, err := parseTaggedLiteral(elem)
    if err != nil {
        return nil, fmt.Errorf("invalid tagged literal in vector: %w", err)
    }
    // parseTaggedLiteral returns a query.Constant — extract the value
    values[i] = val.(query.Constant).Value
```

## Impact

This blocks round-trippability of any query containing `time.Time` or `Identity` values in vector constants (e.g., `TupleGround` with time defaults). `FormatValueEDN` emits valid EDN that the parser rejects.
