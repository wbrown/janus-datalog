# OR clause drops expression-only branches (fallback short-circuit)

## Trigger

A query uses `(or ...)` to combine a data pattern with an expression branch, expecting **union** semantics (both branches contribute results):

```clojure
[:find ?rname
 :where
 [?self :entity/name "Guard Chamber"]
 [?self :entity/area ?target]
 (or [?related :entity/area ?target]
     [(identity ?target) ?related])
 [?related :entity/name ?rname]]
```

**Expected**: 7 results — 6 sibling rooms sharing `:entity/area` + "Coastal Caves" (the area entity itself, bound via `identity`).

**Actual**: 6 results — only the sibling rooms. The `identity` branch is silently dropped.

Verified individually:
- `[?related :entity/area ?target]` alone → 6 rooms (correct)
- `[(identity ?target) ?related]` alone → "Coastal Caves" (correct)
- `(or ...)` combining them → only the 6 rooms

## Root cause

The OR executor has two modes, selected by a binary check at `query_executor.go:1229`:

```go
if query.OrHasExpressions(clause.Branches) {
    result, err = e.executeOrClauseFallback(ctx, clause, groups)
} else {
    result, err = e.executeOrClauseUnion(ctx, clause, groups)
}
```

`OrHasExpressions()` returns `true` because Branch 1 contains an `Expression` (`IdentityFunction`). This forces **fallback semantics**: for each outer tuple, try branches in order and **short-circuit on the first success**.

### The short-circuit

In `or_fallback_relation.go:629-753`, the fallback iterator loops over branches per outer tuple:

1. Branch 0 (`[?related :entity/area ?target]`) — finds 6 rooms sharing the area → **non-empty result**
2. Short-circuit at line 749: `return true` — Branch 1 is **never evaluated**

Since Branch 0 always succeeds (the area has rooms), Branch 1 (`identity`) is never tried for any outer tuple.

### Union mode wouldn't help either

Even if union mode were forced, it calls branches with **no outer binding** (`query_executor.go:1464`):

```go
branchResult, err := e.executeInnerClauses(ctx, branch, nil)
```

The expression `[(identity ?target) ?related]` requires `?target` from outer context. With `nil` binding, `?target` is unbound. In `executeExpression`, this hits the early-exit at line 621-622:

```go
// Skip expression if no relevant relations and expression needs symbols
return groups, nil
```

The expression branch returns `nil` and is skipped.

## Design gap

The codebase has two OR execution modes:

| Mode | Outer binding? | Evaluates all branches? |
|------|---------------|------------------------|
| **Union** | No (`nil`) | Yes |
| **Fallback** | Yes (per-tuple) | No (short-circuits) |

This query needs a third combination: **correlated union** — outer binding YES + evaluate all branches YES. Neither existing mode supports it.

Fallback semantics were designed for the "try data first, use default if not found" pattern:

```clojure
(or [?scenario :task/count ?count]
    [(ground 0) ?count])
```

The `identity` pattern is fundamentally different — it's not a default, it's an alternative data source whose results should be unioned with the data pattern branch.

## Affected queries

Any `(or ...)` where:
1. One branch is a data pattern that typically succeeds
2. Another branch uses an expression referencing an outer variable
3. The user wants **union** (both branches contribute results), not **fallback** (default value)

The `[(identity ?target) ?related]` pattern — "include the bound entity itself alongside pattern matches" — is the canonical example.

## Key files

| File | Lines | Role |
|------|-------|------|
| `executor/query_executor.go` | 1229 | Binary fallback/union routing decision |
| `executor/query_executor.go` | 1443-1520 | Union mode (no outer binding) |
| `executor/query_executor.go` | 1265-1319 | Fallback mode (short-circuits) |
| `executor/or_fallback_relation.go` | 629-753 | Per-tuple short-circuit loop |
| `query/clause.go` | 123-154 | `BranchHasExpressions()` / `OrHasExpressions()` |

## Fix (implemented)

Split `(or ...)` into two distinct clause types with explicit semantics:

- **`(or ...)`** — always **correlated union** (Datomic semantics). All branches contribute results.
- **`(or-default ...)`** — **fallback** (janus extension). First matching branch wins.

The `OrHasExpressions()` heuristic was deleted. Semantics are now explicit in the clause type.

### Changes

- **New types**: `OrDefaultClause`, `OrDefaultJoinClause` in `query/clause.go`
- **New syntax**: `(or-default ...)`, `(or-default-join ...)` parsed in `parser/parser.go`
- **New IR node**: `LateralUnion` in `algebra/types.go` — the algebraic representation of correlated per-tuple branch evaluation, distinct from independent `Union`
- **Executor**: `OrFallbackRelation` parameterized with `shortCircuit` bool. `(or ...)` uses correlated union when branches have expressions; uncorrelated union for pattern-only. `(or-default ...)` always uses fallback.
- **Algebra compiler**: `compileOr` detects correlated predicates (NOT, missing?) and routes to `LateralUnion`. `compileOrDefault` always uses `LateralUnion`.
- **Query builder**: `OrDefault()`, `OrDefaultJoin()` builders
- **Deleted**: `BranchHasExpressions()`, `OrHasExpressions()` — the root cause heuristic
