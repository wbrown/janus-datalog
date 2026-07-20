# Correlated `not-join` Predicate Input Rejected by Algebra Analysis

**Status:** Resolved (2026-07-14) **Reported:** 2026-07-14 **Affected:** v0.14.0–v0.14.2 **Last known working release:** v0.13.3 **Severity:** High — valid Datomic-style queries fail during planning

## Summary

The v0.14 algebra schema-analysis pass rejects an explicit `not-join` when a header variable is supplied by the outer relation and consumed inside the negated body only by a predicate.

```clojure
[:find ?goal
 :where
 [?goal :entity/type :type/goal]
 [?setEvent :event/goal ?goal]
 [?setEvent :event/type ?goalSet]
 (not-join [?goal ?goalSet]
   [?termEvent :event/goal ?goal]
   [?termEvent :event/type ?termType]
   [(!= ?termType ?goalSet)])]
```

`?goalSet` is bound before `not-join`, declared in its header, and used as the right operand of the body predicate. It is a per-outer-tuple correlation input; the body does not and should not produce it as an output relation attribute.

v0.13.3 executed this query. v0.14.x fails during algebra optimization:

```text
refresh algebra schemas: AntiJoin:
anti-join symbol ?goalSet must be produced by both children
```

## Expected Semantics

An explicit `not-join` header defines which outer bindings are available inside the negated subgoal. Each header variable must be bound by the outer relation and must participate in the body either as:

1. A relation attribute produced by the body and compared as an anti-join key; or
2. A free requirement consumed by a predicate or expression in the body.

The body may introduce local variables such as `?termEvent` and `?termType`. Those variables are not header bindings and do not escape the negated subgoal.

## Root Cause

`compileNotJoin` compiles the body independently and emits:

```go
&AntiJoin{
    JoinSymbols:  nj.JoinVars,
    Output:       current.Symbols(),
    ExplicitJoin: true,
}
```

The right child analysis correctly records `?goalSet` as a free requirement: the `Select` predicate consumes it, while the child output remains `[?termEvent ?goal ?termType]`.

`analyzeAntiJoin` then applies the pure equi-anti-join law to every header variable:

```go
for _, symbol := range data.JoinSymbols {
    if !containsSymbol(left.Output, symbol) ||
       !containsSymbol(right.Output, symbol) {
        return error
    }
}
```

That law is correct for right-produced equality keys but incomplete for explicit `not-join` correlation inputs. The algebra node has no field corresponding to `Union.Required`, so the analyzer cannot distinguish the two categories.

The optimizer began exposing the gap when v0.14 added `RefreshSchemas` and `Analyze` after every transform pass. The compiler and executor behavior did not newly become correlated in v0.14; the previously unvalidated intermediate schema became a hard planning error.

## Fix

Extend `AntiJoin` with:

```go
Required []query.Symbol
```

The field means “correlation bindings supplied by the left relation and consumed as free requirements by the right child.”

For explicit `not-join`, compilation derives `Required` strictly:

- The symbol appears in the declared header.
- The left child produces it.
- The right child does not produce it.
- The right analysis lists it as a free requirement.

Validation must reject:

- Header variables not bound by the left relation.
- Header variables neither produced nor required by the right child.
- Right-child outer requirements omitted from the explicit header.
- `Required` entries not present in both the header and right requirements.

This is not a blanket schema exemption. It is a checked correlation contract. The optimizer still lowers back to Datalog, and `decompileAntiJoin` reconstructs the original `NotJoinClause`; the existing executor supplies these bindings per outer tuple.

A new lateral anti-join operator is intentionally not introduced. Direct algebra execution does not exist, and adding a physical operator would duplicate the correlation semantics already represented by `NotJoinClause`.

## Plain `not`

Plain `not` infers its correlation interface instead of declaring a header. Variables produced by both sides are equality keys; right-child free requirements already available on the left are predicate/expression correlation inputs. Local variables produced only inside the body remain local. Compilation must include both inferred categories when it lowers plain `not` to an explicit `NotJoinClause`, while still rejecting right requirements that are not bound by the outer relation.

## Separate `or-join` Migration Note

v0.14 also began rejecting `or-join` headers that declare a variable not bound by every branch:

```clojure
(or-join [?entity ?crawl ?world]
  [?entity :entity/crawl ?crawl]
  [?entity :entity/world ?world])
```

That rejection is correct and Datomic-compatible. The header is an output interface, so every branch must bind every header variable. If `?crawl` and `?world` are branch-specific input filters, the correct header is:

```clojure
(or-join [?entity]
  [?entity :entity/crawl ?crawl]
  [?entity :entity/world ?world])
```

The implementation should retain the validation, improve the diagnostic, and document the v0.13-to-v0.14 migration.

## Regression Tests

- Exact downstream `not-join` reproduction with a predicate-only outer input.
- Optimized/off semantic differential with one excluded and one retained goal.
- Plain `not` inference of a predicate-only outer input.
- Plain `not` rejects a right requirement unavailable from the outer relation.
- Structural assertion for equality keys versus correlation requirements.
- Missing explicit-header correlation input rejected during planning.
- Header variable neither produced nor required by the body rejected.
- Compile/optimize/decompile round trip preserves the complete header.
- `or-join` branch-schema validation remains strict and emits an actionable diagnostic.

## Verification Performed

The following gates were run against the completed fix on 2026-07-14:

```bash
go test -count=1 ./...
go vet ./...
go test -race -count=1 ./datalog/algebra ./datalog/storage
go test -count=1 -shuffle=on ./datalog/algebra ./datalog/storage
```

All completed successfully. The optimized/off execution differential is `TestCorrelatedNotJoinPredicateInputMatchesUnoptimizedExecution`; structural and diagnostic coverage lives in the algebra package tests listed above.

## Resolution Criteria

- [x] The reported query plans and matches v0.13.3 execution semantics.
- [x] Correlation inputs are represented explicitly in the algebra IR.
- [x] Invalid or omitted header bindings fail before execution.
- [x] Pure anti-join schema checks remain unchanged.
- [x] `or-join` branch equality is not weakened.
- [x] Upgrade documentation explains `not-join` correlation requirements and the `or-join` header tightening.
- [x] Semantic, structural, invalid-header, and diagnostic regressions pass.
