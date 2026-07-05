# Bug: Vector Values Hash Degenerately (Address-Constant Default)

**Status**: FIXED (2026-07-04).
`hashValue` gained a `[]interface{}` content-hash case (order-dependent
FNV over element hashes — vectors are ordered), and the address-based
default was replaced by the loud value-domain panic. On first full-suite
contact the panic exposed a further instance of the same defect: the
storage vector-matching path (`matchVectorWithBindings`) produces **typed
slices** (`[]string`) in relation tuples, which the equality layer already
treats as vectors (`ValuesEqual` compares every slice kind element-wise
via reflection, so `[]string{"a"}` equals `[]interface{}{"a"}`). The hash
layer therefore hashes all slice kinds through the same generic element
iteration, preserving `ValuesEqual(a,b) ⇒ hash(a)==hash(b)` across
representations (`TestTypedSliceValuesHashLikeVectors`). All tests below
are green; the intermittent join-drop reproduction has been stable across
repeated full-package runs since the content hash landed.
**Discovered**: 2026-07-04, while enumerating the value domain for
`BUG_PULL_WITH_ORDER_BY_PANICS.md`'s class fix

## Symptoms

Vector values (`[]interface{}`, bound by cardinality-vector reads like
`[?e :product/tags ?vec]`) all hash to a **single value**: 100 distinct
vectors produce exactly 1 distinct tuple-key hash
(`TestDistinctVectorValuesHashWithSpread`, currently red).

Two consequences, one live and one latent:

1. **Live — performance pathology.** Every `TupleKeyMap` keyed on vector
   values degrades to a linear equality scan: all entries collide, and
   every lookup walks the collision chain calling `datalog.ValuesEqual`
   (recursive slice comparison) per entry. Hash joins and deduplication
   over vector-valued columns are effectively O(n²).
2. **OBSERVED — silent wrong results under stack movement.** Correctness
   survives *only* while equal-and-unequal values alike share the constant
   and the equality fallback resolves them. But the "constant" is the
   address of a local variable (`tuple_key.go`, default case:
   `uintptr(unsafe.Pointer(&v))`) — stable only while the goroutine stack
   state does not differ between hashing sites. Go copies stacks on
   growth, so the constant diverges between a hash join's build and probe
   under real execution conditions and **silently drops matching rows**.
   Initially assessed as latent (solo runs of the reproduction pass —
   identical call chains reuse the same stack slot), this was then
   **observed directly**: `TestEqualVectorValuesJoin` fails
   intermittently (~half of runs) when executed within the full executor
   package suite — the joined result comes back empty for equal vector
   keys — while passing deterministically in isolation. The flakiness is
   not test noise; it is the defect: current releases silently drop
   vector-keyed join matches depending on runtime stack state.
   (`TestEqualVectorValuesDeduplicate` and
   `TestEqualVectorValuesHashConsistently` remain same-call-chain shapes
   that pass by the address-stability accident.)

## Root Cause

`hashValue` (`datalog/executor/tuple_key.go`) enumerates the value domain
but is missing the `[]interface{}` case, so vectors fall to the default:

```go
default:
    // Fallback: use pointer as hash
    return uint64(uintptr(unsafe.Pointer(&v)))
```

This is the same failure mode the `[]byte` and `time.Time` cases in that
switch were individually added to fix — a silent wrong-answer default
instead of a loud one. (Compare `datalog.Type()` in
`datalog/value_encoding.go`, which panics on unknown types — the loud
convention already exists in the value layer.)

## Fix

Part of the value-domain enforcement in the pull-relocation change:

1. `case []interface{}:` — order-**dependent** content hash (FNV over
   element hashes; vectors are ordered, RGA semantics), satisfying the
   invariant `ValuesEqual(a,b) ⇒ hash(a) == hash(b)`.
2. The default case becomes a loud panic naming the type, mirroring
   `datalog.Type()` — the value domain becomes an asserted invariant, so
   the next non-value to reach the hash layer fails at introduction
   instead of silently corrupting joins.

## Test Coverage

`datalog/executor/vector_value_hashing_test.go`:
- `TestDistinctVectorValuesHashWithSpread` — red pre-fix (the degenerate
  hash), pins content hashing with spread.
- `TestEqualVectorValuesDeduplicate` / `TestEqualVectorValuesJoin` /
  `TestEqualVectorValuesHashConsistently` — green pre-fix **by the
  address-stability accident**; pin that the fix preserves correctness
  while replacing its foundation.
