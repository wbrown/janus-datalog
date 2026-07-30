# The scan-sharing key is a rendered string, and it is rendered unconditionally

**Status**: Open, recorded 2026-07-29. Found while fixing the vector-literal
collision in the same function (recorded under item 31's amendment in
`docs/wip/DECISION_LEDGER.md`). That collision is an *instance* of what this
records: injectivity bugs are what a rendered key produces, and the rendering is
paid on every unbound match whether or not anything reads it.

Not a wrong-answer bug on its own. It is the shape that produced one, plus a cost
paid for a consumer that is usually absent.

## What the fingerprint is for

`ScanQueryFingerprint` is the identity of an unbound scan.
`ScanSharingMatcher.Match` computes it and calls
`registry.GetOrCreate(fp, …)`: on a miss it runs the real match and stores a
`SharedScan` holding a replayable `TupleSeq`; on a hit it returns that sequence
and remaps its symbols to the current pattern's variable names. Bound scans never
reach it — they are specialized to their binding values.

Two layers of identity:

- `ScanFingerprint` — the logical scan: source, then each pattern element by
  position, with variables canonicalized to *position* rather than name, which is
  what lets `[?t :task/root ?s]` and `[?x :task/root ?y]` share.
- `ScanQueryFingerprint` — adds the physical requirements, order-by and limit,
  because a scan materialized under `:limit 5` or sorted one way cannot serve a
  consumer wanting all rows or another order.

**Equal fingerprint asserts interchangeable results**, and nothing re-checks the
patterns afterward. That is why a collision is a wrong-answer bug rather than a
missed optimization: one pattern is served another pattern's tuples.

## Defect 1: the key is rendered, from values that already have binary identity

The key's only operation is equality — it is a map key, never parsed, ordered, or
persisted. It is nonetheless built by formatting:

- `writeFingerprintText` is `fmt.Fprintf(b, "%d:%s:%d:%s;", …)` — a reflective
  format parse with four boxed arguments — called once per component.
- each constant additionally pays `ValueBytes` (for a `Keyword`, that is
  `[]byte(val.String())`, a copy of the keyword text), a `fmt.Sprintf` for the
  type tag, and a `hex.EncodeToString` producing text at twice the byte length.
- and `strings.Builder.String()` at the end.

For one unbound `[?e :attr ?v]` with no order-by and no limit that is five
reflective `Fprintf` calls and at least four allocations, per pattern, per query
execution.

The values being rendered already carry canonical binary identity: a `Keyword`
*is* an interned pointer, an `Identity` *is* a 20-byte content hash. Encoding
those as hex text to key a map is render-to-key — comparison by rendered form,
which the value rules name as the tell of the wrong model.

**The vector collision was a symptom of this shape.** A rendered key inherits
every non-injectivity of its renderer: `FormatValueEDN` prints `int64(1)` and
`float64(1)` both as `"1"` because `FormatFloat` drops a zero fraction, while
`ValuesEqual` holds the two distinct — so `[1]` and `[1.0]` were one key. A typed
accumulation cannot have that bug, because it never formats.

## Defect 2: rendered unconditionally for a consumer behind a guard

The string has a second consumer: `scan_sharing_matcher.go` puts it in the
`scan-sharing/cache-hit` / `cache-miss` annotation payload, where a human reads it
to see why two scans did or did not share. That consumer is behind
`if m.handler != nil`.

The fingerprint is computed *before* that guard, because the same value is the map
key. So the text is produced on every unbound match, and discarded unread whenever
no handler is attached — which is the production configuration. The
existence-guard rule (the guard belongs at the call site so it also gates argument
preparation) is satisfied at the emit and defeated upstream.

## The correct fix

The two roles are separable, and separating them is the fix for both defects.

**Equality**: key on typed components, the way `TupleKeyMap` already does — an FNV
accumulation over `(kind tag, value)` for bucketing, plus an exact comparison of
the component list on collision. A bare 64-bit hash is *not* sufficient on its
own: a hash collision there serves another pattern's tuples, which is the same
failure the vector fix closed. Hash-for-bucketing plus exact-compare is the pair
that gives both no allocation and no false sharing.

**Diagnostics**: build the human-readable form only under the handler guard, from
the same typed components.

A byproduct the typed shape gets for free: `hashValue` normalizes `+0.0` and
`-0.0` to one bucket, because Go equality holds them equal and the hash invariant
requires it. The rendered key does not — `orderedFloat64` maps them to different
bytes, so `[?e :a 0.0]` and `[?e :a -0.0]` fingerprint differently today and
decline to share a scan they could share. That direction is safe (fingerprint
inequality only costs sharing, never correctness), which is why it is recorded
here rather than as a defect of its own.

## What was verified

By reading, at the sites named above:

- `ScanSharingMatcher.Match` is the only production caller, and it uses the
  fingerprint as a `GetOrCreate` key with no subsequent pattern comparison.
- `PatternElement` has exactly four implementations — `Variable`, `Blank`,
  `Constant`, `VectorConstant`.
- `parsePatternElement`'s `edn.NodeVector` case builds `VectorConstant`, and
  `parseDataPattern` routes every data-pattern element through it, so a vector
  literal in a pattern reaches the fingerprint.
- `FormatValueEDN` renders `int64(1)` and `float64(1)` identically.
- The fingerprint is computed outside the `handler != nil` guard that gates its
  only text consumer.

## Reproducer

The collision half is fixed and pinned by
`TestScanFingerprint_VectorElementTypes`. This document's own subject — the
rendering shape and its unconditional cost — is a performance and design defect
with no wrong answer attached, so the pin for it is a benchmark rather than a
correctness test: allocations per `ScanQueryFingerprint` call, which the typed
shape should take to zero.
