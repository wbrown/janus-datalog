# The scan-sharing key was a rendered string, and it was rendered unconditionally

**Status**: Resolved. The vector-literal collision in the same function was
fixed first, during the value-domain enforcement arc
(`BUG_VALUE_DOMAIN_UNENFORCED_IN_COMPARISON`); that collision was an instance
of what this records: injectivity defects are what a rendered key produces,
and the rendering was paid on every unbound match whether or not anything
read it.

## What the fingerprint is for

A scan fingerprint is the identity of an unbound scan. `ScanSharingMatcher.Match`
computes it and calls `registry.GetOrCreate(fp, …)`: on a miss it runs the
real match and stores a `SharedScan` holding a replayable `TupleSeq`; on a hit
it returns that sequence and remaps its symbols to the current pattern's
variable names. Bound scans never reach it — they are specialized to their
binding values.

The identity has two layers: the logical scan, which is the source and each
pattern element by position, with variables canonicalized to position rather
than name, which is what lets `[?t :task/root ?s]` and `[?x :task/root ?y]`
share; and the physical requirements, order-by and limit, because a scan
materialized under `:limit 5` or sorted one way cannot serve a consumer
wanting every tuple or another order.

**Equal fingerprints assert interchangeable results**, and nothing re-checks
the patterns afterward. That is why a collision is a wrong-answer defect
rather than a missed optimization: one pattern is served another pattern's
tuples.

## Defect 1: the key was rendered, from values that already had binary identity

The key's only operation is equality — it is a registry key, never parsed,
ordered, or persisted. It was nonetheless built by formatting: a reflective
`fmt.Fprintf` with four boxed arguments per component, and for each constant
a copy of its value bytes, a `fmt.Sprintf` for the type tag, and a hex
rendering at twice the byte length, joined through a `strings.Builder`.

The values being rendered already carry canonical binary identity: a
`Keyword` is an interned pointer, an `Identity` a 20-byte content hash.
Encoding those as hex text to key a map was render-to-key, comparison by
rendered form, which the value rules name as the tell of the wrong model.

The vector collision was a symptom of this shape: a rendered key inherits
every non-injectivity of its renderer, and `FormatValueEDN` printed `int64(1)`
and `float64(1)` both as `"1"` while `ValuesEqual` holds the two distinct.

## Defect 2: rendered unconditionally for a consumer behind a guard

The string had a second consumer: `scan_sharing_matcher.go` put it in the
`scan-sharing/cache-hit` / `cache-miss` annotation payload, where a reader
sees why two scans did or did not share. That consumer sits behind
`if m.handler != nil`. The fingerprint was computed before that guard, because
the same value was the registry key, so the text was produced on every
unbound match and discarded unread whenever no handler was attached, which is
the production configuration.

## What exists

`ScanFingerprint` (`datalog/executor/scan_fingerprint.go`) is a value built
from typed components:

- the source symbol;
- one element per pattern position, holding its kind and, for a constant,
  the value itself, or for a vector literal, the pattern's own element slice;
- the order-by keys, a pattern variable canonicalized to its position and a
  symbol the pattern does not bind kept as itself;
- the limit.

`FingerprintScan(q, pattern)` builds it with an FNV-1a fold over the
components for bucketing.

`Equal` compares the components one by one, values through
`datalog.ValuesEqual`.

The elements sit in a fixed array sized by `query.PatternPositions`, the one
statement of how many positions a pattern has, so a fingerprint with no
order-by allocates nothing.

`ScanRegistry` buckets entries by the fingerprint's hash and serves an entry
only when its fingerprint equals the one asked for. `Get`, `Put` and
`GetOrCreate` take the fingerprint.

`ScanFingerprint.String()` renders the diagnostic text, every component typed
and length-delimited. `ScanSharingMatcher.Match` calls it only inside the
handler guard, for the annotation payload.

## Measurement

`BenchmarkFingerprintScan` (`scan_fingerprint_test.go`), the unbound scan of
a constant attribute between two variables with no order-by and no limit, on
Apple M5:

| | ns/op | B/op | allocs/op |
| --- | --: | --: | --: |
| rendered key | 540 | 760 | 15 |
| typed fingerprint | 21 | 0 | 0 |

## Tests

- `TestFingerprintScanAllocatesNothingForAnUnboundPattern` pins the zero
  allocations through `testing.AllocsPerRun`.
- `TestScanFingerprint_VectorElementTypes`, `TestScanFingerprint_BoundValue`,
  `TestScanFingerprint_ConstantEntity`, `TestScanFingerprint_DifferentSources`
  and `TestScanFingerprintTypedConstantsAndSentinelsDoNotCollide` pin that
  distinct scans have unequal fingerprints: `[1]` against `[1.0]`, a vector
  against its bare element, a bound against an unbound position, one source
  against another, a variable or blank against a string sentinel, an integer
  against its decimal text, and two constants against the same bytes split at
  another point.
- `TestScanFingerprint_SamePatternDifferentVars` and
  `TestScanQueryFingerprintCanonicalizesRenamedOrderVariables` pin that
  renamed variables, in the pattern and in an order-by key, share.
- `TestScanQueryFingerprintIncludesPhysicalRequirements` pins that an
  order-by or a different limit separates two scans of one pattern.
- `scan_registry_test.go` keys the registry on fingerprints of unbound scans,
  and `public_contract_test.go` pins `(*ScanRegistry).Put` as taking a
  `ScanFingerprint`.
