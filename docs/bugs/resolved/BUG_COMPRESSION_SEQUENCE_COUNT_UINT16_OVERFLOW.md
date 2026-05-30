# BUG: Compression Header Sequence Counts Overflow uint16 (Large Value Data Loss)

**Date**: 2026-05-30
**Severity**: Correctness / Data Loss (High)
**Status**: RESOLVED (2026-05-30) — header counts widened to `uint32` (`CompressionVersion` 0x01 → 0x02), `Decompress` dual-reads both formats, and a belt-and-suspenders `Compress` guard declines (stores raw) above the `uint32` ceiling. See [Resolution](#resolution).
**Affected**: `datalog/codec/compress.go` (LZJ codec), all string/`[]byte` values large and compressible enough to exceed 65,535 LZ77 sequences; compression is on by default (threshold 512 bytes), so this is reachable without opt-in.

## Summary

The LZJ compression header stores the LZ77 sequence count and match count as
`uint16`. For a sufficiently large, compressible, *structured* value, the number
of sequences exceeds 65,535 and the count silently truncates on write.
`Compress` still succeeds and produces a smaller blob, so the transaction commits
cleanly and the value is persisted. The corruption is latent: the value can only
be recovered by `Decompress`, which detects the stream-length mismatch and
returns an error. The value becomes permanently unreadable.

This is a write-accept / read-fail data-loss bug. There is no guard in `Compress`
against the count exceeding `uint16`.

## Root Cause

`Compress` writes the sequence and match counts as `uint16`
(`datalog/codec/compress.go:64-70`):

```go
// Header: [version:1][originalLen:4 BE][numSequences:2][numMatches:2][numLiterals:4]
result = append(result, CompressionVersion)
result = binary.BigEndian.AppendUint32(result, uint32(len(input)))
result = binary.BigEndian.AppendUint16(result, uint16(len(sb.Sequences))) // <-- truncates above 65535
result = binary.BigEndian.AppendUint16(result, uint16(numMatches))         // <-- truncates above 65535
result = binary.BigEndian.AppendUint32(result, uint32(len(sb.Literals)))
```

`Decompress` reads those fields back and validates the per-stream code counts
against them (`compress.go:99-145`):

```go
numSequences := int(binary.BigEndian.Uint16(compressed[5:7]))
numMatches   := int(binary.BigEndian.Uint16(compressed[7:9]))
...
litLenCodes, n, err := readDecompressBlock(compressed, pos, numSequences)
...
if len(litLenCodes) != numSequences {
    return nil, fmt.Errorf("compress: litLen count %d != numSequences %d", ...)
}
```

The FSE blocks themselves store their decompressed length as `uint32`
(`compressOrRaw`, `compress.go:228`), so on decode the block yields the *true*
count (e.g. 65,661) while the header claims the *truncated* count (e.g. 125 =
65,661 mod 65,536). `readDecompressBlock`'s `len(decompressed) != expectedLen`
check fires and `Decompress` returns an error. The value is unrecoverable.

Note that `originalLen` and `numLiterals` are already `uint32`; only these two
sequence-count fields are `uint16`.

## Why It Is Reachable (and why most content is safe)

The sequence count is *not* proportional to value size. A `Sequence` is emitted
only when the matcher finds a back-reference (`lz77.go:90-112`); runs of
non-matching bytes accumulate as literals without adding sequences. So
`numSequences ≈ numMatches + 1`, and the real question is how many *distinct,
short, non-extending* matches the LZ77 pass produces.

Two properties of the matcher keep that number small for most content:

1. **It hashes 4 bytes** (`hash4`, `lz77.go:40`). A match is only found when a
   4-byte window recurs, so 3-byte-only repeats with a varying 4th byte are never
   found — they become literals (poor compression, then the
   `len(result) >= len(input)` safety net at `compress.go:80` stores the value
   raw, which is safe).
2. **It extends matches greedily up to 258 bytes** with a 1 MB window
   (`lz77MaxMatch = 258`, `lz77WindowSize = 1 << 20`). Any periodic or run-like
   content collapses into a few long matches.

Measured sequence counts for a 1 MB value (via `FindMatches`):

| content                         | sequences @ 1 MB | bytes / sequence |
|---------------------------------|------------------|------------------|
| random (incompressible)         | ~100 (stored raw — safe) | — |
| run of one byte (`aaaa…`)       | ~4,065           | ~258 |
| repeated English prose          | ~4,066           | ~258 |
| JSON array of small objects     | ~38,900          | ~27  |

So runs, logs, prose, and natural text stay far under the limit at many MB. The
count is high only for **densely structured records** — many small distinct
fields, e.g. a JSON/CSV array of little objects.

## Measured Thresholds

Sequence count crosses 65,535 (and the round trip breaks) at:

| content                                   | last good | first broken |
|-------------------------------------------|-----------|--------------|
| JSON-ish records (realistic worst case)   | ~1.66 MB  | ~1.73 MB     |
| adversarial 4-byte tokens + random separator | ~320 KB | ~384 KB    |

- **Realistic danger zone:** a multi-megabyte (> ~1.5 MB) compressible blob of
  many small records stored as a single attribute value.
- **Absolute floor:** each sequence needs `litLen + matchLen ≥ 3` bytes, so fewer
  than ~190 KB cannot physically produce 65,536 sequences. **Any value under
  ~190 KB is immune regardless of content.**

Observed failure on the JSON case at ~1.73 MB:

```
compress: litLen codes: decompressed length 65661 != expected 125
```

## Expected Behavior

A value that compresses must always decompress to the original bytes, or
compression must decline (store raw) so the value is never lost. Value size
must not silently cap correctness.

## Actual Behavior

- `Compress` truncates the count and returns a smaller blob.
- The write/transaction succeeds; the value is persisted.
- A later read routes through `Decompress`, which errors on the count mismatch.
- The value is permanently unreadable. If the value is never read back, the loss
  is silent indefinitely.

## Why This Is Subtle

- Sequence count is decoupled from value size: prose/logs/runs of *many* MB are
  fine, so size-based testing with ordinary content never trips it.
- The failure requires the specific combination of *large* + *compressible* +
  *densely structured*; each alone is safe.
- The safety net (`len(result) >= len(input)`) makes incompressible data safe,
  which hides the issue for the obvious "big blob" test (random bytes).
- It is write-accept / read-fail, so it does not surface at the point of the
  defect — it surfaces later, possibly in a different session, as a decode error.
- `Decompress` *does* detect the mismatch and error (no wrong bytes are
  returned), so it reads like a transient/corruption error rather than a codec
  capacity limit.

## Reproduction Sketch

```go
// Densest realistic content: a JSON-ish array of small uniform records.
func jsonish(n int) []byte {
    var b bytes.Buffer
    b.WriteByte('[')
    for i := 0; b.Len() < n; i++ {
        fmt.Fprintf(&b, `{"id":%d,"name":"alice","active":true,"score":%d},`,
            100000+i, i%97)
    }
    return b.Bytes()[:n]
}

input := jsonish(1800 * 1024) // ~1.8 MB
comp := codec.Compress(input)
if comp == nil {
    t.Fatal("expected compression to apply")
}
out, err := codec.Decompress(comp)
if err != nil || !bytes.Equal(out, input) {
    // BUG: round trip fails — value is unrecoverable
    t.Fatalf("round trip failed: err=%v", err)
}
```

A deliberately adversarial generator (a pool of recurring 4-byte tokens, each
followed by a random separator byte so matches hit the 4-byte hash but cannot
extend and the stream stays non-periodic) reproduces the failure at ~330–380 KB.

## Fix Direction

Two independent decisions:

1. **Correctness guard (do this regardless, needs no format change):** in
   `Compress`, if `len(sb.Sequences) > 0xFFFF || numMatches > 0xFFFF`, return
   `nil` so the value is stored raw instead of written and later unreadable. The
   caller already treats `nil` as "store uncompressed"
   (`datalog/value_encoding.go:242-248`). This is the minimal fix and removes the
   data-loss path immediately. Trade-off: the rare giant structured value is
   stored uncompressed.

2. **Widen the header (optional, to keep compressing huge structured blobs):**
   change both fields to `uint32` and bump `CompressionVersion` (0x01 → 0x02).
   The +4 bytes per value is negligible against the format's existing ~58 bytes
   of fixed header/block overhead plus FSE tables, and `uint32` reuses the same
   `binary.BigEndian.AppendUint32`/`Uint32` already used for the other four count
   fields. 24-bit would save 2 bytes but needs hand-rolled encoding (no stdlib
   `Uint24`) and still leaves a ceiling — not worth it.

   Because the format is "frozen per version" and `Decompress` rejects unknown
   versions (`compress.go:95`), preserving already-written v1 blobs requires
   `Decompress` to branch on version and read 2-byte fields for v1 / 4-byte
   fields for v2. If there is no persisted v1 data to preserve, skip the dual
   path and just bump.

The cleanest combination is both: widen to `uint32` *and* keep the `return nil`
guard as a belt-and-suspenders invariant (it never fires at 4 B sequences, but
documents the limit and guards against regression). The guard-only option is
strictly cheaper if storing rare giant blobs uncompressed is acceptable.

## Verification Plan

Add regression tests that fail before the fix:

- `TestCompress_JSONLikeValueAboveUint16Sequences_RoundTrips` — ~1.8 MB JSON-ish
  value compresses and decompresses to identical bytes.
- `TestCompress_AdversarialDenseSequences_RoundTrips` — adversarial generator at
  ~512 KB round-trips.
- `TestCompress_SequenceCountOverflow_DoesNotProduceUnreadableBlob` — any input
  that would exceed the count cap either decompresses correctly (widened header)
  or is stored raw (`Compress` returns `nil`); it must never produce a blob that
  `Decompress` rejects.
- End-to-end through the DB: `tx.Set` a ~2 MB structured string value, commit,
  reopen, and read it back equal (exercises the storage compression path and
  Tier 2/Tier 3 routing, not just the codec in isolation).

If the header is widened, also add:

- `TestDecompress_V1BlobStillReadable` — a captured v1 (uint16-header) blob
  decodes correctly under the v2 decoder (back-compat), or an explicit decision
  is recorded that no v1 data needs to be preserved.

## Resolution

Both directions from the fix were taken: the header was widened **and** the guard
kept.

- **Header widened to `uint32`, `CompressionVersion` 0x01 → 0x02.** The two count
  fields are now `binary.BigEndian.AppendUint32` (v2 header is 17 bytes vs v1's
  13). The header layout is otherwise unchanged, and the FSE/LZ blocks are
  byte-identical to v1 for the same input — the version bump only widens the two
  counts.
- **Dual-read in `Decompress`.** A `switch` on the version byte reads v1 (uint16,
  offsets 5:7 / 7:9, `pos` 13) or v2 (uint32, offsets 5:9 / 9:13, `pos` 17). It
  runs once per value, outside the per-byte decode loop, so it costs nothing in
  the hot path. Existing on-disk v1 blobs remain readable; `Compress` only ever
  writes v2. Unknown versions still error.
- **Belt-and-suspenders guard.** `Compress` returns `nil` (→ value stored raw via
  the existing safety-net path) if `len(sb.Sequences)` or `numMatches` exceeds
  `maxSequenceCount` (`0xFFFFFFFF`). Unreachable in practice (>12 GB input), it
  documents the format ceiling and guards against a future header-width
  regression.

Regression tests added:

- `codec`: `TestCompress_JSONLikeValueAboveUint16Sequences_RoundTrips`
  (~2 MB JSON-ish, 77,821 sequences), `TestCompress_AdversarialDenseSequences_RoundTrips`
  (~512 KB adversarial, 104,419 sequences), `TestCompress_SequenceCountOverflow_DoesNotProduceUnreadableBlob`
  (round-trip-or-raw invariant), and `TestDecompress_V1BlobStillReadable`, which
  decodes the original v1 golden blobs (genuine captured legacy bytes, retained as
  `v1Hex` fixtures alongside the re-recorded v2 `expectedHex`).
- `storage`: `TestLargeCompressedValue_RoundTripsThroughStorage` — writes a ~2 MB
  structured value, commits, closes, reopens from disk, and reads it back equal,
  exercising Tier-3 blob routing + decode end-to-end.

Full suite green (`go test -count=1 ./...`).
