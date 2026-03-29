# Proposal: Compressed String Values

## Motivation

Databases with large text values (multi-paragraph prose, source code, structured
output) produce oversized storage files and EDN exports. A database with ~3000
tasks storing text content can reach 200MB in EDN, where the text values account
for a significant portion of the size. Natural language and structured text
compress 3-5x with modern algorithms.

Currently, strings are stored verbatim in badger and serialized verbatim in EDN.
There is no mechanism to reduce the storage footprint of large text values.

Additionally, badger has a **64KB key size limit**. Since janus-datalog stores
the entire datom (E, A, V, Tx, Op) in the key, large strings approaching this
limit fail to store. Downstream applications work around this by splitting
multi-paragraph text into `[]string` (cardinality-vector), which increases
datom count and CRDT metadata overhead. Compression addresses both the storage
size problem and the key size limit.

## Design

### Three-Tier Storage

Values are routed through three tiers based on size, with a single compression
pass determining the tier:

```
write(value) →
  if len(value) < threshold → Tier 1: store raw in key
  else →
    compressed = compress(value)
    if len(compressed) fits in key → Tier 2: store compressed in key
    else → Tier 3: hash(compressed) in key, compressed in badger value
```

**Tier 1: Raw** (below threshold, e.g. < 512 bytes)
- String or bytes stored in key as-is. No overhead. No behavior change.
- The vast majority of values: attribute names, codes, short notes.

**Tier 2: Compressed in key** (above threshold, compressed fits in ~60KB)
- Compressed bytes stored directly in the key across all indexes.
- Stays entirely in the LSM tree — no value log touch.
- This is the common case for large text: 3-5x compression means
  original values up to ~200-300KB fit compressed in the key.

**Tier 3: Content hash + value log** (compressed exceeds key limit)
- Content hash of compressed bytes stored in the key.
- Compressed bytes stored in badger's value field.
- Requires a value log dereference on read — slower than Tier 2.
- Strictly better than the status quo: these values currently cannot
  be stored at all without the `[]string` split workaround.

The tier boundaries are determined by the compressed size — a single
compression pass produces the bytes, and the length determines routing.
No double compression. Tier 2 and Tier 3 use the same compressed `[]byte`;
the only difference is where it's stored.

### Applies to Both Variable-Length Types

The two variable-length value types in janus-datalog are `string` and `[]byte`.
The compression and tiering logic is identical for both — the only difference
is the type byte prefix in the key encoding. One code path, two type tags.

### Transparent Compression at the Codec Layer

The query engine sees normal strings and byte slices — compression is
invisible to queries, predicates, and joins.

```
Write path:  value → compress → route by compressed size → store
Read path:   key type tag → raw | decompress from key | fetch value + decompress
Query path:  all comparisons operate on decompressed values (no change)
```

### Value Types

Two new internal value types in the codec, one per tier:

```go
// Tier 2: compressed bytes stored directly in key
type CompressedValue struct {
    Data           []byte // compressed payload
    OriginalLength int    // for pre-allocating decompression buffer
    IsString       bool   // true = string, false = []byte
}

// Tier 3: content hash in key, compressed bytes in badger value
type HashedValue struct {
    Hash     [32]byte // content hash of compressed bytes
    IsString bool     // true = string, false = []byte
}
```

These types are internal to the storage layer — they never appear in query
results, `:find` bindings, or user-facing APIs. The codec detects the type
tag on read and returns a plain `string` or `[]byte` to the caller.

### Compression Algorithm: Custom, Owned Implementation

**The compression format must be deterministic forever.** Compressed bytes
are embedded in badger keys, which are identity — if the same input ever
produces different compressed bytes, AVET lookups return false negatives
and uniqueness constraints silently break. This is silent data corruption.

For this reason, the compression algorithm must be **owned by this project**,
like L85. Depending on a third-party library (e.g., `klauspost/compress/zstd`)
means a minor version bump could change compression output, breaking key
identity across all indexes.

The implementation targets zstd-like ratios (3-5x on text) using the core
techniques that drive zstd's advantage over DEFLATE, without the format
compatibility overhead:

**Core techniques (from [COMPRESSION_RESEARCH.md](COMPRESSION_RESEARCH.md)):**
- **LZ77 with large window** (1MB+, vs DEFLATE's 32KB) — ~40% of improvement
- **Repeat offset tracking** (3 recent offsets) — ~20% of improvement
- **FSE/tANS entropy coding** for sequences — ~20% of improvement
- **Huffman coding** for literals
- **Separated literal/sequence streams**

**Implementation options** (in order of complexity):

| Tier | Approach | Text ratio | Lines of Go |
|------|----------|-----------|-------------|
| 1 | LZ77 + large window + repeat offsets + Huffman everywhere | 3-4x | ~1000 |
| 2 | Above + FSE for sequence streams, Huffman for literals | 3.5-5x | ~1750 |
| 3 | Above but FSE everywhere (one entropy coder, not two) | 3.5-5x | ~1480 |

**What we skip** from full zstd (RFC 8878):
- Magic numbers, content checksums, dictionary support
- Multiple block types, predefined FSE tables
- Multiple Huffman table transmission formats
- Repeat-table mode, skippable frames, multi-frame support

A custom format needs only: LZ77 matches, entropy coding, repeat offsets,
and a minimal block header. See [COMPRESSION_RESEARCH.md](COMPRESSION_RESEARCH.md)
for detailed analysis of each component.

### Threshold

Only compress values above a minimum size. Short strings have poor compression
ratios and the overhead is not worth it.

Suggested default: **512 bytes**. Configurable via `DatabaseOptions`.

Values below the threshold are stored raw (Tier 1, no behavior change).

### All Indexes Stay Complete

**Compression does not skip or alter any index.** Every datom is indexed in
every applicable index (EAVT, AEVT, AVET, VAET), regardless of tier. This
preserves the fundamental database invariant that indexes are complete.

The key encoder uses the same encode path for both writes and lookups. For
AVET exact-match queries like `[?e :attr "specific-string"]`:

1. The search string is compressed using the same deterministic algorithm
2. The compressed size determines the same tier as the stored value
3. The key encoder produces the same bytes as the original write
4. `EncodePrefixRange` creates the correct scan boundaries

This works because compression is deterministic — same input always produces
same output. The compressed bytes ARE the canonical representation.

**Trade-off: `str/starts-with?` prefix range scans.** The AVET key structure
currently supports latent prefix range scans on raw UTF-8 strings (the
infrastructure exists via `EncodePrefixRange` + `incrementLastByte`, though
`str/starts-with?` is not yet pushed to storage). Compressed bytes do not
preserve UTF-8 sort order, so this optimization is foreclosed for compressed
values. This is acceptable because:
- `str/starts-with?` currently evaluates at the Relation layer (post-scan)
- In typical queries, other patterns narrow the result set before the
  string predicate applies — the filter operates on a small Relation
- The storage savings (3-5x across all indexes) outweigh the foregone
  optimization for one uncommon query pattern

### EDN Serialization

Compressed values serialize with a tagged literal in EDN, using L85
encoding for the compressed bytes (the same encoding used for entity IDs
and transaction IDs throughout the codebase):

```clojure
[#identity "abc123" :task/content #lzj "L85-encoded-compressed-data" [100 1] :op/none]
```

Both tiers serialize the same way in EDN — the compressed bytes with a
`#lzj` tagged literal. The tier distinction (key vs value log) is a
storage detail that doesn't affect the logical datom.

L85 has 25% encoding overhead (vs base64's 33%), is terminal/JSON/URL safe,
and is already implemented in `codec/l85.go`. The encoding overhead is more
than offset by the compression ratio (~70-80% reduction).

**Default export is uncompressed** for human readability and tooling
compatibility. A `--compressed` flag enables compressed tagged literals for
size-sensitive use cases (backup, transfer).

On import, the EDN parser recognizes `#lzj`, L85-decodes the
payload, and routes through the same compress-and-tier logic — no
decompress-then-recompress round-trip for Tier 2 values. Tier 3 values
derive the content hash from the compressed bytes at import time.

### CRDT Compatibility

Compression is applied per-datom, per-value. Each version of a cardinality-one
attribute is independently compressed. CRDT resolution (LWW, add-wins, RGA)
operates on datom identity (E, A, Tx), not on values — compression does not
affect conflict resolution.

For equality comparisons in CRDT set operations (cardinality-many), values
must be decompressed before comparison. This is the same cost as a query
predicate — acceptable since set operations on large text values are rare.

### Interaction with Badger's Table Compression

Badger itself supports optional table-level compression (zstd or snappy) on
SST files. This operates on entire SST blocks, not individual values. The
two layers are complementary:

- **Per-value compression** (this proposal): Reduces the key size in all
  indexes. Effective on individual large strings. The compressed form is
  what badger sees as the "value" portion of the key.
- **Table compression** (badger built-in): Compresses entire SST blocks
  including all keys and metadata. Effective on bulk data patterns.

With both: per-value compression shrinks the keys, then table compression
further compresses the blocks. The double compression is not redundant —
per-value targets individual large strings, table compression targets the
surrounding structure and small values. In practice, enabling per-value
compression may reduce table compression effectiveness slightly (compressed
data doesn't compress well again), but the net effect is still a significant
size reduction because the keys are smaller in ALL indexes, not just the SST
files.

### Schema Integration

Global threshold via `DatabaseOptions`:

```go
db.Open(path, db.WithCompressionThreshold(512))
```

This applies automatically to any `string` or `[]byte` value above the
threshold without schema changes. Per-attribute control is deferred unless
a concrete need arises.

### Migration

Existing databases have uncompressed strings. No migration needed:
- Read path: the type tag in the key distinguishes raw, compressed, and
  hashed values. All three coexist.
- Write path: new writes above the threshold are compressed and tiered.
  Old uncompressed values remain until overwritten.
- Export: uncompressed values export as plain strings. Compressed values
  export with `#lzj` tag (if `--compressed` flag is set) or
  decompressed as plain strings (default).
- Import: both plain and `#lzj` tagged values import correctly.

## Trade-offs

| Aspect | Impact |
|--------|--------|
| Read latency (Tier 2) | +10-50μs per decompressed value |
| Read latency (Tier 3) | +value log dereference + decompression |
| Write latency | +20-100μs per compressed value (single pass) |
| Storage size | 3-5x reduction for large text values (Tier 2) |
| Key size limit | Effectively removed (Tier 3 handles overflow) |
| EDN size | 2-3x reduction with `--compressed` flag |
| Memory | Decompressed values are GC'd normally |
| AVET lookups | Compress search term — determinism guarantees match |
| AVET range scans | Foregone for compressed values (acceptable) |
| Complexity | Three-tier routing, custom compression codec |
| `[]string` workaround | No longer needed for most use cases |

## Determinism: The Load-Bearing Requirement

The entire design depends on compression determinism: the same input must
always produce the same compressed bytes, forever. This is not a nice-to-have —
it's a correctness requirement. Violations cause:

- AVET lookups returning false negatives
- Uniqueness constraints silently breaking
- Duplicate logical datoms with different key bytes

This is why the compression algorithm must be owned by this project (like
L85), not delegated to a third-party library whose output could change
between versions. The format must be frozen: fixed algorithm, fixed
parameters, deterministic by construction.

A comprehensive determinism test suite (known inputs → known outputs)
must be maintained as a hard gate on any changes to the compression code.

## Future Direction: Eliminating the []string Split Pattern

With three-tier storage, the primary motivation for the `[]string` split
pattern (the 64KB key limit) is eliminated. A single cardinality-one
attribute can hold multi-paragraph text at lower storage cost than the
split vector.

The remaining reason for splitting is CRDT granularity — editing paragraph
3 of 10 replaces the entire compressed blob. A more ambitious future design
(compressed segments with per-segment CRDT resolution) could address this,
building on the compression infrastructure established here.

## Not In Scope

- Compression of non-variable-length types (integers, keywords, refs, time)
- Dictionary-based compression across values (would require shared state)
- Streaming decompression for very large values
- Compression of the badger WAL or SST files (badger has its own compression)
- Per-attribute compression control (deferred; global threshold is sufficient)
- Pushing `str/starts-with?` to storage (remains at Relation layer)

## Phase 1 Results: FSE Entropy Coder Performance

The FSE (Finite State Entropy) coder is implemented and benchmarked as the
entropy coding foundation. Measured on Apple M5 Max:

### Throughput

| Operation | 256B | 1KB | 4KB | 16KB |
|-----------|------|-----|-----|------|
| Compress | 77 MB/s | 129 MB/s | 157 MB/s | 160 MB/s |
| Decompress | — | 337 MB/s | 343 MB/s | 355 MB/s |

### Per-Value Latency

| Operation | 256B | 1KB | 4KB |
|-----------|------|-----|-----|
| Compress | 3.3μs | 7.9μs | 26μs |
| Decompress | — | 3.0μs | 12μs |

### Allocations

| Operation | Allocs | Notes |
|-----------|--------|-------|
| Compress | 11 | Pooled pairs via sync.Pool, table build dominates |
| Decompress | 2 | Output buffer + bit reader; decode table cached via sync.Map |

### Compression Ratios (FSE alone, no LZ77)

| Data type | Ratio |
|-----------|-------|
| Highly skewed (95/5) | 17.8x |
| English text | 1.58x |

FSE alone achieves modest ratios on natural text (~1.6x) because it only
exploits byte frequency, not repeated patterns. LZ77 (Phase 2) handles
pattern matching and is where the 3-5x target ratio will come from. The
FSE coder's role is to optimally encode the LZ77 output streams.

The decode table cache (`sync.Map`) eliminates table reconstruction on
repeated decompressions with the same distribution — critical for the
query read path where many values share similar byte distributions.

## References

- [COMPRESSION_RESEARCH.md](COMPRESSION_RESEARCH.md) — detailed analysis of
  compression algorithms, order-preserving techniques, and how production
  databases handle key compression