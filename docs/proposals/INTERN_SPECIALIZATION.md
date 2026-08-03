# Intern specialization: the keyword bound and the identity trie

## Status

Proposal, carrying one recorded ruling (2026-08-03): **a keyword longer than
the storage width cannot exist — the bound is enforced at the constructor, and
no other path considers a longer one.** The sections below are design under
that ruling plus the measured case for specializing the identity intern table.
The measurements are in `PERFORMANCE_STATUS.md`'s 2026-08 entries (memory-tree
import, intern-table attribution); the specialization playbook is the EA-cache
trie entry there (2026-07-21).

## Summary

The intern tables were built for a world where every read decodes: Badger
hands back encoded keys, and each datom's E and A intern on the way in. The
typed datom-tree store changed where that cost lives. A built tree hands out
already-interned pointers, so query-time interning vanishes there — the
complex checkpoint on memory-trees shows **zero intern samples**. What remains
is the bulk-import path, where the identity table accounts for **61.4% of all
allocated objects**, and Badger's per-key decode, where the intern path is
3.70% of checkpoint CPU.

Separately, the keyword tables carry structure for a world where keyword
length was unbounded: a string-keyed authority table (any length) fronted by a
`[32]byte`-keyed decode cache (storage width), with two-table coherence rules.
The ruling closes that world. With the bound enforced at the constructor, the
byte-keyed table can be the single authority, the decode path loses its
per-value throwaway string, and every downstream length check becomes dead
code.

## Where the cost lives

One cold process per measurement; the import is the 62 MB JDZL dump (2.7M
datoms, 1.02M entities) into memory-trees (2026-08-03, Apple M5):

| measurement | result |
|---|---|
| complex checkpoint (badger): intern CPU | 3.70% flat, all under `InternIdentityFromHash` |
| complex checkpoint (memory-trees): intern samples | zero |
| import: `InternIdentityFromHash` allocated objects | 3.76M cumulative — 61.4% of the import's objects |
| import: — `identity` payloads | 2.29M |
| import: — `sync.Map` entry + indirect nodes | 1.08M + 0.39M |
| import: direct intern CPU | 1.89% |

The identity table's allocations are not an intern-CPU problem — 1.89% direct
— they are a GC problem: 3.76M objects feed the collector share of import
wall, and the first import in a process retains ~155 MB of interned instances
every later import shares. Under wasm's single-threaded collector both costs
land on the critical path.

The keyword tables' hit paths are already clean, measured by prebuilt-key
benchmarks:

| path | ns/op | allocs/op |
|---|---|---|
| `BenchmarkInternKeywordHit` (string-keyed `sync.Map`) | 12.5 | 0 |
| `BenchmarkInternKeywordFromBytesHit` (byte-keyed map) | 7.7 | 0 |

The keyword cost is not the tables — it is the decode arm that feeds them:
`ValueFromBytes` materializes a throwaway `string(data)` per value-position
keyword, hits included, because the string-keyed path is the only entry the
value decoder uses.

## The ruling: keywords are bounded at the constructor

`InternKeyword` is the single authority every construction path routes through
— `NewKeyword`, `Kw`, EDN-parsed input, `InternKeywordFromBytes`' miss path.
The bound is enforced there, after `NewKeyword`'s colon normalization (the
prepend can push a 32-byte colonless input to 33), and it panics: handing the
domain an invalid keyword is a programming error, the same class `Type()`,
`ValuesEqual`, and `Keyword.Equal` already panic on. Callers that face user
text may pre-validate; the domain owes them nothing.

The EDN package does not participate. EDN is a generic format whose keywords
are unbounded; the bound is a janus domain invariant, and it reaches
EDN-parsed input because the parser constructs keywords through the
constructor like everyone else.

With the invariant at the constructor, the downstream checks test a condition
that can no longer be constructed and are deleted, not kept as belts:

- `validateAttributeLength` (`datalog/storage/database.go`)
- the schema builder's length check (`datalog/schema/builder.go`)
- the `ExportBinary` attribute-length check (`datalog/storage/export_bin.go`)

The truncation-collision test that today proves the A-position rejects long
attributes becomes the constructor-panic pin, covering every entry point. The
intern-table comment ("regardless of length") and the `MaxAttributeBytes` doc
block are rewritten to state the keyword-wide invariant.

This tightens value-position keywords: a program could previously construct
and store a >32-byte keyword *value* (the A-position was always rejected).
Under the invariant, construction itself refuses.

## One width, stated once

The width is currently spelled independently at least four ways:
`datalog.MaxAttributeBytes`, `storage.Attribute [32]byte` (tied to the
constant by compile-time assertion), `attrSize = 32` in
`key_encoder_binary.go`, and bare `[32]byte` in signatures
(`InternKeywordFromBytes`, `DecodeKey`, the byte-cache map). Go array lengths
take constants, so the code side collapses to one definition and one named
type:

```go
const MaxKeywordBytes = 32                  // the one definition
type KeywordBytes [MaxKeywordBytes]byte     // the one array type
```

Every other spelling becomes a use of those two names. A future resize is
then a one-line edit plus recompile on the code side, with nothing to hunt.

The persisted formats are where the width is real, and no constant helps
there: every Badger key carries A at the fixed width inside the fixed key
layout, so a width change is a declared storage-format version. The
text-carrying formats bridge it — EDN carries attribute text, and JDZL's
export-side check is against the string form — so dump, recompile, import is
the migration vehicle, the same as any other format break. New format surface
should self-describe: the persistent-tree page file records its width in the
file header from birth, so a reader rejects or migrates instead of
misparsing (noted in
[PERSISTENT_DATOM_TREES.md](PERSISTENT_DATOM_TREES.md)).

## One keyword table

Today the string-keyed `sync.Map` is canonical because keywords could exceed
the byte form; the `[32]byte`-keyed map is a decode-path cache in front of it,
and the two carry coherence obligations (`ClearInterns` must clear both, the
byte cache may only hold pointers the string cache produced). Under the
invariant every keyword has a total byte form, so the byte-keyed native map
becomes the single authority:

- `InternKeyword` normalizes, checks the bound, pads into a stack
  `KeywordBytes`, and probes the one table.
- `InternKeywordFromBytes` probes it directly, as it does today.
- The string-keyed `sync.Map` and the two-table coherence rules (including
  the `ClearInterns` footnote) are deleted.

The byte-keyed hit path is the faster of the two today (7.7 vs 12.5 ns, both
zero-alloc). The table stays a native map under `RWMutex` — its population is
tiny and read-mostly — with the parallel intern benchmarks arbitrating; if
contention ever shows there, the identity trie below is the shape to
generalize.

## The decode arm

`ValueFromBytes`' keyword arm currently constructs `NewKeyword(string(data))`
— one throwaway string per value-position keyword, hits included, because the
compiler's zero-copy map-probe pattern does not reach through a function
boundary into `sync.Map`. Under the invariant the arm pads into the stack
array and takes the byte path:

```go
case TypeKeyword:
    if len(data) > MaxKeywordBytes {
        return nil, fmt.Errorf("keyword value %d bytes exceeds the %d-byte bound", len(data), MaxKeywordBytes)
    }
    var key KeywordBytes
    copy(key[:], data)
    return InternKeywordFromBytes(key), nil
```

An over-width payload is corrupt or pre-invariant data and errors in-band —
loudly, never a silent slow path. Symbols are untouched: they have no storage
form, no bound, and no measured volume.

## The identity trie and slab payloads

The identity table is a `sync.Map` keyed by `[20]byte`. Every miss boxes the
key (`convTnoptr`) and allocates map entry nodes around the `&identity{}`
payload; the profile above counts 1.47M node objects beside 2.29M payloads on
one import. The specialization follows the EA-cache trie playbook — the same
reasoning that took the cache from 16.7 to 6.1 ns per hit:

- **One specialized trie**, a port-shaped sibling of
  `datalog/storage/hashtriemap.go`: `[20]byte` keys, `Identity` values, no
  type parameters, no per-operation boxing.
- **Routing from the key's own bits.** The key is a SHA1 content address —
  already uniformly distributed — so there is no seeded hasher; routing reads
  the hash directly. Engineered routing collisions only lengthen
  full-key-compared overflow chains, the same accepted threat model as the
  cache trie.
- **One pointer per hash, forever.** `LoadOrStore` semantics transfer
  unchanged; the interning invariant (`Equal` panics on two pointers carrying
  one hash) is the contract the trie must uphold under concurrency.
- **Slab-allocated payloads.** `identity` is a pointer-free 20-byte struct;
  payloads come from chunked backing arrays on the datom-slab pattern — full
  chunks never appended to, so handed-out pointers are stable. The table owns
  its slabs and `ClearInterns` drops table and slabs together. Beyond the
  allocation count, slab chunks are pointer-free memory the collector scans
  as one object rather than 2.29M.

Pins, per the EA-trie precedent: crafted routing collisions (routing is
public, so collision chains are constructed with real keys), concurrent
`LoadOrStore` single-winner, `ClearInterns` integration, and a differential
harness against the current `sync.Map` table.

## Instruments

| instrument | today | expectation |
|---|---|---|
| 62 MB JDZL import (memory-trees), allocs | 5.87M | ~2.5M |
| complex checkpoint (memory-trees), intern samples | zero | zero — must not regress |
| complex checkpoint (badger) | ~3.7% intern CPU | flat or better |
| `BenchmarkInternKeyword*Hit` | 12.5 / 7.7 ns, 0 allocs | 0 allocs holds |

The import expectation is the attribution arithmetic: 2.29M payload
allocations collapse into slab chunks and the 1.47M map-node objects become
the trie's smaller node population.

## Compatibility

- **Construction**: a >32-byte keyword panics at the constructor. Attribute
  definitions and writes already rejected this with errors; the panic extends
  the bound to value-position keywords, which were previously constructible
  and storable.
- **Decode**: a store or dump holding a pre-invariant over-width keyword
  value errors in-band at decode/import. The A-position cannot hold one (it
  was always validated).
- **No format change**: key layouts, JDZL, and EDN are byte-identical for all
  data that satisfies the invariant.

## Open questions

- **The constant's name.** `MaxAttributeBytes` is exported and no longer
  names the scope of the bound; whether it is renamed (`MaxKeywordBytes`)
  with an alias kept, or kept as-is, is a compatibility decision.
- **L85 `EncodeFixed32`/`DecodeFixed32`** carry the width in their exported
  names; same decision class.
- **Sequencing.** The invariant and the width constant touch the same files
  and are one semantic change; the keyword-table consolidation and the decode
  arm ride on them; the identity trie is independent and can land either
  side.

## Relationship to other proposals

[MEMORY_DATOM_INDEXES.md](MEMORY_DATOM_INDEXES.md) is why the query-time
intern cost vanished on trees: scans hand out already-interned pointers, so
interning became a decode-path phenomenon.
[PERSISTENT_DATOM_TREES.md](PERSISTENT_DATOM_TREES.md) reverses that locally:
page-decode-on-fault re-interns a node's worth of E/A/V per cache miss, which
makes the intern tables more load-bearing in that design, not less — and its
page format is the first surface that should record the width it was written
with.
