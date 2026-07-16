# Binary Database Export (JDZL)

Compressed, seekable sibling of the EDN dump. Same semantic contract: the
complete EAVT datom log (not a current-view snapshot), including CRDT ops and
AfterRefs. Values are inlined (Tier-3 blobs are decoded on export and
re-encoded on import). Schema is not part of the dump.

Compression uses the package `codec` LZJ compressor (`Compress` /
`Decompress`) on **independent chunks**. LZJ is whole-buffer only; streaming
I/O is framed chunks, not a byte-stream codec.

## Goals

- Smaller and faster than EDN for backup / migration / wasm host persistence
- Streaming export from an EAVT scan with bounded memory
- Seekable trailer index for parallel import
- Reuse storage value encoding (`datalog.ValueBytes` / `ValueFromBytes`)
- Attributes are keyword **strings** in the fixed 32-byte storage layout
  (zero-padded), not hashes. Entity identities remain 20-byte hashes.

## CLI

```bash
datalog -db mydata.db -export-bin backup.jdzl
datalog -db newdata.db -import-bin backup.jdzl
```

EDN `-export` / `-import` are unchanged.

## File layout

```text
┌─────────────────────────────────────────────┐
│ File header (32 bytes, fixed)               │
├─────────────────────────────────────────────┤
│ Data chunk × N                              │
├─────────────────────────────────────────────┤
│ Trailer index (at header.index_offset)      │
└─────────────────────────────────────────────┘
```

All multi-byte integers are big-endian.

### File header (32 bytes)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | Magic `JDZL` |
| 4 | 1 | Format version (`1`) |
| 5 | 2 | Flags (reserved, write `0`) |
| 7 | 1 | Reserved (`0`) |
| 8 | 4 | Soft budget (uncompressed bytes; advisory) |
| 12 | 4 | Reserved (`0`) |
| 16 | 8 | Absolute file offset of trailer index |
| 24 | 8 | Reserved (`0`) |

Export requires an `io.WriteSeeker` so `index_offset` can be patched after the
index is written. Import requires an `io.ReadSeeker` for indexed / parallel
reads.

### Data chunk frame

| Field | Size | Notes |
|-------|------|-------|
| `type` | 1 | `1` = data |
| `flags` | 1 | bit0 = payload is raw (LZJ declined) |
| `unc_len` | 4 | Uncompressed payload length |
| `cmp_len` | 4 | On-wire payload length |
| `payload` | `cmp_len` | LZJ bytes, or raw when bit0 set |

`Compress` is applied to the uncompressed chunk body. When it returns `nil`
(no size benefit), the chunk is stored raw with bit0 set and
`cmp_len == unc_len`.

### Chunk body (uncompressed)

A concatenation of binary datom records in **EAVT scan order**. No padding
between records.

#### Datom record

| Field | Size | Notes |
|-------|------|-------|
| `E` | 20 | Identity hash |
| `A` | 32 | Keyword string, zero-padded (`copy(a[:], kw.String())`) |
| `Tx` | 16 | ElementID natural order (`ElementID.Bytes()`) |
| `Op` | 1 | `CRDTOp` |
| `flags` | 1 | bit0 = AfterRef present |
| `AfterRef` | 0 or 16 | Present iff bit0; natural-order ElementID |
| `V_type` | 1 | `datalog.ValueType` |
| `V_len` | 4 | Length of `V_bytes` |
| `V_bytes` | `V_len` | `datalog.ValueBytes(V)` (no per-value LZJ; chunk LZJ covers it) |

Values are the decoded logical values from the EAVT scan (blobs already
inlined). Record encoding does not emit Tier-3 blob hashes.

### Trailer index

| Field | Size | Notes |
|-------|------|-------|
| Magic | 4 | `JDZI` |
| `chunk_count` | 4 | |
| `max_lamport` | 8 | Max Tx ElementID (Lamport) |
| `max_replica` | 8 | ReplicaID of that max ElementID |
| Entries | `chunk_count × 56` | See below |

#### Index entry (56 bytes)

| Field | Size |
|-------|------|
| `file_offset` | 8 | Absolute offset of chunk `type` byte |
| `cmp_len` | 4 | On-wire payload length (excludes 10-byte chunk header) |
| `unc_len` | 4 | |
| `first_E` | 20 | First entity hash in chunk |
| `last_E` | 20 | Last entity hash in chunk |

`cmp_len` / `unc_len` describe the payload only. The on-wire chunk occupies
`10 + cmp_len` bytes starting at `file_offset`.

## Entity-aligned soft close

The soft budget does **not** terminate a chunk mid-entity.

1. Append datoms while scanning EAVT.
2. When uncompressed size ≥ soft budget, set `close_soon`.
3. Continue while `datom.E` equals the open entity.
4. On the first datom with a new entity (or end of scan), flush the chunk.

A single entity larger than the soft budget produces one oversized chunk. Do
not split mid-entity: keeping an entity’s datoms in one LZJ window maximizes
compression of the repeated `E` field (and adjacent `A` runs). Chunk and value
lengths are `uint32` on the wire; export fails if an uncompressed chunk,
compressed payload, or value exceeds 4 GiB — it does not truncate.

## Parallel import

1. Read header; seek to `index_offset`; parse index.
2. Fan out over index entries (worker limit configurable).
3. Each worker: seek → read frame → decompress → decode records → `store.Assert`.
4. The first worker error cancels further launches and asks in-flight workers
   to stop between steps; `ImportBinary` then returns that error.
5. After all workers succeed, `clock.Restore` using the trailer max ElementID
   (or `store.MaxElementID()` as a cross-check).

**Error semantics (non-transactional):** import is not all-or-nothing across
chunks. Workers that already completed `Assert` before cancellation leave their
datoms in the store. Which subset committed is scheduling-dependent
(nondeterministic). A failed `ImportBinary` is therefore **not** a clean
rollback, and retrying the same import into the same database is **not** a
safe recovery path. After an error, import into a **fresh** database directory
(or discard the partially mutated target) and run again.

Index entry counts and chunk payload lengths are bounded against remaining file
size before allocation. Chunks never split an entity, so workers do not
coordinate on entity identity. `Assert` rebuilds all eight indices and blobs
for each batch.

## Semantics preserved (same as EDN dump)

1. Complete EAVT log, including removes and RGA ops
2. Exact ElementIDs on Tx / AfterRef
3. Op + AfterRef pairing
4. Entity identity by hash; attribute identity by keyword string
5. Value type fidelity via storage value codec
6. No schema in the dump; import bypasses schema validation (`store.Assert`)
7. Deterministic EAVT order for byte-stable re-export of the binary form when
   the soft budget and encoder settings match

## Versioning

Format version lives in the file header. Unknown versions must be rejected.
LZJ’s internal compression version byte remains inside each compressed
payload; the dump envelope does not reinterpret it.
