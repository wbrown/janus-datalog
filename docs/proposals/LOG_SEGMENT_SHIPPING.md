# Log-Segment Shipping: Windowed Export and the Stable Frontier

**Status:** Proposal
**Author:** wbrown
**Date:** 2026-08-06
**Builds on:**
- [../reference/BINARY_EXPORT.md](../reference/BINARY_EXPORT.md) — JDZL format: chunk framing, trailer index, import error semantics
- [../reference/CRDT.md](../reference/CRDT.md) — resolution as a pure function of the datom set
- [TRANSACTION_ENVELOPES.md](TRANSACTION_ENVELOPES.md) — the allocation-before-commit interleaving defect; envelopes as the future replication unit
- [LIVE_QUERY_SUBSCRIPTIONS.md](LIVE_QUERY_SUBSCRIPTIONS.md) — in-process result deltas; gap-free registration
- [DISTRIBUTED_JANUS.md](DISTRIBUTED_JANUS.md) — the far end: sharded multi-node Janus

---

## Abstract

A process that owns a live database should be able to publish its log to
read-only replicas as it grows: one full dump, then a sequence of small
**segments**, each an ordinary JDZL file containing exactly the datoms in an
ElementID window `(SinceTx, UntilTx]`. A replica hydrates the dump, then
applies each segment in order; CRDT resolution makes application idempotent
and order-insensitive, so the transport can be as dumb as files on a static
host.

Two additions make this exact rather than approximate, and a third makes it
a wire format:

1. **Window bounds on export.** `ExportOptions` / `BinaryExportOptions` gain
   `SinceTx` / `UntilTx` ElementID bounds, and a segment's fixed header
   records the window it covers.
2. **The stable frontier.** A new `Database.StableFrontier()` returns an
   ElementID at or below which no allocated-but-uncommitted operation exists.
   A window whose upper bound is a stable frontier is exact under concurrent
   writers — without requiring snapshot-isolated export scans.
3. **Sequential readability.** Windowed export emits a revised layout —
   fixed metadata front-loaded in an enlarged header, a self-framing chunk
   stream with an explicit end marker, the chunk index demoted to an
   optional trailing section — and import gains a matching `io.Reader` mode,
   so a consumer decodes a segment strictly sequentially, applying datoms as
   the bytes arrive, with no buffering and no seeks.

A bare "everything since X" export — the obvious first design — is not merely
imprecise under concurrent writers; it **silently loses facts forever** (§The
straggler race). The frontier is what makes windowed export a correctness
mechanism rather than a convenience filter.

---

## Motivation

The substrate is an append-only log: new state is exactly new datoms. Yet the
only way to move a database between processes today is the whole dump. A
long-running writer (an ingest, a build, a simulation) whose state an observer
wants to watch has three options, all bad:

- **Republish the full dump per refresh** — correct, O(store) each time, and
  at tens of MB per dump and seconds per hydration it caps refresh frequency
  far below the write rate.
- **Expose the store behind a query server** — couples the writer to a serving
  role, and a browser-resident replica (JDZL's own "wasm host persistence"
  goal) then needs a live connection rather than static artifacts.
- **Application-level change feeds** — re-encode facts the log already
  carries; a shadow representation with its own drift.

Shipping the log tail is the CRDT-native mechanism: the dump format already
carries datoms with their exact ElementIDs, import already `Assert`s them
preserving identity, and resolution is a pure function of the datom set, so a
replica that holds a superset window converges to the same views. The missing
pieces are only (a) expressing "this window of the log" at export, and (b) a
sound upper bound for the window while writers are live.

The unit here is deliberately the **datom window**, not the transaction: this
proposal works against the current pre-envelope format and requires no
grouping metadata. Under [TRANSACTION_ENVELOPES.md](TRANSACTION_ENVELOPES.md)
the natural unit becomes the envelope and the natural bound the published
basis; §Relation to the envelope design describes how this surface survives
that transition with its implementation replaced.

---

## The straggler race

ElementIDs are allocated at operation time (`LamportClock.Next()` per
`Add`/`Set`/`Remove`), but datoms become visible at `Commit` — and nothing
orders commits by allocation. This is the same defect
[TRANSACTION_ENVELOPES.md](TRANSACTION_ENVELOPES.md) documents for `AsOf`
("Transaction-closed AsOf / Current defect"), and it breaks a naive
`SinceTx`-only export the same way:

1. Writer A's open transaction allocates Lamports 100–110. Writer B's
   allocates 111–120.
2. B commits; A is still building.
3. An export of "everything above 90" runs: it emits 111–120. The consumer's
   cursor advances to 120.
4. A commits.
5. The next export of "everything above 120" excludes 100–110 — **forever**.

The replica is silently missing an arbitrary interior slice of the log. Every
resolved view over it is quietly wrong, idempotent re-application cannot
repair it (the datoms are never shipped again), and nothing detects it short
of comparing full datom sets. An unbounded live export has a weaker version of
the same problem: whether 100–110 lands in the file depends on where the scan
cursor is when A commits, so the file's effective coverage is undefined.

Any windowed-export design must therefore answer: *what upper bound makes the
window's contents fully determined?*

---

## Design

### The stable frontier

**Definition.** A *stable frontier* is an ElementID `F` such that every
operation with ElementID ≤ `F` is committed and durable, and no in-flight
transaction holds an allocated ElementID ≤ `F`.

Given a stable frontier, exactness stops depending on scan isolation
entirely:

- Every datom with `Tx ≤ F` is already committed when `F` is computed, and
  append-only storage means it cannot subsequently disappear.
- Every datom the scan encounters with `Tx > F` is excluded by the bound.

So a window `(S, F]` has fully determined contents even when the export scan
is a raggedy, multi-iterator, non-transactional walk racing live writers.
**The window bound converts an isolation problem into a windowing problem** —
which is why this proposal adds no snapshot machinery to the export path.

**Tracking.** The database maintains an in-flight registry keyed by
transaction:

- On a transaction's **first** ID allocation, it registers that first
  ElementID (its low-water) in the registry. Registration and allocation are
  atomic with respect to frontier reads — the registry and the clock share a
  lock for this pair of operations, so a frontier can never be computed
  between "ID allocated" and "registration visible".
- On `Commit` (after the storage commit is durable) or `Rollback`, the
  transaction deregisters.

```go
// StableFrontier returns an ElementID at or below which every allocated
// operation is committed and durable. With no transactions in flight it is
// the clock's current value; otherwise it is one below the minimum
// first-allocated ElementID among in-flight transactions.
func (d *Database) StableFrontier() datalog.ElementID
```

Cost: two locked registry operations per *transaction* (not per operation),
zero cost on the per-datom write path.

Rolled-back transactions burn their allocated IDs — the frontier then passes
over a gap containing no datoms, which is already a legal log shape
([TRANSACTION_ENVELOPES.md](TRANSACTION_ENVELOPES.md): "failed commits burn
sequencer IDs, so gaps between ranges are legal").

**Scope: scalar frontiers assume a single local writer identity.** The
frontier and the window bounds below are scalar ElementIDs, ordered the same
way `AsOf` orders (`Tx ≤ target` under natural `(Lamport, ReplicaID)` order).
That is exact for a producer that is the sole writer of its store between
segments. A store that *absorbs foreign history mid-shipping* (a merge) can
acquire datoms with arbitrary foreign Lamports below an already-shipped
cursor, which a scalar window would skip — the same scalar-vs-frontier
distinction `SnapshotBasis` draws. Frontier-shaped windows are deliberately
left to the envelope design; a producer that merges while shipping is out of
scope here, and §Consumer contract makes the replica refuse the resulting
gap rather than diverge silently.

### Window bounds on export

```go
type BinaryExportOptions struct {
    SoftBudget int
    SkipEntity func(datalog.Identity) bool

    // SinceTx / UntilTx bound the export to datoms with
    // SinceTx < datom.Tx <= UntilTx (natural ElementID order).
    // Zero SinceTx means from genesis; zero UntilTx means unbounded —
    // the current whole-log behavior, which under concurrent writers
    // has undefined coverage (see the straggler race). Callers wanting
    // an exact segment pass UntilTx = db.StableFrontier().
    SinceTx datalog.ElementID
    UntilTx datalog.ElementID
}
```

`ExportOptions` (EDN) gains the same pair for parity. The half-open
`(Since, Until]` convention matches `AsOf`'s inclusive upper bound, so a
consumer's cursor is always "the last bound I applied" and the next request's
`SinceTx` verbatim.

Both bounds compose with `SkipEntity` (intersection of filters).

**Scan strategy.** The TAEV index is Tx-first — its own comment names it "for
clock recovery, audit log" — so gathering a window is a bounded range scan,
O(window), not a filtered O(store) pass. Records are then reordered EAVT and
emitted through the existing entity-aligned chunk writer, preserving the
format's contract (entity-aligned chunks, deterministic EAVT order,
byte-stable re-export). For an unbounded export nothing changes: the existing
EAVT scan path remains, and an options struct with zero bounds produces
byte-identical output to today.

### Segment files are JDZL as a wire format

A segment must be consumable off a stream — an HTTP body, a pipe — by a
strictly sequential reader with no buffering, because the consumer side of
shipping is exactly the side without random access. Trailers are hostile to
that: metadata at the end forces the reader to buffer the whole artifact (or
seek) before it can interpret the first byte. The current trailer's contents
split by what they are, and each half has a different answer:

- **Fixed-size metadata moves to the front.** An enlarged fixed header
  carries what the trailer carries today (`chunk_count`,
  `max_lamport`/`max_replica`) plus the window: `SinceTx` and `UntilTx` as
  two 16-byte natural-order ElementIDs. The window needs no finalize patch
  at all — the bounds are *inputs* chosen before the scan, written at open.
  The outcome fields are reserved as zeros at open and patched by one
  backward seek at finalize — the same discipline the header's
  `index_offset` already uses, so export's existing `io.WriteSeeker`
  requirement is unchanged.
- **The variable-size chunk index stays behind — and becomes optional.** It
  cannot be front-reserved (`chunk_count` is unknown until the scan ends),
  and a sequential reader never needs it: chunks are self-framing (the
  10-byte chunk header), so the stream is walkable without an index. The
  index serves only parallel and selective import from seekable sources;
  `index_offset = 0` now means "no index", and an index-less file imports
  sequentially.
- **An explicit end marker terminates the chunk stream** (a reserved
  chunk-type byte), so a sequential reader needs neither `chunk_count` nor
  end-of-stream semantics from its transport. The patched counts remain as
  integrity cross-checks.

The window in the header is what lets a consumer verify contiguity (below)
from the first bytes of a stream — before fetching the body — with no
out-of-band metadata. `max_lamport`/`max_replica` cannot serve as the
window, because a window's upper bound is the *frontier chosen by the
exporter*, not the maximum datom the file happens to contain: an empty
window is legal and still advances the cursor.

Two properties fall out of reserve-then-patch:

- **An unfinalized file is self-evident.** Zeroed outcome fields with a
  missing end marker mark a crashed or truncated export — a detectably
  incomplete artifact rather than a plausible-looking truncation.
- **A pipe-only producer is possible.** A writer that cannot seek emits the
  version and window up front (both known before the scan), the chunk
  stream, and the end marker; its outcome fields stay zero and it carries no
  index. Such a file is fully consumable by the sequential path — the
  outcomes are derivable from the stream itself — and simply unavailable to
  the parallel path.

In every other respect a windowed export is a valid JDZL file: same datom
records, same chunk framing, loadable as a dump source (`-db seg.jdzl`) like
any other.

The revised layout rides a format version bump — under a policy this
proposal adopts explicitly: **interchange-format bumps are minimized,
additive, and never break the read path.**

- **The bump is scoped to the exchange use.** Only windowed segments emit the
  new version. Unwindowed dumps — the backup/migration artifacts — continue
  to emit the current version byte-identically, so the dominant artifact
  class never changes and a process that ships no segments never encounters
  the new version at all.
- **Older versions stay readable, permanently.** The version byte gates the
  revised layout, never readability: a current reader reads every older
  version exactly as before (`ReadDumpWindow` reports `windowed=false`). BINARY_EXPORT.md's unknown-version rule keeps only its
  original direction — readers predating this feature reject the newer
  version, which is correct because they could not honor a window's contract,
  and only segment consumers ever encounter it.

This bump is deliberately **not** coupled to the envelope design's pending
JDZL revision: that revision is a hard break that rejects pre-envelope files,
and folding this layout into it would both violate the
older-versions-stay-readable policy and defer segments until the envelope
flag day — forfeiting this proposal's works-against-the-current-format
property. The two stay independent; when the envelope revision lands, it
carries this layout forward as part of its own format.

The window fields are the file's own statement of coverage:
`(SinceTx, UntilTx]` with both bounds zero meaning "unwindowed complete log".
A head-read entry point exposes them without seeking:

```go
// ReadDumpWindow reports the window a JDZL file declares itself to cover,
// reading only the fixed header. windowed is false for an unwindowed
// (complete-log) dump.
func ReadDumpWindow(r io.Reader) (since, until datalog.ElementID, windowed bool, err error)
```

Import gains a sequential mode alongside the existing parallel one:

```go
// ImportBinaryStream decodes and asserts chunk-by-chunk as the bytes
// arrive, reading strictly forward — the wire path: hydration proceeds
// while the artifact downloads, and nothing buffers the whole file. The
// parallel ReadSeeker path is unchanged and uses the trailing index when
// present; an index-less file imports sequentially.
func (d *Database) ImportBinaryStream(r io.Reader, opts ...BinaryImportOptions) error
```

A mid-stream failure leaves a deterministic prefix applied — in contrast to
the parallel path's nondeterministic subset — though the replica recovery
contract below is the same either way.

Note that a segment hydrated **alone** is a partial log: entities may be
missing attributes whose datoms fall outside the window. That is consistent
with the dump contract (a dump is a datom log, not a current-view snapshot),
but it means a segment is a *delta to apply*, not a database to open — except
for inspection.

### Producer contract

A publishing writer repeats one step:

```text
F  := db.StableFrontier()
seg := ExportBinary(w, BinaryExportOptions{SinceTx: cursor, UntilTx: F})
publish seg          // a file beside a static page, an HTTP response, anything
cursor = F
```

The base artifact is the same step with `cursor = zero` — meaning even the
initial full dump becomes exact under concurrent writers, which an unbounded
live export today is not. The producer keeps no per-consumer state; the
cursor is the producer's own last-published frontier, and a pull-style
producer (an HTTP endpoint taking `since` from the request) keeps no state at
all.

### Consumer contract

A replica is a read-only database (it mints nothing; its clock only
`Receive`s):

1. Hydrate the base dump (`ImportBinary` into a fresh store). Cursor := the
   dump's `UntilTx`.
2. Per segment: check `segment.SinceTx <= cursor` (overlap is fine —
   application is idempotent) and `segment.UntilTx > cursor`. The window is
   a head-read, so a streaming consumer runs this check on the first bytes
   and rejects a gapped segment before fetching the body. A segment with
   `SinceTx > cursor` is a **gap**: refuse it and re-sync from a base dump.
   Never apply across a gap — a gap is exactly the silent-divergence shape
   the frontier exists to prevent.
3. Apply the segment into the live store — the sequential path off the wire,
   or the parallel path when the artifact is seekable and indexed. Cursor :=
   `UntilTx`.
4. On any apply error: discard the replica and re-hydrate from a base dump.
   This is the recovery path BINARY_EXPORT.md already prescribes — import is
   not transactional across chunks, and retrying into the same store is not
   safe recovery. For a replica the fresh-start is cheap and correct by
   construction, so no repair logic exists to get wrong.

Idempotent overlap is load-bearing for step 2 and must hold at the storage
layer: re-`Assert`ing a datom that is already present (same ElementID, same
content) is a no-op on every index. This is expected of the current encoding
(identical records produce identical index keys) but is promoted to a tested
contract by this proposal rather than assumed.

---

## Relation to the envelope design

[TRANSACTION_ENVELOPES.md](TRANSACTION_ENVELOPES.md) makes three parts of
this proposal simpler, and none of it wasted:

- **The frontier becomes the published basis.** With commits sequenced per
  owner and publication atomic, "no in-flight allocation at or below F" is
  exactly the owner's published basis; `StableFrontier()` keeps its signature
  and drops its registry.
- **The window unit becomes the envelope.** Windowed export selects the
  transaction records whose ranges lie inside the window and ships complete
  envelopes; `TransactionsAfter` (envelope PR 7) is the naturally equivalent
  read. Datom-level windows and envelope-level windows agree because
  authoritative commit ranges are contiguous.
- **Consumer staging replaces the gap check.** A receiver that stages
  envelopes until their `ParentBasis` is satisfied subsumes the cursor
  contiguity rule.

What this proposal provides that the envelope design does not: it works
against the **current** format now, and it defines the shipping surface —
window bounds on export options, the self-describing segment file, the
producer/consumer cursor contract — which the envelope design then makes
cheaper to implement, not obsolete.

[LIVE_QUERY_SUBSCRIPTIONS.md](LIVE_QUERY_SUBSCRIPTIONS.md) is the in-process
complement: it answers "did this query's result change" for a subscriber that
already holds the database. Log-segment shipping moves the *facts* to a
process that doesn't. A replica that wants live-query semantics composes the
two: apply segments, then run subscriptions against the local replica.

[DISTRIBUTED_JANUS.md](DISTRIBUTED_JANUS.md) is the far end — sharding,
routing, quorum durability. This proposal is deliberately the smallest
distributed shape: one writer, N read-only observers, dumb transport, no
coordination, no back-channel.

---

## Rejected approaches

- **`SinceTx` alone.** The straggler race: silently and permanently lost
  facts. Not an imprecision — a divergence with no detection and no repair.
- **`UntilTx` as a caller-guessed filter.** A bound whose value is not a
  stable frontier (e.g. `clock.Current()` sampled while writers run) readmits
  the race for every allocated-but-uncommitted ID at or below it. The frontier
  is the point; the option without the frontier API invites exactly the
  misuse that looks correct in every quiesced test.
- **Snapshot-isolated export instead of windowing.** Ties exactness to a
  storage-layer isolation capability on every backend, and still needs the
  frontier anyway to name *which* snapshot is safe to cut under
  allocation-before-commit. Windowing gets exactness on any backend,
  including non-snapshot scans, from append-only alone.
- **The window in a trailing section.** The natural placement in the current
  layout, and wrong for the wire: a trailer forces a stream consumer to
  buffer the whole artifact (or seek) before the gap check can run. The
  window is an input known before the scan — front-loading it costs nothing
  and makes the check a first-bytes read.
- **Per-commit streaming (a change feed).** A finer-grained push channel
  needs a session protocol, back-pressure, and reconnect semantics; segments
  over dumb transports need none of that, and the envelope design's
  replication unit is the right home for streaming when it comes.
- **A replica-side "apply log" bookkeeping table.** The cursor is the only
  consumer state, it is derivable from the last applied segment's header,
  and anything more is a shadow of the log.

---

## Expected impact

- **Write path:** two locked registry operations per transaction (register on
  first allocation, deregister on commit/rollback). Nothing per datom.
- **Export:** windowed gathering is a bounded TAEV range scan, O(window) plus
  an O(w log w) EAVT reorder for chunk alignment. Unbounded export is
  untouched and byte-identical to today.
- **Import:** gains the sequential `io.Reader` mode — a streaming consumer
  hydrates while the artifact downloads and never buffers the whole file;
  the parallel path is unchanged wherever an index is present. The consumer
  contract's window check is a head-read on the first bytes.
- **Format:** a header-layout revision (front-loaded fixed metadata
  including the window, self-framing chunk stream with an end marker, the
  chunk index optional-trailing) behind a version bump scoped to windowed
  export; unwindowed export keeps emitting the current version
  byte-identically, and readers keep reading every older version.
- **Replica freshness:** bounded by segment cadence, which the producer
  chooses; the cost per refresh drops from O(store) to O(new datoms), which
  is what makes watch-while-writing usable at all.

---

## Verification

- **Straggler regression (the defining test):** two concurrent transactions,
  the earlier-allocated one committing later. A window cut at the frontier
  between the commits excludes the in-flight allocation; the next window
  includes it; the concatenation equals the full log. This must fail against
  a `SinceTx`-only implementation.
- **Frontier property test:** under N concurrent committers, every sampled
  frontier F satisfies: all datoms with `Tx ≤ F` durable at sample time, and
  no later commit ever lands a datom with `Tx ≤ F`.
- **Quiesced frontier:** with nothing in flight, `StableFrontier()` equals
  the clock's current value.
- **Rollback:** a rolled-back transaction deregisters; the next frontier
  passes its burned range; the corresponding window is exact and simply
  contains no datoms from the gap.
- **Segmentation equivalence:** full dump ≡ base + segments, compared as
  resolved current views **and** as raw History (both must match; History
  catches dropped superseded datoms that resolution would mask).
- **Overlap idempotence:** applying overlapping windows yields index state
  byte-identical to exact-once application; datom counts do not inflate.
- **Gap refusal:** a consumer at cursor C refuses a segment with
  `SinceTx > C`.
- **Empty window:** a segment with zero datoms is legal, round-trips, and
  advances the cursor to its `UntilTx`.
- **Exact live export:** a full windowed export (`(0, F]`) taken while
  writers run contains exactly the datoms ≤ F, no more, no less.
- **Format:** windowed files round-trip their window through
  `ReadDumpWindow`; a current reader reads previous-version files unchanged
  (`windowed=false`); readers predating the feature reject the new version;
  unwindowed export remains byte-identical to the previous version;
  `SkipEntity` composes with window bounds; byte-stable re-export holds for
  windowed segments.
- **Sequential decode:** streaming import of a segment yields store state
  identical to parallel import of the same file; the head-read window check
  completes on a pure `io.Reader` with no `Seek` calls; an index-less file
  (`index_offset = 0`) imports sequentially, and the parallel path refuses
  it rather than guessing.
- **Unfinalized detection:** a file with zeroed outcome fields and no end
  marker is rejected as incomplete; a pipe-written segment (end marker
  present, outcomes zero, no index) imports sequentially and round-trips.
- **Deterministic prefix:** an injected mid-stream failure leaves exactly
  the chunks preceding the failure applied.
- **Replica clock:** after each segment apply, the replica's clock is at or
  past the segment's `max_lamport` (existing `clock.Restore`/`Receive`
  behavior, asserted under the segment path).
- **Segment as dump source:** a segment opens as a `-db` temp source for
  inspection, with partial entities understood as the documented shape.
