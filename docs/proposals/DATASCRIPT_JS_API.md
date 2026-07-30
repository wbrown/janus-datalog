# DataScript-Shaped JavaScript / TypeScript API

**Status:** Proposal
**Date:** 2026-07-16
**Builds on:**
- [MEMORY_DATOM_INDEXES.md](MEMORY_DATOM_INDEXES.md) — typed in-memory
  datom indexes
- [LIVE_QUERY_SUBSCRIPTIONS.md](LIVE_QUERY_SUBSCRIPTIONS.md) — commit
  publication, pinned bases, and declarative result subscriptions
- [BRANCHING_AND_SNAPSHOTS.md](BRANCHING_AND_SNAPSHOTS.md) — database values
  pinned to an explicit basis
- [../reference/BINARY_EXPORT.md](../reference/BINARY_EXPORT.md) — compact
  JDZL persistence boundary

---

## Abstract

Janus already compiles under `GOOS=js GOARCH=wasm`. Its memory backend passes
the same public database contracts as the native Badger backend, including
transactions, queries, Pull, History/AsOf, snapshots, and EDN/JDZL
export/import. What does not exist is a JavaScript API: there are no
`syscall/js` exports, JavaScript or TypeScript sources, npm package, worker
host, or JavaScript value conversion contract.

This proposal adds a DataScript-shaped JavaScript / TypeScript interface over
the existing Janus engine:

- Connections with a moving latest-state database value.
- Immutable database views pinned to a Janus basis.
- Transaction data expressed as JavaScript arrays and objects.
- EDN query and Pull strings.
- Ordinary JavaScript arrays and objects as results.
- Transaction reports and listeners.
- Host-managed JDZL persistence.

“DataScript-shaped” means a familiar programming model, not drop-in DataScript
compatibility. Janus retains its own identities, ElementIDs, CRDT operations,
schema semantics, and temporal model. The API must expose those semantics
honestly rather than translating them into DataScript integer entity IDs or
mutable-database behavior.

The recommended runtime is a Web Worker that owns one Go wasm instance. The
public TypeScript API is asynchronous because every database operation crosses
the worker boundary. This keeps query execution off the browser main thread
and gives the package one consistent API in browsers, Node workers, and other
JavaScript hosts.

---

## 1. Current state

The current js/wasm support is Go-level portability, not JavaScript
integration.

Production behavior:

- `datalog/storage/default_store_wasm.go` selects `MemoryStore`.
- `db.OpenMemory` provides the complete `Database` API without Badger.
- `examples/wasm_memory.go` demonstrates write, query, Export, and Import.
- The standard test gate compiles and runs storage/database contracts under
  Node through `go_js_wasm_exec`.

Missing JavaScript-facing pieces:

- No wasm `main` package.
- No `syscall/js` bridge.
- No exported JavaScript functions.
- No worker protocol.
- No JavaScript value conversion contract.
- No transaction-data parser.
- No TypeScript declarations.
- No npm build or package.
- No browser-host persistence API.
- No JavaScript integration tests.

The query engine is not the missing implementation. The missing implementation
is the host boundary and the semantics exposed through it.

---

## 2. Goals and non-goals

### Goals

- Present a familiar DataScript-shaped API without hiding Janus semantics.
- Keep the full query parser, planner, executor, CRDT resolution, schema
  validation, Pull implementation, and temporal behavior in Go.
- Run database work in a Web Worker by default.
- Give every JavaScript value crossing the boundary an explicit type mapping.
- Represent database values as immutable pinned views, not copied databases.
- Return errors to JavaScript with stable operation context.
- Support compact host-managed persistence through JDZL.
- Keep the first release small enough to validate the public contract before
  adding every DataScript operation.
- Make the npm API testable against the same semantic fixtures as the Go API.

### Non-goals for the first release

- Drop-in compatibility with the DataScript npm package.
- DataScript integer entity IDs.
- DataScript tempid resolution.
- `dbWith` / speculative immutable database forks.
- Direct `datoms`, `seekDatoms`, or index-slice APIs.
- Filtered database values.
- Lazy entity objects.
- IndexedDB or OPFS as a live `Store`.
- Synchronous query execution on the browser main thread.
- Incremental query maintenance in the JavaScript bridge.
- Reimplementing identity hashing, query parsing, CRDT resolution, or schema
  behavior in TypeScript.

---

## 3. Core semantic decision

The API follows DataScript’s shape while preserving Janus’s model.

DataScript and Janus differ in load-bearing ways:

- DataScript entity IDs are integers and transaction-local tempids. Janus
  entities are content-derived 20-byte identities with L85 external form.
- DataScript presents persistent database values directly. Janus has an
  append-only store with resolved views pinned by ElementID.
- DataScript transaction retraction and Janus physical `Transaction.Retract`
  are not the same operation. Janus visible removal is
  `Transaction.Remove`, which appends the appropriate CRDT tombstone.
- Janus has distinct CardinalityOne LWW, CardinalityMany add-wins, and
  CardinalityVector RGA semantics.
- Janus transaction bases are `ElementID{Lamport, ReplicaID}`, not scalar
  transaction numbers.

The JavaScript API therefore uses familiar names such as `createConn`, `db`,
`transact`, `q`, `pull`, `listen`, and `unlisten`, but it does not claim that
arbitrary DataScript code can switch packages unchanged.

---

## 4. User-facing API

The initial user experience:

```ts
const conn = await d.createConn(schema)

const alice = d.identity("person:alice")

const report = await d.transact(conn, [
  [":db/add", alice, ":person/name", "Alice"],
  [":db/add", alice, ":person/age", 30n],
])

const rows = await d.q(
  `[:find ?name :where [?e :person/name ?name]]`,
  d.db(conn),
)

const person = await d.pull(d.db(conn), "[:person/name :person/age]", alice)

d.listen(conn, "ui", report => render(report.dbAfter))
```

The TypeScript module exports functions rather than a stateful coordinating
object:

```ts
export function createConn(schema?: Schema, options?: ConnectionOptions):
  Promise<Connection>

export function close(conn: Connection): Promise<void>

export function db(conn: Connection): DatabaseValue

export function transact(
  conn: Connection,
  txData: readonly TransactionItem[],
  txMeta?: unknown,
): Promise<TransactionReport>

export function q(
  query: string,
  source: DatabaseValue,
  ...inputs: readonly DatalogValue[]
): Promise<readonly Tuple[]>

export function pull(
  source: DatabaseValue,
  pattern: string,
  entity: IdentityInput,
): Promise<PullResult | null>

export function pullMany(
  source: DatabaseValue,
  pattern: string,
  entities: readonly IdentityInput[],
): Promise<readonly (PullResult | null)[]>

export function asOf(
  conn: Connection,
  basis: ElementID,
): DatabaseValue

export function history(conn: Connection): DatabaseValue

export function listen(
  conn: Connection,
  key: string,
  callback: (report: TransactionReport) => void,
): void

export function unlisten(conn: Connection, key: string): void

export function exportJDZL(conn: Connection): Promise<Uint8Array>

export function importJDZL(
  conn: Connection,
  bytes: Uint8Array,
): Promise<TransactionReport>
```

`Connection`, `DatabaseValue`, `Identity`, and `ElementID` are opaque branded
values. Application code can inspect documented external representations but
cannot manufacture valid worker handles.

---

## 5. Connection and database-value semantics

### 5.1 Connection

A connection is a JavaScript handle to a mutable latest-state position owned by
the wasm worker. It contains no copied database state in JavaScript.

The worker owns:

- The Go `*storage.Database`.
- The latest committed `SnapshotBasis`.
- Listener registrations.
- Database-value handle lifetime.
- Close state.

### 5.2 Database value

`db(conn)` returns an immutable `DatabaseValue` descriptor:

```ts
export interface DatabaseValue {
  readonly connection: Connection
  readonly basis: SnapshotBasis
  readonly mode: "latest" | "as-of" | "history"
}
```

For an existing commit, query execution uses `Database.AsOf(basis.tx)`.
History uses `Database.History()`.

The empty database cannot use zero `ElementID` as an AsOf value because zero is
already the History sentinel in the current engine. This proposal reuses the
explicit `SnapshotBasis` required by
`LIVE_QUERY_SUBSCRIPTIONS.md`:

```go
type SnapshotBasis struct {
    Tx    datalog.ElementID
    Empty bool
}
```

There must be one canonical basis type shared by snapshots, subscriptions,
transaction reports, and the JavaScript bridge.

### 5.3 Latest-state descriptor

`db(conn)` captures the connection’s current basis when called. It does not
remain live as later transactions commit. This is the database-as-a-value
contract:

```ts
const before = d.db(conn)
await d.transact(conn, txData)
const after = d.db(conn)

await d.q(query, before) // pre-transaction state
await d.q(query, after)  // post-transaction state
```

No database copy is required. Both descriptors query the same append-only store
through different visibility bases.

---

## 6. Identity model

Janus identity construction remains authoritative in Go.

```ts
export interface IdentitySeed {
  readonly type: "identity-seed"
  readonly seed: string
}

export interface Identity {
  readonly type: "identity"
  readonly l85: string
}

export type IdentityInput = Identity | IdentitySeed

export function identity(seed: string): IdentitySeed
export function identityFromL85(encoded: string): Identity
```

`identity(seed)` creates a descriptor, not a second TypeScript implementation
of Janus identity hashing. The worker converts the seed with
`datalog.NewIdentity`. `identityFromL85` is validated and decoded by Go before
use.

Query and Pull results return canonical `Identity` values containing the full
L85 representation. They never return truncated identifiers.

Entity maps in the first release require an explicit `":db/id"`. Automatic
tempids are deferred because they would introduce a second identity allocation
model and a tempid-resolution map not present in Janus.

---

## 7. JavaScript value contract

Every Datalog value has one JavaScript representation.

### Scalar mappings

- Janus `string` → JavaScript `string`.
- Janus `int64` → JavaScript `bigint`.
- Janus `float64` → JavaScript `number`.
- Janus `bool` → JavaScript `boolean`.
- Janus `time.Time` → JavaScript `Date`.
- Janus `[]byte` → JavaScript `Uint8Array`.
- Janus `Identity` → branded `Identity`.
- Janus `Keyword` → branded `Keyword`.
- Janus `Symbol` → branded `SymbolValue`.
- Janus `ElementID` → branded `ElementID`.
- Janus vectors → readonly JavaScript arrays of converted values.

`int64` does not map to JavaScript `number`; values above `2^53-1` would lose
information.

### Keywords and symbols

Attributes in transaction-operation position use strings such as
`":person/name"` because their type is unambiguous. Stored keyword and symbol
values use explicit constructors:

```ts
export interface Keyword {
  readonly type: "keyword"
  readonly value: string
}

export interface SymbolValue {
  readonly type: "symbol"
  readonly value: string
}

export function keyword(value: string): Keyword
export function symbol(value: string): SymbolValue
```

This preserves the distinction between the string `":status/active"` and the
keyword `:status/active`.

### ElementIDs

```ts
export interface ElementID {
  readonly type: "element-id"
  readonly lamport: bigint
  readonly replicaId: bigint
}
```

The bridge validates both components as unsigned 64-bit integers.

### Boundary conversion

Conversion is recursive, typed, and centralized in the wasm bridge. Query,
Pull, transaction, schema, listener, and persistence APIs all use the same
converter. No API defines its own parallel conversion rules.

---

## 8. Transaction data

The first release accepts operation arrays and entity maps.

```ts
export type TransactionOperation =
  | readonly [":db/add", IdentityInput, string, DatalogValue]
  | readonly [":db/set", IdentityInput, string, DatalogValue]
  | readonly [":db/remove", IdentityInput, string, DatalogValue]

export interface EntityMap {
  readonly ":db/id": IdentityInput
  readonly [attribute: string]: DatalogValue | IdentityInput
}

export type TransactionItem = TransactionOperation | EntityMap
```

Operation semantics are Janus semantics:

- `:db/add` → `Transaction.Add`.
- `:db/set` → `Transaction.Set`.
- `:db/remove` → `Transaction.Remove`.

The bridge does not expose Janus physical `Transaction.Retract` under
DataScript’s `:db/retract` spelling. Physical retraction deletes matching
stored datoms; it is not the visible-state CRDT operation an application
expects from DataScript transaction data.

Entity maps are converted to `Transaction.AddEntity` after removing
`":db/id"`. Schema cardinality determines whether an add is LWW, add-wins, or
RGA append.

Schema validation remains eager. If any transaction item is invalid, the
operation rejects and no partial commit is visible.

---

## 9. Transaction reports and listeners

A transaction report describes what actually committed, not merely what the
JavaScript caller requested.

```ts
export interface TransactionReport {
  readonly basisBefore: SnapshotBasis
  readonly basisAfter: SnapshotBasis
  readonly dbBefore: DatabaseValue
  readonly dbAfter: DatabaseValue
  readonly txData: readonly Datom[]
  readonly txMeta: unknown
}
```

The Go transaction path must produce the report. The TypeScript wrapper must
not reconstruct it from submitted transaction data because:

- Schema-aware operations may produce different CRDT datoms.
- CardinalityVector uniqueness can make an add a no-op.
- Transaction metadata receives its own ElementID.
- Import and future replication may commit data through paths other than
  `transact`.

The commit report and the commit-publication point described by
`LIVE_QUERY_SUBSCRIPTIONS.md` must share one implementation. They are different
consumer contracts:

- Transaction listeners receive committed operations.
- Query subscriptions receive changes to resolved query results.

Listener callbacks run in JavaScript after the worker has published the report.
They never run while Go transaction, cache, registry, or storage locks are
held.

`listen` keys are scoped to one connection. Re-registering the same key
replaces that callback. `unlisten` is idempotent. Closing the connection removes
all listeners.

---

## 10. Query and Pull

### Query

Queries remain EDN strings and are parsed and planned in Go:

```ts
const tuples = await d.q(
  `[:find ?name
    :in $ ?minimum
    :where [?person :person/name ?name]
           [?person :person/age ?age]
           [(>= ?age ?minimum)]]`,
  d.db(conn),
  18n,
)
```

Results are materialized JavaScript arrays:

```ts
type Tuple = readonly DatalogValue[]
```

The bridge does not truncate result relations. Future cursor/stream APIs may
avoid materializing large results, but they do not change `q` semantics.

### Pull

Pull patterns remain strings and are interpreted by the existing Go Pull
implementation. Pull results use attribute strings as object keys and the
central value conversion for values.

An absent entity returns `null`. A non-nil Go error rejects the operation.

### Prepared queries

The JavaScript API may expose prepared-query handles after the Go
`PREPARED_QUERIES.md` contract exists. The first release relies on the current
parse/plan caches and does not invent a JavaScript-only preparation layer.

---

## 11. Worker and wasm architecture

### 11.1 Default runtime

The package runs one Go wasm instance in a dedicated Worker:

```text
application
    → TypeScript API
    → worker request / response
    → syscall/js bridge
    → Janus Database
    → MemoryStore
```

The Worker owns all Go references. JavaScript receives opaque numeric handles
plus immutable typed descriptors.

### 11.2 Asynchronous API

All database operations return Promises. This is deliberate even though
DataScript’s JavaScript API is synchronous:

- Query execution can be CPU-intensive.
- The current wasm/Node benchmark is approximately 5.2× native time on the same
  MemoryStore code.
- Main-thread synchronous execution would freeze rendering and input handling.
- The same Promise contract works in browsers and Node workers.

A direct synchronous bridge is not part of the first public package.

### 11.3 wasm entry point

Add a dedicated main package:

```text
cmd/janus-wasm/
    main.go
```

It registers a small request dispatcher through `syscall/js`, keeps the Go
runtime alive, and owns the handle registry. Database behavior stays in
existing packages; the command contains boundary conversion and dispatch only.

### 11.4 Request protocol

Each worker request contains:

- Monotonic request ID.
- Operation name.
- Connection/database handle.
- Typed arguments.

Each response contains either:

- Converted result, or
- Structured error.

Listeners are worker-to-host events, separate from request responses.

The protocol transfers complete operation arguments and results in batches.
It does not make one wasm boundary call per tuple component or datom.

### 11.5 Lifecycle

- `createConn` allocates one database handle.
- Database values reference that connection plus a basis; they do not retain a
  separate Go database.
- `close` rejects new work, closes the database, releases handles, and removes
  listeners.
- Worker termination is a terminal failure for every outstanding Promise.

---

## 12. Persistence

The first release uses host-managed snapshots rather than a live browser
storage backend.

```ts
const bytes = await d.exportJDZL(conn)
await indexedDBStore("world", bytes)

const restored = await d.createConn(schema)
await d.importJDZL(restored, await indexedDBLoad("world"))
```

JDZL is the primary browser persistence format because it is compact,
versioned, and preserves the complete EAVT datom log including CRDT operations
and AfterRefs.

`ExportBinary` requires `io.WriteSeeker`. The wasm boundary therefore needs an
in-memory seekable writer that returns one `Uint8Array` after export. Import
uses `bytes.Reader`, which already implements `io.ReadSeeker`.

EDN export/import may also be exposed for debugging and interoperability, but
it is not the default persistence path.

IndexedDB and OPFS adapters remain host concerns in this stage:

- The application decides when to persist.
- The TypeScript package provides bytes, not a second database state model.
- A later IndexedDB/OPFS `Store` proposal can replace snapshot persistence
  without changing the public query/transaction API.

---

## 13. Package layout

Proposed source layout:

```text
cmd/janus-wasm/
    main.go

js/
    package.json
    src/
        index.ts
        worker.ts
        protocol.ts
        types.ts
    test/
        contracts.test.ts
```

Published package contents:

```text
dist/
    index.js
    index.d.ts
    worker.js
    janus.wasm
    wasm_exec.js
```

`wasm_exec.js` is copied from the active Go toolchain during the build. It is
not maintained as an independent source file because it must match the Go wasm
runtime that produced `janus.wasm`.

The package exports the Worker-backed API from one module. Internal protocol
and handle types are not public.

---

## 14. Error contract

Every Go error rejects the corresponding Promise.

```ts
export interface JanusError extends Error {
  readonly code: string
  readonly operation: string
}
```

Initial error codes cover stable boundaries:

- `INVALID_ARGUMENT`
- `INVALID_IDENTITY`
- `INVALID_VALUE`
- `QUERY_PARSE`
- `QUERY_EXECUTION`
- `SCHEMA_VALIDATION`
- `TRANSACTION_CLOSED`
- `DATABASE_CLOSED`
- `IMPORT`
- `EXPORT`
- `WORKER_TERMINATED`

The Go error remains the source of the human-readable message. The bridge adds
operation context and classifies only errors with a stable public meaning.
Unknown errors use `INTERNAL` and retain the original message.

No error becomes `null`, an empty relation, or a default value. Absence is
represented only where the Go API already defines absence, such as Pull of a
missing entity.

---

## 15. Relationship to typed memory indexes

`MEMORY_DATOM_INDEXES.md` is not a prerequisite for the JavaScript API.
MemoryStore already supports the complete database contract under js/wasm.

The typed-datom-tree work remains important:

- It removes binary encode/decode from the in-memory hot path.
- It avoids mirroring Badger’s physical keys in browser memory.
- It provides the natural substrate for future `datoms` and `seekDatoms`
  JavaScript APIs.
- It improves a backend whose current wasm runtime is already slower than
  native execution.

The JavaScript API should launch against the current MemoryStore contract and
adopt typed trees behind that contract when implemented. The public
JavaScript API does not depend on either physical representation.

---

## 16. Staged implementation

### Stage 0 — boundary contract

1. Define TypeScript value, identity, basis, database-value, transaction-data,
   and error contracts.
2. Add Go round-trip tests for every boundary value.
3. Define the transaction-report Go type and its relationship to the existing
   commit path.
4. Reuse the explicit `SnapshotBasis` design for empty/latest/as-of/history.
5. Lock the worker request/event protocol with contract fixtures.

### Stage 1 — wasm worker core

1. Add `cmd/janus-wasm`.
2. Add the worker loader and handle registry.
3. Implement `createConn`, `close`, and `db`.
4. Implement identity and value conversion.
5. Implement transaction arrays/entity maps and transaction reports.
6. Implement `q`, `pull`, `pullMany`, `asOf`, and `history`.
7. Implement listener delivery for committed transaction reports.
8. Propagate structured errors and worker termination.

### Stage 2 — persistence and packaging

1. Add in-memory seekable JDZL export.
2. Add JDZL import from `Uint8Array`.
3. Add EDN import/export for diagnostics.
4. Produce ESM, declarations, Worker code, wasm, and matching
   `wasm_exec.js`.
5. Add package metadata and reproducible build commands.

### Stage 3 — JavaScript contract suite

1. Run the package in Node Worker context.
2. Run the package in a real browser Worker.
3. Share semantic fixtures with the Go backend contract tests.
4. Add lifecycle, concurrency, listener, and persistence tests.
5. Record wasm size, startup, boundary conversion, transaction, query, Pull,
   and persistence benchmarks.

### Stage 4 — broader DataScript-shaped surface

Only after the initial API contract is stable:

- `dbWith` backed by a real overlay/fork design.
- Direct typed `datoms` / `seekDatoms`.
- Live query subscriptions from `LIVE_QUERY_SUBSCRIPTIONS.md`.
- Prepared query handles from `PREPARED_QUERIES.md`.
- Filtered database values.
- Optional IndexedDB/OPFS live stores.
- Streaming result cursors for large relations.

---

## 17. Correctness test matrix

### Values

- String, empty string, int64 boundaries, float64, bool.
- Date with nanosecond-to-millisecond conversion documented.
- Empty and non-empty `Uint8Array`.
- Identity seed and full L85 identity.
- Keyword distinct from keyword-looking string.
- Symbol distinct from string.
- ElementID with values above JavaScript’s safe integer range.
- Nested vectors and all supported vector element types.
- Invalid and unsupported values reject.

### Transactions

- Entity map with explicit identity.
- Add/Set/Remove for CardinalityOne.
- Add/Remove/add-wins for CardinalityMany.
- RGA append/remove/set for CardinalityVector.
- Schema validation rejects before commit.
- Multi-item atomic failure.
- Transaction report contains actual committed datoms.
- Listener order follows commit publication order.
- Listener callback never runs before transaction completion.

### Database values

- Empty basis.
- `db(conn)` remains pinned after later commits.
- `dbBefore` and `dbAfter` return different resolved states.
- AsOf and History preserve Go semantics.
- Closed connection invalidates dependent handles.

### Query and Pull

- Query without inputs.
- Scalar, tuple, collection, and relation inputs.
- Identity, keyword, symbol, date, bytes, ElementID, and vector results.
- Aggregates, subqueries, NOT, OR/fallback, order, and limit.
- Pull explicit, wildcard, nested reference, and missing entity.
- Parse, planning, execution, iterator, and Pull errors propagate.

### Persistence

- JDZL export/import byte transport through `Uint8Array`.
- Complete datom history survives round-trip.
- CRDT Remove and RGA AfterRef survive round-trip.
- Imported basis advances connection state.
- Corrupt/truncated JDZL rejects with no reported success.
- Repeated export/import does not change query results.

### Worker lifecycle

- Concurrent requests resolve to the correct request IDs.
- Close with outstanding work.
- Worker termination rejects outstanding Promises.
- Listener replacement and idempotent removal.
- No callbacks after close.
- Repeated create/close does not leak Go or JavaScript handles.

---

## 18. Performance measurements

Measure before optimizing:

- wasm download size and compressed transfer size.
- Worker and Go runtime startup time.
- Connection creation time.
- Boundary conversion cost by value type and tuple count.
- Transaction time by datom count.
- Query time separated into Go execution and result conversion.
- Pull and PullMany conversion time.
- JDZL export/import throughput and peak memory.
- Retained memory after close.

The existing native-vs-wasm benchmark is an engine baseline, not a
JavaScript-API benchmark. The new measurements must include Worker messaging,
`syscall/js`, structured cloning, and JavaScript object creation.

Large-result performance must be measured with complete relations. Benchmarks
must not truncate model data or result tuples to improve presentation.

---

## 19. Recommendation

Build a Worker-backed, asynchronous, DataScript-shaped Janus package with:

- Janus identities and ElementIDs exposed explicitly.
- Immutable database values implemented as pinned bases.
- `:db/add`, `:db/set`, and `:db/remove` transaction data.
- EDN query/Pull strings executed entirely in Go.
- Central typed JavaScript value conversion.
- Real transaction reports produced by the Go commit path.
- JDZL `Uint8Array` persistence.
- No claim of drop-in DataScript compatibility.

Do not wait for typed memory indexes, IndexedDB/OPFS, prepared queries, or live
query subscriptions before building the first boundary contract. Those
features should integrate through their canonical Go APIs when implemented,
not be independently recreated in the TypeScript wrapper.
