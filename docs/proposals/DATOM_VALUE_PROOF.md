# A datom's value, as a proof

## Sources

- The code: `datalog/value_encoding.go` (`NormalizeValue`, `normalize`, `normalizeVector`, `ValidateValue`, `Type`, `EncodeValue`), `datalog/compare.go` (`IsVector`, `CompareValuesTagOrder`), `datalog/storage/database.go` (`Transaction.Add`, `Transaction.Remove`, `Transaction.Set`, `Transaction.Retract`, `validateValueStorable`, `toAnySlice`, `memberKey`, `Transaction.Commit`, `convertInputValue`), `datalog/storage/convenience.go` (`Database.Assert`), `datalog/storage/key_encoder_binary.go` (`BinaryKeyEncoder.EncodeValueBytes`, `encodeBoundComponent`), `datalog/storage/matcher.go` (`encodeValueForSearch`), `datalog/storage/badger_store.go` (`BadgerTx.Assert`, `BadgerTx.Retract`), `datalog/storage/memory_store.go` (`memoryStoreTx.Commit`, `assertMemoryDatom`, `retractMemoryDatom`), `datalog/storage/memory_tree_store.go` (`memoryTreeStoreTx.Assert`, `memoryTreeStoreTx.Retract`, `matchingStoredDatoms`), `datalog/storage/store_version.go` (`versionBuilder.addDatom`), `datalog/storage/datom_tree.go` (`datomTree.compare`), `datalog/storage/datom_compare.go` (`compareComponent`), `datalog/storage/memory_tree_iterator.go` (`boundProbeDatom`), `datalog/query/validate_values.go` and `datalog/query/pull.go` (the query side's callers of `ValidateValue`), `cmd/datalog/main.go` (`addInteractiveData`, `parseAddLine`, `ednNodeToGo`).
- The design: `docs/reference/CRDT.md`, the write semantics of the three cardinalities.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- The **scalars** S are the values `ValidateValue`'s type switch answers nil for, after `NormalizeValue`: a string, an int64, a float64 other than NaN, a bool, a time.Time, a []byte, a non-nil Identity, a Keyword, a Symbol, an ElementID by value or by non-nil pointer.
- A **vector** is any Go slice but []byte (`IsVector`).
- The **value domain** Q is S together with every vector whose elements are in Q: what `ValidateValue` admits.
- The **datom value domain** D is S: the values a stored datom carries (P5).
- A **write** is `Add`, `Remove`, `Retract` or `Set`. The **datom values of a write** are, for `Add`, `Remove`, `Retract` and `Set` under cardinality one, the value given; for `Set` under cardinality many or vector, each element of the slice given.
- The present check **C_old(v)** is `ValidateValue(v) == nil`, asked by `validateValueStorable` of the value given, before `Set` unpacks it. The proposed check **C_new(v)** is `ValidateDatomValue(v) == nil`, asked of each datom value of the write.

## 2. Preconditions

- **P1.** `Add` appends `Datom{V: v}`, v as given after `NormalizeValue`, under every cardinality arm and the schemaless arm. `Remove` appends a tombstone with `V: v` under cardinality one and many; under cardinality vector it opens an EAVT scan bound on (e, a, v) and appends a tombstone carrying the matched element's own value, or returns nil having appended nothing when none matches. `Retract` appends `Datom{V: v}` to the transaction's retracts. `Set` appends `Datom{V: v}` under cardinality one, and under many and vector unpacks the slice through `toAnySlice` and appends one datom per element; under many it first keys its membership map through `memberKey`, which classifies each element through `Type`, and under vector it compares each element with the stored list through `ValuesEqual`, which handles a vector. `Database.Assert`, `AddEntity` and `AddMap` write through `Add`.
- **P2.** `NormalizeValue` maps a vector to a vector of the same Go type and a non-vector to a non-vector: the arms of `normalize` and `normalizeVector`.
- **P3.** `validateValueStorable` has four callers (`gopls references`): `Add`, `Remove`, `Set`, on the whole value before its cardinality switch, and `Retract`. It asks `ValidateValue`.
- **P4.** `ValidateValue`'s other callers are the query side's: `convertInputValue` (a query input), `validateClauseConstants` and its arms (a query's and a rule body's constants), `PullPattern.Validate` (a pull default), and its own recursion over a vector's elements. None is a write.
- **P5.** `Type` (`datalog/value_encoding.go`) has a case for each scalar of S and none for a slice other than []byte; its default panics naming the type. `BinaryKeyEncoder.EncodeValueBytes` classifies every value it encodes through `Type`: directly when the encoder has no compression threshold, and through `EncodeValue` otherwise, whose string and []byte arms are the only ones that do not call `Type`. `CompareValuesTagOrder` (`datalog/compare.go`) has one dispatch over the same-type scalar pairs; for every other pair its tail runs `at, bt := Type(a), Type(b)` before its tag switch and before the `CompareValues` call that follows it, so a pair with a vector on either side reaches `Type` with the vector. Its doc states the same: a value `Type` refuses "panics exactly as that composition does".
- **P6.** `Transaction.Commit` writes the transaction's retracts, then its datoms, then one `:db/txInstant` datom, through one store transaction. On Badger, `BadgerTx.Assert` encodes each datom through `assertDatom` and `EncodeValueBytes`, and `BadgerTx.Retract` encodes each through `retractDatom` and `encodeValueForSearch`, which is `EncodeValueBytes` without the blob. On the emulator, `memoryStoreTx.Commit` does the same through `assertMemoryDatom` and `retractMemoryDatom`. On the tree store, `memoryTreeStoreTx.Assert` inserts each datom into the eight index trees, comparing it with the stored datoms through `compareComponent`, which compares V through `CompareValuesTagOrder`; VAET's first component is V. `memoryTreeStoreTx.Retract` seeks each retract's (E, A, V) on EAVT with a probe datom holding the value as given (`matchingStoredDatoms`, `boundProbeDatom`).
- **P7.** A scan bound's V is encoded through `encodeBoundComponent`, which is `encodeValueForSearch`, on Badger and the emulator; on the tree store it is placed in a probe datom (`boundProbeDatom`) and compared through `compareComponent`.
- **P8.** The CLI's `.add` loop calls `tx.Add` once per line, prints an error it answers as `Error: …` and reads the next line; `parseAddLine` reads the value through `ednNodeToGo`, which builds a `[]interface{}` from a vector form.

## 3. Theorems

**T1 (a vector as a datom's value never commits).** On every backend, a transaction one of whose datoms carries a vector as V does not commit: it panics in `Type`.

**T2 (no committing write changes).** For a write whose datom values are all scalars, C_new answers what C_old answered of each, and the write's outcome is unchanged.

**T3 (what changes).** Exactly these calls change outcome, each from a panic or a silent no-op to an error at the call with nothing appended:

- `Add`, `Remove` under cardinality one and many, `Retract`, and `Set` under cardinality one, given a vector: a panic at `Commit` (T1).
- `Set` under cardinality many, given a slice with a vector among its elements: a panic inside `Set`, where `memberKey` classifies the element through `Type` while the membership map is built (P1), before any datom is appended.
- `Set` under cardinality vector, given the same: a panic at `Commit` (T1), since the prefix diff compares the element through `ValuesEqual` (P1) and appends it as given.
- `Remove` under cardinality vector, given a vector: on Badger and the emulator a panic inside `Remove`, at the bound's encoding (P7); on the tree store a panic when the (E, A) holds a datom, whose V the probe is compared with, and a silent no-op when it holds none (L3).
- `Retract` given a vector, on the tree store: a panic at `Commit` when the (E, A) holds a datom and a silent no-op when it holds none (L3).

The two no-op cases are the only outcomes that were not panics. Each removed nothing, since no datom carries the value named (T1); the error says why.

**T4 (the CLI).** An add line whose value is a vector prints `Error: …` and the session continues; the block's other lines commit (P8, T3).

**T5 (the query side is unchanged).** Every query-side boundary keeps `ValidateValue` (P4), whose answer is unchanged for every value: `ValidateDatomValue`'s answer for a non-vector, the element walk for a vector, the same cases in the same order.

**T6 (cost).** `validateValueStorable` keeps its one call, now to `ValidateDatomValue`, which is `ValidateValue`'s switch. A scalar write runs the same switch. A vector write is refused by one `IsVector` kind read where a walk over its elements ran. `Set`'s many and vector arms ask the switch once per element, where `ValidateValue` recursed once per element. No frame is added on a scalar write; the compiler's inlining report after the change is the measurement.

## 4. Lemmas

### L1. A vector reaches `Type` on every backend (T1)

Let d be a datom with a vector V in a transaction that reaches `Commit`. On Badger and the emulator the store's assert encodes d.V through `EncodeValueBytes`, which classifies it through `Type`, which panics on a slice (P5, P6). On the tree store d is inserted into VAET, whose first component is V. If the tree holds any datom, the descent compares d.V with a stored V through `CompareValuesTagOrder`; its dispatch matches no pair holding a slice, and its tail's `Type(a), Type(b)` runs on the pair before anything else (P5), so `Type` is called with the slice. If the tree holds none, d is its first datom, and the commit's own `:db/txInstant` datom, asserted next in the same store transaction (P6), is compared with d at V on its own insert, through the same tail. Either way `Type` is reached with a slice and panics. ∎

### L2. The check agrees on every scalar (T2, T5)

`ValidateDatomValue`'s switch is `ValidateValue`'s switch over the same cases in the same order, and `ValidateValue` becomes that switch for a non-vector and the element walk for a vector. For a non-vector v the two functions answer alike before and after. For `Set` under many or vector, `ValidateValue(v)` on the slice walked each element through `ValidateValue`: a scalar element passed the switch, and a vector element passed the walk and reached `Type`, inside `Set` through `memberKey` under many (P1) and at `Commit` under vector (L1). After the change each element passes through `ValidateDatomValue`: a scalar element passes the same switch; a vector element is refused. So a slice of scalars commits as before, and the query side, which keeps `ValidateValue`, answers as before. ∎

### L3. The no-op cases (T3)

`Remove` under cardinality vector seeks (e, a, v) on EAVT. On the tree store the probe's V is compared only with datoms sharing its E and A, EAVT's first two components; under an (E, A) holding no datom the run is empty, no V is compared, and `Remove` returns nil having appended nothing (P1). `Retract` on the tree store seeks the same way at `Commit` through `matchingStoredDatoms`, removes nothing, and the commit publishes. On Badger and the emulator the bound's V is encoded before any seek (P7), so the panic comes first. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named.

- A CLI test on Badger: inside `.add`, a line whose value is a vector form is answered with `Error: …`, the valid line after it commits, and the session exits 0 (T4).
- A `db` test over every backend the build offers: `Add`, `Remove`, `Retract` and `Set` given a vector on an attribute the schema does not declare answer an error and append nothing, and the transaction's other writes commit and read back; on a declared vector attribute `Add` given a vector answers an error while `Add` per element and `Set` with the slice write the list; `Set` with a slice holding a vector answers an error on a vector attribute and on a many attribute (T2, T3).
- A `datalog` test: `ValidateDatomValue` refuses every vector shape, naming the rule, and answers for each scalar and each non-value as `ValidateValue` does; `ValidateValue` admits a vector and a nested vector and refuses a vector holding a non-value (T5).
- Every write and read test of `storage` and `db`, unchanged by T2.
- The compiler's inlining report for `validateValueStorable` and `ValidateDatomValue` (T6).

## 6. The change the proof licenses

- `datalog`: `ValidateDatomValue(v Value) error`, the scalar switch, refusing a vector with an error that states the rule; `ValidateValue` is `ValidateDatomValue` for a non-vector and the element walk for a vector.
- `storage`: `validateValueStorable` asks `ValidateDatomValue`. `Set` asks it of the value given in its cardinality-one arm and of each element in its many and vector arms, after unpacking. `Add`, `Remove` and `Retract` keep their call.
- The CLI is unchanged: `tx.Add`'s error is the line's.

## 7. What the proof does not cover

- The import and replay paths assert datoms without the write API. A dump's values were encoded (P5), so no vector is among them; a hand-written EDN dump carrying a vector value is outside these sources and behaves as today.
- `schema.ValidateDatom`'s answer for a vector on a declared attribute is not read here. Where it refused a vector before the datom reached the encoder, the refusal's text becomes the domain's, since `validateValueStorable` runs first in every arm; the outcome, an error, is unchanged.
