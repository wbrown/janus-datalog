# Plan: Make ElementID a First-Class Value in the Query Engine

## Context

The `InternedTupleBuilder` stores Tx values as `*datalog.ElementID` (pointer) in tuples to avoid per-datom interface boxing allocations. User input bindings pass `datalog.ElementID` (value). Many downstream consumers only handle one form, or only handle the legacy `uint64` representation. This causes silent failures: `TestElementIDBinding_ScalarInput` passes an ElementID as a scalar binding and gets 0 results instead of 1.

The pointer optimization is valid (16-byte struct boxing matters at scale) and stays. The fix is to teach all consumers about both `ElementID` and `*ElementID`.

## Changes Already Made (in working tree, uncommitted)

- `datalog/compare.go` — Added `derefElementID` helper (unexported); updated `CompareValues` and `ValuesEqual` to handle `*ElementID`
- `datalog/executor/tuple_key.go` — Added `ElementID` and `*ElementID` cases to `hashValue`
- `datalog/storage/elementid_binding_test.go` — Reproduction test (currently failing)

## Files to Modify

| File | Changes | Priority |
|------|---------|----------|
| `datalog/compare.go` | Export `derefElementID` → `DerefElementID` | Prerequisite |
| `datalog/value_encoding.go` | Add `*ElementID` to `Type()` (line 56) and `ValueBytes()` (line 108) | Critical |
| `datalog/storage/matcher.go` | `chooseIndex` (line 662): handle ElementID; `matchesDatom` (line 683): deref `*ElementID` | Critical |
| `datalog/storage/hash_join_matcher.go` | `valueToHashKey` (line 452): add ElementID; `chooseIndexForValues` TAEV (line 394): fill in; `compareJoinKeys` (line 669): add ElementID | Critical |
| `datalog/storage/matcher_iterator_reusing.go` | `calculateSeekKey` (line 281): extract T; "moved past" (line 178): handle `*ElementID` | Medium |
| `datalog/storage/simple_batch_scanner.go` | `valueToKey` (line 106) and `buildKey` TAEV (line 249): add ElementID | Medium |
| `datalog/executor/join.go` | Tx detection (line 339) and extraction (lines 367, 390): add ElementID types | Medium |
| `datalog/executor/constraints_impl.go` | `equalityConstraint` (line 51): use `DerefElementID` | Medium |
| `datalog/query/history.go` | `TxRangePredicate.Eval` (line 121): add ElementID cases, remove broken duck-type | Low |

## Execution Order

### Phase 1: Write ALL tests (expected to fail)

Write every test listed in the "New Tests" section below. Run them to confirm they all fail. This proves the bugs exist before any fix code is written.

```bash
# Must ALL fail
go test ./datalog/ -run "TestElementIDPointerValue" -v -count=1
go test ./datalog/storage/ -run "TestElementIDBinding" -v -count=1
go test ./datalog/storage/ -run "TestCompareJoinKeys" -v -count=1
```

### Phase 2: Apply fixes (tests start passing)

Apply the implementation changes listed below. After each file is modified, re-run tests to see which ones start passing. Continue until all tests pass.

### Phase 3: Regression verification

Run the full suite to confirm no regressions.

---

## Implementation Details

### 1. `datalog/compare.go` — Export helper

Rename `derefElementID` → `DerefElementID`. Update the two call sites at lines ~70 and ~297.

### 2. `datalog/value_encoding.go` — Prevent panics

**`Type()`** (after line 56 `case ElementID:`): add `*ElementID` case returning `TypeElementID`. Place it near the other pointer cases at lines 30-37.

**`ValueBytes()`** (after line 108 `case ElementID:`): add `*ElementID` case: dereference, return `val.Bytes()`.

### 3. `datalog/storage/matcher.go` — Core matching

**`chooseIndex()` lines 662-670**: Replace the uint64-only TAEV path. Use `datalog.DerefElementID` and existing `NewTxFromElementID()` (`types.go:41`):

```go
} else if tx != nil {
    var storageTx Tx
    if txID, ok := tx.(uint64); ok {
        storageTx = NewTxFromUint(txID)
    } else if eid, ok := datalog.DerefElementID(tx); ok {
        storageTx = NewTxFromElementID(eid)
    }
    if storageTx != (Tx{}) {
        start, end := encoder.EncodePrefixRange(TAEV, storageTx[:])
        return TAEV, start, end
    }
}
```

**`matchesDatom()` line 683**: Add `*ElementID` dereference alongside existing `*uint64`:

```go
if ptr, ok := tx.(*datalog.ElementID); ok {
    tx = *ptr
}
```

### 4. `datalog/storage/hash_join_matcher.go`

**`valueToHashKey()` lines 455-457**: Add ElementID deref after the existing `*uint64` deref:

```go
if eid, ok := datalog.DerefElementID(v); ok {
    return eid.String()
}
```

**`chooseIndexForValues()` TAEV case lines 394-398**: Fill in the empty case body:

```go
case TAEV:
    if tx != nil {
        if txID, ok := tx.(uint64); ok {
            storageTx := NewTxFromUint(txID)
            startParts = append(startParts, storageTx[:])
            endParts = append(endParts, storageTx[:])
        } else if eid, ok := datalog.DerefElementID(tx); ok {
            storageTx := NewTxFromElementID(eid)
            startParts = append(startParts, storageTx[:])
            endParts = append(endParts, storageTx[:])
        }
    }
```

**`compareJoinKeys()` after line 688**: Add ElementID case:

```go
case datalog.ElementID:
    if bEid, ok := datalog.DerefElementID(b); ok {
        return aVal.Compare(bEid)
    }
case *datalog.ElementID:
    if aVal != nil {
        if bEid, ok := datalog.DerefElementID(b); ok {
            return (*aVal).Compare(bEid)
        }
    }
```

### 5. `datalog/storage/matcher_iterator_reusing.go`

**`calculateSeekKey()` lines 280-310**: Add T extraction (mirrors existing E, A, V extraction). Change `var e, a, v interface{}` to `var e, a, v, tx interface{}`. Add:

```go
// Get T value
if c, ok := it.pattern.GetT().(query.Constant); ok {
    tx = c.Value
} else if sym, ok := it.pattern.GetT().(query.Variable); ok {
    if idx, found := colIndex[sym.Name]; found && idx < len(bindingTuple) {
        tx = bindingTuple[idx]
    }
}
```

Change line 309 from `it.matcher.chooseIndex(e, a, v, nil)` to `it.matcher.chooseIndex(e, a, v, tx)`.

**"Moved past" case 3 lines 178-189**: Consolidate using `DerefElementID`:

```go
case 3:
    if eid, ok := datalog.DerefElementID(bindingTuple[0]); ok {
        if datom.Tx != eid {
            movedPast = true
        }
    } else if expectedTx, ok := bindingTuple[0].(uint64); ok {
        if datom.Tx.Lamport != expectedTx {
            movedPast = true
        }
    }
```

### 6. `datalog/storage/simple_batch_scanner.go`

**`valueToKey()` lines 107-113**: Add ElementID deref alongside existing `*uint64`:

```go
} else if eid, ok := datalog.DerefElementID(v); ok {
    v = eid
```

Add `case datalog.ElementID:` in the switch at line 115: `return val.String()`.

**`buildKey()` TAEV case lines 249-255**: Add ElementID path:

```go
case 4: // TAEV
    if eid, ok := datalog.DerefElementID(value); ok {
        txBytes := NewTxFromElementID(eid)
        parts := [][]byte{txBytes[:]}
        return encoder.EncodePrefix(s.index, parts...)
    } else if tx, ok := value.(uint64); ok {
        txBytes := NewTxFromUint(tx)
        parts := [][]byte{txBytes[:]}
        return encoder.EncodePrefix(s.index, parts...)
    }
```

### 7. `datalog/executor/join.go` — Tx dedup

**Tx detection line 339-340**: Add ElementID types to the type switch:

```go
case uint64, int64, int, datalog.ElementID, *datalog.ElementID:
```

**Tx extraction lines 367-375 and 390-398**: Add ElementID cases extracting `.Lamport` for the uint64-based comparison:

```go
case datalog.ElementID:
    txID = v.Lamport
case *datalog.ElementID:
    if v != nil { txID = v.Lamport }
```

### 8. `datalog/executor/constraints_impl.go`

**`equalityConstraint.Evaluate()` lines 51-58**: Use `DerefElementID`:

```go
case 3:
    if eid, ok := datalog.DerefElementID(c.value); ok {
        return datom.Tx == eid
    }
    if tx, ok := c.value.(uint64); ok {
        return datom.Tx.Lamport == tx
    }
```

### 9. `datalog/query/history.go`

**`TxRangePredicate.Eval()` lines 121-135**: Add explicit ElementID cases before the `default`. Remove the broken `interface{ GetLamport() uint64 }` duck-type (ElementID has a `.Lamport` field, not a method):

```go
case datalog.ElementID:
    lamport = v.Lamport
case *datalog.ElementID:
    if v != nil { lamport = v.Lamport }
```

## New Tests

### Unit Tests: `datalog/compare_test.go` — Pointer/Value Cross-Comparison

Add `TestElementIDPointerValueCrossComparison` covering the `*ElementID` ↔ `ElementID` gaps in existing tests (which only test value-vs-value):

| Assertion | Purpose |
|-----------|---------|
| `CompareValues(&a, a) == 0` | Pointer left, value right |
| `CompareValues(a, &a) == 0` | Value left, pointer right |
| `CompareValues(&a, &b) == 0` | Both pointers, equal |
| `CompareValues(&a, &c) < 0` | Both pointers, different |
| `ValuesEqual(&a, a) == true` | Pointer/value mix |
| `ValuesEqual(a, &a) == true` | Value/pointer mix |
| `ValuesEqual(&a, &c) == false` | Both pointers, different |
| `ValuesEqual((*ElementID)(nil), a) == false` | Nil pointer vs value |

### Unit Tests: `datalog/value_encoding_test.go` — `*ElementID` Encoding

Add `TestElementIDPointerValueEncoding` verifying `*ElementID` doesn't panic in `Type()` and `ValueBytes()`:

| Assertion | Purpose |
|-----------|---------|
| `Type(&eid) == TypeElementID` | Pointer returns correct type |
| `ValueBytes(&eid)` matches `ValueBytes(eid)` | Pointer produces same bytes as value |
| `Type((*ElementID)(nil))` doesn't panic | Nil pointer handling |

### Integration Tests: `datalog/storage/elementid_binding_test.go`

All tests follow the same data setup pattern as the existing `TestElementIDBinding_ScalarInput`: write 2-3 entities in separate transactions, extract ElementIDs from query results, then use them as bindings.

#### `TestElementIDBinding_CollectionInput`
- **Purpose**: Collection binding `[:in $ [?tx ...]]` with multiple ElementIDs
- **Query**: `[:find ?e ?v :in $ [?tx ...] :where [?e :person/name ?v ?tx]]`
- **Input**: Slice of 2 ElementIDs (alice's tx, bob's tx)
- **Assert**: Returns 2 results (both datoms match)
- **Then**: Pass only alice's tx → returns 1 result
- **Exercises**: `hashValue(ElementID)`, `ValuesEqual(*ElementID, ElementID)`, collection→relation conversion

#### `TestElementIDBinding_RelationInput`
- **Purpose**: Relation binding `[:in $ [[?e ?tx] ...]]` with ElementID + Identity pairs
- **Query**: `[:find ?v :in $ [[?e ?tx] ...] :where [?e :person/name ?v ?tx]]`
- **Input**: `[][]interface{}{{aliceIdentity, aliceTx}, {bobIdentity, bobTx}}`
- **Assert**: Returns 2 results with correct names
- **Exercises**: Multi-symbol hash join with ElementID, `valueToHashKey(ElementID)`, `compareJoinKeys(ElementID)`

#### `TestElementIDBinding_TxJoin`
- **Purpose**: Two patterns joined on `?tx` where Tx symbol holds `*ElementID`
- **Setup**: 2 entities with 2 attributes each, written in separate transactions
- **Query**: `[:find ?name ?age :where [?e1 :person/name ?name ?tx] [?e2 :person/age ?age ?tx]]`
- **Assert**: Each transaction's name/age pairs are joined correctly
- **Exercises**: Hash join on `*ElementID` symbol (both sides from storage), `TupleKey` hashing

#### `TestElementIDBinding_ComparisonPredicate`
- **Purpose**: `[(= ?tx ?target-tx)]` predicate with ElementID binding
- **Query**: `[:find ?e ?v :in $ ?target-tx :where [?e :person/name ?v ?tx] [(= ?tx ?target-tx)]]`
- **Input**: Alice's ElementID
- **Assert**: Returns 1 result (Alice only)
- **Exercises**: `CompareValues(*ElementID, ElementID)`, `equalityConstraint` with `DerefElementID`

#### `TestElementIDBinding_HistoryQuery`
- **Purpose**: `d.History()` returns all writes with ElementID Tx values
- **Setup**: Write "Alice" then overwrite with "Alice2" (CardinalityOne, LWW)
- **Query**: `d.History().Query([:find ?v ?tx :where [?e :person/name ?v ?tx]])`
- **Assert**: Returns 2 results (both historical values), each `?tx` is an ElementID
- **Then**: Use one of the returned ElementIDs as a scalar binding to filter
- **Exercises**: Full round-trip — ElementID from storage → user code → binding → back to storage

#### `TestElementIDBinding_MultipleEntitiesSameTx`
- **Purpose**: Multiple entities written in the same transaction share one ElementID
- **Setup**: Write alice and bob in the SAME transaction
- **Query**: `[:find ?name :in $ ?tx :where [?e :person/name ?name ?tx]]`
- **Input**: The shared transaction's ElementID
- **Assert**: Returns 2 results (both Alice and Bob)
- **Exercises**: TAEV index selection with ElementID (`chooseIndex`), single-tx scan efficiency

### Unit Tests: `datalog/storage/merge_join_test.go` — `compareJoinKeys` with ElementID

Extend existing `TestCompareJoinKeys` (line 496) with ElementID cases:

| Left | Right | Expected | Purpose |
|------|-------|----------|---------|
| `ElementID{100,5}` | `ElementID{100,5}` | `0` | Value-value equal |
| `&ElementID{100,5}` | `ElementID{100,5}` | `0` | Pointer-value equal |
| `ElementID{100,5}` | `&ElementID{200,5}` | `-1` | Value-pointer less |
| `&ElementID{200,5}` | `&ElementID{100,5}` | `1` | Pointer-pointer greater |

## Verification

```bash
# New unit tests — pointer/value cross-comparison
go test ./datalog/ -run "TestElementIDPointerValue" -v -count=1

# New integration tests — all binding types
go test ./datalog/storage/ -run "TestElementIDBinding" -v -count=1

# compareJoinKeys extension
go test ./datalog/storage/ -run "TestCompareJoinKeys" -v -count=1

# Existing CRDT and cache tests — no regression
go test ./datalog/storage/ -run "TestCacheMatrix|TestMatcherCache|TestEACacheBypass" -v

# EATV vector transition tests — no regression
go test ./datalog/storage/ -run "TestEATV_VectorTransition|TestCountRepro" -v -count=5

# Full suite
go test ./...
```

---

## Implementation Record

**Status: COMPLETE** — All items implemented, tested, and committed as `8577edd` on `fix/elementid-first-class` branch.

### Additional fix not in original plan

- `datalog/query/tuple_builder_interned.go` — Added `sync.Mutex` to protect `txCache` map. The `ProjectFromPattern` fix caused `txCache` to be accessed from parallel goroutines during execution. Fixed with manual lock/unlock (not defer) for hot-path performance.

### Follow-on work

See `BUG_ELEMENTID_ASOF_THROUGHOUT.md` for the next phase: changing `Commit()`, `AsOf()`, `MatchAsOf()`, and internal `txID` fields from `uint64` to `ElementID` throughout the storage layer.
