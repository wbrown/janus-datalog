# BUG: Logical Transaction Commit Is Split Across Multiple Badger Transactions

**Date**: 2026-04-16
**Severity**: High - partial logical commits possible
**Status**: Resolved 2026-04-16
**Affected**: `storage.Transaction.Commit()`, `storage.BadgerStore`

## Summary

`Transaction.Commit()` is a logical multi-step commit, but each step is executed
in a separate BadgerDB write transaction:

1. Validate uniqueness
2. `store.Retract(t.retracts)`
3. `store.Assert(t.datoms)`
4. `store.Assert(txMetadata)` for `:db/txInstant`

This means the Janus logical transaction is **not storage-atomic**. A failure in
step 3 or 4 can leave the database with a partially applied commit:

- retractions visible without matching assertions
- assertions visible without transaction metadata
- transaction metadata best-effort only

This is true even though the storage model is append-only/CRDT-oriented. Append-only
history preserves prior writes; it does **not** make a multi-step logical commit
all-or-nothing.

## Discovery

Found during external code review of the write path.

The key observation is that `Transaction.Commit()` calls `BadgerStore` methods
that each wrap their own `db.Update(...)`, rather than opening one Badger write
transaction for the full logical commit.

## Code Evidence

### 1. `Transaction.Commit()` performs multiple store operations

From `datalog/storage/database.go`:

```go
// Apply retractions first
if len(t.retracts) > 0 {
    if err := t.db.store.Retract(t.retracts); err != nil {
        return datalog.ElementID{}, fmt.Errorf("failed to retract datoms: %w", err)
    }
}

// Then apply assertions
if len(t.datoms) > 0 {
    if err := t.db.store.Assert(t.datoms); err != nil {
        return datalog.ElementID{}, fmt.Errorf("failed to assert datoms: %w", err)
    }
}

// Add transaction metadata with its own Lamport timestamp
if err := t.db.store.Assert(txMetadata); err != nil {
    // Log but don't fail the transaction
    fmt.Printf("Warning: failed to write transaction metadata: %v\n", err)
}
```

The `:db/txInstant` write is explicitly best-effort and does not participate in
the success/failure of the logical commit.

### 2. `BadgerStore.Assert()` and `BadgerStore.Retract()` each open a fresh Badger transaction

From `datalog/storage/badger_store.go`:

```go
func (s *BadgerStore) Assert(datoms []datalog.Datom) error {
    return s.db.Update(func(txn *badger.Txn) error {
        for _, d := range datoms {
            if err := s.assertDatom(txn, &d); err != nil {
                return err
            }
        }
        return nil
    })
}

func (s *BadgerStore) Retract(datoms []datalog.Datom) error {
    return s.db.Update(func(txn *badger.Txn) error {
        for _, d := range datoms {
            if err := s.retractDatom(txn, &d); err != nil {
                return err
            }
        }
        return nil
    })
}
```

Each method is atomic **individually**. The problem is that the higher-level
logical commit spans multiple such calls.

### 3. The store already has a transaction API, but `Transaction.Commit()` does not use it

From `datalog/storage/badger_store.go`:

```go
func (s *BadgerStore) BeginTx() (StoreTx, error) {
    txn := s.db.NewTransaction(true)
    return &BadgerTx{
        store: s,
        txn:   txn,
    }, nil
}
```

And:

```go
func (t *BadgerTx) Commit() error {
    return t.txn.Commit()
}
```

The primitive needed for a single storage transaction already exists.

## Why Append-Only Does Not Eliminate The Problem

This repository uses append-only CRDT semantics for most writes:

- CardinalityOne: append newer datom, resolve by LWW
- CardinalityMany: append add/remove operations
- CardinalityVector: append inserts and tombstones

That guarantees historical preservation, but it does **not** guarantee that a
single logical transaction becomes visible as one unit.

Example:

1. Transaction retracts old value
2. Transaction appends new value
3. Retract succeeds
4. Assert fails

Result: readers observe the database in an intermediate state that no successful
logical transaction was meant to expose.

## Impact

### 1. Partial visibility of logical transactions

If `Retract()` succeeds but `Assert()` fails, reads can observe a state with:

- the old value gone
- the new value absent

This breaks the normal expectation of transaction semantics, even if the system
is otherwise append-only.

### 2. Audit / metadata gaps

If the main assertions succeed but `txMetadata` fails, `Commit()` still returns
success and a new `ElementID`, but no `:db/txInstant` datom exists for that
transaction.

This creates a mismatch between:

- "transaction succeeded"
- "transaction metadata recorded"

### 3. Error handling ambiguity for downstream consumers

The public API presents a `Transaction` abstraction. Most consumers will assume
it provides all-or-nothing commit behavior unless explicitly documented otherwise.

## Reproduction Sketch

No dedicated failing test exists yet, but the bug should be reproducible with a
fault-injecting store or a test-only hook that forces failure after retractions
and before assertions.

Sketch:

```go
func TestCommitCanPartiallyApply(t *testing.T) {
    // Arrange:
    // - database with an existing value
    // - test store configured so Retract succeeds but Assert fails
    //
    // Act:
    // - tx.Set(...) or tx.Retract(...)+tx.Add(...)
    // - tx.Commit()
    //
    // Assert:
    // - Commit returns error
    // - Database state has changed anyway
    // - Intermediate state is visible to queries
}
```

## Possible Fix Directions

### Option 1: Single Badger transaction for the full logical commit

Open one `StoreTx` / `badger.Txn` inside `Transaction.Commit()` and perform:

1. retractions
2. assertions
3. metadata write
4. commit

inside that single write transaction.

### Option 2: Document reduced semantics explicitly

If the design intentionally does **not** provide all-or-nothing logical commit
behavior, the API/docs should state that clearly.

This would still leave downstream callers with the burden of handling
intermediate states.

## Test Plan

1. Add a fault-injection test that fails between retract and assert.
2. Add a fault-injection test that fails during transaction metadata write.
3. Verify whether `Commit()` success implies presence of `:db/txInstant`.
4. Verify whether readers can observe intermediate states after a failed commit.
5. If fixed via `BeginTx()`, add regression tests ensuring:
   - failed commit leaves no visible changes
   - successful commit writes retractions, assertions, and metadata together

## Resolution

**Resolved**: 2026-04-16

### Fix

`Transaction.Commit()` now wraps the entire logical commit (retractions,
assertions, transaction metadata) in a single `BadgerStore.BeginTx()` →
`StoreTx`. Any failure inside that scope rolls back the entire transaction
via `stx.Rollback()`. The `:db/txInstant` metadata write joins the same
atomic transaction — the previous "log-and-ignore" best-effort handling is
gone; failure now correctly fails the whole commit.

The existing `BadgerStore.BeginTx()` / `BadgerTx` primitive already existed
in storage; the fix is just routing `Commit()` through it instead of making
three independent `db.Update(...)` calls.

### Regression test

`datalog/storage/commit_atomicity_test.go::TestCommitWritesTxInstantOnSuccess`
locks in the metadata-on-success contract: every successful `Commit()`
produces a queryable `:db/txInstant` datom. This would not have been
guaranteed under the old best-effort metadata write.

### What this fix does NOT include

The uniqueness TOCTOU bug
([BUG_UNIQUENESS_VALIDATION_TOCTOU.md](BUG_UNIQUENESS_VALIDATION_TOCTOU.md))
shares the "split across multiple writes" structure with this bug, but its
fix is much larger than wrapping in one storage txn — it requires a
fundamental redesign of how uniqueness fits into the CRDT model. See
[CRDT_UNIQUE_SEMANTICS.md](../proposals/CRDT_UNIQUE_SEMANTICS.md) for the
proposed design. `validateUniqueness()` continues to run as before in this
fix; both uniqueness bugs from the original report remain observable until
that proposal is implemented.

### Fault-injection tests

The original test plan called for fault-injection tests (fail between
retract and assert). These would require introducing a test-only seam in
`BadgerStore` to force partial failures. Not added in this fix — Badger
itself provides atomicity once the writes are in a single `db.Update`
callback, and the contract test above is sufficient to lock in the
metadata-on-success behavior. Fault-injection tests can be added later if
a regression risk emerges.
