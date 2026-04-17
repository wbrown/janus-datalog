# BUG: Uniqueness Validation Is a TOCTOU Check Outside the Write Transaction

**Date**: 2026-04-16
**Severity**: High - uniqueness races and false rejections possible
**Status**: Resolved 2026-04-17 — validateUniqueness deleted on `feature/crdt-unique-resolution`

> **Note (2026-04-16)**: The bugs described below were real, but the proposed
> "fix `validateUniqueness`" framing was wrong for this codebase's CRDT-aligned
> architecture. The right resolution was a read-time (A, V)-LWW redesign that
> removes write-time enforcement entirely. See
> [docs/reference/CRDT_UNIQUE_SEMANTICS.md](../reference/CRDT_UNIQUE_SEMANTICS.md)
> for the design discussion and target model.
>
> **Resolution (2026-04-17, Commit 1 of the redesign)**: `validateUniqueness`
> and its call site in `Transaction.Commit` have been deleted. Both the TOCTOU
> race and the retract-and-reassign rejection described below no longer occur
> because there is no write-time gate. The three Datomic-strict tests that
> encoded the old behavior (`TestSchemaUniquenessValue`,
> `TestSchemaUniquenessWithinTransaction`, `TestSchemaUniquenessIdempotent`)
> were deleted in the same commit. At this point in the redesign, read-side
> (A, V)-LWW resolution is not yet implemented — reads for unique attributes
> with multiple claimants will return all claimants. Subsequent commits
> (Commits 2–5 of the redesign, per CRDT_UNIQUE_SEMANTICS.md) introduce the
> read-time resolution layer that makes unique attributes behave correctly
> again under the new semantics.
**Affected**: `storage.Transaction.validateUniqueness()`

## Summary

Uniqueness validation runs **before** any writes are committed and executes as a
read query against the current store state. It is therefore a classic
time-of-check/time-of-use (TOCTOU) validation:

1. Query the database to see if a unique value already exists
2. If not, proceed
3. Later, write the datoms in a separate Badger transaction

Two problems fall out of this:

1. **Concurrent commits can race**: two transactions can both observe "value does
   not exist" and both proceed to write it.
2. **Pending retractions are ignored**: a transaction that removes a unique
   value from one entity and assigns it to another in the same logical commit
   can be rejected even though the final state would be valid.

## Discovery

Found during external code review of the write path and uniqueness enforcement.

The code review also noted that there are tests for:

- uniqueness within one transaction
- uniqueness against already-committed data

but no obvious tests for:

- concurrent uniqueness races
- move/reassign semantics when the same commit contains both retracts and asserts

## Code Evidence

### 1. Validation runs before writes

From `datalog/storage/database.go`:

```go
func (t *Transaction) Commit() (datalog.ElementID, error) {
    // Validate uniqueness constraints before committing
    if err := t.validateUniqueness(); err != nil {
        return datalog.ElementID{}, err
    }

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
}
```

The uniqueness check is not part of the same write transaction as the commit.

### 2. Validation is implemented as a normal matcher query

From `validateUniqueness()`:

```go
matcher := NewBadgerMatcher(t.db.store)

pattern := &query.DataPattern{
    Elements: []query.PatternElement{
        query.Variable{Name: datalog.NewSymbol("?e")},
        query.Constant{Value: d.A},
        query.Constant{Value: d.V},
        query.Blank{},
    },
}

results, err := matcher.Match(pattern, nil)
```

This is a read against current committed state, not a write-serialized check.

### 3. Only `t.datoms` are considered; `t.retracts` are ignored

`validateUniqueness()` iterates over `t.datoms` only:

```go
for _, d := range t.datoms {
    def := s.GetAttribute(d.A)
    if def == nil || def.Unique == "" {
        continue
    }
    ...
}
```

There is no logic that treats a pending retract in `t.retracts` as freeing a
value for re-use inside the same logical commit.

## Failure Mode 1: Concurrent Commit Race

### Scenario

Two goroutines try to commit the same unique email to different users:

```text
Tx A: Set(alice, :user/email, "shared@example.com")
Tx B: Set(bob,   :user/email, "shared@example.com")
```

Possible interleaving:

1. Tx A runs `validateUniqueness()` -> no match found
2. Tx B runs `validateUniqueness()` -> no match found
3. Tx A writes
4. Tx B writes

If that interleaving occurs, uniqueness is violated even though both commits
individually "passed" validation.

### Why it matters

There is no global commit lock around uniqueness validation + write, and the
validation is not delegated to a single storage transaction.

## Failure Mode 2: Valid Reassign / Move Rejected

### Scenario

Current database state:

```text
alice has :user/email = "alice@example.com"
```

One logical commit wants to move that unique value:

```text
Retract(alice, :user/email, "alice@example.com")
Set(bob, :user/email, "alice@example.com")
```

This should be valid if the final state is:

```text
alice no longer has the email
bob now has the email
```

But `validateUniqueness()` checks `t.datoms` against the current database state
and does not consider pending `t.retracts`, so it will still see Alice's value
and reject Bob's assignment.

### Why it matters

This is the opposite of a race bug: instead of letting an invalid state through,
it rejects a valid state transition because validation sees only the pre-commit
store state.

## Reproduction Sketches

### 1. Concurrent race

```go
func TestUniqueValueConcurrentRace(t *testing.T) {
    // schema: :user/email unique
    // start two goroutines that both Set() the same value on different entities
    // synchronize so both goroutines reach Commit() at roughly the same time
    //
    // Expectation:
    // exactly one commit should succeed
    //
    // Suspected bug:
    // both can succeed depending on interleaving
}
```

### 2. Reassign within one commit

```go
func TestUniqueValueMoveInSingleCommit(t *testing.T) {
    // Setup: alice already owns the email
    //
    // In one transaction:
    //   Retract(alice, email, "alice@example.com")
    //   Set(bob, email, "alice@example.com")
    //
    // Expected:
    // transaction succeeds, final owner is bob
    //
    // Suspected bug:
    // validateUniqueness() rejects because it still sees alice's committed value
}
```

## Impact

### 1. Data integrity under concurrency

If the concurrent race is real, uniqueness constraints are advisory rather than
strict in multi-writer or multi-goroutine usage.

### 2. Incomplete transactional semantics

If the reassign/move case fails, uniqueness is enforced against pre-commit state
rather than final logical state.

That makes certain valid migrations and ownership transfers impossible.

## Existing Coverage Gaps

The current tests in `datalog/storage/database_schema_test.go` cover:

- uniqueness against already-committed data
- duplicate values in one transaction
- idempotent re-assertion by the same entity

They do **not** obviously cover:

- concurrent commit races
- retract+reassign within one logical commit

## Possible Fix Directions

### Option 1: Move uniqueness enforcement into the same write transaction

Open one storage write transaction and perform the uniqueness lookup + write
serialization inside it.

This is the most direct fix for the TOCTOU race.

### Option 2: Validate against the logical final state, not just current store state

For within-transaction moves:

- subtract pending retracts
- add pending assertions
- validate uniqueness against the resulting final state

### Option 3: Introduce explicit semantics for "move unique value"

If the intended API is that unique values cannot be moved in a single commit,
that needs to be documented explicitly. Currently the behavior looks accidental,
not designed.

## Test Plan

1. Add a concurrent uniqueness race test with two goroutines and a barrier.
2. Add a same-transaction retract+reassign test.
3. Add a same-transaction swap test (A gets B's value, B gets A's value).
4. Verify behavior with `UniqueValue` and `UniqueIdentity`.
5. If fixed, ensure:
   - exactly one concurrent writer succeeds
   - valid in-transaction moves succeed
   - invalid final states still fail
