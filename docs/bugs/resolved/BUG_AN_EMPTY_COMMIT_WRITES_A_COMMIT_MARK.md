# An empty commit writes a commit mark

**Status**: Resolved. The body states the defect as filed; the Resolution at the end states the fix.

## What the commit does

`Transaction.Commit` (`datalog/storage/database.go`) builds a `:db/txInstant` datom on the entity `tx:<Lamport>`, stamped with the commit's last Lamport. It writes that datom whether or not the transaction holds anything. So a transaction with no `Add`, `Set`, `Remove` or `Retract` writes one datom. `Commit` returns that datom's ElementID as the point for `AsOf`.

## Why it is wrong

- An empty transaction asserts nothing and withdraws nothing. The mark it writes records that the writer called `Commit`, which is no fact about the data.
- A commit's mark is the bound of its commit's datoms, and an empty commit has none.
- The point `Commit` returns needs no write. `Store.MaxElementID` is the newest ElementID in the store, and `AsOf` at it reads the state the empty commit's mark would. An empty store answers the zero ElementID. `AsOf` at the zero point reads the empty state, since a view carries its mode beside its point.

## The use

`SaveStruct` writes no datom for a state it would not change, so a writer that re-saves an unchanged state and commits makes an empty commit. A downstream build runs fixpoints whose iterations re-save their state, and asks of each iteration whether anything was written after the point the previous one committed at. Every iteration answers yes, since each commit writes its mark. So "nothing was written after the point" cannot be the fixpoint's convergence test.

## The fix

- `Commit` on a transaction that holds no datom and no retraction writes nothing to storage.
- It returns `Store.MaxElementID`.
- It closes the transaction and releases it from the database's active transactions, as every commit does, so a `TruncateTo` draining writers is not held by it.
- `TRANSACTION_ENVELOPES.md` replaces the physical mark with one envelope per commit. The same rule holds there: a transaction with no operations writes no envelope.

## Tests

- A commit of a transaction holding nothing writes no datom. The store holds no datom past the point it held before the commit.
- That commit returns the point `Store.MaxElementID` answers, and `AsOf` at it answers what the latest view answers.
- On an empty store that commit returns the zero ElementID, and `AsOf` at it answers nothing.
- A commit holding one datom writes its mark, as `TestCommitWritesTxInstantOnSuccess` (`datalog/storage/commit_atomicity_test.go`) pins. That test's doc says every successful `Commit` writes a mark, and it narrows to every commit that writes a datom.

## Resolution

- `Transaction.Commit` (`datalog/storage/database.go`): after its `closed` and `doomed` checks, a transaction holding no datom and no retraction reads `Store.MaxElementID`, closes its read session, marks itself closed, leaves the database's active transactions, and returns that ElementID. It opens no storage transaction and writes no mark.
- `Transaction.deregister` holds the drain protocol that `Commit`'s success path and `Rollback` share: it removes the transaction from the active transactions and wakes a draining rewind.
- `Commit`'s doc states the mark, the point it returns, and the empty case.
- `TestCommitWritesTxInstantOnSuccess`'s doc names every commit that writes a datom.
- The docs that said every commit writes a mark say every commit that writes a datom does:
  - `ARCHITECTURE.md`, the transaction lifecycle
  - `DATOMIC_COMPATIBILITY.md`, the transaction entity
  - `docs/proposals/RULES_WITNESSES_AND_SPECULATIVE_VALUES.md`, its premise
  - `docs/proposals/DATOM_VALUE_PROOF.md`, P6
  - `docs/proposals/A_SINCE_VIEW_READS_THE_DATOMS_AFTER_A_POINT.md`, §5
- `TRANSACTION_ENVELOPES.md` already states that a transaction with no operations writes no envelope.
- The tests, in `datalog/storage/empty_commit_test.go`, on every backend:
  - `TestAnEmptyCommitWritesNoMarkAndAnswersTheStoresHighestElementID`
  - `TestAnEmptyCommitOnAnEmptyStoreAnswersTheZeroElementID`
  - `TestCommittingAStructTheViewAlreadyHoldsWritesNothing`
  - `TestAnEmptyCommitClosesItsTransactionAndReleasesItFromARewind`
