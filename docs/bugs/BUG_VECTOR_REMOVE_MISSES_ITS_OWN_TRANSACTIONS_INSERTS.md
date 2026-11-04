# A vector Remove misses its own transaction's inserts

**Status**: Open, recorded 2026-11-03 against janus-datalog v0.16.1. Found by
the Starflight disassembler (`disasm`), which records a Forth machine's stacks
as vectors and reads them back as of each step. Worked around there
(`factWriter.remove` commits before such a pop); the fix belongs here.

`Transaction.Remove` on a cardinality-vector attribute tombstones "the most
recently added element matching value". It finds that element by scanning the
committed store, and of the open transaction it consults the pending
tombstones but not the pending inserts. When the most recently added element
of the value was added earlier in the same transaction, Remove does not see
it. It then either writes no tombstone at all, or tombstones an older,
committed element of the same value. Both are silent: Remove returns nil.

## The route

The vector arm of `Transaction.Remove` (`datalog/storage/database.go`, lines
1963-2038 at v0.16.1):

1. It collects `pendingTombstones`, the `AfterRef` of every `OpRGATombstone`
   in `t.datoms` for this (E, A), so that "multiple Remove() calls in the same
   transaction ... work correctly".
2. It opens an EAVT scan of `t.db.store` bound to [E A V]. Tx descends, so a
   tombstone is seen before the insert it deletes.
3. The first `OpRGAInsert` whose Tx is in neither the scanned tombstones nor
   `pendingTombstones` is the target.
4. With no target, "Remove is a no-op (not an error)".

`t.datoms` is read for tombstones only. An `OpRGAInsert` that `Add` appended
to `t.datoms` (lines 1826-1869) is never a candidate, so:

- If no committed insert of the value is live, there is no target and no
  tombstone. The element added in this transaction stays.
- If an older committed insert of the value is live, it is the target. The
  element added in this transaction stays, and the older one goes.

`Add` reads no storage and chains each element after the last one added in
the transaction (`lastVectorElement`), so the pending insert is already the
newest element of the vector by ElementID. The scan simply never looks there.

## What was observed

Observed on `db.OpenMemory` with the schema's vector attribute `:run/stack`
(int64), through `Add` and `Remove` on one transaction, then `PullInto`
after `Commit`:

| Writes | Answer | Expected |
|---|---|---|
| tx1: Add 5, Add 7, Add 5, Remove 5, Add 9 | `[5 7 5 9]` | `[5 7 9]` |
| tx1: Add 5 (committed); tx2: Add 7, Add 5, Remove 5, Add 9 | `[7 5 9]` | `[5 7 9]` |
| tx1: Add 5, Add 6 (committed); tx2: Add 7, Add 8, Remove 8, Add 9 | `[5 6 7 8 9]` | `[5 6 7 9]` |

The third row also shows that `Add` after a commit is right: 7 lands after the
committed 6, not at the head. Only the Remove is wrong.

With a Remove of a value whose last Add was committed in an earlier
transaction, every row answers as expected, which is the case the existing
tests cover.

In the application, a stack written one push (`Add`) and pop (`Remove`) per
operation, many operations to a transaction, grew to hundreds of cells where
the machine's stack held a handful: nearly every pop was of a cell pushed in
the same batch.

## Contracts it contradicts

- `Transaction.Remove`'s doc: "Cardinality-Vector: RGA LIFO tombstone; removes
  most recently added matching element". The most recently added matching
  element is the pending one.
- The design note in the vector arm and on `TestVectorRemoveMostRecent`
  (`crdt_vector_test.go`): "CRDT-friendly: 'most recent' = 'the one I most
  recently added'".
- `Transaction.Commit`'s per-operation Lamport: "datoms already have their Tx
  set by Add/Remove/Set". Each operation in a transaction is ordered, and a
  Remove after an Add in that order is answered as if the Add had not
  happened.
- The vector arm's own handling of the transaction: it reads `t.datoms` for
  the tombstones so that several Removes in one transaction compose. Inserts
  in the same transaction do not compose with them.

## Reach

Any caller that adds and removes the same vector value in one transaction:
a stack, an undo log, a work queue. `TestVectorRemoveMostRecent` and
`TestVectorRemoveNonExistent` commit their Adds before the Remove, so no test
in the repository reaches the route. The answer is wrong at every read
afterward (latest, `AsOf`, `PullInto`), since the datoms themselves are wrong:
a tombstone is missing, or names the wrong element.

## What was not established

The vector arm of `Transaction.Set` (lines 2303-2386) reads the current
vector through the EA cache (`GetOrResolve`) or `ResolveEntry`, both over
committed state, and diffs the new slice against it. It applies no pending
operation of the transaction, where the cardinality-many arm does (lines
2199-2222: "This ensures Set() sees Add/Remove ops from earlier in the same
transaction"). By reading, a vector `Set` after an `Add` or a `Set` of the
same (E, A) in one transaction diffs against the wrong vector. That was read
in the code and not run.

## The fix, if taken

In the vector arm of `Remove`, before the store scan: walk `t.datoms` from
the newest back, and take the first `OpRGAInsert` for (E, A) whose value
matches and whose Tx is not in `pendingTombstones`. Only when none is found,
scan the store as now. A pending insert always has a higher ElementID than
any committed one, so the newest match is in the transaction whenever there
is one there. This is the order the cardinality-many arm of `Set` already
uses: the transaction's pending operations over the committed state.

The same overlay would give the vector arm of `Set` the effective vector
(committed, then the transaction's inserts and tombstones) to diff against.

## Reproducer

Written for the `storage` package against v0.16.1 and not run there. The
three cases were observed in the application through the same calls
(`Add`, `Remove`, `Commit`, `PullInto`) on an in-memory database.

```go
package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestVectorRemoveSeesItsOwnTransactionsInserts pins that a vector Remove
// takes the most recently added element of the value when that element was
// added earlier in the same transaction.
func TestVectorRemoveSeesItsOwnTransactionsInserts(t *testing.T) {
	type stack struct {
		Cells []string `datalog:"stack/cells"`
	}
	for _, c := range []struct {
		name  string
		first []string // added and committed first
		then  []string // added in the open transaction, then the last removed, then "9" added
		want  []string
	}{
		{"an insert in the same transaction", nil, []string{"5", "7", "5"}, []string{"5", "7", "9"}},
		{"an equal value committed before", []string{"5"}, []string{"7", "5"}, []string{"5", "7", "9"}},
		{"inserts after a commit", []string{"5", "6"}, []string{"7", "8"}, []string{"5", "6", "7", "9"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, err := schema.NewBuilder().
				Attribute(":stack/cells").Type(schema.TypeString).Vector().Add().
				Build()
			require.NoError(t, err)
			db, err := NewDatabaseWithOptions(DatabaseOptions{Store: NewBadgerEmulatorStore(nil), Schema: s})
			require.NoError(t, err)
			defer db.Close()
			cells := datalog.NewKeyword(":stack/cells")
			e := datalog.NewIdentity("vector-remove:stack")

			tx := db.NewTransaction()
			for _, v := range c.first {
				require.NoError(t, tx.Add(e, cells, v))
			}
			_, err = tx.Commit()
			require.NoError(t, err)

			tx = db.NewTransaction()
			for _, v := range c.then {
				require.NoError(t, tx.Add(e, cells, v))
			}
			require.NoError(t, tx.Remove(e, cells, c.then[len(c.then)-1]))
			require.NoError(t, tx.Add(e, cells, "9"))
			_, err = tx.Commit()
			require.NoError(t, err)

			var got stack
			require.NoError(t, db.PullInto(e, &got))
			assert.Equal(t, c.want, got.Cells)
		})
	}
}
```

Expected to fail on all three cases, answering `[5 7 5 9]`, `[7 5 9]` and
`[5 6 7 8 9]`.

## Related

`TestVectorRemoveMostRecent` (`crdt_vector_test.go`) is the LIFO contract
across transactions, which holds. `TestAsOfVectorResolution`
(`asof_vector_test.go`) covers as-of reads of vectors written by whole-vector
`Set`, one per transaction, which this route does not touch.
