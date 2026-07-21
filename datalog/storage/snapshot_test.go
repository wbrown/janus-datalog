package storage

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wbrown/janus-datalog/datalog"
)

// snapTestAddName writes a single :person/name fact in its own transaction.
func snapTestAddName(t *testing.T, d *Database, idStr, name string) {
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(datalog.NewIdentity(idStr), datalog.NewKeyword(":person/name"), name))
	_, err := tx.Commit()
	require.NoError(t, err)
}

// snapTestNames returns the sorted set of :person/name values visible through a handle.
func snapTestNames(t *testing.T, d *Database) []string {
	var ns []string
	require.NoError(t, d.QueryInto(&ns, `[:find ?n :where [?e :person/name ?n]]`))
	sort.Strings(ns)
	return ns
}

// snapTestHistoryNames returns the :person/name values physically present, via History.
func snapTestHistoryNames(t *testing.T, d *Database) []string {
	var ns []string
	require.NoError(t, d.History().QueryInto(&ns, `[:find ?n :where [?e :person/name ?n]]`))
	sort.Strings(ns)
	return ns
}

func TestSnapshotCaptureAndAsOf(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode)

			snapTestAddName(t, d, "alice", "Alice")

			snap, err := d.Snapshot("cp1")
			require.NoError(t, err)
			assert.Equal(t, "cp1", snap.Name)
			assert.False(t, snap.At.IsZero(), "captured point should be non-zero after a write")

			// Write after the snapshot.
			snapTestAddName(t, d, "bob", "Bob")

			// Live DB sees both; the snapshot handle sees only the pre-snapshot state.
			assert.Equal(t, []string{"Alice", "Bob"}, snapTestNames(t, d))

			asof, err := d.AsOfSnapshot("cp1")
			require.NoError(t, err)
			assert.Equal(t, []string{"Alice"}, snapTestNames(t, asof))
		})
	}
}

func TestSnapshotEmptyDatabase(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode)

			snap, err := d.Snapshot("empty")
			require.NoError(t, err)
			assert.True(t, snap.At.IsZero(), "snapshot of an empty DB captures the zero point")

			asof, err := d.AsOfSnapshot("empty")
			require.NoError(t, err)
			assert.Empty(t, snapTestNames(t, asof))

			// Writing after the empty snapshot is visible live; the AsOf view stays empty.
			snapTestAddName(t, d, "alice", "Alice")
			assert.Equal(t, []string{"Alice"}, snapTestNames(t, d))

			asof2, err := d.AsOfSnapshot("empty")
			require.NoError(t, err)
			assert.Empty(t, snapTestNames(t, asof2))
		})
	}
}

func TestSnapshotListCausalOrder(t *testing.T) {
	d, err := NewDatabase(t.TempDir())
	require.NoError(t, err)
	defer d.Close()

	snapTestAddName(t, d, "alice", "Alice")
	_, err = d.Snapshot("a")
	require.NoError(t, err)
	snapTestAddName(t, d, "bob", "Bob")
	_, err = d.Snapshot("b")
	require.NoError(t, err)

	snaps, err := d.Snapshots()
	require.NoError(t, err)
	require.Len(t, snaps, 2)
	assert.Equal(t, "a", snaps[0].Name)
	assert.Equal(t, "b", snaps[1].Name)
	assert.True(t, snaps[0].At.Less(snaps[1].At), "snapshot a captured before b")
}

func TestSnapshotDuplicateName(t *testing.T) {
	d, err := NewDatabase(t.TempDir())
	require.NoError(t, err)
	defer d.Close()

	_, err = d.Snapshot("x")
	require.NoError(t, err)
	_, err = d.Snapshot("x")
	require.ErrorIs(t, err, ErrSnapshotExists)
}

func TestDeleteSnapshot(t *testing.T) {
	d, err := NewDatabase(t.TempDir())
	require.NoError(t, err)
	defer d.Close()

	snapTestAddName(t, d, "alice", "Alice")
	_, err = d.Snapshot("x")
	require.NoError(t, err)

	require.NoError(t, d.DeleteSnapshot("x"))

	snaps, err := d.Snapshots()
	require.NoError(t, err)
	assert.Empty(t, snaps)

	_, err = d.AsOfSnapshot("x")
	require.ErrorIs(t, err, ErrSnapshotNotFound)

	require.ErrorIs(t, d.DeleteSnapshot("nope"), ErrSnapshotNotFound)
}

func TestTruncateToRemovesLaterWrites(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode)

			snapTestAddName(t, d, "alice", "Alice")
			_, err := d.Snapshot("cp1")
			require.NoError(t, err)
			snapTestAddName(t, d, "bob", "Bob")
			snapTestAddName(t, d, "carol", "Carol")

			require.Equal(t, []string{"Alice", "Bob", "Carol"}, snapTestNames(t, d))

			require.NoError(t, d.TruncateTo("cp1"))

			// Live reads equal the snapshot exactly.
			assert.Equal(t, []string{"Alice"}, snapTestNames(t, d))

			// Physically gone, not just hidden: History no longer shows Bob/Carol.
			assert.Equal(t, []string{"Alice"}, snapTestHistoryNames(t, d))

			// The target snapshot survives.
			snaps, err := d.Snapshots()
			require.NoError(t, err)
			require.Len(t, snaps, 1)
			assert.Equal(t, "cp1", snaps[0].Name)
		})
	}
}

func TestTruncateToResumesClockWithoutCollision(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode)

			snapTestAddName(t, d, "alice", "Alice")
			_, err := d.Snapshot("cp1")
			require.NoError(t, err)
			snapTestAddName(t, d, "bob", "Bob")

			require.NoError(t, d.TruncateTo("cp1"))

			// The next write must succeed and read back, with no collision against rewound datoms.
			snapTestAddName(t, d, "dave", "Dave")
			assert.Equal(t, []string{"Alice", "Dave"}, snapTestNames(t, d))
			assert.Equal(t, []string{"Alice", "Dave"}, snapTestHistoryNames(t, d))
		})
	}
}

func TestTruncateToPrunesLaterSnapshots(t *testing.T) {
	d, err := NewDatabase(t.TempDir())
	require.NoError(t, err)
	defer d.Close()

	snapTestAddName(t, d, "alice", "Alice")
	_, err = d.Snapshot("cp1")
	require.NoError(t, err)
	snapTestAddName(t, d, "bob", "Bob")
	_, err = d.Snapshot("cp2")
	require.NoError(t, err)

	// Truncating to cp1 erases everything after cp1's marker, cp2 included.
	require.NoError(t, d.TruncateTo("cp1"))

	snaps, err := d.Snapshots()
	require.NoError(t, err)
	require.Len(t, snaps, 1)
	assert.Equal(t, "cp1", snaps[0].Name)

	_, err = d.AsOfSnapshot("cp2")
	assert.ErrorIs(t, err, ErrSnapshotNotFound)
}

func TestTruncateToLatestIsNoop(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode)

			snapTestAddName(t, d, "alice", "Alice")
			_, err := d.Snapshot("cp1")
			require.NoError(t, err)

			// No writes after the snapshot: truncate keeps everything up to and including it.
			require.NoError(t, d.TruncateTo("cp1"))
			assert.Equal(t, []string{"Alice"}, snapTestNames(t, d))

			// Still writable afterward.
			snapTestAddName(t, d, "bob", "Bob")
			assert.Equal(t, []string{"Alice", "Bob"}, snapTestNames(t, d))
		})
	}
}

func TestTruncateThenSnapshotRewoundTimeline(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode)

			snapTestAddName(t, d, "alice", "Alice")
			_, err := d.Snapshot("cp1")
			require.NoError(t, err)
			snapTestAddName(t, d, "bob", "Bob")

			require.NoError(t, d.TruncateTo("cp1"))

			// Snapshot the rewound timeline and read it back.
			_, err = d.Snapshot("cp2")
			require.NoError(t, err)
			snapTestAddName(t, d, "carol", "Carol")

			asof, err := d.AsOfSnapshot("cp2")
			require.NoError(t, err)
			assert.Equal(t, []string{"Alice"}, snapTestNames(t, asof))
			assert.Equal(t, []string{"Alice", "Carol"}, snapTestNames(t, d))

			// truncate -> write -> truncate is stable.
			require.NoError(t, d.TruncateTo("cp2"))
			assert.Equal(t, []string{"Alice"}, snapTestNames(t, d))
		})
	}
}

func TestTruncateToUnknownSnapshot(t *testing.T) {
	d, err := NewDatabase(t.TempDir())
	require.NoError(t, err)
	defer d.Close()
	assert.ErrorIs(t, d.TruncateTo("nope"), ErrSnapshotNotFound)
}

func TestAsOfSnapshotRejectsWrites(t *testing.T) {
	d, err := NewDatabase(t.TempDir())
	require.NoError(t, err)
	defer d.Close()

	_, err = d.Snapshot("cp1")
	require.NoError(t, err)
	asof, err := d.AsOfSnapshot("cp1")
	require.NoError(t, err)

	assert.Panics(t, func() {
		asof.NewTransaction()
	}, "AsOfSnapshot handle must reject writes")
}

// TestStoreTruncateAfterAndMaxTxForEntity exercises the two BadgerStore primitives
// directly, independent of the snapshot machinery layered on top.
func TestStoreDatomsAfterDeleteAndMaxTxForEntity(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode)

			snapTestAddName(t, d, "alice", "Alice")
			mid, err := d.store.MaxElementID()
			require.NoError(t, err)

			snapTestAddName(t, d, "bob", "Bob")

			// Alice's entity max Tx does not exceed the high-water captured before Bob.
			maxAlice, ok, err := d.store.MaxTxForEntity(datalog.NewIdentity("alice"))
			require.NoError(t, err)
			require.True(t, ok)
			assert.False(t, mid.Less(maxAlice), "alice's max Tx should not exceed the pre-bob high-water")

			// Removing everything after mid takes Bob's datoms (and their tx metadata) with it.
			datoms, err := d.store.DatomsAfter(mid)
			require.NoError(t, err)
			n, err := d.store.DeleteDatoms(datoms)
			require.NoError(t, err)
			assert.Greater(t, n, 0, "should have deleted Bob's datoms")
			assert.Equal(t, []string{"Alice"}, snapTestHistoryNames(t, d))

			// An entity with no datoms reports not-found.
			_, ok, err = d.store.MaxTxForEntity(datalog.NewIdentity("ghost"))
			require.NoError(t, err)
			assert.False(t, ok)
		})
	}
}
