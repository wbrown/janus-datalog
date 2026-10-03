package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// TruncateTo destructively rewinds this handle's writable timeline to the named snapshot.
// Every datom written after the snapshot's own marker is PHYSICALLY removed (not
// tombstoned), so it disappears from History() as well as from current reads; the clock
// resumes from the snapshot point so the next write does not collide. Snapshots taken after
// the target are pruned along with the timeline they indexed; the target snapshot and
// earlier ones survive. The record of a fork from a surviving snapshot survives with it,
// since a TruncateTo past that snapshot reads the record. See
// docs/proposals/BRANCHING_AND_SNAPSHOTS.md §12.1 (Slice B).
//
// Concurrency (§12.1, "Rollback safety"): TruncateTo serializes against other rollbacks
// and snapshot deletions (rollbackMu), drains in-flight write transactions, and drops
// writes started while it runs (they return ErrRollbackInProgress). An in-flight write
// opened before the rollback is allowed to commit; its post-snapshot datoms are then erased
// like any other Tx > markerMax. Reads are never locked — BadgerDB MVCC keeps them
// consistent, and the cache in-flight window (BeginInFlight → delete → InvalidateRewind)
// keeps them from caching a soon-to-be-stale value while the rewind is visible mid-flight.
func (d *Database) TruncateTo(name string) error {
	if d.temporalTxID != nil {
		return fmt.Errorf("TruncateTo: cannot rewind a read-only temporal handle (AsOf/History)")
	}

	// Serialize this whole operation against other rollbacks and snapshot deletions.
	d.rollbackMu.Lock()
	defer d.rollbackMu.Unlock()

	info, err := d.lookupSnapshot(name)
	if err != nil {
		return err
	}
	if info == nil {
		return fmt.Errorf("TruncateTo %q: %w", name, ErrSnapshotNotFound)
	}

	// The deletion floor is the marker entity's own highest Tx, not the captured point:
	// the marker was written just after the capture, so deleting strictly above it keeps
	// the marker itself while erasing everything written later (including snapshots taken
	// after this one, whose markers sit above the floor).
	markerMax, err := d.snapshotMarkerMax(name)
	if err != nil {
		return err
	}

	// Hold writers so the clock rewind below cannot collide with a concurrent commit.
	release := d.holdWriters(nil)
	defer release()

	// A branch this database forked past the target inherited datoms the rewind would
	// erase, leaving its fork point off its parent's timeline (§10.7). Fork records the
	// branch in a transaction it opens before it looks up the snapshot, so a Fork under way
	// when the rollback began was drained above and its record is read here, and a Fork
	// begun since is refused.
	after, err := d.branchesForkedAfter(markerMax)
	if err != nil {
		return fmt.Errorf("TruncateTo %q: %w", name, err)
	}
	if len(after) > 0 {
		return fmt.Errorf("TruncateTo %q: %w: %q", name, ErrBranchedAfterSnapshot, after)
	}

	// A fork record written past the floor stays when the point the branch reads as of is
	// at or below the floor: that is the point of a snapshot the rewind keeps. Every other
	// record past the floor goes with the snapshot it names.
	type forkPoint struct {
		Record  datalog.Identity `datalog:"?b"`
		Lamport int64            `datalog:"?lamport"`
		Replica int64            `datalog:"?replica"`
	}
	var forks []forkPoint
	if err := d.QueryInto(&forks, `[:find ?b ?lamport ?replica
		:where [?b :db.branch/at-lamport ?lamport]
		       [?b :db.branch/at-replica ?replica]]`); err != nil {
		return fmt.Errorf("TruncateTo %q: %w", name, err)
	}
	kept := make(map[datalog.Identity]bool, len(forks))
	for _, f := range forks {
		if !markerMax.Less(datalog.ElementID{Lamport: uint64(f.Lamport), ReplicaID: uint64(f.Replica)}) {
			kept[f.Record] = true
		}
	}

	// Collect the datoms to remove and their touched (E,A) keys BEFORE deleting, so the
	// cache window opens before the delete is visible. The set is stable: writers are
	// drained and new ones dropped.
	written, err := d.store.DatomsAfter(markerMax)
	if err != nil {
		return fmt.Errorf("TruncateTo %q: scan: %w", name, err)
	}
	datoms := make([]datalog.Datom, 0, len(written))
	for _, datom := range written {
		if !kept[datom.E] {
			datoms = append(datoms, datom)
		}
	}
	keys := touchedCacheKeys(datoms)

	if d.cache != nil {
		d.cache.BeginInFlight(keys)
	}

	if _, err := d.store.DeleteDatoms(datoms); err != nil {
		if d.cache != nil {
			d.cache.InvalidateRewind(keys) // close the window even on failure
		}
		return fmt.Errorf("TruncateTo %q: delete: %w", name, err)
	}

	// No writer holds a Lamport above markerMax now; the rewind is collision-free.
	d.clock.Restore(markerMax)

	if d.cache != nil {
		d.cache.InvalidateRewind(keys)
	}
	return nil
}

// holdWriters turns away every write transaction opened from now on and waits until each
// one already in flight, own aside, has committed or rolled back; the function it returns
// lets writers in again. The caller holds rollbackMu, so no other rollback is holding
// writers at the same time. drainCond.Wait releases d.mu while blocked, letting a draining
// commit reacquire it to deregister and signal; holding d.mu across the wait would
// deadlock against the very commits being waited on.
func (d *Database) holdWriters(own *Transaction) (release func()) {
	d.mu.Lock()
	d.rollbackInProgress = true
	for {
		inFlight := len(d.activeTx)
		if d.activeTx[own] {
			inFlight--
		}
		if inFlight == 0 {
			break
		}
		if d.onDrainWait != nil {
			d.onDrainWait()
		}
		d.drainCond.Wait()
	}
	d.mu.Unlock()

	return func() {
		d.mu.Lock()
		d.rollbackInProgress = false
		d.mu.Unlock()
	}
}

// touchedCacheKeys returns the deduplicated (E,A) cache keys for a set of datoms.
func touchedCacheKeys(datoms []datalog.Datom) []CacheKey {
	seen := make(map[CacheKey]struct{}, len(datoms))
	keys := make([]CacheKey, 0, len(datoms))
	for i := range datoms {
		sd := ToStorageDatom(datoms[i])
		k := CacheKey{E: sd.E, A: sd.A}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	return keys
}
