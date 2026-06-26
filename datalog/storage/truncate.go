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
// earlier ones survive. See docs/proposals/BRANCHING_AND_SNAPSHOTS.md §12.1 (Slice B).
//
// Concurrency (§12.1, "Rollback safety"): TruncateTo serializes against other rollbacks
// (rollbackMu), drains in-flight write transactions, and drops writes started while it runs
// (they return ErrRollbackInProgress). An in-flight write opened before the rollback is
// allowed to commit; its post-snapshot datoms are then erased like any other Tx > markerMax.
// Reads are never locked — BadgerDB MVCC keeps them consistent, and the cache in-flight
// window (BeginInFlight → delete → InvalidateRewind) keeps them from caching a soon-to-be-
// stale value while the rewind is visible mid-flight.
func (d *Database) TruncateTo(name string) error {
	if d.temporalTxID != nil {
		return fmt.Errorf("TruncateTo: cannot rewind a read-only temporal handle (AsOf/History)")
	}

	// Serialize this whole operation against other rollbacks.
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

	// Gate new writers and drain in-flight ones so the clock rewind below cannot collide
	// with a concurrent commit. drainCond.Wait releases d.mu while blocked, letting a
	// draining commit reacquire it to deregister and signal; holding d.mu across the wait
	// would deadlock against the very commits being waited on.
	d.mu.Lock()
	d.rollbackInProgress = true
	for len(d.activeTx) > 0 {
		d.drainCond.Wait()
	}
	d.mu.Unlock()

	// Reopen the gate on every exit path below so writers resume.
	defer func() {
		d.mu.Lock()
		d.rollbackInProgress = false
		d.mu.Unlock()
	}()

	// Collect the datoms to remove and their touched (E,A) keys BEFORE deleting, so the
	// cache window opens before the delete is visible. The set is stable: writers are
	// drained and new ones dropped.
	datoms, err := d.store.DatomsAfter(markerMax)
	if err != nil {
		return fmt.Errorf("TruncateTo %q: scan: %w", name, err)
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
