package storage

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

// Snapshots are recorded as ordinary entities in a reserved :db.snapshot/* namespace,
// not in a side table. Listing snapshots is therefore a Datalog query, and causal
// ordering is intrinsic to each marker's Tx and to the captured point it stores. See
// docs/proposals/BRANCHING_AND_SNAPSHOTS.md §12.1 (Slice A).

// snapshotEntityPrefix derives a snapshot marker's entity id from its name, so a name
// maps to exactly one marker entity.
const snapshotEntityPrefix = "db.snapshot/"

// The marker's attributes, interned once. Well-known because these are package
// variables: ClearInterns replaces the intern tables, and a variable holding a
// pre-clear instance is an orphan that panics in Keyword.Equal against the next
// fresh intern of the same name. Registering is not conditional on having
// traced a comparison to a panic — the rule is that a package variable holding
// an interned instance is registered, so that nobody has to trace one.
var (
	snapshotNameAttr      = datalog.WellKnownKeyword(":db.snapshot/name")
	snapshotAtLamportAttr = datalog.WellKnownKeyword(":db.snapshot/at-lamport")
	snapshotAtReplicaAttr = datalog.WellKnownKeyword(":db.snapshot/at-replica")
	snapshotCreatedAttr   = datalog.WellKnownKeyword(":db.snapshot/created")
)

var (
	// ErrSnapshotExists is returned by Snapshot when the name is already taken.
	ErrSnapshotExists = errors.New("snapshot already exists")
	// ErrSnapshotNotFound is returned when a named snapshot does not exist.
	ErrSnapshotNotFound = errors.New("snapshot not found")
	// ErrRollbackInProgress is returned by a write (Add/Retract/Commit) started while a
	// TruncateTo rollback is running on the same handle. The write is dropped, not queued;
	// retry on a fresh transaction once the rollback completes.
	ErrRollbackInProgress = errors.New("write rejected: a rollback (TruncateTo) is in progress")
)

// SnapshotInfo is the decoded snapshot marker. Fields are additive: the branching round
// adds Path/Parent without changing existing callers (their absence ⇒ root).
type SnapshotInfo struct {
	Name    string
	At      datalog.ElementID // captured high-water point (the AsOf read point)
	Created time.Time
}

// snapshotEntity returns the marker entity identity for a snapshot name.
func snapshotEntity(name string) datalog.Identity {
	return datalog.NewIdentity(snapshotEntityPrefix + name)
}

// Snapshot names the current state of this handle as a :db.snapshot/* entity. It captures
// store.MaxElementID() as the snapshot point and writes the marker in a single
// transaction. Returns ErrSnapshotExists if the name is already in use.
func (d *Database) Snapshot(name string) (SnapshotInfo, error) {
	if d.temporalTxID != nil {
		return SnapshotInfo{}, fmt.Errorf("Snapshot: cannot snapshot a read-only temporal handle (AsOf/History)")
	}
	if name == "" {
		return SnapshotInfo{}, fmt.Errorf("Snapshot: name must not be empty")
	}

	existing, err := d.lookupSnapshot(name)
	if err != nil {
		return SnapshotInfo{}, err
	}
	if existing != nil {
		return SnapshotInfo{}, fmt.Errorf("Snapshot %q: %w", name, ErrSnapshotExists)
	}

	before, err := d.store.MaxElementID()
	if err != nil {
		return SnapshotInfo{}, fmt.Errorf("Snapshot %q: capture high-water: %w", name, err)
	}

	created := time.Now()
	e := snapshotEntity(name)

	tx := d.NewTransaction()
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := tx.Add(e, snapshotNameAttr, name); err != nil {
		return SnapshotInfo{}, fmt.Errorf("Snapshot %q: %w", name, err)
	}
	if err := tx.Add(e, snapshotAtLamportAttr, int64(before.Lamport)); err != nil {
		return SnapshotInfo{}, fmt.Errorf("Snapshot %q: %w", name, err)
	}
	if err := tx.Add(e, snapshotAtReplicaAttr, int64(before.ReplicaID)); err != nil {
		return SnapshotInfo{}, fmt.Errorf("Snapshot %q: %w", name, err)
	}
	if err := tx.Add(e, snapshotCreatedAttr, created); err != nil {
		return SnapshotInfo{}, fmt.Errorf("Snapshot %q: %w", name, err)
	}
	if _, err := tx.Commit(); err != nil {
		return SnapshotInfo{}, fmt.Errorf("Snapshot %q: commit: %w", name, err)
	}
	committed = true

	return SnapshotInfo{Name: name, At: before, Created: created}, nil
}

// AsOfSnapshot returns a read-only handle viewing the database as of the named snapshot.
// The returned handle rejects writes (NewTransaction panics on a temporal handle).
//
// It reads at the marker's own high-water (snapshotMarkerMax), which is domain-equivalent
// to the captured point At — the marker datoms it adds are reserved-namespace and filtered
// from domain queries — and, unlike At, is always non-zero. That matters for an
// empty-database snapshot (At == zero ElementID): AsOf of the zero ElementID would collide
// with the matcher's History sentinel and show everything instead of nothing.
func (d *Database) AsOfSnapshot(name string) (*Database, error) {
	info, err := d.lookupSnapshot(name)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, fmt.Errorf("AsOfSnapshot %q: %w", name, ErrSnapshotNotFound)
	}
	point, err := d.snapshotMarkerMax(name)
	if err != nil {
		return nil, err
	}
	return d.AsOf(point), nil
}

// Snapshots lists every snapshot, in causal order (by captured point, then name).
func (d *Database) Snapshots() ([]SnapshotInfo, error) {
	type snapshotFields struct {
		Name    string    `datalog:"?name"`
		Lamport int64     `datalog:"?lamport"`
		Replica int64     `datalog:"?replica"`
		Created time.Time `datalog:"?created"`
	}
	var found []snapshotFields
	err := d.QueryInto(&found, `[:find ?name ?lamport ?replica ?created
		:where [?s :db.snapshot/name ?name]
		       [?s :db.snapshot/at-lamport ?lamport]
		       [?s :db.snapshot/at-replica ?replica]
		       [?s :db.snapshot/created ?created]]`)
	if err != nil {
		return nil, fmt.Errorf("Snapshots: %w", err)
	}

	out := make([]SnapshotInfo, 0, len(found))
	for _, r := range found {
		out = append(out, SnapshotInfo{
			Name:    r.Name,
			At:      datalog.ElementID{Lamport: uint64(r.Lamport), ReplicaID: uint64(r.Replica)},
			Created: r.Created,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if c := out[i].At.Compare(out[j].At); c != 0 {
			return c < 0
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// DeleteSnapshot removes the named snapshot from the registry by retracting its marker.
// The rewindable timeline is untouched; only the registry entry goes away.
func (d *Database) DeleteSnapshot(name string) error {
	if d.temporalTxID != nil {
		return fmt.Errorf("DeleteSnapshot: cannot modify a read-only temporal handle (AsOf/History)")
	}
	info, err := d.lookupSnapshot(name)
	if err != nil {
		return err
	}
	if info == nil {
		return fmt.Errorf("DeleteSnapshot %q: %w", name, ErrSnapshotNotFound)
	}

	e := snapshotEntity(name)
	tx := d.NewTransaction()
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// Retracting the name attribute is what removes the snapshot from the registry (it
	// breaks the join in Snapshots/lookupSnapshot). The remaining retracts are cleanup.
	if err := tx.Retract(e, snapshotNameAttr, name); err != nil {
		return fmt.Errorf("DeleteSnapshot %q: %w", name, err)
	}
	if err := tx.Retract(e, snapshotAtLamportAttr, int64(info.At.Lamport)); err != nil {
		return fmt.Errorf("DeleteSnapshot %q: %w", name, err)
	}
	if err := tx.Retract(e, snapshotAtReplicaAttr, int64(info.At.ReplicaID)); err != nil {
		return fmt.Errorf("DeleteSnapshot %q: %w", name, err)
	}
	if err := tx.Retract(e, snapshotCreatedAttr, info.Created); err != nil {
		return fmt.Errorf("DeleteSnapshot %q: %w", name, err)
	}
	if _, err := tx.Commit(); err != nil {
		return fmt.Errorf("DeleteSnapshot %q: commit: %w", name, err)
	}
	committed = true
	return nil
}

// lookupSnapshot resolves a snapshot by name via a Datalog query. Returns nil if no such
// snapshot exists.
func (d *Database) lookupSnapshot(name string) (*SnapshotInfo, error) {
	type snapshotFields struct {
		Lamport int64     `datalog:"?lamport"`
		Replica int64     `datalog:"?replica"`
		Created time.Time `datalog:"?created"`
	}
	var found []snapshotFields
	err := d.QueryInto(&found, `[:find ?lamport ?replica ?created
		:in $ ?name
		:where [?s :db.snapshot/name ?name]
		       [?s :db.snapshot/at-lamport ?lamport]
		       [?s :db.snapshot/at-replica ?replica]
		       [?s :db.snapshot/created ?created]]`, name)
	if err != nil {
		return nil, fmt.Errorf("lookup snapshot %q: %w", name, err)
	}
	if len(found) == 0 {
		return nil, nil
	}
	r := found[0]
	return &SnapshotInfo{
		Name:    name,
		At:      datalog.ElementID{Lamport: uint64(r.Lamport), ReplicaID: uint64(r.Replica)},
		Created: r.Created,
	}, nil
}

// snapshotMarkerMax returns the highest Tx of a snapshot's marker entity — the snapshot's
// effective point. AsOfSnapshot reads there and TruncateTo truncates there. It is always
// non-zero (a marker always has datoms), which keeps an empty-database snapshot from
// resolving to the zero ElementID the matcher reserves for History mode.
func (d *Database) snapshotMarkerMax(name string) (datalog.ElementID, error) {
	mm, ok, err := d.store.MaxTxForEntity(snapshotEntity(name))
	if err != nil {
		return datalog.ElementID{}, fmt.Errorf("snapshot %q: locate marker: %w", name, err)
	}
	if !ok {
		return datalog.ElementID{}, fmt.Errorf("snapshot %q: marker entity has no datoms", name)
	}
	return mm, nil
}
