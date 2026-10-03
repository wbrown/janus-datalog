package storage

import (
	"errors"
	"fmt"
	"slices"

	"github.com/wbrown/janus-datalog/datalog"
)

// A branch is a writable database that starts from one of its parent's
// snapshots and from then on writes independently of it, as a replica of its
// own: its own ReplicaID and its own Lamport clock. The parent records each fork
// as an entity of its own, named for the branch's ReplicaID. See
// docs/proposals/BRANCHING_AND_SNAPSHOTS.md §10.7 and §12.2.

// A fork's record: the name of the snapshot the branch was forked from, the
// point that snapshot captured, the point the branch holds its parent's state
// as of, and the ReplicaID the branch writes as. The captured point ties the
// record to the snapshot while the snapshot holds that point, and the fork
// point is where the branch's state ends on its parent's timeline. A record
// holds one value of each, so it resolves without a schema declaring them.
// Well-known for the reason the snapshot marker's attributes are.
var (
	branchSnapshotAttr    = datalog.WellKnownKeyword(":db.branch/snapshot")
	branchAtLamportAttr   = datalog.WellKnownKeyword(":db.branch/at-lamport")
	branchAtReplicaAttr   = datalog.WellKnownKeyword(":db.branch/at-replica")
	branchForkLamportAttr = datalog.WellKnownKeyword(":db.branch/fork-lamport")
	branchForkReplicaAttr = datalog.WellKnownKeyword(":db.branch/fork-replica")
	branchReplicaAttr     = datalog.WellKnownKeyword(":db.branch/replica")
)

// ErrBranchedAfterSnapshot is returned by TruncateTo when the database forked a
// branch from a snapshot after the one it is asked to truncate to.
var ErrBranchedAfterSnapshot = errors.New("a branch was forked after the snapshot")

// forker is a store that can start a branch: a store of its own holding this
// one's current published state as of a ceiling, and writing independently of
// it.
type forker interface {
	Fork(ceiling datalog.ElementID) (Store, error)
}

// Fork starts a branch from the snapshot named name.
//
// The branch holds what this database held as of the snapshot, the state
// AsOfSnapshot reads, and neither sees what the other commits afterward. It
// validates against this database's schema, plans with its planner options,
// and carries its own EA cache. It writes as a replica of its own: a fresh
// ReplicaID, and a clock started past every ElementID it holds, so every write
// it makes orders after everything it inherited.
//
// Fork records the fork in this database: the snapshot's name, the point the
// snapshot captured, the point the branch holds this database's state as of,
// and the branch's ReplicaID. While a snapshot of that name holds the captured
// point, TruncateTo refuses to pass the branch's point; deleting the snapshot,
// or a later take of its name that captures another point, releases the branch.
// Fork opens the transaction that records the branch before it looks up the
// snapshot, so a TruncateTo that starts holding writers while the Fork runs
// waits for the record. A Fork begun after a TruncateTo starts holding writers
// is refused with ErrRollbackInProgress. An AsOf or History handle is read-only
// and cannot fork, and neither can a store with no way to start a branch.
func (d *Database) Fork(name string) (branch *Database, err error) {
	if d.temporalTxID != nil {
		return nil, fmt.Errorf("Fork: cannot fork a read-only temporal handle (AsOf/History)")
	}
	if name == "" {
		return nil, fmt.Errorf("Fork: snapshot name must not be empty")
	}
	tx := d.NewTransaction()
	committed := false
	defer func() {
		if !committed {
			err = errors.Join(err, tx.Rollback())
		}
	}()
	if tx.doomed {
		return nil, fmt.Errorf("Fork %q: %w", name, ErrRollbackInProgress)
	}

	snapshot, err := d.lookupSnapshot(name)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, fmt.Errorf("Fork %q: %w", name, ErrSnapshotNotFound)
	}
	source, ok := d.store.(forker)
	if !ok {
		return nil, fmt.Errorf("Fork %q: a %T store cannot fork a branch", name, d.store)
	}
	ceiling, err := d.snapshotMarkerMax(name)
	if err != nil {
		return nil, err
	}

	forked, err := source.Fork(ceiling)
	if err != nil {
		return nil, fmt.Errorf("Fork %q: %w", name, err)
	}
	plannerOptions := *d.plannerOptions
	branch, err = NewDatabaseWithOptions(DatabaseOptions{
		Store:          forked,
		Schema:         d.Schema(),
		PlannerOptions: &plannerOptions,
		DisableCache:   d.cache == nil,
		schemaAsGiven:  true,
	})
	if err != nil {
		return nil, errors.Join(fmt.Errorf("Fork %q: %w", name, err), forked.Close())
	}
	if err := addBranchRecord(tx, name, snapshot.At, ceiling, branch.ReplicaID()); err != nil {
		return nil, errors.Join(err, branch.Close())
	}
	if _, err := tx.Commit(); err != nil {
		return nil, errors.Join(fmt.Errorf("Fork %q: commit: %w", name, err), branch.Close())
	}
	committed = true
	return branch, nil
}

// addBranchRecord adds to tx the record that a branch writing as branchReplica
// was forked from the snapshot named name, which captured the point at, and
// holds the database's state as of forkAt. The record's ElementIDs carry the
// database's ReplicaID, which is what makes it a branch that database forked.
func addBranchRecord(tx *Transaction, name string, at, forkAt datalog.ElementID, branchReplica uint64) error {
	b := datalog.NewIdentity(fmt.Sprintf("db.branch/%d", branchReplica))
	if err := tx.Add(b, branchSnapshotAttr, name); err != nil {
		return fmt.Errorf("Fork %q: %w", name, err)
	}
	if err := tx.Add(b, branchAtLamportAttr, int64(at.Lamport)); err != nil {
		return fmt.Errorf("Fork %q: %w", name, err)
	}
	if err := tx.Add(b, branchAtReplicaAttr, int64(at.ReplicaID)); err != nil {
		return fmt.Errorf("Fork %q: %w", name, err)
	}
	if err := tx.Add(b, branchForkLamportAttr, int64(forkAt.Lamport)); err != nil {
		return fmt.Errorf("Fork %q: %w", name, err)
	}
	if err := tx.Add(b, branchForkReplicaAttr, int64(forkAt.ReplicaID)); err != nil {
		return fmt.Errorf("Fork %q: %w", name, err)
	}
	if err := tx.Add(b, branchReplicaAttr, int64(branchReplica)); err != nil {
		return fmt.Errorf("Fork %q: %w", name, err)
	}
	return nil
}

// branchesForkedAfter names the snapshots d forked a branch from whose fork
// point is past point: the snapshots whose branches a TruncateTo to point would
// cut beneath. A record counts while a snapshot of the name it records holds
// the point that snapshot captured, so deleting the snapshot, or a later take
// of its name that captures another point, releases the branch. A record d
// inherited from its own parent was written under another ReplicaID and is not
// one of them.
func (d *Database) branchesForkedAfter(point datalog.ElementID) ([]string, error) {
	type branchRecord struct {
		Snapshot    string            `datalog:"?snapshot"`
		ForkLamport int64             `datalog:"?forkLamport"`
		ForkReplica int64             `datalog:"?forkReplica"`
		Written     datalog.ElementID `datalog:"?written"`
	}
	var records []branchRecord
	err := d.QueryInto(&records, `[:find ?snapshot ?forkLamport ?forkReplica ?written
		:where [?b :db.branch/replica _ ?written]
		       [?b :db.branch/snapshot ?snapshot]
		       [?b :db.branch/at-lamport ?lamport]
		       [?b :db.branch/at-replica ?replica]
		       [?b :db.branch/fork-lamport ?forkLamport]
		       [?b :db.branch/fork-replica ?forkReplica]
		       [?s :db.snapshot/name ?snapshot]
		       [?s :db.snapshot/at-lamport ?lamport]
		       [?s :db.snapshot/at-replica ?replica]]`)
	if err != nil {
		return nil, fmt.Errorf("branches forked after %v: %w", point, err)
	}

	var after []string
	for _, r := range records {
		forkPoint := datalog.ElementID{Lamport: uint64(r.ForkLamport), ReplicaID: uint64(r.ForkReplica)}
		if r.Written.ReplicaID == d.replicaID && point.Less(forkPoint) {
			after = append(after, r.Snapshot)
		}
	}
	slices.Sort(after)
	return slices.Compact(after), nil
}
