package storage

import (
	"errors"
	"fmt"
	"sort"

	"github.com/wbrown/janus-datalog/datalog"
)

// A branch is a writable database that starts from one of its parent's
// snapshots and from then on writes independently of it, as a replica of its
// own: its own ReplicaID and its own Lamport clock. The parent records the
// branch on that snapshot's marker by the branch's ReplicaID. See
// docs/proposals/BRANCHING_AND_SNAPSHOTS.md §10.7 and §12.2.

// snapshotBranchReplicaAttr records on a snapshot's marker the ReplicaID of a
// branch forked from it. Well-known for the reason the other marker attributes
// are.
var snapshotBranchReplicaAttr = datalog.WellKnownKeyword(":db.snapshot/branch-replica")

// ErrBranchedAfterSnapshot is returned by TruncateTo when the database forked a
// branch from a snapshot after the one it is asked to truncate to.
var ErrBranchedAfterSnapshot = errors.New("a branch was forked after the snapshot")

// forker is a store that can start a branch: a store of its own holding this
// one's current published state, reading it as of a ceiling, and writing
// independently of it.
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
// Fork records the branch on the snapshot's marker, and TruncateTo refuses to
// pass a snapshot a branch was forked from. An AsOf or History handle is
// read-only and cannot fork, and neither can a store with no way to start a
// branch.
func (d *Database) Fork(name string) (*Database, error) {
	if d.temporalTxID != nil {
		return nil, fmt.Errorf("Fork: cannot fork a read-only temporal handle (AsOf/History)")
	}
	if name == "" {
		return nil, fmt.Errorf("Fork: snapshot name must not be empty")
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
	branch, err := NewDatabaseWithOptions(DatabaseOptions{
		Store:          forked,
		Schema:         d.schema,
		PlannerOptions: &plannerOptions,
		DisableCache:   d.cache == nil,
	})
	if err != nil {
		return nil, errors.Join(fmt.Errorf("Fork %q: %w", name, err), forked.Close())
	}
	if err := d.recordBranch(name, branch.ReplicaID()); err != nil {
		return nil, errors.Join(err, branch.Close())
	}
	return branch, nil
}

// recordBranch records on the marker of the snapshot named name that a branch
// writing as branchReplica was forked from it. The datom's ElementID carries
// d's ReplicaID, which is what makes it a branch d forked.
func (d *Database) recordBranch(name string, branchReplica uint64) (err error) {
	tx := d.NewTransaction()
	committed := false
	defer func() {
		if !committed {
			err = errors.Join(err, tx.Rollback())
		}
	}()

	if err := tx.Add(snapshotEntity(name), snapshotBranchReplicaAttr, int64(branchReplica)); err != nil {
		return fmt.Errorf("Fork %q: %w", name, err)
	}
	if _, err := tx.Commit(); err != nil {
		return fmt.Errorf("Fork %q: commit: %w", name, err)
	}
	committed = true
	return nil
}

// branchesForkedAfter names the snapshots past point that d forked a branch
// from: the snapshots whose branches a TruncateTo to point would cut beneath.
// A branch record d inherited from its own parent was written under another
// ReplicaID and is not one of them.
func (d *Database) branchesForkedAfter(point datalog.ElementID) ([]string, error) {
	type branchRecord struct {
		Name    string            `datalog:"?name"`
		Lamport int64             `datalog:"?lamport"`
		Replica int64             `datalog:"?replica"`
		Written datalog.ElementID `datalog:"?written"`
	}
	var records []branchRecord
	err := d.QueryInto(&records, `[:find ?name ?lamport ?replica ?written
		:where [?s :db.snapshot/branch-replica _ ?written]
		       [?s :db.snapshot/name ?name]
		       [?s :db.snapshot/at-lamport ?lamport]
		       [?s :db.snapshot/at-replica ?replica]]`)
	if err != nil {
		return nil, fmt.Errorf("branches forked after %v: %w", point, err)
	}

	var after []string
	for _, r := range records {
		forkPoint := datalog.ElementID{Lamport: uint64(r.Lamport), ReplicaID: uint64(r.Replica)}
		if r.Written.ReplicaID == d.replicaID && point.Less(forkPoint) {
			after = append(after, r.Name)
		}
	}
	sort.Strings(after)
	return after, nil
}
