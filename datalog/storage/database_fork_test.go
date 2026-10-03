package storage

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// forkingModes splits the optimizer-mode axis by whether the backend can fork a
// branch. The switch has no silent default: a new backend fails here until
// someone says which side of that line it falls on.
func forkingModes(t *testing.T) (forking, refusing []optimizerMode) {
	t.Helper()
	for _, mode := range optimizerModes {
		switch mode.backend.Name {
		case "memory-trees":
			forking = append(forking, mode)
		case "memory", "badger":
			refusing = append(refusing, mode)
		default:
			t.Fatalf("forkingModes: backend %q is unclassified — can it fork a branch?",
				mode.backend.Name)
		}
	}
	return forking, refusing
}

// branchOf forks parent from its snapshot and closes the branch when the test
// ends.
func branchOf(t *testing.T, parent *Database, snapshot string) *Database {
	t.Helper()
	branch, err := parent.Fork(snapshot)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, branch.Close()) })
	return branch
}

// factsOf returns every datom d's current view resolves to, as [e a v] tuples.
func factsOf(t *testing.T, d *Database) [][]interface{} {
	t.Helper()
	tuples, err := executor.CollectTuples(d.Query(`[:find ?e ?a ?v :where [?e ?a ?v]]`))
	require.NoError(t, err)
	return tuples
}

// snapshotNames lists d's snapshot registry by name.
func snapshotNames(t *testing.T, d *Database) []string {
	t.Helper()
	snaps, err := d.Snapshots()
	require.NoError(t, err)
	names := make([]string, 0, len(snaps))
	for _, s := range snaps {
		names = append(names, s.Name)
	}
	return names
}

// branchTestPerson pulls :person/name through the EA cache.
type branchTestPerson struct {
	Name string `datalog:"person/name"`
}

// TestForkHoldsItsSnapshot: a branch holds exactly what its parent holds as of
// the snapshot it forked from, whatever the parent wrote after the snapshot and
// whatever branches it forked from it before.
func TestForkHoldsItsSnapshot(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("s")
			require.NoError(t, err)
			snapTestAddName(t, parent, "bob", "Bob")

			atSnapshot, err := parent.AsOfSnapshot("s")
			require.NoError(t, err)
			want := factsOf(t, atSnapshot)

			first := branchOf(t, parent, "s")
			second := branchOf(t, parent, "s")
			require.ElementsMatch(t, want, factsOf(t, first), "first branch")
			require.ElementsMatch(t, want, factsOf(t, second), "second branch")
			require.Equal(t, []string{"Alice"}, snapTestNames(t, second))
		})
	}
}

// TestForkTakesNoSnapshot: forking reads a snapshot the parent already holds
// and adds none, so the registry lists the same snapshots after any number of
// forks.
func TestForkTakesNoSnapshot(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("s")
			require.NoError(t, err)

			branch := branchOf(t, parent, "s")
			branchOf(t, parent, "s")
			require.Equal(t, []string{"s"}, snapshotNames(t, parent))
			require.Equal(t, []string{"s"}, snapshotNames(t, branch))
		})
	}
}

// TestForkOfAMissingSnapshot: there is nothing to fork from a snapshot the
// database does not hold, and an empty name names none.
func TestForkOfAMissingSnapshot(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")

			_, err := parent.Fork("absent")
			require.ErrorIs(t, err, ErrSnapshotNotFound)
			_, err = parent.Fork("")
			require.Error(t, err)
			require.Empty(t, snapshotNames(t, parent))
		})
	}
}

// TestAsOfSnapshotIgnoresBranchesForkedFromIt: forking from a snapshot leaves
// what the snapshot reads unchanged.
func TestAsOfSnapshotIgnoresBranchesForkedFromIt(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("s")
			require.NoError(t, err)
			snapTestAddName(t, parent, "bob", "Bob")

			before, err := parent.AsOfSnapshot("s")
			require.NoError(t, err)
			want := factsOf(t, before)

			branchOf(t, parent, "s")
			after, err := parent.AsOfSnapshot("s")
			require.NoError(t, err)
			require.ElementsMatch(t, want, factsOf(t, after))
			require.Equal(t, []string{"Alice"}, snapTestNames(t, after))
		})
	}
}

// TestBranchAndParentWritesStayApart: after the fork, what the branch writes is
// invisible to its parent and what the parent writes is invisible to the
// branch, read through a query and through each handle's EA cache. Each side
// supersedes a value both cached before the fork.
func TestBranchAndParentWritesStayApart(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	alice := datalog.NewIdentity("alice")
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			var pulled branchTestPerson
			require.NoError(t, parent.PullInto(alice, &pulled))
			require.Equal(t, "Alice", pulled.Name)
			_, err := parent.Snapshot("s")
			require.NoError(t, err)

			branch := branchOf(t, parent, "s")
			require.NoError(t, branch.PullInto(alice, &pulled))
			require.Equal(t, "Alice", pulled.Name)

			snapTestAddName(t, parent, "alice", "Alicia")
			snapTestAddName(t, parent, "carol", "Carol")
			snapTestAddName(t, branch, "alice", "Ally")
			snapTestAddName(t, branch, "dave", "Dave")

			require.Equal(t, []string{"Alicia", "Carol"}, snapTestNames(t, parent))
			require.Equal(t, []string{"Ally", "Dave"}, snapTestNames(t, branch))

			var inParent, inBranch branchTestPerson
			require.NoError(t, parent.PullInto(alice, &inParent))
			require.Equal(t, "Alicia", inParent.Name)
			require.NoError(t, branch.PullInto(alice, &inBranch))
			require.Equal(t, "Ally", inBranch.Name)
		})
	}
}

// TestBranchIsItsOwnWriteStream: every branch writes under a ReplicaID that is
// neither its parent's nor any sibling's, and its writes order after everything
// it inherited. The parent writes enough first that a branch clock starting
// from nothing would order before what it inherited.
func TestBranchIsItsOwnWriteStream(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			for i := 0; i < 32; i++ {
				snapTestAddName(t, parent, "alice", fmt.Sprintf("Alice %d", i))
			}
			_, err := parent.Snapshot("s")
			require.NoError(t, err)

			streams := map[uint64]string{parent.ReplicaID(): "parent"}
			for _, branchName := range []string{"first", "second", "third"} {
				branch := branchOf(t, parent, "s")
				inherited, err := branch.Store().MaxElementID()
				require.NoError(t, err)

				owner, taken := streams[branch.ReplicaID()]
				require.False(t, taken, "%s writes as %s's replica", branchName, owner)
				streams[branch.ReplicaID()] = branchName

				committed := snapTestAddName(t, branch, "alice", branchName)
				require.Equal(t, branch.ReplicaID(), committed.ReplicaID,
					"%s committed under another write identity", branchName)
				require.True(t, inherited.Less(committed),
					"%s's write does not order after what it inherited", branchName)
			}
		})
	}
}

// TestSiblingBranchesCommitConcurrently: branches of one snapshot commit at the
// same time as each other and as the parent, and each ends holding what it
// inherited and its own writes, nothing another writer committed.
func TestSiblingBranchesCommitConcurrently(t *testing.T) {
	const (
		siblings  = 6
		perWriter = 24
	)
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	name := datalog.NewKeyword(":person/name")
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			var inherited []string
			for i := 0; i < 8; i++ {
				shared := fmt.Sprintf("shared-%d", i)
				snapTestAddName(t, parent, shared, shared)
				inherited = append(inherited, shared)
			}
			_, err := parent.Snapshot("s")
			require.NoError(t, err)

			// The siblings are writers 0 through siblings-1; the parent writes last.
			writers := make([]*Database, 0, siblings+1)
			for i := 0; i < siblings; i++ {
				writers = append(writers, branchOf(t, parent, "s"))
			}
			writers = append(writers, parent)
			writesOf := func(writer int) []string {
				var own []string
				for j := 0; j < perWriter; j++ {
					own = append(own, fmt.Sprintf("writer-%d-%d", writer, j))
				}
				return own
			}

			var wg sync.WaitGroup
			failures := make(chan error, len(writers))
			for w, d := range writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for _, value := range writesOf(w) {
						tx := d.NewTransaction()
						if err := tx.Add(datalog.NewIdentity(value), name, value); err != nil {
							failures <- fmt.Errorf("writer %d: %w", w, err)
							return
						}
						if _, err := tx.Commit(); err != nil {
							failures <- fmt.Errorf("writer %d: %w", w, err)
							return
						}
					}
				}()
			}
			wg.Wait()
			close(failures)
			for err := range failures {
				t.Error(err)
			}

			for w, d := range writers {
				want := append(append([]string(nil), inherited...), writesOf(w)...)
				sort.Strings(want)
				require.Equal(t, want, snapTestNames(t, d), "writer %d", w)
			}
		})
	}
}

// TestBranchCarriesItsParentSchema: a branch validates writes against its
// parent's schema.
func TestBranchCarriesItsParentSchema(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	e := datalog.NewIdentity("e")
	attr := datalog.NewKeyword(":test/string")
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: neverZeroSchema(t)})
			_, err := parent.Snapshot("s")
			require.NoError(t, err)
			branch := branchOf(t, parent, "s")

			tx := branch.NewTransaction()
			err = tx.Add(e, attr, "")
			require.Error(t, err)
			require.Contains(t, err.Error(), "schema validation failed")
			require.NoError(t, tx.Add(e, attr, "ok"))
			_, err = tx.Commit()
			require.NoError(t, err)
		})
	}
}

// TestAsOfAndHistoryOnABranch: AsOf on a branch shows the branch at that point,
// a point before the fork included, and History on a branch shows what it
// inherited plus its own writes.
func TestAsOfAndHistoryOnABranch(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			beforeFork := snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("s")
			require.NoError(t, err)

			branch := branchOf(t, parent, "s")
			withBob := snapTestAddName(t, branch, "bob", "Bob")
			snapTestAddName(t, branch, "carol", "Carol")

			require.Equal(t, []string{"Alice"}, snapTestNames(t, branch.AsOf(beforeFork)))
			require.Equal(t, []string{"Alice", "Bob"}, snapTestNames(t, branch.AsOf(withBob)))
			require.Equal(t, []string{"Alice", "Bob", "Carol"}, snapTestHistoryNames(t, branch))
			require.Equal(t, []string{"Alice"}, snapTestHistoryNames(t, parent))
		})
	}
}

// TestTruncateToOnABranch: TruncateTo on a branch returns the branch to its
// state at the snapshot — one it took itself, or one it inherited from before
// the fork — and leaves its parent and siblings unchanged. The branch writes on
// from there.
func TestTruncateToOnABranch(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("before-bob")
			require.NoError(t, err)
			snapTestAddName(t, parent, "bob", "Bob")
			_, err = parent.Snapshot("with-bob")
			require.NoError(t, err)

			branch := branchOf(t, parent, "with-bob")
			sibling := branchOf(t, parent, "with-bob")
			_, err = branch.Snapshot("in-branch")
			require.NoError(t, err)
			snapTestAddName(t, branch, "carol", "Carol")

			require.NoError(t, branch.TruncateTo("in-branch"))
			require.Equal(t, []string{"Alice", "Bob"}, snapTestNames(t, branch))

			require.NoError(t, branch.TruncateTo("before-bob"))
			require.Equal(t, []string{"Alice"}, snapTestNames(t, branch))
			require.Equal(t, []string{"Alice"}, snapTestHistoryNames(t, branch))

			snapTestAddName(t, branch, "dave", "Dave")
			require.Equal(t, []string{"Alice", "Dave"}, snapTestNames(t, branch))

			require.Equal(t, []string{"Alice", "Bob"}, snapTestNames(t, parent))
			require.Equal(t, []string{"Alice", "Bob"}, snapTestNames(t, sibling))
		})
	}
}

// TestTruncateToRefusesToPassALaterBranch: TruncateTo errors when a branch was
// forked from this database at a snapshot after the target, names that
// snapshot, and removes nothing. That includes a snapshot taken right after the
// target with nothing written between: the target's commit ends with its
// :db/txInstant mark, above the marker the rewind keeps. A rewind that keeps the
// snapshot a branch forked from keeps the record of the branch too, so a later
// TruncateTo still refuses to pass it.
func TestTruncateToRefusesToPassALaterBranch(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name+"/written_between", func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("s")
			require.NoError(t, err)
			snapTestAddName(t, parent, "bob", "Bob")
			_, err = parent.Snapshot("later")
			require.NoError(t, err)
			branchOf(t, parent, "later")

			err = parent.TruncateTo("s")
			require.ErrorIs(t, err, ErrBranchedAfterSnapshot)
			require.Contains(t, err.Error(), `"later"`)
			require.Equal(t, []string{"Alice", "Bob"}, snapTestNames(t, parent))
		})
		t.Run(mode.name+"/taken_right_after", func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("s")
			require.NoError(t, err)
			_, err = parent.Snapshot("right-after")
			require.NoError(t, err)
			branchOf(t, parent, "right-after")

			err = parent.TruncateTo("s")
			require.ErrorIs(t, err, ErrBranchedAfterSnapshot)
			require.Contains(t, err.Error(), `"right-after"`)
			require.Equal(t, []string{"s", "right-after"}, snapshotNames(t, parent))
		})
		t.Run(mode.name+"/after_a_rewind_that_kept_it", func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("z")
			require.NoError(t, err)
			snapTestAddName(t, parent, "bob", "Bob")
			_, err = parent.Snapshot("a")
			require.NoError(t, err)
			snapTestAddName(t, parent, "carol", "Carol")
			_, err = parent.Snapshot("b")
			require.NoError(t, err)
			branchOf(t, parent, "a")

			require.NoError(t, parent.TruncateTo("b"))
			err = parent.TruncateTo("z")
			require.ErrorIs(t, err, ErrBranchedAfterSnapshot)
			require.Contains(t, err.Error(), `"a"`)
			require.Equal(t, []string{"Alice", "Bob", "Carol"}, snapTestNames(t, parent))
		})
	}
}

// TestTruncateToPassesBranchesItDoesNotPrecede: the branches that do not stop a
// TruncateTo — one forked from the target itself, one forked from an earlier
// snapshot, and, on a branch, a branch its parent forked, whose record the
// branch inherited.
func TestTruncateToPassesBranchesItDoesNotPrecede(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name+"/forked_from_the_target", func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("s")
			require.NoError(t, err)
			branchOf(t, parent, "s")
			snapTestAddName(t, parent, "bob", "Bob")

			require.NoError(t, parent.TruncateTo("s"))
			require.Equal(t, []string{"Alice"}, snapTestNames(t, parent))
		})
		t.Run(mode.name+"/forked_from_an_earlier_snapshot", func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("earlier")
			require.NoError(t, err)
			branchOf(t, parent, "earlier")
			_, err = parent.Snapshot("s")
			require.NoError(t, err)
			snapTestAddName(t, parent, "bob", "Bob")

			require.NoError(t, parent.TruncateTo("s"))
			require.Equal(t, []string{"Alice"}, snapTestNames(t, parent))
		})
		t.Run(mode.name+"/inherited_record", func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("t")
			require.NoError(t, err)
			snapTestAddName(t, parent, "bob", "Bob")
			_, err = parent.Snapshot("a")
			require.NoError(t, err)
			branchOf(t, parent, "a")
			_, err = parent.Snapshot("b")
			require.NoError(t, err)
			second := branchOf(t, parent, "b")

			require.NoError(t, second.TruncateTo("t"))
			require.Equal(t, []string{"Alice"}, snapTestNames(t, second))
		})
	}
}

// TestForkRefusesTemporalViews: an AsOf or History handle is read-only and a
// fork is a new writable timeline, so forking either returns an error and
// records no branch in the database.
func TestForkRefusesTemporalViews(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			committed := snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("t")
			require.NoError(t, err)
			snapTestAddName(t, parent, "bob", "Bob")
			_, err = parent.Snapshot("s")
			require.NoError(t, err)

			_, err = parent.AsOf(committed).Fork("s")
			require.Error(t, err)
			require.Contains(t, err.Error(), "temporal")
			_, err = parent.History().Fork("s")
			require.Error(t, err)
			require.Contains(t, err.Error(), "temporal")

			require.NoError(t, parent.TruncateTo("t"))
			require.Equal(t, []string{"Alice"}, snapTestNames(t, parent))
		})
	}
}

// TestForkRefusesStoresThatCannotFork: a backend whose store has no way to
// start a branch returns an error from Fork and records no branch.
func TestForkRefusesStoresThatCannotFork(t *testing.T) {
	_, refusing := forkingModes(t)
	require.NotEmpty(t, refusing)
	for _, mode := range refusing {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("t")
			require.NoError(t, err)
			snapTestAddName(t, parent, "bob", "Bob")
			_, err = parent.Snapshot("s")
			require.NoError(t, err)

			_, err = parent.Fork("s")
			require.Error(t, err)
			require.Contains(t, err.Error(), "cannot fork")

			require.NoError(t, parent.TruncateTo("t"))
			require.Equal(t, []string{"Alice"}, snapTestNames(t, parent))
		})
	}
}

// TestBranchesCloseIndependently: closing a branch leaves its parent and its
// sibling open, and closing the parent leaves its branches open.
func TestBranchesCloseIndependently(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("s")
			require.NoError(t, err)
			first, err := parent.Fork("s")
			require.NoError(t, err)
			second := branchOf(t, parent, "s")

			require.NoError(t, first.Close())
			snapTestAddName(t, parent, "bob", "Bob")
			require.Equal(t, []string{"Alice", "Bob"}, snapTestNames(t, parent))

			require.NoError(t, parent.Close())
			snapTestAddName(t, second, "carol", "Carol")
			require.Equal(t, []string{"Alice", "Carol"}, snapTestNames(t, second))
		})
	}
}

// TestForkOfABranch: a branch forks from its own snapshots like any database,
// and the three generations stay apart: neither descendant sees what its
// parent wrote after the snapshot it forked from.
func TestForkOfABranch(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("p")
			require.NoError(t, err)
			child := branchOf(t, parent, "p")
			snapTestAddName(t, parent, "dave", "Dave")

			snapTestAddName(t, child, "bob", "Bob")
			_, err = child.Snapshot("c")
			require.NoError(t, err)
			snapTestAddName(t, child, "erin", "Erin")
			grandchild := branchOf(t, child, "c")
			snapTestAddName(t, grandchild, "carol", "Carol")

			require.Equal(t, []string{"Alice", "Dave"}, snapTestNames(t, parent))
			require.Equal(t, []string{"Alice", "Bob", "Erin"}, snapTestNames(t, child))
			require.Equal(t, []string{"Alice", "Bob", "Carol"}, snapTestNames(t, grandchild))
		})
	}
}

// TestForkDuringATruncateToRecordsNothing: a fork whose record would land while
// its parent is rewinding is dropped like any write started during the
// rollback. Fork reports ErrRollbackInProgress, and the parent, once the
// rewind completes, holds its snapshots and no record of the branch. The
// rollback is held in its drain by a transaction opened before it.
func TestForkDuringATruncateToRecordsNothing(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("cp0")
			require.NoError(t, err)
			_, err = parent.Snapshot("cp1")
			require.NoError(t, err)

			blocker := parent.NewTransaction()
			require.NoError(t, blocker.Add(datalog.NewIdentity("bob"), datalog.NewKeyword(":person/name"), "Bob"))
			done := make(chan error, 1)
			go func() { done <- parent.TruncateTo("cp1") }()
			require.Eventually(t, func() bool {
				parent.mu.Lock()
				defer parent.mu.Unlock()
				return parent.rollbackInProgress
			}, 2*time.Second, time.Millisecond, "rollback should enter its drain")

			_, err = parent.Fork("cp1")
			require.ErrorIs(t, err, ErrRollbackInProgress)

			_, err = blocker.Commit()
			require.NoError(t, err)
			require.NoError(t, <-done)

			require.Equal(t, []string{"cp0", "cp1"}, snapshotNames(t, parent))
			require.Equal(t, []string{"Alice"}, snapTestNames(t, parent))
			require.NoError(t, parent.TruncateTo("cp0"))
		})
	}
}

// TestDeletingABranchRecordReleasesTruncateTo: deleting the snapshot a branch
// forked from removes the branch's record with it, so the branch no longer
// stops a TruncateTo, and a snapshot that takes the name afterward is a plain
// snapshot.
func TestDeletingABranchRecordReleasesTruncateTo(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("s")
			require.NoError(t, err)
			snapTestAddName(t, parent, "bob", "Bob")
			_, err = parent.Snapshot("later")
			require.NoError(t, err)
			branchOf(t, parent, "later")
			require.ErrorIs(t, parent.TruncateTo("s"), ErrBranchedAfterSnapshot)

			require.NoError(t, parent.DeleteSnapshot("later"))
			_, err = parent.Snapshot("later")
			require.NoError(t, err)

			require.NoError(t, parent.TruncateTo("s"))
			require.Equal(t, []string{"Alice"}, snapTestNames(t, parent))
		})
	}
}

// TestTruncateToRefusesToPassALaterBranchOnABranch: a branch's own TruncateTo
// refuses to pass a branch it forked from a later snapshot of its own.
func TestTruncateToRefusesToPassALaterBranchOnABranch(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("p")
			require.NoError(t, err)
			child := branchOf(t, parent, "p")
			_, err = child.Snapshot("s")
			require.NoError(t, err)
			snapTestAddName(t, child, "bob", "Bob")
			_, err = child.Snapshot("g")
			require.NoError(t, err)
			branchOf(t, child, "g")

			err = child.TruncateTo("s")
			require.ErrorIs(t, err, ErrBranchedAfterSnapshot)
			require.Contains(t, err.Error(), `"g"`)
			require.Equal(t, []string{"Alice", "Bob"}, snapTestNames(t, child))
		})
	}
}
