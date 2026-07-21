package storage

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wbrown/janus-datalog/datalog"
)

// cacheKeyFor builds the cache key for an (entity, attribute) pair the way the storage
// layer does, for inspecting cache state in tests.
func cacheKeyFor(e datalog.Identity, a datalog.Keyword) CacheKey {
	sd := ToStorageDatom(datalog.Datom{E: e, A: a})
	return CacheKey{E: sd.E, A: sd.A}
}

// TestTruncateToInvalidatesCache verifies the rewind drops the cached view AND resets the
// monotonic freshness tracking for a rewound (E, A) — not just the entry. If maxVersions
// were left stranded at the pre-rollback high-water, the rebuilt lower-version entry would
// never compare fresh again.
func TestTruncateToInvalidatesCache(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode)

			snapTestAddName(t, d, "alice", "Alice")
			_, err := d.Snapshot("cp1")
			require.NoError(t, err)

			e := datalog.NewIdentity("alice")
			ageAttr := datalog.NewKeyword(":person/age")
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(e, ageAttr, int64(30)))
			_, err = tx.Commit()
			require.NoError(t, err)

			// Read the age so the cache holds a resolved view for (alice, :person/age).
			var ages []int64
			require.NoError(t, d.QueryInto(&ages, `[:find ?a :in $ ?e :where [?e :person/age ?a]]`, e))
			require.Equal(t, []int64{30}, ages)

			key := cacheKeyFor(e, ageAttr)
			slot, hadSlot := d.cache.slots.Load(key)
			require.True(t, hadSlot && slot.entry != nil, "the read should have populated the cache")

			// Rewind past the age write.
			require.NoError(t, d.TruncateTo("cp1"))

			if _, ok := d.cache.slots.Load(key); ok {
				t.Error("rewound slot must be dropped entirely — a surviving slot leaves either a stale entry or a stranded version high-water mark")
			}

			// The read now reflects the erasure, not a stale-cached 30.
			ages = nil
			require.NoError(t, d.QueryInto(&ages, `[:find ?a :in $ ?e :where [?e :person/age ?a]]`, e))
			assert.Empty(t, ages, "age must read as absent after rollback, not stale-cached")
		})
	}
}

// TestTruncateToDropsWritesStartedDuringRollback covers both writer semantics: an in-flight
// transaction (opened before the rollback) is allowed to clear, while a transaction opened
// while the rollback is in progress is dropped with ErrRollbackInProgress. It is made
// deterministic by holding the in-flight transaction open, which parks the rollback in its
// drain.
func TestTruncateToDropsWritesStartedDuringRollback(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode)

			snapTestAddName(t, d, "alice", "Alice")
			_, err := d.Snapshot("cp1")
			require.NoError(t, err)

			// Created BEFORE the rollback → in activeTx → the drain waits for it (allowed to clear).
			blocker := d.NewTransaction()
			require.NoError(t, blocker.Add(datalog.NewIdentity("bob"), datalog.NewKeyword(":person/name"), "Bob"))

			done := make(chan error, 1)
			go func() { done <- d.TruncateTo("cp1") }()

			// Wait until the rollback has entered its drain.
			require.Eventually(t, func() bool {
				d.mu.Lock()
				defer d.mu.Unlock()
				return d.rollbackInProgress
			}, 2*time.Second, time.Millisecond, "rollback should enter its drain")

			// Created DURING the rollback → dropped.
			doomed := d.NewTransaction()
			addErr := doomed.Add(datalog.NewIdentity("carol"), datalog.NewKeyword(":person/name"), "Carol")
			require.ErrorIs(t, addErr, ErrRollbackInProgress, "a write started during a rollback must be dropped")
			_, commitErr := doomed.Commit()
			require.ErrorIs(t, commitErr, ErrRollbackInProgress, "committing a dropped tx must report the rollback")

			// Releasing the in-flight write lets it clear and the rollback complete.
			_, blockErr := blocker.Commit()
			require.NoError(t, blockErr, "an in-flight write opened before the rollback must be allowed to clear")
			require.NoError(t, <-done)

			// Exactly the snapshot: Bob (cleared, but post-snapshot) was erased; Carol never landed.
			assert.Equal(t, []string{"Alice"}, snapTestNames(t, d))
			assert.Equal(t, []string{"Alice"}, snapTestHistoryNames(t, d))
		})
	}
}

// TestTruncateToConcurrentWritersNoCollision stresses concurrent writers against repeated
// rollbacks and asserts the core safety invariant: no two datoms share a Tx. A botched
// rewind (restoring the clock while a writer holds a Lamport above the floor) would produce
// a duplicate (Lamport, ReplicaID). Run under -race to also catch unsynchronized access to
// the drain state.
func TestTruncateToConcurrentWritersNoCollision(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode)

			snapTestAddName(t, d, "seed", "Seed")
			_, err := d.Snapshot("cp1")
			require.NoError(t, err)

			const writers = 8
			const perWriter = 25
			var wg sync.WaitGroup

			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < perWriter; i++ {
						tx := d.NewTransaction()
						e := datalog.NewIdentity(fmt.Sprintf("w%d-i%d", w, i))
						if err := tx.Add(e, datalog.NewKeyword(":person/name"), fmt.Sprintf("n%d-%d", w, i)); err != nil {
							if errors.Is(err, ErrRollbackInProgress) {
								_ = tx.Rollback()
								continue
							}
							t.Errorf("unexpected Add error: %v", err)
							return
						}
						if _, err := tx.Commit(); err != nil && !errors.Is(err, ErrRollbackInProgress) {
							t.Errorf("unexpected Commit error: %v", err)
							return
						}
					}
				}(w)
			}

			for r := 0; r < 3; r++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := d.TruncateTo("cp1"); err != nil {
						t.Errorf("TruncateTo error: %v", err)
					}
				}()
			}

			wg.Wait()

			// No two datoms may share a Tx.
			all, err := d.store.DatomsAfter(datalog.ElementID{})
			require.NoError(t, err)
			seen := make(map[datalog.ElementID]struct{}, len(all))
			for i := range all {
				if _, dup := seen[all[i].Tx]; dup {
					t.Fatalf("duplicate Tx %v — clock collision from a rewind", all[i].Tx)
				}
				seen[all[i].Tx] = struct{}{}
			}

			// Still consistent and queryable; the pre-snapshot seed always survives a rollback to cp1.
			var names []string
			require.NoError(t, d.QueryInto(&names, `[:find ?n :where [?e :person/name ?n]]`))
			sort.Strings(names)
			assert.Contains(t, names, "Seed", "the pre-snapshot datom must survive every rollback to cp1")
		})
	}
}
