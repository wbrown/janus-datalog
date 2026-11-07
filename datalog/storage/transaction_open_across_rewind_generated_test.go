package storage

import (
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// rewindHistory is one generated history: the batches committed before a
// snapshot and after it, and the writes of a transaction whose NewTransaction
// overlaps the rewind to the snapshot. log names each write for a failure
// message.
type rewindHistory struct {
	before, after [][]resolutionWrite
	writes        []resolutionWrite
	log           []string
}

// generateRewindHistory draws a history over entities: their names, up to two
// batches before the snapshot, one to three after it, and one to four writes
// by the overlapping transaction.
func generateRewindHistory(r *rand.Rand, entities []datalog.Identity) rewindHistory {
	var h rewindHistory
	batches := func(label string, n int) [][]resolutionWrite {
		out := make([][]resolutionWrite, n)
		for i := range out {
			for k := 1 + r.Intn(4); k > 0; k-- {
				w := drawResolutionWrite(r, entities, resolutionAttributes)
				h.log = append(h.log, label+": "+w.text)
				out[i] = append(out[i], w)
			}
		}
		return out
	}
	var names []resolutionWrite
	for n, e := range entities {
		e, name := e, fmt.Sprintf("e%d", n)
		names = append(names, resolutionWrite{"Add name " + name, func(tx *Transaction) error { return tx.Add(e, resolutionName, name) }})
	}
	h.before = append([][]resolutionWrite{names}, batches("before the snapshot", r.Intn(3))...)
	h.after = batches("after the snapshot", 1+r.Intn(3))
	for k := 1 + r.Intn(4); k > 0; k-- {
		w := drawResolutionWrite(r, entities, resolutionAttributes)
		h.log = append(h.log, "overlapping: "+w.text)
		h.writes = append(h.writes, w)
	}
	return h
}

// rewindKey is one (E, A) a rewind outcome reads.
type rewindKey struct {
	e datalog.Identity
	a datalog.Keyword
}

// rewindOutcome is what a run of a history answers: the overlapping
// transaction's lookups before it commits, and the database's lookups once
// the rewind and the commit have both returned.
type rewindOutcome struct {
	views, finals map[rewindKey]resolutionHeld
}

// commitRewindBatches commits each batch in a transaction of its own.
func commitRewindBatches(t *testing.T, d *Database, batches [][]resolutionWrite) {
	t.Helper()
	for _, batch := range batches {
		tx := d.NewTransaction()
		for _, w := range batch {
			require.NoError(t, w.apply(tx))
		}
		_, err := tx.Commit()
		require.NoError(t, err)
	}
}

// writeAndReadRewindTransaction makes the history's overlapping writes on tx,
// reads every (E, A) through tx and commits it.
func writeAndReadRewindTransaction(t *testing.T, tx *Transaction, h rewindHistory, entities []datalog.Identity) map[rewindKey]resolutionHeld {
	t.Helper()
	for _, w := range h.writes {
		require.NoError(t, w.apply(tx))
	}
	views := map[rewindKey]resolutionHeld{}
	for _, e := range entities {
		for _, a := range resolutionAttributes {
			value, found, err := tx.LookupAttribute(e, a)
			require.NoError(t, err)
			views[rewindKey{e, a}] = resolutionHeldOf(a, value, found)
		}
	}
	_, err := tx.Commit()
	require.NoError(t, err)
	return views
}

// readRewindDatabase reads every (E, A) through d.
func readRewindDatabase(t *testing.T, d *Database, entities []datalog.Identity) map[rewindKey]resolutionHeld {
	t.Helper()
	finals := map[rewindKey]resolutionHeld{}
	for _, e := range entities {
		for _, a := range resolutionAttributes {
			value, found, err := d.LookupAttribute(e, a)
			require.NoError(t, err)
			finals[rewindKey{e, a}] = resolutionHeldOf(a, value, found)
		}
	}
	return finals
}

// runRacedRewind runs h with the overlapping transaction's NewTransaction
// held inside its read session's open while a TruncateTo is called. drained
// is whether the call holds the database's lock there, so that the rewind
// waits for the transaction's admission and drains it; otherwise the rewind
// runs to its end before the admission.
func runRacedRewind(t *testing.T, h rewindHistory, entities []datalog.Identity, disableCache bool) (rewindOutcome, bool) {
	t.Helper()
	gate := rewindGateStore{Store: NewBadgerEmulatorStore(nil), armed: &atomic.Bool{}, opened: make(chan struct{}), release: make(chan struct{})}
	d, err := NewDatabaseWithOptions(DatabaseOptions{Store: gate, Schema: resolutionSchema(t), DisableCache: disableCache})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	commitRewindBatches(t, d, h.before)
	_, err = d.Snapshot("s")
	require.NoError(t, err)
	commitRewindBatches(t, d, h.after)

	gate.armed.Store(true)
	opening := make(chan *Transaction, 1)
	go func() { opening <- d.NewTransaction() }()
	select {
	case <-gate.opened:
	case tx := <-opening:
		require.NoError(t, tx.Rollback())
		require.FailNow(t, "NewTransaction opened no read session")
	}
	var rewound chan error
	drained := !d.mu.TryLock()
	if drained {
		rewound = make(chan error, 1)
		go func() { rewound <- d.TruncateTo("s") }()
		close(gate.release)
	} else {
		d.mu.Unlock()
		require.NoError(t, d.TruncateTo("s"))
		close(gate.release)
	}
	views := writeAndReadRewindTransaction(t, <-opening, h, entities)
	if rewound != nil {
		require.NoError(t, <-rewound)
	}
	return rewindOutcome{views: views, finals: readRewindDatabase(t, d, entities)}, drained
}

// runReferenceRewind runs h with the overlapping transaction ordered on one
// side of the rewind: committed before the TruncateTo when drained, and
// opened after it returns otherwise.
func runReferenceRewind(t *testing.T, h rewindHistory, entities []datalog.Identity, disableCache, drained bool) rewindOutcome {
	t.Helper()
	d, err := NewDatabaseWithOptions(DatabaseOptions{Store: NewBadgerEmulatorStore(nil), Schema: resolutionSchema(t), DisableCache: disableCache})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	commitRewindBatches(t, d, h.before)
	_, err = d.Snapshot("s")
	require.NoError(t, err)
	commitRewindBatches(t, d, h.after)
	if !drained {
		require.NoError(t, d.TruncateTo("s"))
	}
	views := writeAndReadRewindTransaction(t, d.NewTransaction(), h, entities)
	if drained {
		require.NoError(t, d.TruncateTo("s"))
	}
	return rewindOutcome{views: views, finals: readRewindDatabase(t, d, entities)}
}

// TestATransactionOverlappingARewindAnswersAsOneOnItsSideOfTheRewindOnGeneratedHistories
// runs generated histories in which a transaction's NewTransaction is opening
// its read session when a TruncateTo is called. Its lookups before it commits,
// and the database's once the rewind and the commit have returned, are those
// of a transaction ordered on the side of the rewind it is admitted on, in a
// database the race never touches.
func TestATransactionOverlappingARewindAnswersAsOneOnItsSideOfTheRewindOnGeneratedHistories(t *testing.T) {
	entities := []datalog.Identity{
		datalog.NewIdentity("rewind-generated:first"),
		datalog.NewIdentity("rewind-generated:second"),
	}
	for _, disableCache := range []bool{false, true} {
		for seed := int64(1); seed <= 150; seed++ {
			t.Run(fmt.Sprintf("cache-off=%v/seed=%d", disableCache, seed), func(t *testing.T) {
				h := generateRewindHistory(rand.New(rand.NewSource(seed)), entities)
				raced, drained := runRacedRewind(t, h, entities, disableCache)
				reference := runReferenceRewind(t, h, entities, disableCache, drained)
				side := "after the rewind"
				if drained {
					side = "before the rewind, drained by it"
				}
				require.Equal(t, reference.views, raced.views, "the overlapping transaction's view, admitted %s\n%s", side, strings.Join(h.log, "\n"))
				require.Equal(t, reference.finals, raced.finals, "the database after the rewind and the commit, the transaction admitted %s\n%s", side, strings.Join(h.log, "\n"))
			})
		}
	}
}
