package storage

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
)

func countSessionIndex(t *testing.T, r StoreReader, index IndexType) int {
	t.Helper()
	iter, err := r.ScanKeysOnly(ScanBound{Index: index})
	require.NoError(t, err)
	defer iter.Close()
	count := 0
	for iter.Next() {
		count++
	}
	require.NoError(t, iter.Error())
	return count
}

// countSessionEntity counts the datoms a reader observes for one entity, by
// scanning that entity's EAVT run. Membership of a specific datom is the
// degenerate case of the same scan with every component bound.
func countSessionEntity(t *testing.T, r StoreReader, e datalog.Identity) int {
	t.Helper()
	iter, err := r.ScanKeysOnly(ScanBound{Index: EAVT, Prefix: []datalog.Value{e}})
	require.NoError(t, err)
	defer iter.Close()
	count := 0
	for iter.Next() {
		count++
	}
	require.NoError(t, iter.Error())
	return count
}

// TestReadSessionVBoundRunExcludesValueExtensions pins the membership rule on
// the read-session path.
//
// TestStoreBackendVBoundRunExcludesValueExtensions pins the same rule, but it
// calls the store's own Scan/ScanKeysOnly and so reaches MemoryStore.scan and
// BadgerStore's iterator constructor. The sessions carry their own copy of the
// membership assignment; deleting the one in read_session_memory.go leaves the
// store-path test green and only the wasm leg red. This closes that gap on both
// legs and both backends.
func TestReadSessionVBoundRunExcludesValueExtensions(t *testing.T) {
	for _, testCase := range storeContractCases() {
		t.Run(testCase.name, func(t *testing.T) {
			store := testCase.open(t, &BinaryKeyEncoder{})
			defer store.Close()

			attr := datalog.NewKeyword(":session/tag")
			short := datalog.NewIdentity("session:short")
			long := datalog.NewIdentity("session:long")
			require.NoError(t, store.Assert([]datalog.Datom{
				{E: short, A: attr, V: "abc", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
				{E: long, A: attr, V: "abcd", Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
			}))

			session, err := store.NewReadSession()
			require.NoError(t, err)
			defer session.Close()

			// "abcd" sorts inside the byte range for "abc"; only the length
			// test separates them, and only if the session applied it.
			iter, err := session.ScanKeysOnly(ScanBound{
				Index:  AVET,
				Prefix: []datalog.Value{attr, "abc"},
			})
			require.NoError(t, err)
			defer iter.Close()

			var got []datalog.Identity
			for iter.Next() {
				datom, err := iter.Datom()
				require.NoError(t, err)
				got = append(got, datom.E)
			}
			require.NoError(t, iter.Error())
			require.Equal(t, []datalog.Identity{short}, got,
				"the session's run must hold only the datom carrying the bound value")
		})
	}
}

func TestReadSessionSnapshotIsolation(t *testing.T) {
	for _, testCase := range storeContractCases() {
		t.Run(testCase.name, func(t *testing.T) {
			store := testCase.open(t, &BinaryKeyEncoder{})
			defer store.Close()

			entity := datalog.NewIdentity("session:e1")
			attr := datalog.NewKeyword(":session/value")
			require.NoError(t, store.Assert([]datalog.Datom{
				{E: entity, A: attr, V: int64(1), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			}))

			session, err := store.NewReadSession()
			require.NoError(t, err)
			defer session.Close()

			// A write committed after the session opened must be invisible
			// through the session and visible through the store.
			later := datalog.NewIdentity("session:e2")
			require.NoError(t, store.Assert([]datalog.Datom{
				{E: later, A: attr, V: int64(2), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
			}))

			require.Equal(t, 1, countSessionIndex(t, session, EAVT),
				"session must observe its snapshot, not the live store")
			require.Equal(t, 2, countStoreIndex(t, store, EAVT),
				"the store itself must observe the later write")

			// High-water marks through the session reflect the snapshot.
			maxID, err := session.MaxElementID()
			require.NoError(t, err)
			assert.Equal(t, datalog.ElementID{Lamport: 1, ReplicaID: 1}, maxID)

			// Per entity through the session: the pre-snapshot entity is
			// present, the post-snapshot one is not. Scanning each entity's
			// run asks the question directly, without the test having to know
			// a Tx in order to name a key.
			assert.Equal(t, 1, countSessionEntity(t, session, entity),
				"pre-snapshot entity must be visible")
			assert.Equal(t, 0, countSessionEntity(t, session, later),
				"post-snapshot entity must be invisible")
		})
	}
}

func TestReadSessionCloseSemantics(t *testing.T) {
	for _, testCase := range storeContractCases() {
		t.Run(testCase.name, func(t *testing.T) {
			store := testCase.open(t, &BinaryKeyEncoder{})
			defer store.Close()

			entity := datalog.NewIdentity("close:e1")
			attr := datalog.NewKeyword(":close/value")
			require.NoError(t, store.Assert([]datalog.Datom{
				{E: entity, A: attr, V: int64(1), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			}))

			session, err := store.NewReadSession()
			require.NoError(t, err)

			// Closing with an iterator still open must not panic; the
			// straggler's own Close afterwards is safe.
			iter, err := session.Scan(ScanBound{Index: EAVT})
			require.NoError(t, err)
			require.True(t, iter.Next())
			require.NoError(t, session.Close())
			require.NoError(t, iter.Close())

			// Double close is safe; reads after close fail loudly.
			require.NoError(t, session.Close())
			_, err = session.Scan(ScanBound{Index: EAVT})
			require.Error(t, err, "scan on a closed session must error")
			_, err = session.ScanKeysOnly(ScanBound{Index: EAVT})
			require.Error(t, err, "keys-only scan on a closed session must error")
		})
	}
}

func TestReadSessionConcurrentScans(t *testing.T) {
	for _, testCase := range storeContractCases() {
		t.Run(testCase.name, func(t *testing.T) {
			store := testCase.open(t, &BinaryKeyEncoder{})
			defer store.Close()

			attr := datalog.NewKeyword(":concurrent/value")
			var datoms []datalog.Datom
			for i := 0; i < 64; i++ {
				datoms = append(datoms, datalog.Datom{
					E:  datalog.NewIdentity("concurrent:" + strings.Repeat("x", i%7) + "e"),
					A:  attr,
					V:  int64(i),
					Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 1},
				})
			}
			require.NoError(t, store.Assert(datoms))

			session, err := store.NewReadSession()
			require.NoError(t, err)
			defer session.Close()

			// Multiple goroutines scan through one session simultaneously —
			// the executor's parallel workers do exactly this within a query.
			var wg sync.WaitGroup
			errs := make([]error, 8)
			counts := make([]int, 8)
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					iter, err := session.ScanKeysOnly(ScanBound{Index: EAVT})
					if err != nil {
						errs[g] = err
						return
					}
					defer iter.Close()
					for iter.Next() {
						counts[g]++
					}
					errs[g] = iter.Error()
				}(g)
			}
			wg.Wait()
			expected := countStoreIndex(t, store, EAVT)
			for g := 0; g < 8; g++ {
				require.NoError(t, errs[g], "goroutine %d", g)
				require.Equal(t, expected, counts[g], "goroutine %d", g)
			}
		})
	}
}

// TestQuerySnapshotConsistency pins the semantic the read session exists for:
// a write landing between two storage scans of one query must not produce a
// torn result — a tuple pairing values from two different database states.
func TestQuerySnapshotConsistency(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			e1 := datalog.NewIdentity("torn:e1")
			attrA := datalog.NewKeyword(":snap/a")
			attrB := datalog.NewKeyword(":snap/b")

			// After the first pattern's scan completes, flip both attributes to 2.
			// Without a session, the second pattern's scan runs against the store
			// post-write and pairs a=1 with b=2 (or b=1 with a=2) — a state that
			// never existed. With a per-query session both scans read the snapshot.
			//
			// The write must land synchronously between the two scans and against
			// this same database — a second handle over the same store would give
			// the writer its own EA cache and change what this test exercises. So
			// the handler closes over d, which is assigned once below, before the
			// query that fires any event. A single forward reference, not a target
			// that moves: the handler reports to the same place for its whole life.
			var d *Database
			var once sync.Once
			handler := func(event annotations.Event) {
				if !strings.HasPrefix(event.Name, "pattern/") {
					return
				}
				once.Do(func() {
					wtx := d.NewTransaction()
					if err := wtx.Add(e1, attrA, int64(2)); err != nil {
						t.Errorf("mid-query write add a: %v", err)
						return
					}
					if err := wtx.Add(e1, attrB, int64(2)); err != nil {
						t.Errorf("mid-query write add b: %v", err)
						return
					}
					if _, err := wtx.Commit(); err != nil {
						t.Errorf("mid-query write commit: %v", err)
					}
				})
			}
			d = createOptimizerModeDB(t, mode, DatabaseOptions{
				AnnotationHandler: handler,
			})

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(e1, attrA, int64(1)))
			require.NoError(t, tx.Add(e1, attrB, int64(1)))
			_, err := tx.Commit()
			require.NoError(t, err)

			rel, err := d.Query(`[:find ?va ?vb :where [?e :snap/a ?va] [?e :snap/b ?vb]]`)
			require.NoError(t, err)
			iter := rel.Iterator()
			defer iter.Close()
			require.True(t, iter.Next(), "query must produce a tuple")
			tuple := iter.Tuple()
			require.Len(t, tuple, 2)
			assert.Equal(t, int64(1), tuple[0], "?va must come from the query's snapshot")
			assert.Equal(t, int64(1), tuple[1], "?vb must come from the query's snapshot")
			require.False(t, iter.Next(), "exactly one tuple expected")
			require.NoError(t, iter.Error())
		})
	}
}
