package storage

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

var (
	readSessionName  = datalog.NewKeyword(":rs/name")
	readSessionTags  = datalog.NewKeyword(":rs/tags")
	readSessionCells = datalog.NewKeyword(":rs/cells")
	readSessionOrder = datalog.NewKeyword(":rs/order")
)

var errSessionOpen = errors.New("read session open failure")

// sessionOpenFailingStore is a store whose read sessions fail to open while
// failing holds.
type sessionOpenFailingStore struct {
	Store
	failing *atomic.Bool
}

func (s sessionOpenFailingStore) NewReadSession() (ReadSession, error) {
	if s.failing.Load() {
		return nil, errSessionOpen
	}
	return s.Store.NewReadSession()
}

// readSessionDatabase opens a database over store with a value, a set, a
// vector and an ordered set declared, and holds e's name "n", tags [t],
// cells [c] and order [o].
func readSessionDatabase(t *testing.T, store Store, e datalog.Identity) *Database {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(readSessionName.String()).Type(schema.TypeString).Add().
		Attribute(readSessionTags.String()).Type(schema.TypeString).Many().Add().
		Attribute(readSessionCells.String()).Type(schema.TypeString).Vector().Add().
		Attribute(readSessionOrder.String()).Type(schema.TypeString).OrderedSet().Add().
		Build()
	require.NoError(t, err)
	d, err := NewDatabaseWithOptions(DatabaseOptions{Store: store, Schema: s})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(e, readSessionName, "n"))
	require.NoError(t, tx.Add(e, readSessionTags, "t"))
	require.NoError(t, tx.Add(e, readSessionCells, "c"))
	require.NoError(t, tx.Add(e, readSessionOrder, "o"))
	_, err = tx.Commit()
	require.NoError(t, err)
	return d
}

// A transaction opens its read session when it opens. When the open fails,
// every read of the transaction's view answers the failure, and a
// transaction that writes without reading its view commits.
func TestATransactionAnswersAFailedOpenOfItsReadSessionAtEveryReadOfItsView(t *testing.T) {
	type named struct {
		ID   datalog.Identity `datalog:"-,id"`
		Name string           `datalog:"rs/name"`
	}
	e := datalog.NewIdentity("read-session:open")
	reads := []struct {
		name string
		read func(tx *Transaction) error
	}{
		{"LookupAttribute of a value", func(tx *Transaction) error {
			_, _, err := tx.LookupAttribute(e, readSessionName)
			return err
		}},
		{"LookupAttribute of a set", func(tx *Transaction) error {
			_, _, err := tx.LookupAttribute(e, readSessionTags)
			return err
		}},
		{"LookupAttribute of a vector", func(tx *Transaction) error {
			_, _, err := tx.LookupAttribute(e, readSessionCells)
			return err
		}},
		{"LookupAllAttributes of a vector", func(tx *Transaction) error {
			_, err := tx.LookupAllAttributes(e, readSessionCells)
			return err
		}},
		{"Set of a vector", func(tx *Transaction) error {
			return tx.Set(e, readSessionCells, []interface{}{"c", "d"})
		}},
		{"Set of a set", func(tx *Transaction) error {
			return tx.Set(e, readSessionTags, []interface{}{"t", "u"})
		}},
		{"Remove of a vector's element", func(tx *Transaction) error {
			return tx.Remove(e, readSessionCells, "c")
		}},
		{"Add to an ordered set", func(tx *Transaction) error {
			return tx.Add(e, readSessionOrder, "p")
		}},
		{"SaveStruct", func(tx *Transaction) error {
			_, err := tx.SaveStruct(&named{ID: e, Name: "m"})
			return err
		}},
	}
	for _, r := range reads {
		t.Run(r.name, func(t *testing.T) {
			var failing atomic.Bool
			d := readSessionDatabase(t, sessionOpenFailingStore{Store: NewBadgerEmulatorStore(nil), failing: &failing}, e)
			failing.Store(true)
			tx := d.NewTransaction()
			require.ErrorIs(t, r.read(tx), errSessionOpen, "the read answers the failed open of the transaction's read session")
			require.NoError(t, tx.Rollback())
		})
	}
	t.Run("a transaction that writes without reading commits", func(t *testing.T) {
		var failing atomic.Bool
		d := readSessionDatabase(t, sessionOpenFailingStore{Store: NewBadgerEmulatorStore(nil), failing: &failing}, e)
		failing.Store(true)
		tx := d.NewTransaction()
		require.NoError(t, tx.Add(e, readSessionName, "m"))
		require.NoError(t, tx.Add(e, readSessionTags, "u"))
		require.NoError(t, tx.Add(e, readSessionCells, "d"))
		_, err := tx.Commit()
		require.NoError(t, err)
		failing.Store(false)
		name, found, err := d.LookupAttribute(e, readSessionName)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "m", name)
	})
}

// A transaction closes its read session when it commits or rolls back, and a
// failed close reaches the caller once. A commit whose session fails to close
// commits nothing.
func TestATransactionClosesItsReadSessionAtCommitAndRollback(t *testing.T) {
	e := datalog.NewIdentity("read-session:close")
	t.Run("a commit whose session fails to close commits nothing", func(t *testing.T) {
		var failing atomic.Bool
		var failed atomic.Int32
		d := readSessionDatabase(t, sessionCloseFailingStore{Store: NewBadgerEmulatorStore(nil), failing: &failing, failed: &failed}, e)
		failing.Store(true)
		tx := d.NewTransaction()
		require.NoError(t, tx.Add(e, readSessionName, "m"))
		_, err := tx.Commit()
		require.ErrorIs(t, err, errSessionClose, "the commit answers its session's close failure")
		require.Equal(t, 1, occurrences(err, errSessionClose), "the failed close is answered once: %v", err)
		require.NoError(t, tx.Rollback(), "the rollback after the failed commit closes no session again")
		require.Equal(t, int32(1), failed.Load(), "the session is closed once")
		failing.Store(false)
		name, found, err := d.LookupAttribute(e, readSessionName)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "n", name, "the failed commit wrote nothing")
	})
	t.Run("a rollback answers its session's close failure", func(t *testing.T) {
		var failing atomic.Bool
		var failed atomic.Int32
		d := readSessionDatabase(t, sessionCloseFailingStore{Store: NewBadgerEmulatorStore(nil), failing: &failing, failed: &failed}, e)
		failing.Store(true)
		tx := d.NewTransaction()
		err := tx.Rollback()
		require.ErrorIs(t, err, errSessionClose, "the rollback answers its session's close failure")
		require.Equal(t, 1, occurrences(err, errSessionClose), "the failed close is answered once: %v", err)
		require.Equal(t, int32(1), failed.Load(), "the session is closed once")
		failing.Store(false)
	})
}

// A transaction's lookups after it commits answer that it is closed, as its
// writes do.
func TestATransactionsLookupAfterItsCommitAnswersThatItIsClosed(t *testing.T) {
	e := datalog.NewIdentity("read-session:closed")
	d := readSessionDatabase(t, NewBadgerEmulatorStore(nil), e)
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(e, readSessionName, "m"))
	_, err := tx.Commit()
	require.NoError(t, err)
	_, _, err = tx.LookupAttribute(e, readSessionName)
	require.ErrorContains(t, err, "transaction is closed", "LookupAttribute after the commit")
	_, err = tx.LookupAllAttributes(e, readSessionCells)
	require.ErrorContains(t, err, "transaction is closed", "LookupAllAttributes after the commit")
}

// A transaction admitted while a TruncateTo holds writers is dropped, and
// every read of its view answers the drop: it holds no view, since a read
// session opened while the rewind deletes reads either side of it.
func TestATransactionAdmittedWhileARewindHoldsWritersAnswersTheDropAtEveryReadOfItsView(t *testing.T) {
	type named struct {
		ID   datalog.Identity `datalog:"-,id"`
		Name string           `datalog:"rs/name"`
	}
	e := datalog.NewIdentity("read-session:dropped")
	reads := []struct {
		name string
		read func(tx *Transaction) error
	}{
		{"LookupAttribute of a value", func(tx *Transaction) error {
			_, _, err := tx.LookupAttribute(e, readSessionName)
			return err
		}},
		{"LookupAllAttributes of a vector", func(tx *Transaction) error {
			_, err := tx.LookupAllAttributes(e, readSessionCells)
			return err
		}},
		{"SaveStruct of the value the view holds", func(tx *Transaction) error {
			_, err := tx.SaveStruct(&named{ID: e, Name: "m"})
			return err
		}},
	}
	for _, r := range reads {
		t.Run(r.name, func(t *testing.T) {
			d := readSessionDatabase(t, NewBadgerEmulatorStore(nil), e)
			_, err := d.Snapshot("before")
			require.NoError(t, err)
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(e, readSessionName, "m"))
			_, err = tx.Commit()
			require.NoError(t, err)

			draining := make(chan struct{}, 1)
			d.onDrainWait = func() {
				select {
				case draining <- struct{}{}:
				default:
				}
			}
			inFlight := d.NewTransaction()
			rewound := make(chan error, 1)
			go func() { rewound <- d.TruncateTo("before") }()
			<-draining

			dropped := d.NewTransaction()
			require.ErrorIs(t, r.read(dropped), ErrRollbackInProgress, "the read answers the drop of a transaction admitted while the rewind holds writers")
			require.NoError(t, dropped.Rollback())
			require.NoError(t, inFlight.Rollback())
			require.NoError(t, <-rewound)
		})
	}
}
