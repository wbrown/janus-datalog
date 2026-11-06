package storage

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

var errSessionClose = errors.New("read session close failure")

// sessionCloseFailingStore is a store whose read sessions fail to close while
// failing holds.
type sessionCloseFailingStore struct {
	Store
	failing *atomic.Bool
	// failed counts the session closes that failed.
	failed *atomic.Int32
}

func (s sessionCloseFailingStore) NewReadSession() (ReadSession, error) {
	session, err := s.Store.NewReadSession()
	if err != nil || !s.failing.Load() {
		return session, err
	}
	return sessionCloseFailing{ReadSession: session, failed: s.failed}, nil
}

type sessionCloseFailing struct {
	ReadSession
	failed *atomic.Int32
}

func (s sessionCloseFailing) Close() error {
	s.failed.Add(1)
	if err := s.ReadSession.Close(); err != nil {
		return errors.Join(err, errSessionClose)
	}
	return errSessionClose
}

// occurrences counts the members of err's tree that are target itself.
func occurrences(err, target error) int {
	if err == nil {
		return 0
	}
	if err == target {
		return 1
	}
	switch joined := err.(type) {
	case interface{ Unwrap() []error }:
		total := 0
		for _, member := range joined.Unwrap() {
			total += occurrences(member, target)
		}
		return total
	case interface{ Unwrap() error }:
		return occurrences(joined.Unwrap(), target)
	}
	return 0
}

// A read session that fails to close is answered once per failed close,
// through every reader of a query's result and through an open, which reads
// the store's schema facts by a query.
func TestAReadSessionsCloseFailureIsAnsweredOncePerFailedClose(t *testing.T) {
	score := datalog.NewKeyword(":item/score")
	first := datalog.NewIdentity("session-close:first")
	second := datalog.NewIdentity("session-close:second")
	facts := []datalog.Datom{
		{E: first, A: score, V: int64(3)},
		{E: second, A: score, V: int64(5)},
	}
	var failing atomic.Bool
	var failed atomic.Int32
	db, err := NewDatabaseWithOptions(DatabaseOptions{Store: sessionCloseFailingStore{Store: NewBadgerEmulatorStore(nil), failing: &failing, failed: &failed}})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	tx := db.NewTransaction()
	for _, fact := range facts {
		require.NoError(t, tx.Add(fact.E, fact.A, fact.V))
	}
	_, err = tx.Commit()
	require.NoError(t, err)
	failing.Store(true)

	text := `[:find ?e ?v :where [?e :item/score ?v]]`
	readers := []struct {
		name   string
		answer func(t *testing.T) error
	}{
		{"opening a database", func(t *testing.T) error {
			opened, err := NewDatabaseWithOptions(DatabaseOptions{Store: sessionCloseFailingStore{Store: NewBadgerEmulatorStore(nil), failing: &failing, failed: &failed}})
			if err == nil {
				require.NoError(t, opened.Close())
			}
			return err
		}},
		{"collected tuples", func(t *testing.T) error {
			_, err := executor.CollectTuples(db.Query(text))
			return err
		}},
		{"ForEach", func(t *testing.T) error {
			rel, err := db.Query(text)
			require.NoError(t, err)
			return executor.ForEach(rel, func(executor.Tuple) error { return nil })
		}},
		{"QueryInto", func(t *testing.T) error {
			var answers []struct {
				E datalog.Identity `datalog:"?e"`
				V int64            `datalog:"?v"`
			}
			return db.QueryInto(&answers, text)
		}},
	}
	for _, reader := range readers {
		t.Run(reader.name, func(t *testing.T) {
			failed.Store(0)
			err := reader.answer(t)
			require.ErrorIs(t, err, errSessionClose, "the session's close failure reaches the caller")
			require.Equal(t, int(failed.Load()), occurrences(err, errSessionClose), "each failed close is answered once: %v", err)
		})
	}
}
