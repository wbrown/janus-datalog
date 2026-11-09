package storage

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

var errStopReading = errors.New("the reader stops")

// A query's read session that fails to close is answered once when its reader
// stops before the result's end.
func TestAReadSessionsCloseFailureIsAnsweredOnceWhenItsReaderStopsEarly(t *testing.T) {
	score := datalog.NewKeyword(":item/score")
	var failing atomic.Bool
	var failed atomic.Int32
	db, err := NewDatabaseWithOptions(DatabaseOptions{Store: sessionCloseFailingStore{Store: NewBadgerEmulatorStore(nil), failing: &failing, failed: &failed}})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(datalog.NewIdentity("session-close-early:first"), score, int64(3)))
	require.NoError(t, tx.Add(datalog.NewIdentity("session-close-early:second"), score, int64(5)))
	_, err = tx.Commit()
	require.NoError(t, err)

	failing.Store(true)
	rel, err := db.Query(`[:find ?e ?v :where [?e :item/score ?v]]`)
	require.NoError(t, err)
	err = executor.ForEach(rel, func(executor.Tuple) error { return errStopReading })
	failing.Store(false)
	require.ErrorIs(t, err, errStopReading, "the reader's own failure reaches the caller")
	require.ErrorIs(t, err, errSessionClose, "the session's close failure reaches the caller")
	require.Equal(t, int(failed.Load()), occurrences(err, errSessionClose), "each failed close is answered once: %v", err)
}

// scanCloseFailingStore is a store whose scans, its own and its read
// sessions', opened while failing holds, close with errStoreClose and, while
// failRead holds too, end at their first entry with errStoreScan. scans counts
// the scans opened while failing holds.
type scanCloseFailingStore struct {
	Store
	failing  *atomic.Bool
	failRead *atomic.Bool
	scans    *atomic.Int32
}

func (s scanCloseFailingStore) Scan(bound ScanBound) (Iterator, error) {
	return s.failingScan(s.Store.Scan(bound))
}

func (s scanCloseFailingStore) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	return s.failingScan(s.Store.ScanKeysOnly(bound))
}

func (s scanCloseFailingStore) NewReadSession() (ReadSession, error) {
	session, err := s.Store.NewReadSession()
	if err != nil {
		return nil, err
	}
	return scanCloseFailingSession{ReadSession: session, store: s}, nil
}

// failingScan is it, closing with errStoreClose and reading as failRead
// says, when failing holds.
func (s scanCloseFailingStore) failingScan(it Iterator, err error) (Iterator, error) {
	if err != nil || !s.failing.Load() {
		return it, err
	}
	s.scans.Add(1)
	return scanCloseFailingIterator{Iterator: it, failRead: s.failRead.Load()}, nil
}

type scanCloseFailingSession struct {
	ReadSession
	store scanCloseFailingStore
}

func (s scanCloseFailingSession) Scan(bound ScanBound) (Iterator, error) {
	return s.store.failingScan(s.ReadSession.Scan(bound))
}

func (s scanCloseFailingSession) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	return s.store.failingScan(s.ReadSession.ScanKeysOnly(bound))
}

type scanCloseFailingIterator struct {
	Iterator
	failRead bool
}

func (s scanCloseFailingIterator) Next() bool {
	if s.failRead {
		return false
	}
	return s.Iterator.Next()
}

func (s scanCloseFailingIterator) Error() error {
	if s.failRead {
		return errStoreScan
	}
	return s.Iterator.Error()
}

func (s scanCloseFailingIterator) Close() error {
	if err := s.Iterator.Close(); err != nil {
		return errors.Join(err, errStoreClose)
	}
	return errStoreClose
}

// A relation whose scan the query shares (PlannerOptions.EnableScanSharing)
// answers each failure of its scan once: a scan that fails at its first entry,
// a scan read to its end, and a scan its reader stops reading before the end.
func TestAScanSharedRelationAnswersEachFailureOfItsScanOnce(t *testing.T) {
	score := datalog.NewKeyword(":item/score")
	all := `[:find ?e ?v :where [?e :item/score ?v]]`
	first := `[:find ?e ?v :where [?e :item/score ?v] :limit 1]`
	readers := []struct {
		name   string
		answer func(t *testing.T, db *Database, text string) error
	}{
		{"collected tuples", func(t *testing.T, db *Database, text string) error {
			_, err := executor.CollectTuples(db.Query(text))
			return err
		}},
		{"ForEach", func(t *testing.T, db *Database, text string) error {
			rel, err := db.Query(text)
			require.NoError(t, err)
			return executor.ForEach(rel, func(executor.Tuple) error { return nil })
		}},
		{"QueryInto", func(t *testing.T, db *Database, text string) error {
			var answers []struct {
				E datalog.Identity `datalog:"?e"`
				V int64            `datalog:"?v"`
			}
			return db.QueryInto(&answers, text)
		}},
	}
	cases := []struct {
		name     string
		text     string
		failRead bool
	}{
		{"a scan that fails at its first entry", all, true},
		{"a scan read to its end", all, false},
		{"a scan its reader stops reading before the end", first, false},
	}
	for _, mode := range legsWhere(optimizerModes, func(m optimizerMode) bool { return m.sharing }) {
		t.Run(mode.name, func(t *testing.T) {
			var failing, failRead atomic.Bool
			var scans atomic.Int32
			options := mode.plannerOptions()
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Store:          scanCloseFailingStore{Store: NewBadgerEmulatorStore(nil), failing: &failing, failRead: &failRead, scans: &scans},
				PlannerOptions: &options,
			})
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close()) }()
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(datalog.NewIdentity("scan-shared:first"), score, int64(3)))
			require.NoError(t, tx.Add(datalog.NewIdentity("scan-shared:second"), score, int64(5)))
			_, err = tx.Commit()
			require.NoError(t, err)

			for _, c := range cases {
				for _, reader := range readers {
					t.Run(c.name+"/"+reader.name, func(t *testing.T) {
						failRead.Store(c.failRead)
						scans.Store(0)
						failing.Store(true)
						err := reader.answer(t, db, c.text)
						failing.Store(false)
						require.NotZero(t, scans.Load(), "the query opens a scan while failing holds")
						require.ErrorIs(t, err, errStoreClose, "the scan's close failure reaches the caller")
						require.Equal(t, int(scans.Load()), occurrences(err, errStoreClose), "each failed close is answered once: %v", err)
						if c.failRead {
							require.ErrorIs(t, err, errStoreScan, "the scan's read failure reaches the caller")
							require.Equal(t, int(scans.Load()), occurrences(err, errStoreScan), "each failed read is answered once: %v", err)
						}
					})
				}
			}
		})
	}
}
