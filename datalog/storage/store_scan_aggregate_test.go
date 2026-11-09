package storage

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

var (
	errStoreScan  = errors.New("store scan failure")
	errStoreClose = errors.New("store scan close failure")
)

// scanFaultingStore is a store whose read sessions' scans, while faulting
// holds, end at their first entry with a read failure and close with a
// failure.
type scanFaultingStore struct {
	Store
	faulting *atomic.Bool
}

func (s scanFaultingStore) NewReadSession() (ReadSession, error) {
	session, err := s.Store.NewReadSession()
	if err != nil {
		return nil, err
	}
	return scanFaultingSession{ReadSession: session, faulting: s.faulting}, nil
}

type scanFaultingSession struct {
	ReadSession
	faulting *atomic.Bool
}

func (s scanFaultingSession) Scan(bound ScanBound) (Iterator, error) {
	it, err := s.ReadSession.Scan(bound)
	if err != nil || !s.faulting.Load() {
		return it, err
	}
	return scanFaultingIterator{Iterator: it}, nil
}

func (s scanFaultingSession) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	it, err := s.ReadSession.ScanKeysOnly(bound)
	if err != nil || !s.faulting.Load() {
		return it, err
	}
	return scanFaultingIterator{Iterator: it}, nil
}

type scanFaultingIterator struct {
	Iterator
}

func (scanFaultingIterator) Next() bool   { return false }
func (scanFaultingIterator) Error() error { return errStoreScan }
func (s scanFaultingIterator) Close() error {
	if err := s.Iterator.Close(); err != nil {
		return errors.Join(err, errStoreClose)
	}
	return errStoreClose
}

// A pure aggregate over a store whose scan fails at its first entry and whose
// close fails answers the scan failure on every leg of the optimizer matrix,
// and answers the close failure on each leg exactly when the reference leg
// does.
func TestAPureAggregateAnswersAStoreScansFailuresAlikeOnEveryLeg(t *testing.T) {
	score := datalog.NewKeyword(":item/score")
	answers := map[string]error{}
	for _, mode := range optimizerModes {
		var faulting atomic.Bool
		options := mode.plannerOptions()
		db, err := NewDatabaseWithOptions(DatabaseOptions{
			Store:          scanFaultingStore{Store: NewBadgerEmulatorStore(nil), faulting: &faulting},
			PlannerOptions: &options,
		})
		require.NoError(t, err)
		tx := db.NewTransaction()
		require.NoError(t, tx.Add(datalog.NewIdentity("store-scan:first"), score, int64(3)))
		require.NoError(t, tx.Add(datalog.NewIdentity("store-scan:second"), score, int64(5)))
		_, err = tx.Commit()
		require.NoError(t, err)
		faulting.Store(true)
		_, err = executor.CollectTuples(db.Query(`[:find (count ?v) :where [?e :item/score ?v]]`))
		faulting.Store(false)
		require.NoError(t, db.Close())
		t.Logf("%s: %v", mode.name, err)
		require.ErrorIs(t, err, errStoreScan, "%s: the scan failure reaches the caller", mode.name)
		answers[mode.name] = err
	}
	reference := optimizerModes[0].name
	for _, mode := range optimizerModes {
		require.Equal(t, errors.Is(answers[reference], errStoreClose), errors.Is(answers[mode.name], errStoreClose),
			"the close failure reaches the caller on %s exactly when it does on %s: %v against %v", mode.name, reference, answers[mode.name], answers[reference])
	}
}
