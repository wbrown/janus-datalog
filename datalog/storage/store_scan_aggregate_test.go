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
// close fails answers the same error whether streaming aggregation is on or
// off.
func TestAPureAggregateAnswersAStoreScansFailuresAlikeUnderEitherAggregationOption(t *testing.T) {
	score := datalog.NewKeyword(":item/score")
	answers := map[bool]error{}
	for _, streaming := range []bool{true, false} {
		var faulting atomic.Bool
		options := DefaultPlannerOptions()
		options.EnableStreamingAggregation = streaming
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
		t.Logf("streaming aggregation %v: %v", streaming, err)
		require.ErrorIs(t, err, errStoreScan, "streaming aggregation %v: the scan failure reaches the caller", streaming)
		answers[streaming] = err
	}
	require.Equal(t, errors.Is(answers[true], errStoreClose), errors.Is(answers[false], errStoreClose),
		"the close failure reaches the caller under one option exactly when it does under the other: on %v, off %v", answers[true], answers[false])
}
