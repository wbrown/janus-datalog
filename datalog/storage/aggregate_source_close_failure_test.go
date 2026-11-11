package storage

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// failingSourceIterator ends before its first tuple with errStoreScan and
// closes with errStoreClose.
type failingSourceIterator struct{}

func (failingSourceIterator) Next() bool            { return false }
func (failingSourceIterator) Tuple() executor.Tuple { return nil }
func (failingSourceIterator) RequiresCopy() bool    { return false }
func (failingSourceIterator) Close() error          { return errStoreClose }
func (failingSourceIterator) Error() error          { return errStoreScan }

// failingStreamSource answers every match with a stream over the pattern's
// symbols, read through a failingSourceIterator.
type failingStreamSource struct{}

func (failingStreamSource) Match(q *query.Query, _ executor.Relations) (executor.Relation, error) {
	pattern, err := q.SingleDataPattern()
	if err != nil {
		return nil, err
	}
	return executor.NewStreamingRelation(pattern.Symbols(), failingSourceIterator{}), nil
}

// An aggregate over a source passed through WithSources, whose scan fails
// before its first tuple and whose close fails, answers both failures on
// every leg of the optimizer matrix: a pure aggregate and a grouped one, over
// a database and over a source answering a stream.
func TestAnAggregateOverASourceAnswersItsScanFailureAndCloseFailureOnEveryLeg(t *testing.T) {
	score := datalog.NewKeyword(":item/score")
	queries := []struct {
		name string
		text string
	}{
		{"a pure aggregate", `[:find (count ?v) :in $ $src :where [$src ?e :item/score ?v]]`},
		{"a grouped aggregate", `[:find ?e (count ?v) :in $ $src :where [$src ?e :item/score ?v]]`},
	}
	sources := []struct {
		name string
		// open answers the source and the function that makes its scans fail.
		open func(t *testing.T) (executor.PatternMatcher, func())
	}{
		{"a database", func(t *testing.T) (executor.PatternMatcher, func()) {
			var failing, failRead atomic.Bool
			var scans atomic.Int32
			source, err := NewDatabaseWithOptions(DatabaseOptions{Store: scanCloseFailingStore{Store: NewBadgerEmulatorStore(nil), failing: &failing, failRead: &failRead, scans: &scans}})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, source.Close()) })
			tx := source.NewTransaction()
			require.NoError(t, tx.Add(datalog.NewIdentity("source-scan:first"), score, int64(3)))
			require.NoError(t, tx.Add(datalog.NewIdentity("source-scan:second"), score, int64(5)))
			_, err = tx.Commit()
			require.NoError(t, err)
			return source, func() {
				failRead.Store(true)
				failing.Store(true)
			}
		}},
		{"a source answering a stream", func(t *testing.T) (executor.PatternMatcher, func()) {
			return failingStreamSource{}, func() {}
		}},
	}
	for _, s := range sources {
		for _, q := range queries {
			for _, mode := range legsFor(optimizerModes) {
				t.Run(fmt.Sprintf("%s/%s/%s", s.name, q.name, mode.name), func(t *testing.T) {
					source, fail := s.open(t)
					db := createOptimizerModeDB(t, mode, DatabaseOptions{})

					fail()
					_, err := executor.CollectTuples(db.Query(q.text, WithSources(map[query.Symbol]executor.PatternMatcher{
						datalog.NewSymbol("$src"): source,
					})))
					require.ErrorIs(t, err, errStoreScan, "the source's scan failure reaches the caller")
					require.ErrorIs(t, err, errStoreClose, "the source's close failure reaches the caller beside its scan failure")
				})
			}
		}
	}
}
