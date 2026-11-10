package executor_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// unboundScanRecorder is a source that records each match made with no
// bindings, by the attribute the match names and in the order the matches
// were made, with the closes of the iterators the match's relation hands out.
type unboundScanRecorder struct {
	inner PatternMatcher
	mu    sync.Mutex
	scans map[datalog.Keyword][]*recordedScan
}

// recordedScan is one match made with no bindings and the closes of the
// iterators its relation handed out.
type recordedScan struct {
	mu     sync.Mutex
	closes int
}

func (s *recordedScan) closed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closes
}

func (m *unboundScanRecorder) Match(q *query.Query, bindings Relations) (Relation, error) {
	rel, err := m.inner.Match(q, bindings)
	if err != nil || len(bindings) > 0 {
		return rel, err
	}
	pattern, err := q.SingleDataPattern()
	if err != nil {
		return nil, err
	}
	attr := pattern.GetA().(query.Constant).Value.(datalog.Keyword)
	scan := &recordedScan{}
	m.mu.Lock()
	m.scans[attr] = append(m.scans[attr], scan)
	m.mu.Unlock()
	return &closeRecordingRelation{Relation: rel, scan: scan}, nil
}

// firstScan is the first match of attr made with no bindings.
func (m *unboundScanRecorder) firstScan(t *testing.T, attr datalog.Keyword) *recordedScan {
	m.mu.Lock()
	defer m.mu.Unlock()
	require.NotEmpty(t, m.scans[attr], "a match of %s with no bindings was made", attr)
	return m.scans[attr][0]
}

type closeRecordingRelation struct {
	Relation
	scan *recordedScan
}

func (r *closeRecordingRelation) Iterator() Iterator {
	return &closeRecordingIterator{Iterator: r.Relation.Iterator(), scan: r.scan}
}

type closeRecordingIterator struct {
	Iterator
	scan *recordedScan
}

func (it *closeRecordingIterator) Close() error {
	it.scan.mu.Lock()
	it.scan.closes++
	it.scan.mu.Unlock()
	return it.Iterator.Close()
}

// A Context given to ExecuteWithRelations is read and never written, and
// each query runs on a scan registry of its own, so queries given one
// context, in turn or at once, share no scan, and one query's end closes none
// of another's open scans. Each query reads one fingerprint at two sites, so
// under scan sharing its scan is shared and listed in a registry.
func TestQueriesGivenOneContextShareNoScan(t *testing.T) {
	const entities = 300
	score, weight := datalog.NewKeyword(":item/score"), datalog.NewKeyword(":item/weight")
	var facts []datalog.Datom
	for i := 1; i <= entities; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("item:%d", i))
		facts = append(facts,
			datalog.Datom{E: e, A: score, V: int64(i), Tx: datalog.ElementID{Lamport: uint64(2*i - 1)}},
			datalog.Datom{E: e, A: weight, V: int64(i), Tx: datalog.ElementID{Lamport: uint64(2 * i)}},
		)
	}
	scores, err := parser.ParseQuery(`[:find ?e ?f :where [?e :item/score ?v] [?f :item/score ?v]]`)
	require.NoError(t, err)
	weights, err := parser.ParseQuery(`[:find ?e ?f :where [?e :item/weight ?v] [?f :item/weight ?v]]`)
	require.NoError(t, err)

	for _, mode := range LegsFor(t, SharingOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			t.Run("one query's end leaves another's scan open", func(t *testing.T) {
				source := &unboundScanRecorder{inner: factsDatabase(t, facts), scans: map[datalog.Keyword][]*recordedScan{}}
				exec := NewExecutorWithOptions(source, nil, mode.PlannerOptions())
				ctx := NewContext()

				started, err := exec.ExecuteWithRelations(ctx, scores, nil)
				require.NoError(t, err)
				it := started.Iterator()
				require.True(t, it.Next(), "the started query answers its first tuple")
				// The started query's own scan of its attribute is the first
				// match of it made with no bindings: the executor matches the
				// first clause before the second clause's existence check and
				// windows run at the first pull.
				scan := source.firstScan(t, score)
				require.Zero(t, scan.closed(), "the started query's scan is open while its result is being read")

				ended, err := CollectTuples(exec.ExecuteWithRelations(ctx, weights, nil))
				require.NoError(t, err)
				require.Len(t, ended, entities)
				require.Zero(t, scan.closed(), "the ended query closed none of the started query's open scans")

				answered := 1
				for it.Next() {
					answered++
				}
				require.NoError(t, it.Error())
				require.NoError(t, it.Close())
				require.Equal(t, entities, answered, "the started query answers whole after the other query's end")
			})

			t.Run("ten queries at once each answer whole", func(t *testing.T) {
				matcher := factsDatabase(t, facts)
				ctx := NewContext()
				const queries = 10
				counts := make([]int, queries)
				errs := make([]error, queries)
				var wait sync.WaitGroup
				for i := 0; i < queries; i++ {
					wait.Add(1)
					go func(index int) {
						defer wait.Done()
						exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())
						tuples, err := CollectTuples(exec.ExecuteWithRelations(ctx, scores, nil))
						counts[index], errs[index] = len(tuples), err
					}(i)
				}
				wait.Wait()
				for i := 0; i < queries; i++ {
					require.NoError(t, errs[i], "query %d", i)
					require.Equal(t, entities, counts[i], "query %d answers whole", i)
				}
			})
		})
	}
}
