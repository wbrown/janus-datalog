package db_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
)

// TestVerboseHandlerIsSafeUnderParallelEmission drives the engine's own verbose
// installer end to end on a query shaped to fork, with no serializing wrapper
// anywhere between the workers and the formatter.
//
// There is nothing to serialize. The formatter holds no state between events —
// every scan line is rendered from the event that carries it — so concurrent
// calls have nothing to interleave. That is the property this exercises, and it
// is a property of the formatter, not of a lock around it: a formatter that
// remembered the last index and bound would render one worker's run against
// another's counts, and a mutex would serialize the writes while leaving every
// line wrong.
//
// **This is not a proven reproducer of concurrent emission.** It drives the
// shape most likely to fork — a relation input, which is what reaches
// executeRealizedWithRelationInputIterationParallel and its four default
// workers — but whether db.Query plans this relation as an iteration or as a
// join is not established here, so overlapping emission is not demonstrated.
// What it does establish is that the public verbose path runs clean, which is
// the claim that would have gone unchecked when the wrapper was deleted.
//
// The callback deliberately does nothing. The state at issue is the formatter
// inside the option, not anything the caller supplies — a test whose callback
// touched shared state would pin the caller's mistake rather than the engine's.
func TestVerboseHandlerIsSafeUnderParallelEmission(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, err := db.OpenMemory(
				db.WithPlannerOptions(mode.plannerOptions()),
				db.WithVerboseCallback(func(string) {}),
			)
			require.NoError(t, err)
			defer d.Close()

			name := datalog.NewKeyword(":task/name")
			score := datalog.NewKeyword(":task/score")
			tx := d.NewTransaction()
			const tasks = 64
			inputs := make([][]any, 0, tasks)
			for i := 0; i < tasks; i++ {
				label := fmt.Sprintf("task-%03d", i)
				e := datalog.NewIdentity(label)
				require.NoError(t, tx.Set(e, name, label))
				require.NoError(t, tx.Set(e, score, int64(i)))
				inputs = append(inputs, []any{label, int64(i)})
			}
			_, err = tx.Commit()
			require.NoError(t, err)

			// A relation input is what reaches
			// executeRealizedWithRelationInputIterationParallel, which forks
			// four workers by default and hands each a context sharing the one
			// collector. Every worker's events pass through the handler the
			// option installed. A correlated subquery with a scalar :in does
			// not reach it, which is why this drives the relation form.
			result, err := d.Query(`[:find ?e
			     :in $ [[?n ?v] ...]
			     :where [?e :task/name ?n]
			            [?e :task/score ?v]]`, inputs)
			require.NoError(t, err)
			iter := result.Iterator()
			tuples := 0
			for iter.Next() {
				tuples++
			}
			require.NoError(t, iter.Error())
			require.NoError(t, iter.Close())
			require.Equal(t, tasks, tuples, "every input tuple matches one entity")
		})
	}
}
