package executor_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// With the EA arm serving the branch, a subject's iteration error and its
// close error are still the result's: the arm reads the window's run, and
// the subject is read by the window reader. The arm is reached as a query
// reaches it, through the fallback's prefetch trigger under
// EnableEntityPrefetch: under a cap of 128 the subject's seventh window
// holds 64 entities, the first above the threshold, so it is prefetched and
// every later window's branch is built from the cache.
func TestOrFallbackSurfacesTheSubjectsErrorsUnderTheEAArm(t *testing.T) {
	// Under a cap of 128 a subject of 300 is read in windows of 1, 2, 4, 8,
	// 16, 32, 64, 128 and 45 tuples.
	const subjectSize = 300
	const seventhWindow, eighthWindow, ninthWindow = 64, 128, 45
	entitySymbol := datalog.NewSymbol("?entity")
	valueSymbol := datalog.NewSymbol("?value")
	attr := datalog.NewKeyword(":item/value")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	var datoms []datalog.Datom
	subject := make([]Tuple, 0, subjectSize)
	for i := 0; i < subjectSize; i++ {
		entity := datalog.NewIdentity(fmt.Sprintf("ea-cache-outer-error:item%d", i))
		datoms = append(datoms, datalog.Datom{E: entity, A: attr, V: int64(i), Tx: tx})
		subject = append(subject, Tuple{entity})
	}
	base := NewMaterializedRelation([]query.Symbol{entitySymbol}, subject)
	branch := []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entitySymbol},
		query.Constant{Value: attr},
		query.Variable{Name: valueSymbol},
	}}}

	testCases := []struct {
		name     string
		outer    Relation
		expected error
		lookups  int // the entities of the windows the arm built before the failure
	}{
		{
			name:     "iteration",
			outer:    NewFailingRelation(base, 200, nil),
			expected: ErrInjectedIterator,
			lookups:  seventhWindow, // the eighth window fails while it is read
		},
		{
			name:     "close",
			outer:    NewFailingRelation(base, 1000, errors.New("EA cache outer close failure")),
			expected: errors.New("EA cache outer close failure"),
			lookups:  seventhWindow + eighthWindow + ninthWindow,
		},
	}

	for _, mode := range OptimizerModesWhere(func(m OptimizerMode) bool { return m.Prefetch() }) {
		for _, testCase := range testCases {
			t.Run(mode.Name()+"/"+testCase.name, func(t *testing.T) {
				var triggers, lookups int
				handler := mode.Recording(func(e annotations.Event) {
					switch e.Name {
					case annotations.PrefetchTrigger:
						triggers++
					case annotations.StorageResolveComplete:
						// A lookup reports the values it served at its attribute,
						// and names no pattern.
						_, served := e.Data[annotations.KeyValuesServed]
						_, ofPattern := e.Data[annotations.KeyPattern]
						if served && !ofPattern && e.Data[annotations.KeyAttribute] == attr {
							lookups++
						}
					}
				})
				plannerOpts := mode.PlannerOptions()
				d, err := storage.OpenFacts(datoms, storage.DatabaseOptions{AnnotationHandler: handler, PlannerOptions: &plannerOpts})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, d.Close()) })
				options := ExecutorOptionsFromPlanner(plannerOpts)
				options.ScopeWindowCap = 128
				options.Handler = handler
				relation, err := NewOrFallbackRelation(
					NewQueryExecutor(d.Matcher(), nil, options),
					NewContext(),
					[]query.Symbol{entitySymbol},
					[]query.Symbol{valueSymbol},
					[][]query.Clause{branch},
					testCase.outer,
					options,
					true,
				)
				require.NoError(t, err)
				iterator := relation.Iterator()
				for iterator.Next() {
				}
				err = iterator.Error()
				if closeErr := iterator.Close(); err == nil {
					err = closeErr
				}
				require.EqualError(t, err, testCase.expected.Error())
				require.NotZero(t, triggers, "the trigger ran before the failure, so the arm served the branch")
				want := testCase.lookups
				if mode.Fusion() {
					// A leg carrying fusion fetches the branch for the six
					// windows before the trigger, 1, 2, 4, 8, 16 and 32
					// entities, through one lookup per entity; the arm's
					// lookups are the windows after it.
					want += 1 + 2 + 4 + 8 + 16 + 32
				}
				require.Equal(t, want, lookups, "the fused fetch of the windows before the trigger, where the leg fuses, and the arm's build of every window after it from the cache")
			})
		}
	}
}
