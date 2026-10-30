package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A value binding on a constant attribute resolving as cardinality one is
// served above the matcher's boundary by one resolved scan of the attribute
// under AETV probed by value, and at or below it by the candidate-and-validate
// arm, with the same answer on either path: the entities whose current value
// is bound, an entity whose bound value a later write superseded left out
// (docs/proposals/A_VALUE_BINDING_ABOVE_THE_BOUNDARY_SCANS_ONCE_PROOF.md, T1,
// T2). The store holds one entity past the boundary, every tenth of them
// superseded, and the two bindings carry one value past the boundary and the
// boundary's count.
func TestValueBindingAboveTheBoundaryIsServedByOneScan(t *testing.T) {
	mark := datalog.NewKeyword(":quire/mark")
	entities := executor.SmallBindingBoundary + 1
	quire := func(i int) datalog.Identity { return datalog.NewIdentity(fmt.Sprintf("quire:%d", i)) }
	bound := func(i int) string { return fmt.Sprintf("mark-%d", i) }
	superseded := func(i int) bool { return i%10 == 0 }

	var want []datalog.Identity
	for i := 0; i < entities; i++ {
		if !superseded(i) {
			want = append(want, quire(i))
		}
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var events []annotations.Event
			db := createOptimizerModeDB(t, mode, DatabaseOptions{
				DisableCache:      true,
				AnnotationHandler: func(e annotations.Event) { events = append(events, e) },
			})
			s, err := schema.NewBuilder().
				Attribute(":quire/mark").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, db.SetSchema(s))

			tx := db.NewTransaction()
			for i := 0; i < entities; i++ {
				require.NoError(t, tx.Set(quire(i), mark, bound(i)))
			}
			_, err = tx.Commit()
			require.NoError(t, err)
			tx = db.NewTransaction()
			for i := 0; i < entities; i++ {
				if superseded(i) {
					require.NoError(t, tx.Set(quire(i), mark, fmt.Sprintf("stale-%d", i)))
				}
			}
			_, err = tx.Commit()
			require.NoError(t, err)

			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: mark},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			}
			binding := func(from int) executor.Relation {
				var tuples []executor.Tuple
				for i := from; i < entities; i++ {
					tuples = append(tuples, executor.Tuple{bound(i)})
				}
				return executor.NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?v")}, tuples)
			}
			answers := func(rel executor.Relation) []datalog.Identity {
				tuples, err := executor.CollectTuples(rel, nil)
				require.NoError(t, err)
				var got []datalog.Identity
				for _, tuple := range tuples {
					got = append(got, tuple[0].(datalog.Identity))
				}
				return got
			}
			m := db.Matcher().(*PatternMatcher)

			events = nil
			rel, err := m.MatchWithConstraints(query.PatternQuery(pattern), executor.Relations{binding(0)}, nil)
			require.NoError(t, err)
			require.ElementsMatch(t, want, answers(rel), "one value past the boundary: the entities whose current mark is bound")
			require.Nil(t, lastScanComplete(events, annotations.ScanVValidation), "one value past the boundary is served by no candidate scan")
			scanned := lastScanComplete(events, annotations.ScanHashJoin)
			require.NotNil(t, scanned, "one value past the boundary is served by the hash-join scan")
			require.Equal(t, AETV, scanned.Data[annotations.KeyIndex], "the one scan reads the attribute under AETV, whose first entry per entity is the current value")

			events = nil
			rel, err = m.MatchWithConstraints(query.PatternQuery(pattern), executor.Relations{binding(1)}, nil)
			require.NoError(t, err)
			require.ElementsMatch(t, want, answers(rel), "the boundary's count of values: the same entities, the superseded first one bound by neither")
			require.Nil(t, lastScanComplete(events, annotations.ScanHashJoin), "at the boundary no scan of the attribute runs")
			require.NotNil(t, lastScanComplete(events, annotations.ScanVValidation), "at the boundary the candidate-and-validate arm serves the binding")
		})
	}
}
