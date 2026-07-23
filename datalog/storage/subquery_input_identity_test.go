package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// TestCorrelatedSubqueryDistinguishesTypedInputCombinations is the end-to-end
// pin for BUG_SUBQUERY_INPUT_DEDUP_STRING_COLLISION (resolved): two outer
// rows whose correlate values render to the same string — int64(5) and "5" —
// are distinct input combinations and must produce distinct subquery
// executions. The unit pin lives on the projection primitive; this pin covers
// the full path through d.Query on both planner modes, where the subquery's
// input combinations are the outer relation projected onto the input symbols
// and dedup is the projection's set semantics under typed value identity. On
// the collision-era extraction, the two rows collapsed into one subquery
// execution and one row's aggregate went missing or wrong.
func TestCorrelatedSubqueryDistinguishesTypedInputCombinations(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode)

			intInput := datalog.NewIdentity("input:int")
			strInput := datalog.NewIdentity("input:str")
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(intInput, datalog.NewKeyword(":input/val"), int64(5)))
			require.NoError(t, tx.Add(strInput, datalog.NewKeyword(":input/val"), "5"))
			// Two matches for the int value, one for the string value: the
			// per-combination counts differ, so a collapsed execution cannot
			// produce both correct rows.
			for i, val := range []interface{}{int64(5), int64(5), "5"} {
				m := datalog.NewIdentity("match:" + string(rune('a'+i)))
				require.NoError(t, tx.Add(m, datalog.NewKeyword(":thing/val"), val))
			}
			_, err := tx.Commit()
			require.NoError(t, err)

			rel, err := d.Query(`[:find ?x ?cnt
			                      :where
			                      [?i :input/val ?x]
			                      [(q [:find (count ?m)
			                           :in $ ?v
			                           :where [?m :thing/val ?v]]
			                          $ ?x) [[?cnt]]]]`)
			require.NoError(t, err)

			tuples, err := executor.CollectTuples(rel, nil)
			require.NoError(t, err)
			require.Len(t, tuples, 2,
				"both typed-distinct combinations must survive input dedup")
			got := map[bool]int64{}
			for _, tuple := range tuples {
				require.Len(t, tuple, 2)
				cnt, ok := tuple[1].(int64)
				require.True(t, ok, "?cnt must be int64, got %T", tuple[1])
				switch v := tuple[0].(type) {
				case int64:
					require.Equal(t, int64(5), v)
					got[true] = cnt
				case string:
					require.Equal(t, "5", v)
					got[false] = cnt
				default:
					t.Fatalf("?x must be int64 or string, got %T", tuple[0])
				}
			}
			require.Equal(t, map[bool]int64{true: 2, false: 1}, got,
				"each combination evaluates its own subquery: two int matches, one string match")
		})
	}
}
