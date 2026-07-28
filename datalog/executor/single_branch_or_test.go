package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A single-branch or/or-join is a legal degenerate form: it is exactly its
// branch, restricted to the header's declared interface. The algebra bridge
// previously rejected it as an invalid union
// (docs/bugs/BUG_ALGEBRA_NOT_REJECTS_SINGLE_BRANCH_ORJOIN.md); the NOT-inner
// case is pinned by TestNotClauseWithOrJoinBody, and this pins the plain
// positional case on both planner modes with exact tuples.
func TestSingleBranchOrJoinExecutesAsItsBranch(t *testing.T) {
	valAttr := datalog.NewKeyword(":item/val")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("item:1"), A: valAttr, V: int64(1), Tx: tx},
		{E: datalog.NewIdentity("item:2"), A: valAttr, V: int64(2), Tx: tx},
	}
	matcher := NewMemoryPatternMatcher(datoms)

	v := datalog.NewSymbol("?v")
	e := datalog.NewSymbol("?e")
	branch := []query.Clause{
		&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: e},
			query.Constant{Value: valAttr},
			query.Variable{Name: v},
		}},
	}

	shapes := map[string]query.Clause{
		"or-join": &query.OrJoinClause{
			JoinVars: []query.Symbol{v},
			Branches: [][]query.Clause{branch},
		},
		"or": &query.OrClause{
			Branches: [][]query.Clause{branch},
		},
	}

	for shapeName, clause := range shapes {
		t.Run(shapeName, func(t *testing.T) {
			q := &query.Query{
				Find:  []query.FindElement{query.FindVariable{Symbol: v}},
				Where: []query.Clause{clause},
			}
			for _, mode := range optimizerModes {
				t.Run(mode.name, func(t *testing.T) {
					exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
					rel, err := exec.Execute(q)
					if err != nil {
						t.Fatalf("single-branch %s failed: %v", shapeName, err)
					}
					tuples, err := CollectTuples(rel, nil)
					if err != nil {
						t.Fatal(err)
					}
					got := map[int64]bool{}
					for _, tuple := range tuples {
						got[tuple[0].(int64)] = true
					}
					if len(got) != 2 || !got[1] || !got[2] {
						t.Fatalf("single-branch %s must produce exactly its branch's tuples {1 2}, got %v", shapeName, tuples)
					}
				})
			}
		})
	}
}
