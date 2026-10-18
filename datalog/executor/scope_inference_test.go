package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestOrJoinWithARequiredOnlyHeaderFiltersTheOuter: an or whose branches
// read the outer relation's ?n and bind nothing of their own has a header
// that is all required, and it keeps the outer tuples some branch accepts.
func TestOrJoinWithARequiredOnlyHeaderFiltersTheOuter(t *testing.T) {
	nAttr := datalog.NewKeyword(":t/n")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("t:1"), A: nAttr, V: int64(1), Tx: tx},
		{E: datalog.NewIdentity("t:2"), A: nAttr, V: int64(2), Tx: tx},
		{E: datalog.NewIdentity("t:5"), A: nAttr, V: int64(5), Tx: tx},
	}
	matcher := NewMemoryPatternMatcher(datoms)
	q, err := parser.ParseQuery(`[:find ?n :where [?e :t/n ?n] (or [(> ?n 3)] [(< ?n 2)])]`)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
			result, err := executor.Execute(q)
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			got := map[int64]bool{}
			for i := 0; i < result.Size(); i++ {
				got[result.Get(i)[0].(int64)] = true
			}
			want := map[int64]bool{1: true, 5: true}
			if len(got) != len(want) {
				t.Fatalf("the or keeps the tuples some branch accepts: got %v, want %v", got, want)
			}
			for n := range want {
				if !got[n] {
					t.Errorf("the or keeps %d, which a branch accepts: got %v", n, got)
				}
			}
		})
	}
}
