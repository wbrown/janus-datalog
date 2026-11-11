package executor_test

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// A plain not nested inside a not-join body is inferred under the body's
// header before the header check: a variable of the not that occurs nowhere
// else in the body is the not's own, never a requirement the not-join's
// header must declare.
func TestNotJoinAcceptsNestedExistentialNot(t *testing.T) {
	goalAttr := datalog.NewKeyword(":event/goal")
	nameAttr := datalog.NewKeyword(":goal/name")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	goalA := datalog.NewIdentity("goal:a")
	goalB := datalog.NewIdentity("goal:b")
	matcher := factsDatabase(t, []datalog.Datom{
		{E: goalA, A: nameAttr, V: "a", Tx: tx},
		{E: goalB, A: nameAttr, V: "b", Tx: tx},

		{E: datalog.NewIdentity("event:1"), A: goalAttr, V: goalA, Tx: tx},
	})

	q, err := parser.ParseQuery(`[:find ?name
	  :where
	  [?goal :goal/name ?name]
	  (not-join [?goal]
	    [?ev :event/goal ?goal]
	    (not [?flagger :other/flag ?goal]))]`)
	if err != nil {
		t.Fatalf("parser rejected a legal nested-existential not-join: %v", err)
	}

	for _, mode := range LegsFor(t) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())
			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("execution rejected a legal nested-existential not-join: %v", err)
			}
			// goalA has an event and no flagger exists, so the body's
			// nested NOT holds, the body matches, and goalA is excluded.
			// goalB has no event and survives.
			tuples, err := CollectTuples(result, nil)
			if err != nil {
				t.Fatalf("reading the result: %v", err)
			}
			if len(tuples) != 1 {
				t.Fatalf("expected only the event-less goal to survive, got %d tuples: %v", len(tuples), tuples)
			}
			if !datalog.ValuesEqual(tuples[0][0], "b") {
				t.Fatalf("expected goal b to survive, got %v", tuples[0])
			}
		})
	}
}
