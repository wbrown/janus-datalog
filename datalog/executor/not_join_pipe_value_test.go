package executor_test

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestNotJoinClause_PipeInValue_E2E verifies that NOT-JOIN does not
// erroneously filter outer tuples whose join-key values collide under
// stringification with values matched by the inner clause.
//
// Setup:
//
//	a: (name="x|y", tag="z",   archived)
//	b: (name="x",   tag="y|z")
//
// Outer tuples for (?name, ?tag):
//
//	("x|y", "z")  — stringifies to "x|y|z"
//	("x",   "y|z") — also stringifies to "x|y|z"  (collision)
//
// The NOT-JOIN over [?name ?tag] checks whether ANY entity with the same
// (name, tag) is archived. Only the first combo matches an archived entity
// (a). With correct keying, the result is exactly ("x", "y|z") for entity b.
// With the stringification collision, both combos share a key — depending
// on iteration order, either both are filtered (0 results) or neither is
// (2 results).
//
// Exercises: query_executor.go filterWithNotJoinClause end-to-end
func TestNotJoinClause_PipeInValue_E2E(t *testing.T) {
	nameAttr := datalog.NewKeyword(":item/name")
	tagAttr := datalog.NewKeyword(":item/tag")
	archivedAttr := datalog.NewKeyword(":item/archived")

	a := datalog.NewIdentity("a")
	b := datalog.NewIdentity("b")

	datoms := []datalog.Datom{
		{E: a, A: nameAttr, V: "x|y", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: a, A: tagAttr, V: "z", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: a, A: archivedAttr, V: true, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},

		{E: b, A: nameAttr, V: "x", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: b, A: tagAttr, V: "y|z", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := factsDatabase(t, datoms)

	// [:find ?name ?tag
	//  :where [?e :item/name ?name]
	//         [?e :item/tag ?tag]
	//         (not-join [?name ?tag]
	//                   [?other :item/name ?name]
	//                   [?other :item/tag ?tag]
	//                   [?other :item/archived true])]
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: datalog.NewSymbol("?name")},
			query.FindVariable{Symbol: datalog.NewSymbol("?tag")},
		},
		Where: []query.Clause{
			&query.DataPattern{Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?e")},
				query.Constant{Value: nameAttr},
				query.Variable{Name: datalog.NewSymbol("?name")},
			}},
			&query.DataPattern{Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?e")},
				query.Constant{Value: tagAttr},
				query.Variable{Name: datalog.NewSymbol("?tag")},
			}},
			&query.NotJoinClause{
				RequiredVars: []query.Symbol{
					datalog.NewSymbol("?name"),
					datalog.NewSymbol("?tag"),
				},
				Clauses: []query.Clause{
					&query.DataPattern{Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?other")},
						query.Constant{Value: nameAttr},
						query.Variable{Name: datalog.NewSymbol("?name")},
					}},
					&query.DataPattern{Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?other")},
						query.Constant{Value: tagAttr},
						query.Variable{Name: datalog.NewSymbol("?tag")},
					}},
					&query.DataPattern{Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?other")},
						query.Constant{Value: archivedAttr},
						query.Constant{Value: true},
					}},
				},
			},
		},
	}

	for _, mode := range LegsFor(t, SharingOption, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())

			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("execution failed: %v", err)
			}

			tuples, err := CollectTuples(result, nil)
			if err != nil {
				t.Fatalf("reading the result: %v", err)
			}
			if len(tuples) != 1 {
				t.Fatalf("expected 1 result (name=x, tag=y|z) for entity b, got %d", len(tuples))
			}

			tuple := tuples[0]
			name, _ := tuple[0].(string)
			tag, _ := tuple[1].(string)
			if name != "x" || tag != "y|z" {
				t.Errorf("expected (x, y|z), got (%q, %q)", name, tag)
			}
		})
	}
}
