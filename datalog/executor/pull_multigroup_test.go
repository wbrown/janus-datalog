package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// Pull rendering must reach results whose find symbols span multiple
// disjoint relation groups in the last phase — the Product() path in
// DefaultQueryExecutor.Execute. Two shapes below aim at that branch (a
// ground-bound disjoint singleton, and a pure-aggregate subquery binding);
// both assert the user-visible contract end-to-end: the pull symbol renders
// as a map regardless of which internal path produced the relation.
func TestPullWithDisjointFindGroups(t *testing.T) {
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("user:alice"), A: nameAttr, V: "Alice", Tx: tx},
		{E: datalog.NewIdentity("user:alice"), A: ageAttr, V: int64(34), Tx: tx},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher, nil)

	t.Run("ground-bound disjoint symbol", func(t *testing.T) {
		q, err := parser.ParseQuery(`[:find (pull ?e [:user/name]) ?tag
		                              :where [?e :user/name ?name]
		                                     [(ground "tagged") ?tag]]`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		result, err := executor.Execute(q)
		if err != nil {
			t.Fatalf("execute: %v", err)
		}
		if result.Size() != 1 {
			t.Fatalf("expected 1 row, got %d", result.Size())
		}
		pulled, ok := result.Get(0)[0].(map[string]interface{})
		if !ok {
			t.Fatalf("expected pulled map in result slot 0, got %T", result.Get(0)[0])
		}
		if name := pulled["user/name"]; name != "Alice" {
			t.Errorf("expected pulled name Alice, got %v", name)
		}
		if tag := result.Get(0)[1]; tag != "tagged" {
			t.Errorf("expected tag symbol, got %v", tag)
		}
	})

	t.Run("subquery-bound disjoint symbol", func(t *testing.T) {
		q, err := parser.ParseQuery(`[:find (pull ?e [:user/name]) ?count
		                              :where [?e :user/name ?name]
		                                     [(q [:find (count ?x)
		                                          :where [?x :user/age _]]
		                                         $) [[?count]]]]`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		result, err := executor.Execute(q)
		if err != nil {
			t.Fatalf("execute: %v", err)
		}
		if result.Size() != 1 {
			t.Fatalf("expected 1 row, got %d", result.Size())
		}
		pulled, ok := result.Get(0)[0].(map[string]interface{})
		if !ok {
			t.Fatalf("expected pulled map in result slot 0, got %T", result.Get(0)[0])
		}
		if name := pulled["user/name"]; name != "Alice" {
			t.Errorf("expected pulled name Alice, got %v", name)
		}
		if count := result.Get(0)[1]; count != int64(1) {
			t.Errorf("expected count 1, got %v", count)
		}
	})
}
