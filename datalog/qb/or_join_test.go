package qb

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestOrJoinRequire(t *testing.T) {
	e, a, v := NewVar("e"), NewVar("a"), NewVar("v")
	clause, err := OrJoin(e).Require(a).
		Branch(Pat(e, a, v)).
		Branch(Pat(e, Kw(":t/tag"), Kw(":t/none"))).
		toClause()
	if err != nil {
		t.Fatalf("toClause: %v", err)
	}
	ojc, ok := clause.(*query.OrJoinClause)
	if !ok {
		t.Fatalf("clause = %T, want an or-join", clause)
	}
	if got, want := ojc.String(), "(or-join [[?a] ?e] [?e ?a ?v] [?e :t/tag :t/none])"; got != want {
		t.Errorf("clause = %s, want %s", got, want)
	}
}
