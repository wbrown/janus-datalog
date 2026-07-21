package query

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

func walkPattern(name string) *DataPattern {
	return &DataPattern{Elements: []PatternElement{
		Variable{Name: datalog.NewSymbol("?" + name)},
		Constant{Value: datalog.NewKeyword(":t/" + name)},
		Blank{},
	}}
}

// TestWalkClausesVisitsNestedClauses pins the traversal: every clause is
// visited in order, descending through NOT/NOT-JOIN bodies and
// OR/OR-JOIN/OR-DEFAULT branches. Subquery inner queries are not descended
// into — a subquery's :where is its own scope.
func TestWalkClausesVisitsNestedClauses(t *testing.T) {
	inner := walkPattern("inner")
	subInner := walkPattern("subinner")
	clauses := []Clause{
		walkPattern("top"),
		&NotClause{Clauses: []Clause{walkPattern("not")}},
		&NotJoinClause{Clauses: []Clause{walkPattern("notjoin")}},
		&OrClause{Branches: [][]Clause{{walkPattern("or1")}, {walkPattern("or2")}}},
		&OrJoinClause{Branches: [][]Clause{{walkPattern("orjoin")}}},
		&OrDefaultClause{Branches: [][]Clause{{walkPattern("ordefault")}}},
		&OrDefaultJoinClause{Branches: [][]Clause{{inner}}},
		&Comparison{Op: datalog.SymLT, Left: ConstantTerm{Value: int64(1)}, Right: ConstantTerm{Value: int64(2)}},
		&SubqueryPattern{
			Query:   &Query{Where: []Clause{subInner}},
			Binding: ScalarBinding{Variable: datalog.NewSymbol("?s")},
		},
	}

	var visited []Clause
	WalkClauses(clauses, func(c Clause) bool {
		visited = append(visited, c)
		return true
	})

	// 9 top-level + 7 nested branch/body clauses; the subquery's inner
	// pattern is out of scope.
	if len(visited) != 16 {
		t.Fatalf("visited %d clauses, want 16: %v", len(visited), visited)
	}
	for _, c := range visited {
		if c == Clause(subInner) {
			t.Error("WalkClauses must not descend into subquery inner queries")
		}
	}
	if visited[len(visited)-3] != Clause(inner) {
		// inner is the or-default-join branch clause, visited immediately
		// after its parent and before the trailing comparison and subquery.
		t.Errorf("nested clauses must be visited in order; got %v", visited)
	}
}

// TestWalkClausesDescendControl pins that visit's return value gates descent:
// false skips a compound clause's children without stopping the walk.
func TestWalkClausesDescendControl(t *testing.T) {
	skipped := walkPattern("skipped")
	after := walkPattern("after")
	clauses := []Clause{
		&NotClause{Clauses: []Clause{skipped}},
		after,
	}

	var visited []Clause
	WalkClauses(clauses, func(c Clause) bool {
		visited = append(visited, c)
		_, isNot := c.(*NotClause)
		return !isNot
	})

	if len(visited) != 2 {
		t.Fatalf("visited %d clauses, want 2 (not-clause and the sibling): %v", len(visited), visited)
	}
	for _, c := range visited {
		if c == Clause(skipped) {
			t.Error("returning false must skip the compound clause's children")
		}
	}
	if visited[1] != Clause(after) {
		t.Error("skipping children must not stop the walk over siblings")
	}
}
