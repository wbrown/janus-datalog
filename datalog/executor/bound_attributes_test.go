package executor

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The attributes an input binds into a pattern's attribute position are read
// from the variable's position in its relation, found by symbol: a relation
// carrying the variable yields each keyword at it once, and a relation lacking
// the variable its :in entry binds is the query's error naming the variable,
// never a relation read past.
func TestBoundAttributesReadsTheVariableBySymbol(t *testing.T) {
	a := datalog.NewSymbol("?a")
	q := &query.Query{
		Find: []query.FindElement{query.FindVariable{Symbol: datalog.NewSymbol("?e")}},
		In:   []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.ScalarInput{Symbol: a}},
		Where: []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")}, query.Variable{Name: a}, query.Blank{},
		}}},
	}
	kind := datalog.NewKeyword(":t/kind")

	carrying := NewMaterializedRelation([]query.Symbol{a}, []Tuple{{kind}, {kind}})
	roster, err := boundAttributes(q, []query.Symbol{a}, []Relation{carrying})
	if err != nil {
		t.Fatalf("boundAttributes over a relation carrying ?a: %v", err)
	}
	if len(roster) != 1 || roster[0] != kind {
		t.Fatalf("roster = %v, want the one keyword the relation binds to ?a", roster)
	}

	lacking := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?b")}, []Tuple{{kind}})
	_, err = boundAttributes(q, []query.Symbol{a}, []Relation{lacking})
	if err == nil || !strings.Contains(err.Error(), "?a") {
		t.Fatalf("boundAttributes over a relation lacking ?a = %v, want an error naming ?a", err)
	}
}
