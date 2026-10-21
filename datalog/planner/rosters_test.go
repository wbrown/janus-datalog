package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// rosterQuery is a query naming three attributes, one at the top level and
// one in each branch of an or-join.
func rosterQuery() *query.Query {
	entity := query.Variable{Name: datalog.NewSymbol("?e")}
	named := func(attr string) *query.DataPattern {
		return &query.DataPattern{Elements: []query.PatternElement{entity, query.Constant{Value: datalog.NewKeyword(attr)}, query.Blank{}}}
	}
	return &query.Query{
		Find: []query.FindElement{query.FindVariable{Symbol: entity.Name}},
		Where: []query.Clause{
			named(":t/a"),
			&query.OrJoinClause{OutputVars: []query.Symbol{entity.Name}, Branches: [][]query.Clause{
				{named(":t/b")},
				{named(":t/c")},
			}},
		},
	}
}

// assertRosterNames asserts that a plan's roster names the three attributes
// of rosterQuery, the branches' included.
func assertRosterNames(t *testing.T, plan *RealizedPlan) {
	t.Helper()
	if plan.Rosters == nil {
		t.Fatal("the plan holds its query's rosters; got none")
	}
	got := plan.Rosters.DefaultSourceAttributes(nil)
	want := []datalog.Keyword{datalog.NewKeyword(":t/a"), datalog.NewKeyword(":t/b"), datalog.NewKeyword(":t/c")}
	if len(got) != len(want) {
		t.Fatalf("the plan's roster names %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("attribute %d is %s, want %s", i, got[i], want[i])
		}
	}
}

// A plan made under CollectRosters holds what its prepared query names,
// collected at planning: the roster names the attributes the query's
// patterns carry, a header form's branches included. A plan made without the
// option carries none.
func TestRealizedPlanHoldsItsRostersUnderTheOption(t *testing.T) {
	plan, err := NewClauseBasedPlanner(nil, PlannerOptions{CollectRosters: true}).Plan(rosterQuery(), query.Bindings{}, nil)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	assertRosterNames(t, plan)

	plan, err = NewClauseBasedPlanner(nil, PlannerOptions{}).Plan(rosterQuery(), query.Bindings{}, nil)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.Rosters != nil {
		t.Fatalf("a plan made without the option carries no roster; got %v", plan.Rosters.DefaultSourceAttributes(nil))
	}
}

// A plan the cache serves carries the roster made with it: the second plan
// of one text under one cache is the first, roster and all.
func TestPlanServedFromTheCacheCarriesItsRosters(t *testing.T) {
	cache := NewPlanCache(10, 0)
	planner := NewClauseBasedPlanner(nil, PlannerOptions{Cache: cache, CollectRosters: true})
	first, err := planner.Plan(rosterQuery(), query.Bindings{}, nil)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	served, err := planner.Plan(rosterQuery(), query.Bindings{}, nil)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if hits, _, _ := cache.Stats(); hits != 1 {
		t.Fatalf("the second plan is served from the cache; got %d hits", hits)
	}
	if served != first {
		t.Fatal("the cache serves the plan it holds; got another")
	}
	assertRosterNames(t, served)
}
