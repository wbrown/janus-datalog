package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestTwoCollections_PlanAndBind traces through the execution to find the issue.
func TestTwoCollections_PlanAndBind(t *testing.T) {
	// Parse query with two collection inputs
	q, err := parser.ParseQuery(`[:find ?e ?a ?v :in $ [?e ...] [?a ...] :where [?e ?a ?v]]`)
	require.NoError(t, err)

	// Simulate what the executor does: mark symbols as bound
	initialBindings := make(map[query.Symbol]bool)
	for _, inputSpec := range q.In {
		switch inp := inputSpec.(type) {
		case query.CollectionInput:
			initialBindings[inp.Symbol] = true
			t.Logf("Marked %v as bound", inp.Symbol)
		}
	}

	// Create planner and get plan
	p := planner.NewClauseBasedPlanner(nil, planner.PlannerOptions{})
	plan, err := p.PlanQueryWithBindings(q, initialBindings)
	require.NoError(t, err)

	t.Logf("Plan:\n%s", plan.String())

	// Check what symbols are available and required in each phase
	for i, phase := range plan.Phases {
		t.Logf("Phase %d:", i)
		t.Logf("  Available: %v", phase.Available)
		t.Logf("  Provides: %v", phase.Provides)
		t.Logf("  Keep: %v", phase.Keep)
		t.Logf("  Query.Where: %v", phase.Query.Where)
	}

	// Create input relations
	entities := []datalog.Identity{
		datalog.NewIdentity("person-1"),
		datalog.NewIdentity("person-2"),
	}
	attrs := []datalog.Keyword{
		datalog.NewKeyword(":person/name"),
		datalog.NewKeyword(":person/age"),
	}

	entityTuples := make([]Tuple, len(entities))
	for i, e := range entities {
		entityTuples[i] = Tuple{e}
	}
	entityRel := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?e")}, entityTuples)

	attrTuples := make([]Tuple, len(attrs))
	for i, a := range attrs {
		attrTuples[i] = Tuple{a}
	}
	attrRel := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?a")}, attrTuples)

	inputRelations := []Relation{entityRel, attrRel}

	// Test BindQueryInputs
	bound := BindQueryInputs(q, inputRelations)
	t.Logf("Bound relation - symbols: %v, size: %d", bound.Symbols(), bound.Size())
	it := bound.Iterator()
	for it.Next() {
		t.Logf("  Bound tuple: %v", it.Tuple())
	}
	it.Close()

	// The bound relation should have 4 tuples with [?e, ?a] symbols
	require.Equal(t, 4, bound.Size(), "should have 4 tuples from cross-product")
}
