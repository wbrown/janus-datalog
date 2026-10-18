package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestExecuteRealizedRejectsInvalidPhysicalPhaseMetadata(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	plan := &planner.RealizedPlan{
		Query: &query.Query{},
		Phases: []planner.RealizedPhase{{
			Query: &query.Query{
				Find: []query.FindElement{query.FindVariable{Symbol: x}},
			},
			Provides: []query.Symbol{y},
		}},
	}

	executor := NewExecutor(NewMemoryPatternMatcher(nil), nil)
	_, err := executor.ExecuteRealized(NewContext(), plan, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid realized plan")
	require.Contains(t, err.Error(), "provides schema")
}

// existenceVerdict is the verdict over groups a find does not read: the unit
// relation when each group has a tuple, nothing when one has none, the unit
// relation over no group, and a group's scan failure as its error. The find
// boundary of Execute takes it over the groups the find does not consume,
// and a phase whose fragment finds nothing takes it over every group.
func TestExistenceVerdictOverGroups(t *testing.T) {
	x := datalog.NewSymbol("?x")
	full := NewMaterializedRelation([]query.Symbol{x}, []Tuple{{int64(1)}})
	empty := NewMaterializedRelation([]query.Symbol{x}, nil)

	groups, err := existenceVerdict([]Relation{full, full}, ExecutorOptions{})
	require.NoError(t, err)
	require.Len(t, groups, 1, "every group has a tuple")
	require.Empty(t, groups[0].Symbols(), "the unit relation carries no symbol")
	require.Equal(t, 1, groups[0].Size(), "the unit relation carries one tuple")

	groups, err = existenceVerdict([]Relation{full, empty}, ExecutorOptions{})
	require.NoError(t, err)
	require.Empty(t, groups, "a group with no tuple empties the result")

	groups, err = existenceVerdict(nil, ExecutorOptions{})
	require.NoError(t, err)
	require.Len(t, groups, 1, "no group is a verdict that holds")

	failing := NewStreamingRelation([]query.Symbol{x}, failingRelation{Relation: full, failAfter: 0}.Iterator())
	_, err = existenceVerdict([]Relation{failing}, ExecutorOptions{})
	require.ErrorIs(t, err, errInjectedIterator, "a group's scan failure is the verdict's error")
}

// executePhase runs a phase's fragment over the groups entering it and
// returns what the phase provides, checked against its Provides: the
// fragment's own result, or, for a fragment finding nothing, the existence
// verdict over that result, with a failure of either named by the phase's
// number.
func TestExecutePhaseTakesTheVerdictForAFragmentFindingNothing(t *testing.T) {
	x := datalog.NewSymbol("?x")
	full := NewMaterializedRelation([]query.Symbol{x}, []Tuple{{int64(1)}})
	empty := NewMaterializedRelation([]query.Symbol{x}, nil)
	queryExecutor := NewExecutor(NewMemoryPatternMatcher(nil), nil).queryExecutor()
	verdictPhase := planner.RealizedPhase{Query: &query.Query{}}
	findingPhase := planner.RealizedPhase{
		Query:    &query.Query{Find: []query.FindElement{query.FindVariable{Symbol: x}}},
		Provides: []query.Symbol{x},
	}

	groups, err := queryExecutor.executePhase(NewContext(), 1, verdictPhase, []Relation{full, full})
	require.NoError(t, err)
	require.Len(t, groups, 1, "a fragment finding nothing over groups with tuples provides the unit relation")
	require.Empty(t, groups[0].Symbols(), "the unit relation carries no symbol")
	require.Equal(t, 1, groups[0].Size(), "the unit relation carries one tuple")

	groups, err = queryExecutor.executePhase(NewContext(), 1, verdictPhase, []Relation{full, empty})
	require.NoError(t, err)
	require.Empty(t, groups, "a fragment finding nothing over a group with no tuple provides nothing")

	groups, err = queryExecutor.executePhase(NewContext(), 1, findingPhase, []Relation{full})
	require.NoError(t, err)
	require.Len(t, groups, 1, "a fragment finding ?x provides its own result")
	require.Equal(t, []query.Symbol{x}, groups[0].Symbols(), "the result carries the fragment's find")
	require.Equal(t, 1, groups[0].Size(), "the result carries the group's tuple")

	failing := NewStreamingRelation([]query.Symbol{x}, failingRelation{Relation: full, failAfter: 0}.Iterator())
	_, err = queryExecutor.executePhase(NewContext(), 3, verdictPhase, []Relation{failing})
	require.ErrorIs(t, err, errInjectedIterator, "a group's scan failure under the verdict is the phase's error")
	require.Contains(t, err.Error(), "phase 3", "the error names the phase")
}

func TestValidatePhaseOutputRequiresExactPhysicalSchema(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	phase := planner.RealizedPhase{Provides: []query.Symbol{x}}

	require.NoError(t, validatePhaseOutput(2, phase, []Relation{
		NewMaterializedRelation([]query.Symbol{x}, []Tuple{{int64(1)}}),
		NewMaterializedRelation([]query.Symbol{x}, []Tuple{{int64(2)}}),
	}))

	err := validatePhaseOutput(2, phase, []Relation{
		NewMaterializedRelation([]query.Symbol{y}, []Tuple{{int64(1)}}),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "phase 2")
	require.Contains(t, err.Error(), "physical output schema")
	require.Contains(t, err.Error(), "?y")
	require.Contains(t, err.Error(), "?x")
}
