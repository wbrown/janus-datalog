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
	_, err := executor.ExecuteRealized(NewContext(nil), plan, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid realized plan")
	require.Contains(t, err.Error(), "provides schema")
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
