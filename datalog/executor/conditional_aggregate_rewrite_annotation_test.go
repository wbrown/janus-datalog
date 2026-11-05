package executor_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestConditionalAggregateRewriteAnnotationUsesDatalogFindClause(t *testing.T) {
	symE := datalog.NewSymbol("?e")
	symValue := datalog.NewSymbol("?value")
	symFilter := datalog.NewSymbol("?filter")
	valueAttr := datalog.NewKeyword(":metric/value")
	filterAttr := datalog.NewKeyword(":metric/include")

	valuePattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: symE},
		query.Constant{Value: valueAttr},
		query.Variable{Name: symValue},
	}}
	filterPattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: symE},
		query.Constant{Value: filterAttr},
		query.Variable{Name: symFilter},
	}}
	find := []query.FindElement{query.FindAggregate{
		Function:  datalog.SymMin,
		Arg:       symValue,
		Predicate: symFilter,
	}}
	phaseQuery := &query.Query{
		Find:  find,
		Where: []query.Clause{valuePattern, filterPattern},
	}
	plan := &planner.RealizedPlan{
		Query: phaseQuery,
		Phases: []planner.RealizedPhase{{
			Query:    phaseQuery,
			Provides: []query.Symbol{datalog.NewSymbol(find[0].String())},
		}},
	}

	entity := datalog.NewIdentity("metric-1")
	matcher := factsDatabase(t, []datalog.Datom{
		{E: entity, A: valueAttr, V: 42.0},
		{E: entity, A: filterAttr, V: true},
	})
	var rewriteEvent *annotations.Event
	opts := DefaultPlannerOptions()
	opts.Handler = func(event annotations.Event) {
		if event.Name == "query/rewrite.conditional-aggregates" {
			captured := event
			rewriteEvent = &captured
		}
	}
	exec := NewExecutorWithOptions(matcher, nil, opts)

	result, err := exec.ExecuteRealized(NewContext(), plan, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, rewriteEvent)
	assert.Equal(t, 1, rewriteEvent.Data["rewritten.subquery.count"])
	assert.Equal(t, "conditional-aggregate-rewriting", rewriteEvent.Data["optimization"])
}
