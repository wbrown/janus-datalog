package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestTerminalSymbolsCoverEveryFinalizationDependency(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	group := datalog.NewSymbol("?group")
	value := datalog.NewSymbol("?value")
	predicate := datalog.NewSymbol("?predicate")
	sortKey := datalog.NewSymbol("?sort")

	aggregateQuery := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: group},
			query.FindAggregate{Function: "max", Arg: value, Predicate: predicate},
			query.FindPull{Variable: entity, Pattern: &query.PullPattern{}},
		},
	}
	require.Equal(t,
		[]query.Symbol{group, value, predicate, entity},
		terminalSymbols(aggregateQuery),
	)

	orderedQuery := &query.Query{
		Find:    []query.FindElement{query.FindVariable{Symbol: group}},
		OrderBy: []query.OrderByClause{{Variable: sortKey, Direction: query.OrderAsc}},
	}
	require.Equal(t,
		[]query.Symbol{group, sortKey},
		terminalSymbols(orderedQuery),
	)
}

func TestRealizedPlanPhysicalContracts(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	score := datalog.NewSymbol("?score")
	payload := datalog.NewSymbol("?payload")
	aggregateResult := datalog.NewSymbol("(max ?payload)")

	valid := &RealizedPlan{
		Query: &query.Query{},
		Phases: []RealizedPhase{
			{
				Query: &query.Query{
					In: []query.InputSpec{
						query.DatabaseInput{Name: datalog.SymDollar},
						query.RelationInput{Symbols: []query.Symbol{entity, score}},
					},
					Find: []query.FindElement{query.FindVariable{Symbol: entity}},
				},
				Available: []query.Symbol{entity, score},
				Provides:  []query.Symbol{entity},
				Keep:      []query.Symbol{entity},
			},
			{
				Query: &query.Query{
					In: []query.InputSpec{
						query.DatabaseInput{Name: datalog.SymDollar},
						query.RelationInput{Symbols: []query.Symbol{entity}},
					},
					Find: []query.FindElement{
						query.FindAggregate{Function: "max", Arg: payload},
					},
					Where: []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
						query.Variable{Name: entity},
						query.Constant{Value: datalog.NewKeyword(":item/payload")},
						query.Variable{Name: payload},
					}}},
				},
				Available: []query.Symbol{entity},
				Provides:  []query.Symbol{aggregateResult},
			},
		},
	}
	require.NoError(t, valid.Validate())

	testCases := []struct {
		name   string
		mutate func(*RealizedPlan)
		want   string
	}{
		{
			name: "available differs from phase input",
			mutate: func(plan *RealizedPlan) {
				plan.Phases[1].Available = []query.Symbol{score}
			},
			want: "available schema",
		},
		{
			name: "provides differs from query output",
			mutate: func(plan *RealizedPlan) {
				plan.Phases[1].Provides = []query.Symbol{payload}
			},
			want: "provides schema",
		},
		{
			name: "non-final keep differs from provides",
			mutate: func(plan *RealizedPlan) {
				plan.Phases[0].Keep = []query.Symbol{score}
			},
			want: "boundary schema",
		},
		{
			name: "adjacent phase schemas disagree",
			mutate: func(plan *RealizedPlan) {
				plan.Phases[1].Query.In = []query.InputSpec{
					query.DatabaseInput{Name: datalog.SymDollar},
					query.RelationInput{Symbols: []query.Symbol{score}},
				}
				plan.Phases[1].Available = []query.Symbol{score}
			},
			want: "does not match previous boundary",
		},
		{
			name: "final phase has keep boundary",
			mutate: func(plan *RealizedPlan) {
				plan.Phases[1].Keep = []query.Symbol{aggregateResult}
			},
			want: "final phase keep",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			plan := cloneRealizedPlanForContractTest(valid)
			testCase.mutate(plan)
			err := plan.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), testCase.want)
		})
	}
}

func TestRealizedPlanRejectsSymbolDroppedBeforeNonAdjacentUse(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	derived := datalog.NewSymbol("?derived")
	plan := &RealizedPlan{
		Query: &query.Query{},
		Phases: []RealizedPhase{
			{
				Query: &query.Query{
					Find: []query.FindElement{query.FindVariable{Symbol: entity}},
					Where: []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
						query.Variable{Name: entity},
						query.Constant{Value: datalog.NewKeyword(":item/id")},
						query.Constant{Value: "present"},
					}}},
				},
				Provides: []query.Symbol{entity},
				Keep:     []query.Symbol{entity},
			},
			{
				Query: &query.Query{
					In: []query.InputSpec{
						query.DatabaseInput{Name: datalog.SymDollar},
						query.RelationInput{Symbols: []query.Symbol{entity}},
					},
					Find: []query.FindElement{query.FindVariable{Symbol: derived}},
					Where: []query.Clause{&query.Expression{
						Function: query.IdentityFunction{
							Arg: query.VariableTerm{Symbol: entity},
						},
						Binding: derived,
					}},
				},
				Available: []query.Symbol{entity},
				Provides:  []query.Symbol{derived},
				Keep:      []query.Symbol{derived},
			},
			{
				Query: &query.Query{
					In: []query.InputSpec{
						query.DatabaseInput{Name: datalog.SymDollar},
						query.RelationInput{Symbols: []query.Symbol{derived}},
					},
					Find: []query.FindElement{query.FindVariable{Symbol: derived}},
					Where: []query.Clause{&query.Comparison{
						Op:    query.OpEQ,
						Left:  query.VariableTerm{Symbol: entity},
						Right: query.ConstantTerm{Value: int64(1)},
					}},
				},
				Available: []query.Symbol{derived},
				Provides:  []query.Symbol{derived},
			},
		},
	}

	err := plan.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires unavailable symbol ?entity")
}

func TestPhysicalFindSymbolsPreserveTypedOutputOrder(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")

	require.Equal(t,
		[]query.Symbol{
			entity,
			datalog.NewSymbol("(sum ?value)"),
			entity,
		},
		physicalFindSymbols([]query.FindElement{
			query.FindVariable{Symbol: entity},
			query.FindAggregate{Function: "sum", Arg: value},
			query.FindPull{Variable: entity, Pattern: &query.PullPattern{}},
		}),
	)
}

func TestClauseBasedPlannerEmitsPhysicalPhaseContracts(t *testing.T) {
	queries := map[string]string{
		"simple": `[:find ?entity ?name
			:where [?entity :person/name ?name]]`,
		"retained sort": `[:find ?name
			:where [?entity :person/name ?name]
			       [?entity :person/score ?score]
			:order-by [[?score :desc]]]`,
		"aggregate": `[:find ?city (count ?person)
			:where [?person :person/city ?city]]`,
		"pull": `[:find (pull ?entity [:person/name])
			:where [?entity :person/name ?name]]`,
		"correlated subquery": `[:find ?entity ?count
			:where [?entity :entity/type :entity.type/scenario]
			       [(q [:find (count ?task)
			            :in $ ?scenario
			            :where [?task :task/root ?scenario]]
			           $ ?entity) [[?count]]]]`,
	}

	planner := NewClauseBasedPlanner(nil, PlannerOptions{EnableAlgebraOptimizer: true})
	for name, source := range queries {
		t.Run(name, func(t *testing.T) {
			parsed, err := parser.ParseQuery(source)
			require.NoError(t, err)
			plan, err := planner.Plan(parsed, nil)
			require.NoError(t, err)
			require.NoError(t, plan.Validate())
		})
	}
}

func cloneRealizedPlanForContractTest(plan *RealizedPlan) *RealizedPlan {
	cloned := &RealizedPlan{
		Query:  plan.Query,
		Phases: make([]RealizedPhase, len(plan.Phases)),
	}
	for i, phase := range plan.Phases {
		cloned.Phases[i] = phase
		cloned.Phases[i].Available = append([]query.Symbol(nil), phase.Available...)
		cloned.Phases[i].Provides = append([]query.Symbol(nil), phase.Provides...)
		cloned.Phases[i].Keep = append([]query.Symbol(nil), phase.Keep...)
		cloned.Phases[i].Query = &query.Query{
			Find:  append([]query.FindElement(nil), phase.Query.Find...),
			In:    append([]query.InputSpec(nil), phase.Query.In...),
			Where: append([]query.Clause(nil), phase.Query.Where...),
		}
	}
	return cloned
}
