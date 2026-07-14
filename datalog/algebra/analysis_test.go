package algebra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestAnalyzeValidatesEveryOperatorContract(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	score := datalog.NewSymbol("?score")
	payload := datalog.NewSymbol("?payload")
	mapped := datalog.NewSymbol("?mapped")
	count := datalog.NewSymbol("(count ?entity)")

	scanScore := algebraTestScan(entity, score)
	scanPayload := algebraTestScan(entity, payload)
	validJoin := &Node{
		Op:       RuleJoin,
		Children: []*Node{scanScore, scanPayload},
		Data: &Join{
			Kind:        InnerJoin,
			JoinSymbols: []query.Symbol{entity},
			Output:      []query.Symbol{entity, score, payload},
		},
	}
	validCases := map[string]*Node{
		"scan": scanScore,
		"select": {
			Op:       RuleSelect,
			Children: []*Node{scanScore},
			Data: &Select{
				Required: []query.Symbol{score},
				Output:   []query.Symbol{entity, score},
			},
		},
		"project": {
			Op:       RuleProject,
			Children: []*Node{scanScore},
			Data:     &Project{Symbols: []query.Symbol{entity}},
		},
		"map": {
			Op:       RuleMap,
			Children: []*Node{scanScore},
			Data: &Map{
				Required: []query.Symbol{score},
				Output:   []query.Symbol{entity, score, mapped},
			},
		},
		"join": validJoin,
		"anti": {
			Op:       RuleAntiJoin,
			Children: []*Node{scanScore, scanPayload},
			Data: &AntiJoin{
				JoinSymbols: []query.Symbol{entity},
				Output:      []query.Symbol{entity, score},
			},
		},
		"union": {
			Op:       RuleUnion,
			Children: []*Node{algebraTestScan(entity, score), algebraTestScan(entity, score)},
			Data:     &Union{Output: []query.Symbol{entity, score}},
		},
		"aggregate": {
			Op:       RuleAggregate,
			Children: []*Node{scanScore},
			Data: &Aggregate{
				GroupBy:   []query.Symbol{score},
				Functions: []query.FindAggregate{{Function: "count", Arg: entity}},
				Bindings:  []query.Symbol{score, count},
				Output:    []query.Symbol{score, count},
			},
		},
		"constant": {
			Op:   RuleConstant,
			Data: &Constant{Symbols: []query.Symbol{mapped}, Values: []interface{}{int64(1)}},
		},
		"lateral": {
			Op:       RuleLateralJoin,
			Children: []*Node{scanScore},
			Data: &LateralJoin{
				CorrelationVars: []query.Symbol{entity},
				InnerQuery: &query.Query{
					Find: []query.FindElement{query.FindVariable{Symbol: payload}},
				},
				Binding: query.TupleBinding{Variables: []query.Symbol{payload}},
				Output:  []query.Symbol{entity, score, payload},
			},
		},
	}

	for name, root := range validCases {
		t.Run(name, func(t *testing.T) {
			analysis, err := Analyze(root)
			require.NoError(t, err)
			require.Equal(t, root.Symbols(), analysis[root].Output)
		})
	}
}

func TestAnalyzeRejectsInvalidOperatorContracts(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	score := datalog.NewSymbol("?score")
	missing := datalog.NewSymbol("?missing")
	scan := algebraTestScan(entity, score)

	testCases := []struct {
		name string
		root *Node
		want string
	}{
		{
			name: "duplicate output",
			root: &Node{
				Op:   RuleScan,
				Data: &Scan{Output: []query.Symbol{entity, entity}},
			},
			want: "duplicate output symbol",
		},
		{
			name: "project missing symbol",
			root: &Node{
				Op:       RuleProject,
				Children: []*Node{scan},
				Data:     &Project{Symbols: []query.Symbol{missing}},
			},
			want: "project symbol ?missing",
		},
		{
			name: "join key missing from right",
			root: &Node{
				Op:       RuleJoin,
				Children: []*Node{scan, algebraTestScan(missing)},
				Data: &Join{
					Kind:        InnerJoin,
					JoinSymbols: []query.Symbol{entity},
					Output:      []query.Symbol{entity, score, missing},
				},
			},
			want: "join symbol ?entity",
		},
		{
			name: "stale join output",
			root: &Node{
				Op:       RuleJoin,
				Children: []*Node{scan, algebraTestScan(entity, missing)},
				Data: &Join{
					Kind:        InnerJoin,
					JoinSymbols: []query.Symbol{entity},
					Output:      []query.Symbol{entity, score},
				},
			},
			want: "join output",
		},
		{
			name: "anti output differs from left",
			root: &Node{
				Op:       RuleAntiJoin,
				Children: []*Node{scan, algebraTestScan(entity)},
				Data: &AntiJoin{
					JoinSymbols: []query.Symbol{entity},
					Output:      []query.Symbol{entity},
				},
			},
			want: "anti-join output",
		},
		{
			name: "union schemas differ",
			root: &Node{
				Op:       RuleUnion,
				Children: []*Node{scan, algebraTestScan(entity)},
				Data:     &Union{Output: scan.Symbols()},
			},
			want: "union branch",
		},
		{
			name: "lateral binding arity mismatch",
			root: &Node{
				Op:       RuleLateralJoin,
				Children: []*Node{scan},
				Data: &LateralJoin{
					CorrelationVars: []query.Symbol{entity},
					InnerQuery: &query.Query{
						Find: []query.FindElement{query.FindVariable{Symbol: score}},
					},
					Binding: query.TupleBinding{Variables: []query.Symbol{score, missing}},
					Output:  []query.Symbol{entity, score, missing},
				},
			},
			want: "lateral binding arity",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := Analyze(testCase.root)
			require.Error(t, err)
			require.Contains(t, err.Error(), testCase.want)
		})
	}
}

func TestAnalyzeTracksFreeRequirementsFromEnvironment(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	input := datalog.NewSymbol("?input")
	result := datalog.NewSymbol("?result")

	mapped := &Node{
		Op:       RuleMap,
		Children: []*Node{algebraTestScan(entity)},
		Data: &Map{
			Required: []query.Symbol{input},
			Output:   []query.Symbol{entity, result},
		},
	}
	mapAnalysis, err := Analyze(mapped)
	require.NoError(t, err)
	require.Equal(t, []query.Symbol{input}, mapAnalysis[mapped].Required)

	correlated := &Node{
		Op: RuleLateralJoin,
		Data: &LateralJoin{
			CorrelationVars: []query.Symbol{entity},
			InnerQuery: &query.Query{
				Find: []query.FindElement{query.FindVariable{Symbol: result}},
			},
			Binding: query.TupleBinding{Variables: []query.Symbol{result}},
			Output:  []query.Symbol{entity, result},
		},
	}
	lateralAnalysis, err := Analyze(correlated)
	require.NoError(t, err)
	require.Equal(t, []query.Symbol{entity}, lateralAnalysis[correlated].Required)

	relationBound := &Node{
		Op: RuleLateralJoin,
		Data: &LateralJoin{
			InnerQuery: &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: entity},
					query.FindVariable{Symbol: result},
				},
			},
			Binding: query.RelationBinding{Variables: []query.Symbol{entity, result}},
			Output:  []query.Symbol{entity, result},
		},
	}
	relationAnalysis, err := Analyze(relationBound)
	require.NoError(t, err)
	require.Empty(t, relationAnalysis[relationBound].Required)

	aggregated := &Node{
		Op:       RuleAggregate,
		Children: []*Node{algebraTestScan(result)},
		Data: &Aggregate{
			GroupBy:   []query.Symbol{input},
			Functions: []query.FindAggregate{{Function: "count", Arg: result}},
			Bindings:  []query.Symbol{input, datalog.NewSymbol("(count ?result)")},
			Output:    []query.Symbol{input, datalog.NewSymbol("(count ?result)")},
		},
	}
	aggregateAnalysis, err := Analyze(aggregated)
	require.NoError(t, err)
	require.Equal(t, []query.Symbol{input}, aggregateAnalysis[aggregated].Required)
}

func TestAnalyzeAntiJoinDoesNotTreatRightOutputAsLeftEnvironment(t *testing.T) {
	correlation := datalog.NewSymbol("?correlation")
	left := &Node{
		Op:   RuleProject,
		Data: &Project{Symbols: []query.Symbol{correlation}},
	}
	right := algebraTestScan(correlation)
	anti := &Node{
		Op:       RuleAntiJoin,
		Children: []*Node{left, right},
		Data: &AntiJoin{
			JoinSymbols: []query.Symbol{correlation},
			Output:      []query.Symbol{correlation},
		},
	}

	analysis, err := Analyze(anti)
	require.NoError(t, err)
	require.Equal(t, []query.Symbol{correlation}, analysis[anti].Required,
		"the right side of an anti-join filters left tuples but cannot satisfy the left environment")
}

func TestAnalyzeCorrelatedAntiJoinRequirements(t *testing.T) {
	goal := datalog.NewSymbol("?goal")
	goalSet := datalog.NewSymbol("?goalSet")
	termType := datalog.NewSymbol("?termType")
	left := algebraTestScan(goal, goalSet)
	rightScan := algebraTestScan(goal, termType)
	right := &Node{
		Op:       RuleSelect,
		Children: []*Node{rightScan},
		Data: &Select{
			Required: []query.Symbol{termType, goalSet},
			Output:   []query.Symbol{goal, termType},
		},
	}
	anti := &Node{
		Op:       RuleAntiJoin,
		Children: []*Node{left, right},
		Data: &AntiJoin{
			JoinSymbols:  []query.Symbol{goal, goalSet},
			Required:     []query.Symbol{goalSet},
			Output:       []query.Symbol{goal, goalSet},
			ExplicitJoin: true,
		},
	}

	analysis, err := Analyze(anti)
	require.NoError(t, err)
	require.Empty(t, analysis[anti].Required)

	unused := datalog.NewSymbol("?unused")
	invalid := *anti
	invalid.Children = []*Node{algebraTestScan(goal, goalSet, unused), right}
	invalidData := *anti.Data.(*AntiJoin)
	invalidData.JoinSymbols = append(cloneSymbols(invalidData.JoinSymbols), unused)
	invalidData.Required = []query.Symbol{unused}
	invalidData.Output = []query.Symbol{goal, goalSet, unused}
	invalid.Data = &invalidData
	_, err = Analyze(&invalid)
	require.Error(t, err)
	require.Contains(t, err.Error(), "correlation requirement ?unused")
	require.Contains(t, err.Error(), "not a free requirement of the right child")
}

func TestAnalyzeRejectsAggregateOutputWithWrongGroupPrefix(t *testing.T) {
	group := datalog.NewSymbol("?group")
	value := datalog.NewSymbol("?value")
	wrong := datalog.NewSymbol("?wrong")
	result := datalog.NewSymbol("?total")
	aggregate := &Node{
		Op:       RuleAggregate,
		Children: []*Node{algebraTestScan(group, value)},
		Data: &Aggregate{
			GroupBy:   []query.Symbol{group},
			Functions: []query.FindAggregate{{Function: "sum", Arg: value}},
			Bindings:  []query.Symbol{group, result},
			Output:    []query.Symbol{wrong, result},
		},
	}

	_, err := Analyze(aggregate)
	require.Error(t, err)
	require.Contains(t, err.Error(), "aggregate output")
}

func TestRefreshSchemasRepairsAggregateGroupPrefix(t *testing.T) {
	group := datalog.NewSymbol("?group")
	value := datalog.NewSymbol("?value")
	wrong := datalog.NewSymbol("?wrong")
	result := datalog.NewSymbol("?total")
	aggregate := &Node{
		Op:       RuleAggregate,
		Children: []*Node{algebraTestScan(group, value)},
		Data: &Aggregate{
			GroupBy:   []query.Symbol{group},
			Functions: []query.FindAggregate{{Function: "sum", Arg: value}},
			Bindings:  []query.Symbol{group, result},
			Output:    []query.Symbol{wrong, result},
		},
	}

	refreshed, err := RefreshSchemas(aggregate)
	require.NoError(t, err)
	require.Equal(t, []query.Symbol{group, result}, refreshed.Symbols())
	require.Equal(t, []query.Symbol{wrong, result}, aggregate.Symbols(),
		"schema refresh must not mutate the input aggregate")
}

func TestAnalyzeUnionNormalizesOuterKeysAndBranchLocalSymbols(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	local := datalog.NewSymbol("?local")

	correlatedBranch := algebraTestScan(entity, value, local)
	defaultBranch := &Node{
		Op:   RuleConstant,
		Data: &Constant{Symbols: []query.Symbol{value}, Values: []interface{}{int64(0)}},
	}
	union := &Node{
		Op:       RuleUnion,
		Children: []*Node{correlatedBranch, defaultBranch},
		Data: &Union{
			Output:   []query.Symbol{entity, value},
			JoinVars: []query.Symbol{entity, value},
			Required: []query.Symbol{entity},
		},
	}

	analysis, err := Analyze(union)
	require.NoError(t, err)
	require.Equal(t, []query.Symbol{entity, value}, analysis[union].Output)
	require.Equal(t, []query.Symbol{entity}, analysis[union].Required)
}

func TestDefaultPassesPreserveAnalyzedSchemaAndAreIdempotent(t *testing.T) {
	parsed, err := parser.ParseQuery(
		`[:find ?entity ?count
		  :where
		  [?entity :entity/type :entity.type/scenario]
		  (or-default
		    [(q [:find (count ?task)
		         :in $ ?scenario
		         :where [?task :task/root ?scenario]]
		        $ ?entity) [[?count]]]
		    [(ground 0) ?count])]`,
	)
	require.NoError(t, err)
	root, err := Compile(parsed)
	require.NoError(t, err)
	before := root.String()
	beforeAnalysis, err := Analyze(root)
	require.NoError(t, err)

	optimizer := NewOptimizer(DefaultPasses()...)
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	require.Equal(t, before, root.String(), "optimization must not mutate the input tree")
	afterAnalysis, err := Analyze(optimized)
	require.NoError(t, err)
	require.Equal(t, beforeAnalysis[root].Output, afterAnalysis[optimized].Output)

	optimizedAgain, err := optimizer.Optimize(optimized)
	require.NoError(t, err)
	require.Equal(t, optimized.String(), optimizedAgain.String())
}

func TestRefreshSchemasRepairsDerivedOutputsWithoutMutatingInput(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	title := datalog.NewSymbol("?title")
	count := datalog.NewSymbol("?count")
	complete := datalog.NewSymbol("?complete")
	payload := datalog.NewSymbol("?payload")

	left := &Node{
		Op:       RuleMap,
		Children: []*Node{algebraTestScan(entity, title, count)},
		Data: &Map{
			Expression: &query.Expression{
				Function: &query.GroundFunction{Value: true},
				Binding:  complete,
			},
			Output: []query.Symbol{entity, count, complete},
		},
	}
	right := algebraTestScan(entity, payload)
	root := &Node{
		Op:       RuleJoin,
		Children: []*Node{left, right},
		Data: &Join{
			Kind:        InnerJoin,
			JoinSymbols: []query.Symbol{entity},
			Output:      []query.Symbol{entity, payload},
		},
	}

	refreshed, err := RefreshSchemas(root)
	require.NoError(t, err)
	require.Equal(t,
		[]query.Symbol{entity, title, count, complete, payload},
		refreshed.Symbols(),
	)
	require.Equal(t,
		[]query.Symbol{entity, title, count, complete},
		refreshed.Children[0].Symbols(),
	)
	require.Equal(t,
		[]query.Symbol{entity, payload},
		root.Symbols(),
		"schema refresh must not mutate the pre-rewrite tree",
	)
	require.Equal(t,
		[]query.Symbol{entity, count, complete},
		root.Children[0].Symbols(),
		"schema refresh must not mutate stale child metadata",
	)
	_, err = Analyze(refreshed)
	require.NoError(t, err)
}

func algebraTestScan(symbols ...query.Symbol) *Node {
	return &Node{
		Op: RuleScan,
		Data: &Scan{
			Output: append([]query.Symbol(nil), symbols...),
		},
	}
}
