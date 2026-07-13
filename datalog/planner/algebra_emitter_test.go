package planner

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/algebra"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestEmitAlgebraPlanPreservesProjectAsMaterializedBoundary(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	payload := datalog.NewSymbol("?payload")

	scoreQuery, err := parser.ParseQuery(
		`[:find ?entity ?score
		  :where [?entity :item/score ?score]
		         [(> ?score 90)]]`,
	)
	require.NoError(t, err)
	scoreTree, err := algebra.Compile(scoreQuery)
	require.NoError(t, err)

	payloadQuery, err := parser.ParseQuery(
		`[:find ?entity ?payload
		  :where [?entity :item/payload ?payload]]`,
	)
	require.NoError(t, err)
	payloadTree, err := algebra.Compile(payloadQuery)
	require.NoError(t, err)

	projected := &algebra.Node{
		Op:       algebra.RuleProject,
		Children: []*algebra.Node{scoreTree},
		Data:     &algebra.Project{Symbols: []query.Symbol{entity}},
	}
	root := &algebra.Node{
		Op:       algebra.RuleJoin,
		Children: []*algebra.Node{projected, payloadTree},
		Data: &algebra.Join{
			Kind:        algebra.InnerJoin,
			JoinSymbols: []query.Symbol{entity},
			Output:      []query.Symbol{entity, payload},
		},
	}
	original, err := parser.ParseQuery(
		`[:find ?entity ?payload
		  :where [?entity :item/score ?score]
		         [(> ?score 90)]
		         [?entity :item/payload ?payload]]`,
	)
	require.NoError(t, err)

	plan, err := emitAlgebraPlan(original, root, nil)
	require.NoError(t, err)
	require.NoError(t, plan.Validate())
	require.Len(t, plan.Phases, 2)

	first := plan.Phases[0]
	require.Empty(t, first.Available)
	require.Equal(t, []query.Symbol{entity}, first.Provides)
	require.Equal(t, []query.Symbol{entity}, first.Keep)
	require.Equal(t,
		[]query.FindElement{query.FindVariable{Symbol: entity}},
		first.Query.Find,
	)
	require.Len(t, first.Query.Where, 2)
	_, ok := first.Query.Where[0].(*query.DataPattern)
	require.True(t, ok)
	_, ok = first.Query.Where[1].(query.Predicate)
	require.True(t, ok)

	second := plan.Phases[1]
	require.Equal(t, []query.Symbol{entity}, second.Available)
	require.Equal(t, []query.Symbol{entity, payload}, second.Provides)
	require.Empty(t, second.Keep)
	require.Equal(t, original.Find, second.Query.Find)
	require.Len(t, second.Query.Where, 1)
	_, ok = second.Query.Where[0].(*query.DataPattern)
	require.True(t, ok)

	require.Equal(t,
		[]query.InputSpec{
			query.DatabaseInput{Name: datalog.SymDollar},
			query.RelationInput{Symbols: []query.Symbol{entity}},
		},
		second.Query.In,
	)
}

func TestEmitAlgebraPlanRejectsInvalidTreeBeforeLowering(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	missing := datalog.NewSymbol("?missing")
	root := &algebra.Node{
		Op:       algebra.RuleProject,
		Children: []*algebra.Node{algebraEmitterScan(entity)},
		Data:     &algebra.Project{Symbols: []query.Symbol{missing}},
	}

	_, err := emitAlgebraPlan(
		&query.Query{Find: []query.FindElement{query.FindVariable{Symbol: missing}}},
		root,
		nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "project symbol ?missing")
}

func TestEmitLinearRegionsSealsProjectBeforeFollowingClauses(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	constant := &algebra.Node{
		Op: algebra.RuleConstant,
		Data: &algebra.Constant{
			Symbols: []query.Symbol{entity, value},
			Values:  []interface{}{int64(1), "value"},
		},
	}
	project := &algebra.Node{
		Op:       algebra.RuleProject,
		Children: []*algebra.Node{constant},
		Data:     &algebra.Project{Symbols: []query.Symbol{entity}},
	}
	mapped := &algebra.Node{
		Op:       algebra.RuleMap,
		Children: []*algebra.Node{project},
		Data: &algebra.Map{
			Expression: &query.Expression{
				Function: &query.GroundFunction{Value: "next"},
				Binding:  value,
			},
			Output: []query.Symbol{entity, value},
		},
	}

	regions, err := emitLinearRegions(mapped)
	require.NoError(t, err)
	require.Len(t, regions, 2)
	require.True(t, regions[0].sealed)
	require.Equal(t, []query.Symbol{entity}, regions[0].boundary)
	require.Len(t, regions[0].clauses, 1)
	require.False(t, regions[1].sealed)
	require.Len(t, regions[1].clauses, 1)
}

func TestEmitLinearRegionsAcceptsExpressionOnlyRoot(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	expression := &query.Expression{
		Function: &query.GetElseFunction{
			Entity:  query.VariableTerm{Symbol: entity},
			Attr:    datalog.NewKeyword(":item/value"),
			Default: int64(0),
		},
		Binding: value,
	}
	root := &algebra.Node{
		Op: algebra.RuleMap,
		Data: &algebra.Map{
			Expression: expression,
			Required:   []query.Symbol{entity},
			Output:     []query.Symbol{value},
		},
	}

	regions, err := emitLinearRegions(root)
	require.NoError(t, err)
	require.Len(t, regions, 1)
	require.Equal(t, []query.Clause{expression}, regions[0].clauses)
}

func TestDecompiledRegionAndClauseAppendPreserveClauseOrder(t *testing.T) {
	x := datalog.NewSymbol("?x")
	constant := &algebra.Node{
		Op:   algebra.RuleConstant,
		Data: &algebra.Constant{Symbols: []query.Symbol{x}, Values: []interface{}{int64(1)}},
	}
	regions, err := decompiledRegion(constant)
	require.NoError(t, err)
	require.Len(t, regions, 1)
	require.Len(t, regions[0].clauses, 1)

	second := &query.Expression{
		Function: &query.GroundFunction{Value: int64(2)},
		Binding:  x,
	}
	regions = appendRegionClauses(regions, second)
	require.Len(t, regions, 1)
	require.Len(t, regions[0].clauses, 2)
	require.Same(t, second, regions[0].clauses[1])

	regions[0].sealed = true
	third := &query.Expression{
		Function: &query.GroundFunction{Value: int64(3)},
		Binding:  x,
	}
	regions = appendRegionClauses(regions, third)
	require.Len(t, regions, 2)
	require.Equal(t, []query.Clause{third}, regions[1].clauses)
}

func TestEmitterInputClassificationAndPhysicalFindConstruction(t *testing.T) {
	scalar := datalog.NewSymbol("?scalar")
	tupleA := datalog.NewSymbol("?tuple-a")
	tupleB := datalog.NewSymbol("?tuple-b")
	relation := datalog.NewSymbol("?relation")
	sortKey := datalog.NewSymbol("?sort")
	inputs := []query.InputSpec{
		query.DatabaseInput{Name: datalog.SymDollar},
		query.ScalarInput{Symbol: scalar},
		query.TupleInput{Symbols: []query.Symbol{tupleA, tupleB}},
		query.RelationInput{Symbols: []query.Symbol{relation}},
	}

	database, external := splitPhaseInputs(inputs)
	require.Equal(t, []query.InputSpec{inputs[0]}, database)
	require.Equal(t, inputs[1:], external)
	require.Equal(t, []query.InputSpec{query.ScalarInput{Symbol: scalar}}, scalarInputs(external))
	require.True(t, inputSpecsContainSymbol(external, tupleB))
	require.False(t, inputSpecsContainSymbol(external, sortKey))

	q := &query.Query{
		Find:    []query.FindElement{query.FindVariable{Symbol: relation}},
		OrderBy: []query.OrderByClause{{Variable: sortKey, Direction: query.OrderDesc}},
	}
	require.Equal(t,
		[]query.FindElement{
			query.FindVariable{Symbol: relation},
			query.FindVariable{Symbol: sortKey},
		},
		appendFinalFind(q),
	)
	require.Equal(t,
		[]query.FindElement{
			query.FindVariable{Symbol: tupleA},
			query.FindVariable{Symbol: tupleB},
		},
		variableFind([]query.Symbol{tupleA, tupleB}),
	)
}

func TestEmitAlgebraPlanPreservesProjectedJoinChildAsRelationSubquery(t *testing.T) {
	entity := datalog.NewSymbol("?entity")

	leftQuery, err := parser.ParseQuery(
		`[:find ?entity :where [?entity :item/type :item.type/scored]]`,
	)
	require.NoError(t, err)
	left, err := algebra.Compile(leftQuery)
	require.NoError(t, err)

	rightQuery, err := parser.ParseQuery(
		`[:find ?entity ?score :where [?entity :item/score ?score]]`,
	)
	require.NoError(t, err)
	rightScan, err := algebra.Compile(rightQuery)
	require.NoError(t, err)
	right := &algebra.Node{
		Op:       algebra.RuleProject,
		Children: []*algebra.Node{rightScan},
		Data:     &algebra.Project{Symbols: []query.Symbol{entity}},
	}
	root := &algebra.Node{
		Op:       algebra.RuleJoin,
		Children: []*algebra.Node{left, right},
		Data: &algebra.Join{
			Kind:        algebra.InnerJoin,
			JoinSymbols: []query.Symbol{entity},
			Output:      []query.Symbol{entity},
		},
	}
	original, err := parser.ParseQuery(
		`[:find ?entity
		  :where [?entity :item/type :item.type/scored]
		         [?entity :item/score ?score]]`,
	)
	require.NoError(t, err)

	plan, err := emitAlgebraPlan(original, root, nil)
	require.NoError(t, err)
	require.Len(t, plan.Phases, 1)
	require.Len(t, plan.Phases[0].Query.Where, 2)

	subquery, ok := plan.Phases[0].Query.Where[1].(*query.SubqueryPattern)
	require.True(t, ok, "projected independent join child must remain an independent relation")
	require.Equal(t,
		[]query.FindElement{query.FindVariable{Symbol: entity}},
		subquery.Query.Find,
	)
	require.Equal(t,
		query.RelationBinding{Variables: []query.Symbol{entity}},
		subquery.Binding,
	)
	require.Len(t, subquery.Query.Where, 1)
	_, ok = subquery.Query.Where[0].(*query.DataPattern)
	require.True(t, ok)
}

func TestEmitAlgebraPlanLowersRootAggregateWithoutNestedReaggregation(t *testing.T) {
	group := datalog.NewSymbol("?group")
	value := datalog.NewSymbol("?value")
	aggregateOutput := datalog.NewSymbol("(sum ?value)")

	childQuery, err := parser.ParseQuery(
		`[:find ?group ?value :where [?group :metric/value ?value]]`,
	)
	require.NoError(t, err)
	child, err := algebra.Compile(childQuery)
	require.NoError(t, err)
	root := &algebra.Node{
		Op:       algebra.RuleAggregate,
		Children: []*algebra.Node{child},
		Data: &algebra.Aggregate{
			GroupBy:   []query.Symbol{group},
			Functions: []query.FindAggregate{{Function: "sum", Arg: value}},
			Output:    []query.Symbol{group, aggregateOutput},
		},
	}
	original, err := parser.ParseQuery(
		`[:find ?group (sum ?value)
		  :where [?group :metric/value ?value]]`,
	)
	require.NoError(t, err)

	plan, err := emitAlgebraPlan(original, root, nil)
	require.NoError(t, err)
	require.Len(t, plan.Phases, 1)
	require.Equal(t, original.Find, plan.Phases[0].Query.Find)
	require.Len(t, plan.Phases[0].Query.Where, 1)
	_, ok := plan.Phases[0].Query.Where[0].(*query.DataPattern)
	require.True(t, ok, "root aggregate child must be the phase body, not a nested aggregate subquery")
	require.Equal(t, []query.Symbol{group, aggregateOutput}, plan.Phases[0].Provides)
}

func TestNestedRelationLoweringPreservesProjectAndAggregateFindShapes(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	aggregateOutput := datalog.NewSymbol("(max ?value)")
	scan := algebraEmitterScan(entity, value)
	scan.Data.(*algebra.Scan).Pattern = &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity},
		query.Constant{Value: datalog.NewKeyword(":metric/value")},
		query.Variable{Name: value},
	}}
	project := &algebra.Node{
		Op:       algebra.RuleProject,
		Children: []*algebra.Node{scan},
		Data:     &algebra.Project{Symbols: []query.Symbol{entity}},
	}
	aggregate := &algebra.Node{
		Op:       algebra.RuleAggregate,
		Children: []*algebra.Node{scan},
		Data: &algebra.Aggregate{
			GroupBy:   []query.Symbol{entity},
			Functions: []query.FindAggregate{{Function: "max", Arg: value}},
			Output:    []query.Symbol{entity, aggregateOutput},
		},
	}

	require.True(t, hasIndependentBoundary(project))
	require.True(t, hasIndependentBoundary(aggregate))
	require.False(t, hasIndependentBoundary(scan))
	require.False(t, hasIndependentBoundary(&algebra.Node{
		Op:   algebra.RuleProject,
		Data: &algebra.Project{Symbols: []query.Symbol{entity}},
	}), "schema placeholders are correlated inputs, not materialized boundaries")
	require.Equal(t,
		[]query.FindElement{
			query.FindVariable{Symbol: entity},
			query.FindAggregate{Function: "max", Arg: value},
		},
		aggregateFind(aggregate.Data.(*algebra.Aggregate)),
	)

	projectClause, err := nestedRelationClause(project)
	require.NoError(t, err)
	require.Equal(t,
		[]query.FindElement{query.FindVariable{Symbol: entity}},
		projectClause.Query.Find,
	)
	require.Equal(t,
		query.RelationBinding{Variables: []query.Symbol{entity}},
		projectClause.Binding,
	)

	aggregateClause, err := nestedRelationClause(aggregate)
	require.NoError(t, err)
	require.Equal(t, aggregateFind(aggregate.Data.(*algebra.Aggregate)), aggregateClause.Query.Find)
	require.Equal(t,
		query.RelationBinding{Variables: []query.Symbol{entity, aggregateOutput}},
		aggregateClause.Binding,
	)
}

func TestNestedRelationLoweringPreservesOperatorsAboveProject(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	score := datalog.NewSymbol("?score")
	parsed, err := parser.ParseQuery(
		`[:find ?entity ?score
		  :where [?entity :item/score ?score]
		         [(> ?score 90)]]`,
	)
	require.NoError(t, err)
	compiled, err := algebra.Compile(parsed)
	require.NoError(t, err)
	selectData := compiled.Data.(*algebra.Select)
	project := &algebra.Node{
		Op:       algebra.RuleProject,
		Children: compiled.Children,
		Data:     &algebra.Project{Symbols: []query.Symbol{entity, score}},
	}
	wrapped := &algebra.Node{
		Op:       algebra.RuleSelect,
		Children: []*algebra.Node{project},
		Data: &algebra.Select{
			Predicate: selectData.Predicate,
			Required:  []query.Symbol{score},
			Output:    []query.Symbol{entity, score},
		},
	}

	clause, err := nestedRelationClause(wrapped)
	require.NoError(t, err)
	require.Len(t, clause.Query.Where, 2)
	inner, ok := clause.Query.Where[0].(*query.SubqueryPattern)
	require.True(t, ok)
	require.Equal(t,
		query.RelationBinding{Variables: []query.Symbol{entity, score}},
		inner.Binding,
	)
	_, ok = clause.Query.Where[1].(query.Predicate)
	require.True(t, ok)
	require.Equal(t,
		[]query.FindElement{
			query.FindVariable{Symbol: entity},
			query.FindVariable{Symbol: score},
		},
		clause.Query.Find,
	)
}

func TestEmitAlgebraPlanLowersEveryClauseOperator(t *testing.T) {
	testCases := []struct {
		name       string
		source     string
		operator   string
		clauseType string
	}{
		{
			name:       "scan",
			source:     `[:find ?entity :where [?entity :item/type :item.type/scored]]`,
			operator:   algebra.RuleScan,
			clauseType: "*query.DataPattern",
		},
		{
			name:       "select",
			source:     `[:find ?entity :where [?entity :item/score ?score] [(> ?score 90)]]`,
			operator:   algebra.RuleSelect,
			clauseType: "*query.Comparison",
		},
		{
			name:       "map",
			source:     `[:find ?entity ?next :where [?entity :item/score ?score] [(+ ?score 1) ?next]]`,
			operator:   algebra.RuleMap,
			clauseType: "*query.Expression",
		},
		{
			name:       "join",
			source:     `[:find ?entity ?value :where [?entity :item/type :item.type/scored] [?entity :item/value ?value]]`,
			operator:   algebra.RuleJoin,
			clauseType: "*query.DataPattern",
		},
		{
			name:       "anti join",
			source:     `[:find ?entity :where [?entity :item/type :item.type/scored] (not [?entity :item/deleted true])]`,
			operator:   algebra.RuleAntiJoin,
			clauseType: "*query.NotJoinClause",
		},
		{
			name:       "union",
			source:     `[:find ?entity ?value :where (or [?entity :item/a ?value] [?entity :item/b ?value])]`,
			operator:   algebra.RuleUnion,
			clauseType: "*query.OrClause",
		},
		{
			name: "lateral join",
			source: `[:find ?entity ?count
				:where [?entity :item/type :item.type/scored]
				       [(q [:find (count ?child)
				            :in $ ?parent
				            :where [?child :item/parent ?parent]]
				           $ ?entity) [[?count]]]]`,
			operator:   algebra.RuleLateralJoin,
			clauseType: "*query.SubqueryPattern",
		},
		{
			name: "lateral union",
			source: `[:find ?entity ?value
				:where [?entity :item/type :item.type/scored]
				       (or-default [?entity :item/value ?value]
				                   [(ground 0) ?value])]`,
			operator:   algebra.RuleLateralUnion,
			clauseType: "*query.OrDefaultClause",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			parsed, err := parser.ParseQuery(testCase.source)
			require.NoError(t, err)
			root, err := algebra.Compile(parsed)
			require.NoError(t, err)
			require.True(t, algebraTreeContains(root, testCase.operator),
				"compiled tree must contain %s:\n%s", testCase.operator, root)

			plan, err := emitAlgebraPlan(parsed, root, nil)
			require.NoError(t, err)
			require.NoError(t, plan.Validate())
			require.NotEmpty(t, plan.Phases)
			require.True(t, phaseClausesContainType(plan.Phases, testCase.clauseType),
				"emitted phases must contain %s", testCase.clauseType)
		})
	}
}

func TestEmitAlgebraPlanLowersConstantAndLeftOuterJoin(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	constant := datalog.NewSymbol("?constant")

	constantRoot := &algebra.Node{
		Op:   algebra.RuleConstant,
		Data: &algebra.Constant{Symbols: []query.Symbol{constant}, Values: []interface{}{int64(7)}},
	}
	constantPlan, err := emitAlgebraPlan(
		&query.Query{Find: []query.FindElement{query.FindVariable{Symbol: constant}}},
		constantRoot,
		nil,
	)
	require.NoError(t, err)
	require.IsType(t, &query.Expression{}, constantPlan.Phases[0].Query.Where[0])

	leftQuery, err := parser.ParseQuery(
		`[:find ?entity :where [?entity :item/type :item.type/scored]]`,
	)
	require.NoError(t, err)
	left, err := algebra.Compile(leftQuery)
	require.NoError(t, err)
	rightQuery, err := parser.ParseQuery(
		`[:find ?entity ?value :where [?entity :item/value ?value]]`,
	)
	require.NoError(t, err)
	right, err := algebra.Compile(rightQuery)
	require.NoError(t, err)
	outerRoot := &algebra.Node{
		Op:       algebra.RuleJoin,
		Children: []*algebra.Node{left, right},
		Data: &algebra.Join{
			Kind:          algebra.LeftOuterJoin,
			JoinSymbols:   []query.Symbol{entity},
			Output:        []query.Symbol{entity, value},
			DefaultValues: []interface{}{int64(0)},
		},
	}
	outerQuery, err := parser.ParseQuery(
		`[:find ?entity ?value :where [?entity :item/type :item.type/scored] [?entity :item/value ?value]]`,
	)
	require.NoError(t, err)
	outerPlan, err := emitAlgebraPlan(outerQuery, outerRoot, nil)
	require.NoError(t, err)
	require.True(t, phaseClausesContainType(outerPlan.Phases, "*query.OrDefaultJoinClause"))
}

func TestClauseBasedPlannerEmitsDirectlyFromOptimizedAlgebra(t *testing.T) {
	parsed, err := parser.ParseQuery(
		`[:find ?entity ?count
		  :where [?entity :item/type :item.type/scored]
		         [(q [:find (count ?child)
		              :in $ ?parent
		              :where [?child :item/parent ?parent]]
		             $ ?entity) [[?count]]]]`,
	)
	require.NoError(t, err)

	var names []string
	handler := func(event annotations.Event) {
		names = append(names, event.Name)
	}
	plan, err := NewClauseBasedPlanner(nil, PlannerOptions{EnableAlgebraOptimizer: true}).
		Plan(parsed, handler)
	require.NoError(t, err)
	require.NoError(t, plan.Validate())
	require.Contains(t, names, "algebra/emitted")
	require.NotContains(t, names, "algebra/bridge-complete")
}

func TestClauseBasedPlannerInsertsJoinProjectBoundary(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	payload := datalog.NewSymbol("?payload")
	parsed, err := parser.ParseQuery(
		`[:find ?entity ?payload
		  :where [?entity :item/score ?score]
		         [(> ?score 90)]
		         [?entity :item/payload ?payload]]`,
	)
	require.NoError(t, err)

	plan, err := NewClauseBasedPlanner(nil, PlannerOptions{
		EnableAlgebraOptimizer:     true,
		EnableJoinProjectInsertion: true,
	}).
		Plan(parsed, nil)
	require.NoError(t, err)
	require.NoError(t, plan.Validate())
	require.Len(t, plan.Phases, 2)
	require.Equal(t, []query.Symbol{entity}, plan.Phases[0].Provides)
	require.Equal(t, []query.Symbol{entity}, plan.Phases[0].Keep)
	require.Equal(t, []query.Symbol{entity}, plan.Phases[1].Available)
	require.Equal(t, []query.Symbol{entity, payload}, plan.Phases[1].Provides)
}

func TestClauseBasedPlannerKeepsInputSymbolsLiveAcrossJoinPlanning(t *testing.T) {
	targetTeam := datalog.NewSymbol("?target-team")
	parsed, err := parser.ParseQuery(
		`[:find ?name
		  :in $ ?target-team
		  :where [?entity :person/name ?name]
		         [?entity :person/team ?target-team]]`,
	)
	require.NoError(t, err)

	plan, err := NewClauseBasedPlanner(nil, PlannerOptions{EnableAlgebraOptimizer: true}).
		Plan(parsed, nil)
	require.NoError(t, err)
	require.NoError(t, plan.Validate())
	require.Len(t, plan.Phases, 1,
		"input-bound pattern variables must not be projected into an uncorrelated subquery")
	require.Contains(t, plan.Phases[0].Available, targetTeam)
	require.Len(t, plan.Phases[0].Query.Where, 2)
	_, isPattern := plan.Phases[0].Query.Where[1].(*query.DataPattern)
	require.True(t, isPattern,
		"input-bound pattern must remain in the phase that receives its binding")
}

func algebraTreeContains(node *algebra.Node, operator string) bool {
	if node.Op == operator {
		return true
	}
	for _, child := range node.Children {
		if algebraTreeContains(child, operator) {
			return true
		}
	}
	return false
}

func phaseClausesContainType(phases []RealizedPhase, clauseType string) bool {
	for _, phase := range phases {
		for _, clause := range phase.Query.Where {
			if fmt.Sprintf("%T", clause) == clauseType {
				return true
			}
		}
	}
	return false
}

func algebraEmitterScan(symbols ...query.Symbol) *algebra.Node {
	return &algebra.Node{
		Op: algebra.RuleScan,
		Data: &algebra.Scan{
			Output: append([]query.Symbol(nil), symbols...),
		},
	}
}
