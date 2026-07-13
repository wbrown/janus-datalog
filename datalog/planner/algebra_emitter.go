package planner

import (
	"fmt"
	"sort"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/algebra"
	"github.com/wbrown/janus-datalog/datalog/query"
)

type algebraRegion struct {
	clauses  []query.Clause
	boundary []query.Symbol
	sealed   bool
}

func emitAlgebraPlan(q *query.Query, root *algebra.Node, initialBindings map[query.Symbol]bool) (*RealizedPlan, error) {
	if q == nil {
		return nil, fmt.Errorf("emit algebra plan: nil query")
	}
	if _, err := algebra.Analyze(root); err != nil {
		return nil, fmt.Errorf("emit algebra plan: %w", err)
	}

	regions, err := emitLinearRegions(root)
	if err != nil {
		return nil, fmt.Errorf("emit algebra plan: %w", err)
	}
	if len(regions) == 0 {
		return nil, fmt.Errorf("emit algebra plan: algebra tree produced no regions")
	}

	databaseInputs, externalInputs := splitPhaseInputs(q.In)
	if len(databaseInputs) == 0 {
		databaseInputs = []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}}
	}
	externalSymbols := physicalInputSymbols(externalInputs)
	if len(initialBindings) > 0 {
		var additionalSymbols []query.Symbol
		for symbol := range initialBindings {
			if !inputSpecsContainSymbol(externalInputs, symbol) {
				additionalSymbols = append(additionalSymbols, symbol)
			}
		}
		sort.Slice(additionalSymbols, func(i, j int) bool {
			return additionalSymbols[i].Compare(additionalSymbols[j]) < 0
		})
		externalSymbols = append(externalSymbols, additionalSymbols...)
	}
	var scalarSymbols []query.Symbol
	for _, input := range externalInputs {
		if scalar, ok := input.(query.ScalarInput); ok {
			scalarSymbols = append(scalarSymbols, scalar.Symbol)
		}
	}
	constantBindable := findConstantBindableScalars(scalarSymbols, q.Where)

	phases := make([]RealizedPhase, len(regions))
	for i, region := range regions {
		isLast := i == len(regions)-1
		inputs := append([]query.InputSpec(nil), databaseInputs...)
		var phaseAvailable []query.Symbol
		if i == 0 {
			phaseAvailable = append(phaseAvailable, externalSymbols...)
		} else {
			phaseAvailable = append(phaseAvailable, phases[i-1].Keep...)
			phaseAvailable = append(phaseAvailable, constantBindable...)
		}
		phaseInputs := buildInClause(phaseAvailable, constantBindable)
		inputs = append(inputs, phaseInputs[1:]...)

		var find []query.FindElement
		if isLast {
			find = appendFinalFind(q)
		} else {
			if len(region.boundary) == 0 {
				return nil, fmt.Errorf("region %d has no materialized boundary schema", i+1)
			}
			find = variableFind(region.boundary)
		}

		phaseQuery := &query.Query{
			Find:  find,
			In:    inputs,
			Where: append([]query.Clause(nil), region.clauses...),
		}
		if len(regions) == 1 &&
			len(region.clauses) == 1 &&
			len(physicalInputSymbols(inputs)) == 0 &&
			len(q.OrderBy) > 0 &&
			q.Limit != nil {
			_, isPattern := region.clauses[0].(*query.DataPattern)
			hasAggregate := false
			for _, element := range q.Find {
				if element.IsAggregate() {
					hasAggregate = true
					break
				}
			}
			if isPattern && !hasAggregate {
				phaseQuery.OrderBy = append([]query.OrderByClause(nil), q.OrderBy...)
				limit := *q.Limit
				phaseQuery.Limit = &limit
			}
		}
		provides := physicalFindSymbols(find)
		var keep []query.Symbol
		if !isLast {
			keep = append([]query.Symbol(nil), provides...)
		}
		phases[i] = RealizedPhase{
			Query:     phaseQuery,
			Available: physicalInputSymbols(inputs),
			Provides:  provides,
			Keep:      keep,
		}
	}

	plan := &RealizedPlan{Query: q, Phases: phases}
	if err := plan.Validate(); err != nil {
		return nil, fmt.Errorf("emit algebra plan: %w", err)
	}
	return plan, nil
}

func emitLinearRegions(node *algebra.Node) ([]algebraRegion, error) {
	switch node.Op {
	case algebra.RuleProject:
		if len(node.Children) != 1 {
			return nil, fmt.Errorf("Project requires one child")
		}
		regions, err := emitLinearRegions(node.Children[0])
		if err != nil {
			return nil, err
		}
		last := len(regions) - 1
		project := node.Data.(*algebra.Project)
		regions[last].boundary = append([]query.Symbol(nil), project.Symbols...)
		regions[last].sealed = true
		return regions, nil

	case algebra.RuleSelect:
		if len(node.Children) != 1 {
			return nil, fmt.Errorf("Select requires one child")
		}
		regions, err := emitLinearRegions(node.Children[0])
		if err != nil {
			return nil, err
		}
		selectData := node.Data.(*algebra.Select)
		return appendRegionClauses(regions, selectData.Predicate), nil

	case algebra.RuleMap:
		if len(node.Children) > 1 {
			return nil, fmt.Errorf("Map requires at most one child")
		}
		var regions []algebraRegion
		if len(node.Children) == 1 {
			var err error
			regions, err = emitLinearRegions(node.Children[0])
			if err != nil {
				return nil, err
			}
		}
		mapData := node.Data.(*algebra.Map)
		return appendRegionClauses(regions, mapData.Expression), nil

	case algebra.RuleJoin:
		join := node.Data.(*algebra.Join)
		if join.Kind != algebra.InnerJoin || len(node.Children) != 2 {
			return decompiledRegion(node)
		}
		driving := node.Children[0]
		nested := node.Children[1]
		regions, err := emitLinearRegions(driving)
		if err != nil {
			return nil, err
		}
		if hasIndependentBoundary(nested) {
			clause, err := nestedRelationClause(nested)
			if err != nil {
				return nil, err
			}
			return appendRegionClauses(regions, clause), nil
		}
		nestedClauses, err := algebra.Decompile(nested)
		if err != nil {
			return nil, err
		}
		return appendRegionClauses(regions, nestedClauses...), nil

	case algebra.RuleAggregate:
		if len(node.Children) != 1 {
			return nil, fmt.Errorf("Aggregate requires one child")
		}
		return emitLinearRegions(node.Children[0])

	default:
		return decompiledRegion(node)
	}
}

func hasIndependentBoundary(node *algebra.Node) bool {
	if node == nil {
		return false
	}
	if (node.Op == algebra.RuleProject && len(node.Children) == 1) || node.Op == algebra.RuleAggregate {
		return true
	}
	for _, child := range node.Children {
		if hasIndependentBoundary(child) {
			return true
		}
	}
	return false
}

func nestedRelationClause(node *algebra.Node) (*query.SubqueryPattern, error) {
	var find []query.FindElement
	var clauses []query.Clause
	var err error
	switch data := node.Data.(type) {
	case *algebra.Project:
		if len(node.Children) != 1 {
			return nil, fmt.Errorf("nested Project requires one child")
		}
		find = variableFind(data.Symbols)
		clauses, err = algebra.Decompile(node.Children[0])
	case *algebra.Aggregate:
		if len(node.Children) != 1 {
			return nil, fmt.Errorf("nested Aggregate requires one child")
		}
		find = aggregateFind(data)
		clauses, err = algebra.Decompile(node.Children[0])
	default:
		regions, regionErr := emitLinearRegions(node)
		if regionErr != nil {
			return nil, regionErr
		}
		if len(regions) < 2 {
			return nil, fmt.Errorf("independent subtree rooted at %s cannot preserve its boundary as one subquery", node.Op)
		}
		var previous query.Clause
		for i, region := range regions {
			regionClauses := append([]query.Clause(nil), region.clauses...)
			if previous != nil {
				regionClauses = append([]query.Clause{previous}, regionClauses...)
			}
			output := region.boundary
			if i == len(regions)-1 {
				output = node.Symbols()
			}
			previous = &query.SubqueryPattern{
				Query: &query.Query{
					Find:  variableFind(output),
					In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
					Where: regionClauses,
				},
				Inputs: []query.PatternElement{query.Constant{Value: datalog.SymDollar}},
				Binding: query.RelationBinding{
					Variables: append([]query.Symbol(nil), output...),
				},
			}
		}
		return previous.(*query.SubqueryPattern), nil
	}
	if err != nil {
		return nil, err
	}
	return &query.SubqueryPattern{
		Query: &query.Query{
			Find:  find,
			In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
			Where: clauses,
		},
		Inputs: []query.PatternElement{query.Constant{Value: datalog.SymDollar}},
		Binding: query.RelationBinding{
			Variables: append([]query.Symbol(nil), node.Symbols()...),
		},
	}, nil
}

func aggregateFind(aggregate *algebra.Aggregate) []query.FindElement {
	find := make([]query.FindElement, 0, len(aggregate.GroupBy)+len(aggregate.Functions))
	for _, symbol := range aggregate.GroupBy {
		find = append(find, query.FindVariable{Symbol: symbol})
	}
	for _, function := range aggregate.Functions {
		find = append(find, function)
	}
	return find
}

func decompiledRegion(node *algebra.Node) ([]algebraRegion, error) {
	clauses, err := algebra.Decompile(node)
	if err != nil {
		return nil, err
	}
	return []algebraRegion{{clauses: clauses}}, nil
}

func appendRegionClauses(regions []algebraRegion, clauses ...query.Clause) []algebraRegion {
	if len(regions) == 0 || regions[len(regions)-1].sealed {
		regions = append(regions, algebraRegion{})
	}
	last := len(regions) - 1
	regions[last].clauses = append(regions[last].clauses, clauses...)
	return regions
}

func splitPhaseInputs(inputs []query.InputSpec) (database, external []query.InputSpec) {
	for _, input := range inputs {
		if _, ok := input.(query.DatabaseInput); ok {
			database = append(database, input)
		} else {
			external = append(external, input)
		}
	}
	return database, external
}

func scalarInputs(inputs []query.InputSpec) []query.InputSpec {
	var scalars []query.InputSpec
	for _, input := range inputs {
		if scalar, ok := input.(query.ScalarInput); ok {
			scalars = append(scalars, scalar)
		}
	}
	return scalars
}

func inputSpecsContainSymbol(inputs []query.InputSpec, symbol query.Symbol) bool {
	for _, candidate := range physicalInputSymbols(inputs) {
		if candidate == symbol {
			return true
		}
	}
	return false
}

func appendFinalFind(q *query.Query) []query.FindElement {
	find := append([]query.FindElement(nil), q.Find...)
	for _, symbol := range query.RetainedSortSymbols(q) {
		find = append(find, query.FindVariable{Symbol: symbol})
	}
	return find
}

func variableFind(symbols []query.Symbol) []query.FindElement {
	find := make([]query.FindElement, len(symbols))
	for i, symbol := range symbols {
		find[i] = query.FindVariable{Symbol: symbol}
	}
	return find
}
