package planner

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Validate verifies that phase metadata describes the physical Datalog
// fragments and materialized boundaries exactly.
func (plan *RealizedPlan) Validate() error {
	if plan == nil {
		return fmt.Errorf("nil realized plan")
	}
	if plan.Query == nil {
		return fmt.Errorf("nil realized plan query")
	}

	for i := range plan.Phases {
		phase := &plan.Phases[i]
		phaseNumber := i + 1
		if phase.Query == nil {
			return fmt.Errorf("phase %d has nil query", phaseNumber)
		}

		input := physicalInputSymbols(phase.Query.In)
		if !samePhysicalSchema(phase.Available, input) {
			return fmt.Errorf("phase %d available schema %v does not match query input schema %v",
				phaseNumber, phase.Available, input)
		}

		output := physicalFindSymbols(phase.Query.Find)
		if !samePhysicalSchema(phase.Provides, output) {
			return fmt.Errorf("phase %d provides schema %v does not match query output schema %v",
				phaseNumber, phase.Provides, output)
		}

		isLast := i == len(plan.Phases)-1
		if isLast {
			if len(phase.Keep) != 0 {
				return fmt.Errorf("phase %d final phase keep must be empty, got %v", phaseNumber, phase.Keep)
			}
			continue
		}
		if !samePhysicalSchema(phase.Keep, phase.Provides) {
			return fmt.Errorf("phase %d boundary schema %v does not match provides schema %v",
				phaseNumber, phase.Keep, phase.Provides)
		}

		nextRelationInput := relationInputSymbols(plan.Phases[i+1].Query.In)
		if !samePhysicalSchema(phase.Keep, nextRelationInput) {
			return fmt.Errorf("phase %d boundary schema %v does not match previous boundary input of phase %d: %v",
				phaseNumber, phase.Keep, phaseNumber+1, nextRelationInput)
		}
	}
	return nil
}

func terminalSymbols(q *query.Query) []query.Symbol {
	var symbols []query.Symbol
	add := func(symbol query.Symbol) {
		if symbol != nil && !containsSymbol(symbols, symbol) {
			symbols = append(symbols, symbol)
		}
	}
	for _, element := range q.Find {
		switch find := element.(type) {
		case query.FindVariable:
			add(find.Symbol)
		case query.FindAggregate:
			add(find.Arg)
			add(find.Predicate)
		case query.FindPull:
			add(find.Variable)
		}
	}
	for _, symbol := range query.RetainedSortSymbols(q) {
		add(symbol)
	}
	return symbols
}

func physicalFindSymbols(find []query.FindElement) []query.Symbol {
	symbols := make([]query.Symbol, 0, len(find))
	for _, element := range find {
		switch value := element.(type) {
		case query.FindVariable:
			symbols = append(symbols, value.Symbol)
		case query.FindAggregate:
			symbols = append(symbols, datalog.NewSymbol(value.String()))
		case query.FindPull:
			symbols = append(symbols, value.Variable)
		}
	}
	return symbols
}

func physicalInputSymbols(inputs []query.InputSpec) []query.Symbol {
	var symbols []query.Symbol
	for _, input := range inputs {
		switch value := input.(type) {
		case query.ScalarInput:
			symbols = append(symbols, value.Symbol)
		case query.CollectionInput:
			symbols = append(symbols, value.Symbol)
		case query.TupleInput:
			symbols = append(symbols, value.Symbols...)
		case query.RelationInput:
			symbols = append(symbols, value.Symbols...)
		}
	}
	return symbols
}

func relationInputSymbols(inputs []query.InputSpec) []query.Symbol {
	var symbols []query.Symbol
	for _, input := range inputs {
		if relation, ok := input.(query.RelationInput); ok {
			symbols = append(symbols, relation.Symbols...)
		}
	}
	return symbols
}

func samePhysicalSchema(left, right []query.Symbol) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func containsSymbol(symbols []query.Symbol, symbol query.Symbol) bool {
	for _, candidate := range symbols {
		if candidate == symbol {
			return true
		}
	}
	return false
}
