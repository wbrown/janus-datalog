package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestReadyPredicateScheduledBeforeUnrelatedScan(t *testing.T) {
	q, err := parser.ParseQuery(
		`[:find ?e ?payload
		  :where
		  [?e :item/score ?score]
		  [?e :item/payload ?payload]
		  [(> ?score 9900)]]`,
	)
	require.NoError(t, err)

	phases, err := createPhasesGreedy(
		q.Where,
		[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?payload")},
		map[query.Symbol]bool{},
	)
	require.NoError(t, err)
	require.Len(t, phases, 1)
	require.Len(t, phases[0].Clauses, 3)

	firstPattern, ok := phases[0].Clauses[0].(*query.DataPattern)
	require.True(t, ok)
	require.Equal(t, datalog.NewKeyword(":item/score"), firstPattern.GetA().(query.Constant).Value)
	require.IsType(t, &query.Comparison{}, phases[0].Clauses[1],
		"the predicate is ready after ?score is bound and must filter before another scan")
	secondPattern, ok := phases[0].Clauses[2].(*query.DataPattern)
	require.True(t, ok)
	require.Equal(t, datalog.NewKeyword(":item/payload"), secondPattern.GetA().(query.Constant).Value)
}

func TestPredicateSpanningTwoScansWaitsForBothInputs(t *testing.T) {
	q, err := parser.ParseQuery(
		`[:find ?e
		  :where
		  [?e :item/min ?min]
		  [?e :item/max ?max]
		  [(< ?min ?max)]]`,
	)
	require.NoError(t, err)

	phases, err := createPhasesGreedy(
		q.Where,
		[]query.Symbol{datalog.NewSymbol("?e")},
		map[query.Symbol]bool{},
	)
	require.NoError(t, err)
	require.Len(t, phases, 1)
	require.Len(t, phases[0].Clauses, 3)
	require.IsType(t, &query.DataPattern{}, phases[0].Clauses[0])
	require.IsType(t, &query.DataPattern{}, phases[0].Clauses[1])
	require.IsType(t, &query.Comparison{}, phases[0].Clauses[2],
		"predicate must remain after the scans that provide both required symbols")
}

func TestReadyPredicateSchedulingExhaustiveDependencyInvariant(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	score := datalog.NewSymbol("?score")
	payload := datalog.NewSymbol("?payload")
	category := datalog.NewSymbol("?category")
	adjusted := datalog.NewSymbol("?adjusted")
	minimum := datalog.NewSymbol("?minimum")
	base := []query.Clause{
		&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity},
			query.Constant{Value: datalog.NewKeyword(":item/score")},
			query.Variable{Name: score},
		}},
		&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity},
			query.Constant{Value: datalog.NewKeyword(":item/payload")},
			query.Variable{Name: payload},
		}},
		&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity},
			query.Constant{Value: datalog.NewKeyword(":item/category")},
			query.Variable{Name: category},
		}},
		&query.Expression{
			Function: query.ArithmeticFunction{
				Op: query.OpAdd,
				Args: []query.Term{
					query.VariableTerm{Symbol: score},
					query.ConstantTerm{Value: int64(1)},
					query.ConstantTerm{Value: int64(2)},
				},
			},
			Binding: adjusted,
		},
		&query.Comparison{
			Op:    query.OpGT,
			Left:  query.VariableTerm{Symbol: adjusted},
			Right: query.VariableTerm{Symbol: minimum},
		},
		&query.NotEqualPredicate{Comparison: query.Comparison{
			Op:    query.OpEQ,
			Left:  query.VariableTerm{Symbol: category},
			Right: query.ConstantTerm{Value: datalog.NewKeyword(":category/blocked")},
		}},
	}
	type dependency struct {
		requires []query.Symbol
		provides []query.Symbol
	}
	dependencies := map[query.Clause]dependency{
		base[0]: {provides: []query.Symbol{entity, score}},
		base[1]: {provides: []query.Symbol{entity, payload}},
		base[2]: {provides: []query.Symbol{entity, category}},
		base[3]: {requires: []query.Symbol{score}, provides: []query.Symbol{adjusted}},
		base[4]: {requires: []query.Symbol{adjusted, minimum}},
		base[5]: {requires: []query.Symbol{category}},
	}
	var permutations [][]query.Clause
	var permute func(int)
	clauses := append([]query.Clause(nil), base...)
	permute = func(position int) {
		if position == len(clauses) {
			permutations = append(permutations, append([]query.Clause(nil), clauses...))
			return
		}
		for i := position; i < len(clauses); i++ {
			clauses[position], clauses[i] = clauses[i], clauses[position]
			permute(position + 1)
			clauses[position], clauses[i] = clauses[i], clauses[position]
		}
	}
	permute(0)
	require.Len(t, permutations, 720)

	for caseIndex, clauses := range permutations {
		phases, err := createPhasesGreedy(
			clauses,
			[]query.Symbol{entity, payload, adjusted, category},
			map[query.Symbol]bool{minimum: true},
		)
		require.NoError(t, err, "case %d", caseIndex)

		for phaseIndex, phase := range phases {
			available := make(map[query.Symbol]bool, len(phase.Available))
			for _, symbol := range phase.Available {
				available[symbol] = true
			}
			for clauseIndex, clause := range phase.Clauses {
				dependency := dependencies[clause]
				for _, required := range dependency.requires {
					require.True(t, available[required],
						"case %d phase %d clause %d scheduled %T before %s was available",
						caseIndex, phaseIndex, clauseIndex, clause, required)
				}
				for _, provided := range dependency.provides {
					available[provided] = true
				}
			}
		}
	}
}
