package executor_test

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// runOrPropertyDifferential checks random fallbacks and unions against the
// reference evaluation, each case's source a memory database holding its
// datoms.
func runOrPropertyDifferential(t *testing.T, seed int64) {
	const cases = 500
	random := rand.New(rand.NewSource(seed))
	entity := datalog.NewSymbol("?entity")
	vector := datalog.NewSymbol("?vector")
	index := datalog.NewSymbol("?index")
	value := datalog.NewSymbol("?value")
	tagAttribute := datalog.NewKeyword(":item/tag")
	outerSymbols := []query.Symbol{entity, vector}

	for caseIndex := 0; caseIndex < cases; caseIndex++ {
		tupleCount := 1 + random.Intn(8)
		outerTuples := make([]Tuple, tupleCount)
		var datoms []datalog.Datom
		for tupleIndex := range outerTuples {
			id := datalog.NewIdentity(fmt.Sprintf("random-or:%d:%d", caseIndex, tupleIndex))
			vectorLength := 1 + random.Intn(4)
			vectorValue := make([]interface{}, vectorLength)
			for i := range vectorValue {
				vectorValue[i] = int64(random.Intn(3))
			}
			outerTuples[tupleIndex] = Tuple{id, vectorValue}

			tagCount := random.Intn(4)
			for tagIndex := 0; tagIndex < tagCount; tagIndex++ {
				datoms = append(datoms, datalog.Datom{
					E:  id,
					A:  tagAttribute,
					V:  fmt.Sprintf("tag-%d", tagIndex),
					Tx: datalog.ElementID{Lamport: uint64(tagIndex + 1), ReplicaID: 1},
				})
			}
		}
		ordering := []query.OrderByClause{{Variable: entity, Descending: false}}
		sort.Slice(outerTuples, func(i, j int) bool {
			return CompareTuplesByOrder(outerTuples[i], outerTuples[j], ordering, []int{0}) < 0
		})
		outer := NewMaterializedRelationWithProperties(
			outerSymbols,
			outerTuples,
			ExecutorOptions{},
			RelationProperties{
				Ordering: ordering,
				Keys:     [][]query.Symbol{{entity}},
			},
		)
		queryExecutor := NewQueryExecutor(
			factsDatabase(t, datoms),
			nil,
			ExecutorOptions{},
		)

		branchCount := 1 + random.Intn(4)
		branches := make([][]query.Clause, branchCount)
		for branchIndex := range branches {
			switch random.Intn(3) {
			case 0:
				branches[branchIndex] = []query.Clause{&query.Expression{
					Function: query.GroundFunction{Value: int64(random.Intn(3))},
					Binding:  value,
				}}
			case 1:
				branches[branchIndex] = []query.Clause{&query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: entity},
						query.Constant{Value: tagAttribute},
						query.Variable{Name: value},
					},
				}}
			case 2:
				branches[branchIndex] = []query.Clause{&query.Expression{
					Function: query.EnumerateFunction{
						VecTerm: query.VariableTerm{Symbol: vector},
					},
					Binding: query.RelationBinding{Variables: []query.Symbol{index, value}},
				}}
			}
		}
		shortCircuit := random.Intn(2) == 0
		relation, err := NewOrFallbackRelation(
			queryExecutor,
			NewContext(),
			[]query.Symbol{entity, vector},
			[]query.Symbol{value},
			branches,
			outer,
			ExecutorOptions{},
			shortCircuit,
		)
		require.NoError(t, err, "case %d", caseIndex)

		actual, err := CollectTypedTuples(relation)
		require.NoError(t, err, "case %d", caseIndex)
		expected := evaluateOrReference(
			branches,
			outerSymbols,
			outerTuples,
			relation.Symbols(),
			shortCircuit,
			datoms,
		)
		// A relation is a set, so the result holds the reference's tuples and
		// as many. The declared ordering ties the tuples of one entity, so
		// their order is no fact of the relation. AssertRelationPropertiesHold
		// checks the declared keys and ordering.
		require.Len(t, actual, len(expected), "case %d: expected %v, got %v", caseIndex, expected, actual)
		for _, tuple := range expected {
			require.True(t, ContainsTuplePairwise(actual, tuple),
				"case %d: expected %v, got %v", caseIndex, expected, actual)
		}
		AssertRelationPropertiesHold(t, relation, actual, caseIndex)
	}
}

func evaluateOrReference(
	branches [][]query.Clause,
	outerSymbols []query.Symbol,
	outerTuples []Tuple,
	outputSymbols []query.Symbol,
	shortCircuit bool,
	datoms []datalog.Datom,
) []Tuple {
	result := make([]Tuple, 0)
	for _, outerTuple := range outerTuples {
		for _, branch := range branches {
			branchSymbols, branchTuples := evaluateGeneratedOrBranch(
				branch,
				outerSymbols,
				outerTuple,
				datoms,
			)
			for _, branchTuple := range branchTuples {
				projected := projectGeneratedTuple(branchTuple, branchSymbols, outputSymbols)
				if !ContainsTuplePairwise(result, projected) {
					result = append(result, projected)
				}
			}
			if shortCircuit && len(branchTuples) > 0 {
				break
			}
		}
	}
	return result
}

func evaluateGeneratedOrBranch(
	branch []query.Clause,
	outerSymbols []query.Symbol,
	outerTuple Tuple,
	datoms []datalog.Datom,
) ([]query.Symbol, []Tuple) {
	if len(branch) != 1 {
		panic("generated OR branch must contain exactly one clause")
	}
	switch clause := branch[0].(type) {
	case *query.Expression:
		switch function := clause.Function.(type) {
		case query.GroundFunction:
			binding := clause.Binding.(query.Symbol)
			symbols := append(append([]query.Symbol(nil), outerSymbols...), binding)
			tuple := append(CopyTuple(outerTuple), function.Value)
			return symbols, []Tuple{tuple}
		case query.EnumerateFunction:
			binding := clause.Binding.(query.RelationBinding)
			symbols := append(append([]query.Symbol(nil), outerSymbols...), binding.Variables...)
			vectorValue := outerTuple[1].([]interface{})
			tuples := make([]Tuple, len(vectorValue))
			for i, value := range vectorValue {
				tuples[i] = append(CopyTuple(outerTuple), int64(i), value)
			}
			return symbols, tuples
		default:
			panic(fmt.Sprintf("unsupported generated expression %T", function))
		}
	case *query.DataPattern:
		entity := outerTuple[0].(datalog.Identity)
		attribute := clause.GetA().(query.Constant).Value.(datalog.Keyword)
		valueSymbol := clause.GetV().(query.Variable).Name
		symbols := append(append([]query.Symbol(nil), outerSymbols...), valueSymbol)
		var tuples []Tuple
		for _, datom := range datoms {
			if datom.E.Equal(entity) && datom.A.Equal(attribute) {
				tuples = append(tuples, append(CopyTuple(outerTuple), datom.V))
			}
		}
		return symbols, tuples
	default:
		panic(fmt.Sprintf("unsupported generated OR clause %T", clause))
	}
}

func projectGeneratedTuple(
	tuple Tuple,
	sourceSymbols []query.Symbol,
	outputSymbols []query.Symbol,
) Tuple {
	positions := make(map[query.Symbol]int, len(sourceSymbols))
	for i, symbol := range sourceSymbols {
		positions[symbol] = i
	}
	result := make(Tuple, len(outputSymbols))
	for i, symbol := range outputSymbols {
		result[i] = tuple[positions[symbol]]
	}
	return result
}
