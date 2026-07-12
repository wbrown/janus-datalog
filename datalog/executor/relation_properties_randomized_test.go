package executor

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestJoinPropertyPropagationRandomizedDifferential(t *testing.T) {
	const cases = 400
	random := rand.New(rand.NewSource(0x4a01))
	joinA := datalog.NewSymbol("?join-a")
	joinB := datalog.NewSymbol("?join-b")
	leftID := datalog.NewSymbol("?left-id")
	rightID := datalog.NewSymbol("?right-id")
	leftValue := datalog.NewSymbol("?left-value")
	rightValue := datalog.NewSymbol("?right-value")

	for caseIndex := 0; caseIndex < cases; caseIndex++ {
		joinSymbols := []query.Symbol{joinA}
		if random.Intn(2) == 0 {
			joinSymbols = append(joinSymbols, joinB)
		}
		leftUnique := random.Intn(2) == 0
		rightUnique := random.Intn(2) == 0
		leftCount := random.Intn(12)
		rightCount := random.Intn(12)
		leftSymbols := append([]query.Symbol{leftID}, joinSymbols...)
		leftSymbols = append(leftSymbols, leftValue)
		rightSymbols := append([]query.Symbol{rightID}, joinSymbols...)
		rightSymbols = append(rightSymbols, rightValue)
		leftTuples := generatedJoinTuples(
			caseIndex,
			"left",
			leftCount,
			len(joinSymbols),
			leftUnique,
			random,
		)
		rightTuples := generatedJoinTuples(
			caseIndex,
			"right",
			rightCount,
			len(joinSymbols),
			rightUnique,
			random,
		)
		leftProperties := RelationProperties{Keys: [][]query.Symbol{{leftID}}}
		if leftUnique {
			leftProperties.Keys = append(leftProperties.Keys, append([]query.Symbol(nil), joinSymbols...))
		}
		rightProperties := RelationProperties{Keys: [][]query.Symbol{{rightID}}}
		if rightUnique {
			rightProperties.Keys = append(rightProperties.Keys, append([]query.Symbol(nil), joinSymbols...))
		}
		mode := random.Intn(4)

		open := func(withProperties bool) (Relation, Relation, ExecutorOptions) {
			lp, rp := RelationProperties{}, RelationProperties{}
			if withProperties {
				lp, rp = leftProperties, rightProperties
			}
			left := generatedJoinRelation(leftSymbols, leftTuples, lp, mode == 1 || mode == 3)
			right := generatedJoinRelation(rightSymbols, rightTuples, rp, mode == 2 || mode == 3)
			return left, right, ExecutorOptions{
				EnableStreamingJoins:    mode != 0,
				EnableSymmetricHashJoin: mode == 3,
			}
		}
		execute := func(withProperties bool) Relation {
			left, right, options := open(withProperties)
			if mode == 3 {
				return SymmetricHashJoinWithOptions(left, right, joinSymbols, options)
			}
			return HashJoinWithOptions(left, right, joinSymbols, options)
		}

		specialized := execute(true)
		propertiesBeforeMaterialization := specialized.Properties().clone()
		specialized.Materialize()
		actual, err := collectTypedTuples(specialized)
		require.NoError(t, err, "case %d", caseIndex)
		require.Equal(t, propertiesBeforeMaterialization, specialized.Properties(),
			"case %d: materialization changed properties", caseIndex)
		baseline, err := collectTypedTuples(execute(false))
		require.NoError(t, err, "case %d", caseIndex)
		require.True(t, tupleSetsEqualPairwise(actual, baseline),
			"case %d mode %d: specialized=%v baseline=%v",
			caseIndex, mode, actual, baseline)
		assertRelationPropertiesHold(t, specialized, actual, caseIndex)

		projectSymbols := randomizedProjection(random, specialized.Symbols())
		projectedSpecialized, err := execute(true).Project(projectSymbols)
		require.NoError(t, err, "case %d", caseIndex)
		projectedBaseline, err := execute(false).Project(projectSymbols)
		require.NoError(t, err, "case %d", caseIndex)
		projectedActual, err := collectTypedTuples(projectedSpecialized)
		require.NoError(t, err, "case %d", caseIndex)
		projectedExpected, err := collectTypedTuples(projectedBaseline)
		require.NoError(t, err, "case %d", caseIndex)
		require.True(t, tupleSetsEqualPairwise(projectedActual, projectedExpected),
			"case %d projection %v: specialized=%v baseline=%v",
			caseIndex, projectSymbols, projectedActual, projectedExpected)
		assertRelationPropertiesHold(t, projectedSpecialized, projectedActual, caseIndex)
	}
}

func generatedJoinTuples(
	caseIndex int,
	side string,
	count int,
	joinWidth int,
	uniqueJoin bool,
	random *rand.Rand,
) []Tuple {
	tuples := make([]Tuple, count)
	for i := 0; i < count; i++ {
		tuple := make(Tuple, joinWidth+2)
		tuple[0] = fmt.Sprintf("%s-id-%d-%d", side, caseIndex, i)
		for joinIndex := 0; joinIndex < joinWidth; joinIndex++ {
			if uniqueJoin {
				if joinIndex == 0 {
					tuple[1+joinIndex] = int64(i)
				} else {
					tuple[1+joinIndex] = int64(i*3 + joinIndex)
				}
			} else {
				tuple[1+joinIndex] = int64(random.Intn(5))
			}
		}
		tuple[len(tuple)-1] = fmt.Sprintf("%s-value-%d", side, i)
		tuples[i] = tuple
	}
	return tuples
}

func generatedJoinRelation(
	symbols []query.Symbol,
	tuples []Tuple,
	properties RelationProperties,
	streaming bool,
) Relation {
	materialized := NewMaterializedRelationWithProperties(
		symbols,
		tuples,
		ExecutorOptions{},
		properties,
	)
	if !streaming {
		return materialized
	}
	return NewStreamingRelationWithProperties(
		symbols,
		materialized.Iterator(),
		ExecutorOptions{EnableTrueStreaming: true},
		properties,
	)
}

func randomizedProjection(random *rand.Rand, symbols []query.Symbol) []query.Symbol {
	count := 1 + random.Intn(len(symbols))
	permutation := random.Perm(len(symbols))
	result := make([]query.Symbol, count)
	for i := 0; i < count; i++ {
		result[i] = symbols[permutation[i]]
	}
	return result
}

func tupleSetsEqualPairwise(left, right []Tuple) bool {
	if len(left) != len(right) {
		return false
	}
	matched := make([]bool, len(right))
	for _, leftTuple := range left {
		found := false
		for i, rightTuple := range right {
			if !matched[i] && tuplesEqualPairwise(leftTuple, rightTuple, nil) {
				matched[i] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func TestUniqueBuildGuaranteeViolationPanics(t *testing.T) {
	join := datalog.NewSymbol("?join")
	value := datalog.NewSymbol("?value")
	left := NewMaterializedRelation(
		[]query.Symbol{join},
		[]Tuple{{int64(1)}, {int64(2)}, {int64(3)}},
	)
	right := NewMaterializedRelationWithProperties(
		[]query.Symbol{join, value},
		[]Tuple{{int64(1), "a"}, {int64(1), "b"}},
		ExecutorOptions{},
		RelationProperties{Keys: [][]query.Symbol{{join}}},
	)

	require.PanicsWithValue(t,
		"hash join build relation violated its candidate-key guarantee",
		func() {
			HashJoinWithOptions(left, right, []query.Symbol{join}, ExecutorOptions{})
		},
	)
}
