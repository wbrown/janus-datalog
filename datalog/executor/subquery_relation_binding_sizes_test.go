package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestSubqueryRelationBinding_ScalesWithInputSize runs the same `[?e → ?age]`
// subquery shape, bound as the relation [[?age]], against input datasets of
// different sizes. Each person has exactly one age, so the result carries one
// tuple per ?e regardless of dataset size.
func TestSubqueryRelationBinding_ScalesWithInputSize(t *testing.T) {
	nameAttr := datalog.NewKeyword(":person/name")
	ageAttr := datalog.NewKeyword(":person/age")

	buildQuery := func() *query.Query {
		return &query.Query{
			Find: []query.FindElement{
				query.FindVariable{Symbol: datalog.NewSymbol("?name")},
				query.FindVariable{Symbol: datalog.NewSymbol("?age")},
			},
			Where: []query.Clause{
				&query.DataPattern{Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: nameAttr},
					query.Variable{Name: datalog.NewSymbol("?name")},
				}},
				&query.SubqueryPattern{
					Inputs: []query.PatternElement{
						query.Constant{Value: datalog.SymDollar},
						query.Variable{Name: datalog.NewSymbol("?e")},
					},
					Query: &query.Query{
						Find: []query.FindElement{
							query.FindVariable{Symbol: datalog.NewSymbol("?age")},
						},
						In: []query.InputSpec{
							query.DatabaseInput{},
							query.ScalarInput{Symbol: datalog.NewSymbol("?e")},
						},
						Where: []query.Clause{
							&query.DataPattern{Elements: []query.PatternElement{
								query.Variable{Name: datalog.NewSymbol("?e")},
								query.Constant{Value: ageAttr},
								query.Variable{Name: datalog.NewSymbol("?age")},
							}},
						},
					},
					Binding: query.RelationBinding{
						Variables: []query.Symbol{datalog.NewSymbol("?age")},
					},
				},
			},
		}
	}

	for _, numPeople := range []int{1, 2, 3, 5, 9, 10, 11, 15, 20} {
		t.Run("numPeople="+itoa(numPeople), func(t *testing.T) {
			var datoms []datalog.Datom
			for i := 0; i < numPeople; i++ {
				e := datalog.NewIdentity(tombstoneSentinel(i))
				datoms = append(datoms,
					datalog.Datom{E: e, A: nameAttr, V: tombstoneSentinel(i), Tx: datalog.ElementID{Lamport: uint64(i + 1)}},
					datalog.Datom{E: e, A: ageAttr, V: int64(20 + i), Tx: datalog.ElementID{Lamport: uint64(i + 1)}},
				)
			}

			for _, mode := range optimizerModes {
				t.Run(mode.name, func(t *testing.T) {
					got := runExecute(t, datoms, buildQuery(), mode.algebra)
					assert.Len(t, got, numPeople,
						"each per-?e subquery returns exactly 1 tuple; outer produces %d tuples, expected %d results",
						numPeople, numPeople)
				})
			}
		})
	}
}

// runExecute executes q over datoms and returns [name, age] pairs for
// comparison. algebra selects the optimizerModes mode under test — see
// docs/wip/OPTIMIZER_MODE_MATRIX.md.
func runExecute(t *testing.T, datoms []datalog.Datom, q *query.Query, algebra bool) [][2]interface{} {
	t.Helper()

	matcher := NewMemoryPatternMatcher(datoms)
	opts := planner.PlannerOptions{
		EnableAlgebraOptimizer: algebra,
	}
	exec := NewExecutorWithOptions(matcher, nil, opts)

	rel, err := exec.Execute(q)
	require.NoError(t, err)

	var out [][2]interface{}
	iter := rel.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		if len(tuple) >= 2 {
			out = append(out, [2]interface{}{tuple[0], tuple[1]})
		}
	}
	iter.Close()
	return out
}

// tombstoneSentinel returns a distinct string per index. Named
// distinctly from similar functions in other test files in this
// package so there's no identifier collision; kept intentionally
// local since this test file is the only caller.
func tombstoneSentinel(i int) string {
	return "user-" + itoa(i)
}

func itoa(i int) string {
	// Minimal local itoa so the test file doesn't pull in strconv for
	// one caller. Small-integer only; the test uses 0..20.
	if i == 0 {
		return "0"
	}
	var digits [20]byte
	n := i
	pos := len(digits)
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	for n > 0 {
		pos--
		digits[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		digits[pos] = '-'
	}
	return string(digits[pos:])
}
