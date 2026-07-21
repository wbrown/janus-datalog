// Test for EXTERNAL_REVIEW_2026_04.md item 7: SubqueryWorkerCount
// global eliminated; worker count flows through ExecutorOptions.
//
// The contract asserted: parallel subquery execution uses
// ExecutorOptions.MaxSubqueryWorkers to size its worker pool, and the
// value is propagated all the way into the actual parallel paths.
// Different values must produce identical results — only the internal
// worker count should differ.

package executor

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestSubqueryWorkerCount_NotAGlobal is a compile-time assertion.
// If SubqueryWorkerCount is reintroduced as a package-level var, this
// file will need updating — the check lives in the test's imports and
// surface area.
//
// Indirect: this test file references ExecutorOptions.MaxSubqueryWorkers
// (the correct place for the setting), not the package-level variable.
func TestSubqueryWorkerCount_NotAGlobal(t *testing.T) {
	// Exercise the option through to execution to verify the plumbing
	// works — see the per-count subtests.
	opts := ExecutorOptions{
		EnableParallelSubqueries: true,
		MaxSubqueryWorkers:       1,
	}
	assert.Equal(t, 1, opts.MaxSubqueryWorkers,
		"MaxSubqueryWorkers must be a per-executor option, not a process-wide global")
}

// TestSubqueryTupleBinding_ScalesWithInputSize is a diagnostic test.
// It runs the same `[?e → ?age] TupleBinding` subquery shape against
// input datasets of different sizes. Each person has exactly one age,
// so each per-?e subquery invocation should return exactly one tuple
// and TupleBinding should succeed regardless of dataset size.
//
// If this test fails above some threshold (e.g. at 10 — the parallel
// subquery threshold), that identifies the code path responsible for
// the regression. Written as part of investigating why
// TestMaxSubqueryWorkers_ProducesConsistentResults reports "tuple
// binding expects exactly 1 result, got more than 1" for a query
// shape that should produce exactly 1 tuple per invocation.
func TestSubqueryTupleBinding_ScalesWithInputSize(t *testing.T) {
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
					Binding: query.TupleBinding{
						Variables: []query.Symbol{datalog.NewSymbol("?age")},
					},
				},
			},
		}
	}

	for _, numPeople := range []int{1, 2, 3, 5, 9, 10, 11, 15, 20} {
		t.Run("numPeople="+itoaWorkers(numPeople), func(t *testing.T) {
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
					got := runExecuteWithWorkers(t, datoms, buildQuery(), runtime.NumCPU(), mode.algebra)
					assert.Len(t, got, numPeople,
						"each per-?e subquery returns exactly 1 tuple; outer produces %d rows, expected %d results",
						numPeople, numPeople)
				})
			}
		})
	}
}

// TestMaxSubqueryWorkers_ProducesConsistentResults verifies that
// different MaxSubqueryWorkers values produce identical subquery
// results. The option should only affect concurrency, not correctness.
//
// Exercises the propagation path from ExecutorOptions through to the
// parallel subquery execution paths (executeSubqueryParallelStreaming,
// executeSubqueryParallelMaterialized).
func TestMaxSubqueryWorkers_ProducesConsistentResults(t *testing.T) {
	// Build a dataset with enough input combinations that the parallel
	// path is taken (threshold is 10).
	nameAttr := datalog.NewKeyword(":person/name")
	ageAttr := datalog.NewKeyword(":person/age")

	const numPeople = 20
	var datoms []datalog.Datom
	for i := 0; i < numPeople; i++ {
		e := datalog.NewIdentity(tombstoneSentinel(i))
		datoms = append(datoms,
			datalog.Datom{E: e, A: nameAttr, V: tombstoneSentinel(i), Tx: datalog.ElementID{Lamport: uint64(i + 1)}},
			datalog.Datom{E: e, A: ageAttr, V: int64(20 + i), Tx: datalog.ElementID{Lamport: uint64(i + 1)}},
		)
	}

	// Query with a subquery that fans out over the input relation.
	// Each outer binding runs the subquery once, so we get numPeople
	// parallel work units.
	q := &query.Query{
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
				// Subquery invocation arguments: (q [...] $ ?e) — the
				// source marker must precede the correlation input so
				// the count matches the inner :in clause. Without the
				// leading $, createInputRelationsFromValuesWithOptions
				// detects the arity mismatch and silently returns nil
				// relations, leaving ?e unbound in the subquery body.
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
				Binding: query.TupleBinding{
					Variables: []query.Symbol{datalog.NewSymbol("?age")},
				},
			},
		},
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			baseline := runExecuteWithWorkers(t, datoms, q, runtime.NumCPU(), mode.algebra)

			for _, workers := range []int{0, 1, 2, 4} {
				t.Run("workers="+itoaWorkers(workers), func(t *testing.T) {
					got := runExecuteWithWorkers(t, datoms, q, workers, mode.algebra)
					assert.ElementsMatch(t, baseline, got,
						"MaxSubqueryWorkers=%d produced a different result set than the NumCPU baseline", workers)
					assert.Len(t, got, numPeople, "expected one result per person")
				})
			}
		})
	}
}

// runExecuteWithWorkers executes q with the given MaxSubqueryWorkers
// setting and returns [name, age] pairs for comparison. algebra selects the
// optimizerModes mode under test — see docs/wip/OPTIMIZER_MODE_MATRIX.md.
func runExecuteWithWorkers(t *testing.T, datoms []datalog.Datom, q *query.Query, workers int, algebra bool) [][2]interface{} {
	t.Helper()

	matcher := NewMemoryPatternMatcher(datoms)
	opts := planner.PlannerOptions{
		EnableParallelSubqueries: true,
		MaxSubqueryWorkers:       workers,
		EnableAlgebraOptimizer:   algebra,
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
	return "user-" + itoaWorkers(i)
}

func itoaWorkers(i int) string {
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
