package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// A stall's refusal names each remaining clause with the variables it waits
// on, says of a variable no input and no clause of the list binds that
// nothing binds it, names a variable a clause provides or an input binds
// bare, and names a clause waiting on no variable as such
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §2.3). The waiting
// pattern reads ?x, which nothing binds, ?y, which the other clause
// provides, and ?z, which an input binds; the other clause waits on nothing.
func TestNoOrderErrorNamesEachStuckClauseAndWhetherAnythingBindsItsVariables(t *testing.T) {
	e, x, y, z := datalog.NewSymbol("?e"), datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?z")
	waiting := FunctionPredicate{Fn: "=", Args: []PatternElement{Variable{Name: x}, Variable{Name: y}, Variable{Name: z}}}
	providing := &DataPattern{Elements: []PatternElement{Variable{Name: e}, Constant{Value: datalog.NewKeyword(":t/a")}, Variable{Name: y}}}
	err := NewNoOrderError([]Clause{waiting, providing}, []Clause{waiting, providing}, map[Symbol]bool{z: true}, map[Symbol]bool{z: true}, nil)
	require.Len(t, err.Stuck, 2)
	require.Equal(t, []Symbol{x, y}, err.Stuck[0].Waits, "the predicate waits on the variables no available symbol binds")
	require.Equal(t, []Symbol{x}, err.Stuck[0].Unbound, "?x is bound by no input and no clause; ?y is provided")
	require.Empty(t, err.Stuck[1].Waits, "the pattern waits on nothing")
	require.Equal(t,
		"cannot order clauses: [(= ?x ?y ?z)] waits on ?x (no input or clause binds it), ?y; [?e :t/a ?y] waits on no variable",
		err.Error())
}
