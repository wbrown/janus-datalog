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

// A stuck clause is no provider of the variables it waits on
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §2.3): an expression that
// binds the variable it reads, [(+ ?w 1) ?w], waits on a variable no input
// and no other clause binds, and the refusal says so, where a variable
// another stuck clause provides is named bare.
func TestNoOrderError_AClauseIsNotItsOwnProvider(t *testing.T) {
	q, v, w := datalog.NewSymbol("?q"), datalog.NewSymbol("?v"), datalog.NewSymbol("?w")
	scanning := &DataPattern{Elements: []PatternElement{Variable{Name: q}, Constant{Value: datalog.NewKeyword(":t/a")}, Blank{}}}
	plus := func(in, out Symbol) *Expression {
		return &Expression{
			Function: ArithmeticFunction{Op: datalog.SymAdd, Args: []Term{VariableTerm{Symbol: in}, ConstantTerm{Value: int64(1)}}},
			Binding:  out,
		}
	}

	t.Run("a clause binding the variable it reads waits on a variable nothing binds", func(t *testing.T) {
		self := plus(w, w)
		err := NewNoOrderError([]Clause{self}, []Clause{scanning, self}, map[Symbol]bool{q: true}, map[Symbol]bool{}, nil)
		require.Len(t, err.Stuck, 1)
		require.Equal(t, []Symbol{w}, err.Stuck[0].Waits)
		require.Equal(t, []Symbol{w}, err.Stuck[0].Unbound, "the clause's own binding is no provider of what it reads")
		require.ErrorContains(t, err, "waits on ?w (no input or clause binds it)")
	})

	t.Run("a variable another stuck clause provides is named bare", func(t *testing.T) {
		forward, back := plus(w, v), plus(v, w)
		err := NewNoOrderError([]Clause{forward, back}, []Clause{scanning, forward, back}, map[Symbol]bool{q: true}, map[Symbol]bool{}, nil)
		require.Len(t, err.Stuck, 2)
		require.Equal(t, []Symbol{w}, err.Stuck[0].Waits)
		require.Empty(t, err.Stuck[0].Unbound, "the other stuck clause provides ?w")
		require.Equal(t, []Symbol{v}, err.Stuck[1].Waits)
		require.Empty(t, err.Stuck[1].Unbound, "the other stuck clause provides ?v")
	})
}
