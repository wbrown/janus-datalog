package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A stall refuses the clauses no order places, naming for each the variables
// it waits on, and says of a variable no input and no clause binds that
// nothing binds it, where a variable a clause binds that no order places
// before the waiting clause is named bare
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §2.3). The first query's
// or-join waits on ?x, which nothing binds; the second's two or-joins each
// bind what the other requires.
func TestCreatePhasesGreedyRefusalSaysWhetherAnythingBindsAWaitedVariable(t *testing.T) {
	for _, c := range noOrderCases {
		t.Run(c.name, func(t *testing.T) {
			q, err := parser.ParseQuery(c.text)
			require.NoError(t, err)
			_, err = createPhasesGreedy(q.Where, physicalFindSymbols(q.Find), map[query.Symbol]bool{}, query.Bindings{})
			require.Error(t, err, "no order places every clause")
			require.Contains(t, err.Error(), "cannot order clauses: ")
			for _, waits := range c.waits {
				require.Contains(t, err.Error(), waits)
			}
			for _, absent := range c.absent {
				require.NotContains(t, err.Error(), absent)
			}
		})
	}
}

// noOrderCases are the two cases of a required run no order binds: a
// variable no input and no clause binds, and one a clause binds that no order
// places before the clause waiting on it. waits are the sentences the refusal
// carries and absent the ones it does not.
var noOrderCases = []struct {
	name, text    string
	waits, absent []string
}{
	{
		"nothing binds the variable",
		`[:find ?e :where (or-join [[?x] ?e] [?e :t/a ?x])]`,
		[]string{"waits on ?x (no input or clause binds it)"},
		nil,
	},
	{
		"a clause binds it that no order places first",
		`[:find ?v ?w :where (or-join [[?w] ?v] [?w :t/b ?v]) (or-join [[?v] ?w] [?v :t/c ?w])]`,
		[]string{"waits on ?w", "waits on ?v"},
		[]string{"(no input or clause binds it)"},
	},
}
