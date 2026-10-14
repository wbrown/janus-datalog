package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A branch of an or unifies with the enclosing scope on every free variable
// it shares with it: a variable one branch alone reads is bound from a group
// that binds it, so the branch reads the attribute the binding names, and
// with nothing outside binding it the branch reads every attribute.
func TestOrBranchReadsAVariableTheEnclosingScopeBinds(t *testing.T) {
	tagged, untagged := datalog.NewIdentity("tagged"), datalog.NewIdentity("untagged")
	tag, n := datalog.NewKeyword(":t/tag"), datalog.NewKeyword(":t/n")
	datoms := []datalog.Datom{
		{E: tagged, A: tag, V: datalog.NewKeyword(":t/kind"), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: tagged, A: n, V: int64(5), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
		{E: untagged, A: n, V: int64(50), Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
	}
	cases := []struct {
		name string
		text string
		want []datalog.Identity
	}{
		{
			"bound outside the or",
			`[:find ?q :where [(ground :t/tag) ?a] [?q :t/n _] (or [?q ?a ?v] [?q :t/tag :t/none])]`,
			[]datalog.Identity{tagged},
		},
		{
			"bound by nothing",
			`[:find ?q :where [?q :t/n _] (or [?q ?a ?v] [?q :t/tag :t/none])]`,
			[]datalog.Identity{tagged, untagged},
		},
		{
			"bound two expressions away",
			`[:find ?q :where [(ground :t/tag) ?b] [(identity ?b) ?a] [?q :t/n _] (or [?q ?a ?v] [?q :t/tag :t/none])]`,
			[]datalog.Identity{tagged},
		},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				exec := NewExecutorWithOptions(NewMemoryPatternMatcher(datoms), nil, mode.plannerOptions())
				rel, err := exec.ExecuteWithRelations(NewContext(), mustParse(t, c.text), nil)
				require.NoError(t, err)
				var tuples []Tuple
				require.NoError(t, collectTuplesInto(&tuples, rel))
				var got []datalog.Identity
				for _, tuple := range tuples {
					got = append(got, tuple[0].(datalog.Identity))
				}
				require.ElementsMatch(t, c.want, got, "the branch's ?a reads what the enclosing scope binds")
			})
		}
	}
}

// scopeBindsABranchLocal reports a binding the union path would lose: a
// group, or the environment, binding a variable one branch alone reads. A
// binding of a variable every branch provides is carried by the union's
// output, and a branch variable nothing binds is lost to nothing.
func TestScopeBindsABranchLocal(t *testing.T) {
	q, a, v := datalog.NewSymbol("?q"), datalog.NewSymbol("?a"), datalog.NewSymbol("?v")
	branches := [][]query.Clause{
		{&query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: q}, query.Variable{Name: a}, query.Variable{Name: v}}}},
		{&query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: q}, query.Constant{Value: datalog.NewKeyword(":t/tag")}, query.Constant{Value: datalog.NewKeyword(":t/none")}}}},
	}
	binding := func(symbols ...query.Symbol) Relation {
		return NewMaterializedRelation(symbols, nil)
	}
	if !scopeBindsABranchLocal(NewContext(), branches, Relations{binding(q, a)}) {
		t.Error("a group binding ?a, which the first branch alone reads, is a binding the union path would lose")
	}
	if scopeBindsABranchLocal(NewContext(), branches, Relations{binding(q)}) {
		t.Error("a group binding ?q, which every branch provides, is carried by the union's output")
	}
	if scopeBindsABranchLocal(NewContext(), branches, nil) {
		t.Error("with nothing bound, the union path loses nothing")
	}
	if !scopeBindsABranchLocal(NewContext().WithEnvironment(binding(a)), branches, nil) {
		t.Error("the environment binding ?a, which the first branch alone reads, is a binding the union path would lose")
	}
}
