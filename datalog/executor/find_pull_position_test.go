package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A find element renders at its own position: the k-th find element naming
// a variable reads the k-th position of the result carrying it.
func TestApplyFindPulls_RendersEachPullAtItsOwnElement(t *testing.T) {
	e := datalog.NewSymbol("?e")
	name, age := datalog.NewKeyword(":t/name"), datalog.NewKeyword(":t/age")
	alice := datalog.NewIdentity("position:alice")
	matcher := NewIndexedMemoryMatcher([]datalog.Datom{
		{E: alice, A: name, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: age, V: int64(30), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	})
	namePattern, err := parser.ParsePullPattern(`[:t/name]`)
	require.NoError(t, err)
	agePattern, err := parser.ParsePullPattern(`[:t/age]`)
	require.NoError(t, err)
	pullName := query.FindPull{Variable: e, Pattern: namePattern}
	pullAge := query.FindPull{Variable: e, Pattern: agePattern}
	plain := query.FindVariable{Symbol: e}
	named := map[string]interface{}{"t/name": "Alice"}
	aged := map[string]interface{}{"t/age": int64(30)}

	cases := []struct {
		name string
		find []query.FindElement
		want []interface{}
	}{
		{"the plain variable then its pull", []query.FindElement{plain, pullName}, []interface{}{alice, named}},
		{"the pull then the plain variable", []query.FindElement{pullName, plain}, []interface{}{named, alice}},
		{"two pulls of one variable", []query.FindElement{pullName, pullAge}, []interface{}{named, aged}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// The result as the find boundary projects it: one position per
			// find element, the variable's value at each of its elements.
			projected := NewMaterializedRelation([]query.Symbol{e, e}, []Tuple{{alice, alice}})
			result, err := applyFindPulls(matcher, nil, projected, c.find)
			require.NoError(t, err)
			tuples, err := CollectTuples(result, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{c.want}, tuples)
		})
	}
}
