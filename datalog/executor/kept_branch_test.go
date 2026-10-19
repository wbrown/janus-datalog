package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A branch's answer is presented under the enclosing names: a symbol the
// substitution names is renamed to the name standing for it, a symbol it does
// not name is dropped, and a collection member's symbol, which stands for its
// argument at the member's grain and for no enclosing name, is kept under its
// own name, so the kept branch holds the member a subject tuple's probe keys
// carry.
func TestPresentKeepsACollectionMembersSymbolUnderItsOwnName(t *testing.T) {
	m, v, x, n, c := datalog.NewSymbol("?m"), datalog.NewSymbol("?v"), datalog.NewSymbol("?x"), datalog.NewSymbol("?n"), datalog.NewSymbol("?c")
	subst, err := query.SubstitutionOf([]query.Symbol{m, v}, []query.Symbol{n, c})
	require.NoError(t, err)
	answer := NewMaterializedRelation([]query.Symbol{m, x, v}, []Tuple{{"a", int64(1), "p"}, {"b", int64(2), "q"}})
	presented, err := framing{subst: subst, members: []query.Symbol{m}}.present(answer)
	require.NoError(t, err)
	assert.Equal(t, []query.Symbol{m, c}, presented.Symbols(), "the member's symbol kept under its own name, ?x dropped, ?v renamed to ?c")
	tuples, err := CollectTuples(presented, nil)
	require.NoError(t, err)
	assert.ElementsMatch(t, [][]interface{}{{"a", "p"}, {"b", "q"}}, tuples)
}

// The kept branch is keyed probe-side by the subject tuple's positions of
// the key symbols and stored-side by the branch tuple's positions of the
// symbols they are stored under: a collection argument's key, stored under
// the member's inner symbol, is found by a probe carrying a member.
func TestNewCachedBranchHoldsAKeyUnderItsStoredSymbol(t *testing.T) {
	e, n, m, h := datalog.NewSymbol("?e"), datalog.NewSymbol("?n"), datalog.NewSymbol("?m"), datalog.NewSymbol("?h")
	e1, h1, h2 := datalog.NewIdentity("kb:e1"), datalog.NewIdentity("kb:h1"), datalog.NewIdentity("kb:h2")
	cb := newCachedBranch([]query.Symbol{m, h}, []query.Symbol{e, n}, []query.Symbol{n}, []query.Symbol{m})
	cb.Add(Tuple{"a", h1})
	cb.Add(Tuple{"b", h2})
	assert.Equal(t, []Tuple{{"a", h1}}, cb.Probe(Tuple{e1, "a"}), "the probe's ?n against the stored ?m")
	assert.Nil(t, cb.Probe(Tuple{e1, "c"}), "a member no stored tuple carries misses")
}
