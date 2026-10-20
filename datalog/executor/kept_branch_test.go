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

// An inner symbol a call writes at an :in place and at a :find place stands
// for two enclosing names, and the presentation carries both: the attribute
// is renamed to the first name and copied under each further one, and a
// collection member's attribute is kept under its own name beside the copy.
func TestPresentCopiesAnAttributeUnderEachFurtherName(t *testing.T) {
	k, h, n, kk := datalog.NewSymbol("?k"), datalog.NewSymbol("?h"), datalog.NewSymbol("?n"), datalog.NewSymbol("?kk")
	subst, err := query.SubstitutionOf([]query.Symbol{k, h, k}, []query.Symbol{n, h, kk})
	require.NoError(t, err)
	answer := NewMaterializedRelation([]query.Symbol{k, h}, []Tuple{{int64(1), "p"}, {int64(2), "q"}})
	presented, err := framing{subst: subst}.present(answer)
	require.NoError(t, err)
	assert.Equal(t, []query.Symbol{n, h, kk}, presented.Symbols(), "?k renamed to ?n and copied under ?kk")
	tuples, err := CollectTuples(presented, nil)
	require.NoError(t, err)
	assert.ElementsMatch(t, [][]interface{}{{int64(1), "p", int64(1)}, {int64(2), "q", int64(2)}}, tuples)

	m, ks, mm := datalog.NewSymbol("?m"), datalog.NewSymbol("?ks"), datalog.NewSymbol("?mm")
	subst, err = query.SubstitutionOf([]query.Symbol{m, h, m}, []query.Symbol{ks, h, mm})
	require.NoError(t, err)
	answer = NewMaterializedRelation([]query.Symbol{m, h}, []Tuple{{"a", "p"}, {"b", "q"}})
	presented, err = framing{subst: subst, members: []query.Symbol{m}}.present(answer)
	require.NoError(t, err)
	assert.Equal(t, []query.Symbol{m, h, mm}, presented.Symbols(), "the member kept under its own name and copied under ?mm")
	tuples, err = CollectTuples(presented, nil)
	require.NoError(t, err)
	assert.ElementsMatch(t, [][]interface{}{{"a", "p", "a"}, {"b", "q", "b"}}, tuples)

	subst, err = query.SubstitutionOf([]query.Symbol{k, h, k}, []query.Symbol{n, h, h})
	require.NoError(t, err)
	answer = NewMaterializedRelation([]query.Symbol{k, h}, []Tuple{{int64(1), "p"}})
	_, err = framing{subst: subst}.present(answer)
	require.Error(t, err, "a further name the presented answer carries would name two attributes, a selection on their equality a presentation never performs")
	assert.Contains(t, err.Error(), "?h")
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
