package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// An inner symbol a call writes at an :in place and at a :find place stands
// for two enclosing names, and the presentation carries both: the attribute
// is renamed to the first name and copied under each further one.
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

	subst, err = query.SubstitutionOf([]query.Symbol{k, h, k}, []query.Symbol{n, h, h})
	require.NoError(t, err)
	answer = NewMaterializedRelation([]query.Symbol{k, h}, []Tuple{{int64(1), int64(1)}, {int64(2), int64(3)}})
	presented, err = framing{subst: subst}.present(answer)
	require.NoError(t, err)
	assert.Equal(t, []query.Symbol{n, h}, presented.Symbols(), "a further name the presented answer carries unifies the two attributes, and nothing is copied")
	tuples, err = CollectTuples(presented, nil)
	require.NoError(t, err)
	assert.ElementsMatch(t, [][]interface{}{{int64(1), int64(1)}}, tuples, "the tuple whose ?k and ?h agree is kept, once")
}

// A name written at two places of a binding unifies the two outputs it
// names, as an invocation's repeated argument does: the presentation selects
// the tuples whose two outputs are equal and renames them to the one name,
// and a tuple whose outputs differ is dropped.
func TestPresentSelectsOnEqualityForANameStandingForTwoSymbols(t *testing.T) {
	e, x, y, h := datalog.NewSymbol("?e"), datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?h")
	subst, err := query.SubstitutionOf([]query.Symbol{e, x, y}, []query.Symbol{e, h, h})
	require.NoError(t, err)
	answer := NewMaterializedRelation([]query.Symbol{e, x, y}, []Tuple{{"e1", int64(1), int64(1)}, {"e2", int64(2), int64(3)}})
	presented, err := framing{subst: subst}.present(answer)
	require.NoError(t, err)
	assert.Equal(t, []query.Symbol{e, h}, presented.Symbols(), "?x renamed to ?h, ?y unified with it and dropped")
	tuples, err := CollectTuples(presented, nil)
	require.NoError(t, err)
	assert.ElementsMatch(t, [][]interface{}{{"e1", int64(1)}}, tuples, "e1's outputs agree and e2's differ")
}

// The kept branch is keyed probe-side by the subject tuple's positions of
// the key symbols and stored-side by the branch tuple's positions of the
// same symbols: a probe carrying a key's value finds the tuples stored under
// it, and a value no stored tuple carries misses.
func TestNewCachedBranchHoldsAKeyUnderItsSymbol(t *testing.T) {
	e, n, h := datalog.NewSymbol("?e"), datalog.NewSymbol("?n"), datalog.NewSymbol("?h")
	e1, h1, h2 := datalog.NewIdentity("kb:e1"), datalog.NewIdentity("kb:h1"), datalog.NewIdentity("kb:h2")
	cb := newCachedBranch([]query.Symbol{n, h}, []query.Symbol{e, n}, []query.Symbol{n})
	cb.Add(Tuple{"a", h1})
	cb.Add(Tuple{"b", h2})
	assert.Equal(t, []Tuple{{"a", h1}}, cb.Probe(Tuple{e1, "a"}), "the probe's ?n against the stored ?n")
	assert.Nil(t, cb.Probe(Tuple{e1, "c"}), "a value no stored tuple carries misses")
}
