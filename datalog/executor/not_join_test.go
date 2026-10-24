package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The not-join's answer yields the window's tuples whose probe of the
// body's index returns nothing, in the window's order, drops the tuples
// with a match, and reports the window answered after the last.
func TestNotJoinAnswerYieldsTheWindowTuplesWithNoMatch(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	e1, e2, e3 := datalog.NewIdentity("anti1"), datalog.NewIdentity("anti2"), datalog.NewIdentity("anti3")
	body := newCachedBranch([]query.Symbol{entity, value}, []query.Symbol{entity}, []query.Symbol{entity})
	body.Add(Tuple{e2, int64(2)})
	it := &notJoinIterator{windowedIterator: windowedIterator{
		reader:      &windowReader{window: []Tuple{{e1}, {e2}, {e3}}},
		branchCache: map[int]*cachedBranch{0: body},
	}}

	require.True(t, it.answer())
	require.Equal(t, Tuple{e1}, it.Tuple(), "the first tuple has no match")
	require.True(t, it.answer())
	require.Equal(t, Tuple{e3}, it.Tuple(), "the second tuple matches the body and is dropped; the third has no match")
	require.False(t, it.answer(), "the window is answered")
}
