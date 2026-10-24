package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The subquery's answer yields, for each window tuple in turn, the one
// body's matches for it, each projected onto the result's symbols with the
// gaps filled from the window tuple, drains a tuple's matches before the
// next tuple, drops a tuple with no match, and reports the window answered
// after the last.
func TestSubqueryAnswerYieldsEachWindowTuplesMatches(t *testing.T) {
	entity, name, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?name"), datalog.NewSymbol("?value")
	e1, e2, e3 := datalog.NewIdentity("sub1"), datalog.NewIdentity("sub2"), datalog.NewIdentity("sub3")
	body := newCachedBranch([]query.Symbol{entity, value}, []query.Symbol{entity, name}, []query.Symbol{entity})
	body.Add(Tuple{e1, int64(1)})
	body.Add(Tuple{e1, int64(2)})
	body.Add(Tuple{e3, int64(3)})
	it := &subqueryIterator{
		windowedIterator: windowedIterator{
			reader:      &windowReader{window: []Tuple{{e1, "n"}, {e2, "m"}, {e3, "o"}}},
			frames:      []windowFrame{{}},
			frameOf:     []int{0},
			branchCache: map[int]*cachedBranch{0: body},
		},
		matchEmit: matchEmit{outerSyms: []query.Symbol{entity, name}, outputSyms: []query.Symbol{entity, name, value}},
	}

	require.True(t, it.answer())
	require.Equal(t, Tuple{e1, "n", int64(1)}, it.Tuple(), "the first tuple's first match, with the name filled from the tuple")
	require.True(t, it.answer())
	require.Equal(t, Tuple{e1, "n", int64(2)}, it.Tuple(), "the first tuple's second match, drained before the next tuple")
	require.True(t, it.answer())
	require.Equal(t, Tuple{e3, "o", int64(3)}, it.Tuple(), "the second tuple has no match and is dropped; the third's match")
	require.False(t, it.answer(), "the window is answered")
}

// A binding symbol the subject carries is joined: of a window tuple's
// matches, the one agreeing with the tuple's value at that symbol is
// yielded, and a tuple no match agrees with is dropped.
func TestSubqueryAnswerJoinsABindingSymbolTheSubjectCarries(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	e1 := datalog.NewIdentity("subjoin1")
	body := newCachedBranch([]query.Symbol{entity, value}, []query.Symbol{entity, value}, []query.Symbol{entity, value})
	body.Add(Tuple{e1, int64(1)})
	body.Add(Tuple{e1, int64(2)})
	it := &subqueryIterator{
		windowedIterator: windowedIterator{
			reader:      &windowReader{window: []Tuple{{e1, int64(2)}, {e1, int64(3)}}},
			frames:      []windowFrame{{}},
			frameOf:     []int{0},
			branchCache: map[int]*cachedBranch{0: body},
		},
		matchEmit: matchEmit{
			outerSyms:  []query.Symbol{entity, value},
			outputSyms: []query.Symbol{entity, value},
			carried:    []carriedOutput{{symbol: value, outerPos: 1}},
		},
	}

	require.True(t, it.answer())
	require.Equal(t, Tuple{e1, int64(2)}, it.Tuple(), "the match agreeing with the tuple's value")
	require.False(t, it.answer(), "the second tuple's value agrees with no match, and the window is answered")
}
