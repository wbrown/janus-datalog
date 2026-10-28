package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// closeCountingSession is a real read session that counts its closes.
type closeCountingSession struct {
	ReadSession
	closes int
}

func (s *closeCountingSession) Close() error {
	s.closes++
	return s.ReadSession.Close()
}

// A session-bound result renamed under a substitution answers the inner
// relation under the new symbols and still releases its session when the
// renamed result is exhausted
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §7).
func TestSessionBoundRelationRenameKeepsTheSessionBoundToTheRenamedResult(t *testing.T) {
	store := NewBadgerEmulatorStore(&BinaryKeyEncoder{})
	defer store.Close()
	opened, err := store.NewReadSession()
	require.NoError(t, err)
	session := &closeCountingSession{ReadSession: opened}

	x := datalog.NewSymbol("?x")
	a := datalog.NewSymbol("?a")
	inner := executor.NewMaterializedRelation([]query.Symbol{x}, []executor.Tuple{{int64(1)}, {int64(2)}})
	bound := newSessionBoundRelation(inner, session, nil)
	subst, err := query.SubstitutionOf([]query.Symbol{x}, []query.Symbol{a})
	require.NoError(t, err)

	renamed, err := bound.Rename(subst)
	require.NoError(t, err)
	require.Equal(t, []query.Symbol{a}, renamed.Symbols())

	tuples, err := executor.CollectTuples(renamed, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{int64(1)}, {int64(2)}}, tuples)
	require.Equal(t, 1, session.closes, "the session closes once, when the renamed result is exhausted")
}

func TestSessionBoundRelationRenameErrorsOnASymbolTheInnerLacks(t *testing.T) {
	store := NewBadgerEmulatorStore(&BinaryKeyEncoder{})
	defer store.Close()
	opened, err := store.NewReadSession()
	require.NoError(t, err)
	defer opened.Close()

	x := datalog.NewSymbol("?x")
	w := datalog.NewSymbol("?w")
	a := datalog.NewSymbol("?a")
	bound := newSessionBoundRelation(executor.NewMaterializedRelation([]query.Symbol{w}, []executor.Tuple{{int64(1)}}), opened, nil)
	subst, err := query.SubstitutionOf([]query.Symbol{x}, []query.Symbol{a})
	require.NoError(t, err)

	_, err = bound.Rename(subst)
	require.Error(t, err, "a symbol the substitution names that the inner relation lacks is an error")
	require.Contains(t, err.Error(), "?x")
}
