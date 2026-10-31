package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A frame's invocation clause stands for the clause its frame was planned
// for, and an evaluation at either continues under the latter. A clause
// nothing stands for continues under itself.
func TestAFramesInvocationClauseContinuesUnderTheClauseItStandsFor(t *testing.T) {
	c := newRuleContinuations()
	scoped := &query.RuleInvocation{Name: datalog.NewSymbol("reach")}
	framed := &query.RuleInvocation{Name: datalog.NewSymbol("reach")}
	require.Same(t, scoped, c.clauseOf(scoped), "a clause of the scope's plan continues under itself")
	require.Same(t, framed, c.clauseOf(framed), "a frame's clause nothing stands for continues under itself")
	c.standFor(framed, scoped)
	require.Same(t, scoped, c.clauseOf(framed), "a frame's clause continues under the clause it stands for")
	require.Same(t, scoped, c.clauseOf(scoped), "the clause stood for continues under itself")
}
