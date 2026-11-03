package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestProjectionPlan(t *testing.T) {
	symE := datalog.NewSymbol("?e")
	symV := datalog.NewSymbol("?v")
	symName := datalog.NewSymbol("?name")
	symOther := datalog.NewSymbol("?other")

	t.Run("identity_when_symbols_match", func(t *testing.T) {
		plan := newProjectionPlan(
			[]query.Symbol{symE, symV},
			[]query.Symbol{symE, symV},
			[]query.Symbol{symE, symName},
		)
		assert.True(t, plan.identity)
	})

	t.Run("order_difference_is_not_identity", func(t *testing.T) {
		plan := newProjectionPlan(
			[]query.Symbol{symV, symE},
			[]query.Symbol{symE, symV},
			nil,
		)
		require.False(t, plan.identity)
		out := plan.project(Tuple{int64(10), "e"}, nil)
		assert.Equal(t, Tuple{"e", int64(10)}, out)
	})

	t.Run("outer_fills_symbols_the_branch_lacks", func(t *testing.T) {
		// Branch produces [?e ?v]; output wants [?e ?v ?name]; ?name comes
		// from the outer tuple [?e ?name].
		plan := newProjectionPlan(
			[]query.Symbol{symE, symV},
			[]query.Symbol{symE, symV, symName},
			[]query.Symbol{symE, symName},
		)
		require.False(t, plan.identity)
		out := plan.project(Tuple{"e1", int64(10)}, Tuple{"e1", "Alice"})
		assert.Equal(t, Tuple{"e1", int64(10), "Alice"}, out)
	})

	t.Run("symbol_in_neither_source_stays_nil", func(t *testing.T) {
		plan := newProjectionPlan(
			[]query.Symbol{symE},
			[]query.Symbol{symE, symOther},
			[]query.Symbol{symName},
		)
		out := plan.project(Tuple{"e1"}, Tuple{"Alice"})
		assert.Equal(t, Tuple{"e1", nil}, out)
	})

	t.Run("short_outer_tuple_is_guarded", func(t *testing.T) {
		// The outer position exists in the symbol list but the tuple is
		// shorter — the position must be bounds-checked, not indexed blindly.
		plan := newProjectionPlan(
			[]query.Symbol{symE},
			[]query.Symbol{symE, symName},
			[]query.Symbol{symE, symName},
		)
		out := plan.project(Tuple{"e1"}, Tuple{"outer-e"})
		assert.Equal(t, Tuple{"e1", nil}, out)
	})
}

func TestProjectionForMemoizesPerBranch(t *testing.T) {
	symE := datalog.NewSymbol("?e")
	symV := datalog.NewSymbol("?v")
	symName := datalog.NewSymbol("?name")

	it := &matchEmit{
		outerSyms:  []query.Symbol{symE, symName},
		outputSyms: []query.Symbol{symE, symV, symName},
	}
	branchSyms := []query.Symbol{symE, symV}

	first := it.projectionFor(0, branchSyms)
	second := it.projectionFor(0, branchSyms)
	require.False(t, first.identity)
	assert.Equal(t, &first.branchPos[0], &second.branchPos[0],
		"repeat lookups must return the memoized plan, not a rebuild")

	// A different branch index gets its own plan.
	other := it.projectionFor(1, []query.Symbol{symE, symV, symName})
	assert.True(t, other.identity)
}
