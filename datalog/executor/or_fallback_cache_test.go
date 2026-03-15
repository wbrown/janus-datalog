package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestIsUncorrelatedBranch(t *testing.T) {
	t.Run("uncorrelated_constant_dollar", func(t *testing.T) {
		branch := []query.Clause{
			&query.SubqueryPattern{
				Inputs: []query.PatternElement{
					query.Constant{Value: datalog.SymDollar},
				},
			},
		}
		assert.True(t, isUncorrelatedBranch(branch))
	})

	t.Run("correlated_variable_input", func(t *testing.T) {
		branch := []query.Clause{
			&query.SubqueryPattern{
				Inputs: []query.PatternElement{
					query.Constant{Value: datalog.SymDollar},
					query.Variable{Name: datalog.NewSymbol("?e")},
				},
			},
		}
		assert.False(t, isUncorrelatedBranch(branch))
	})

	t.Run("ground_only_branch", func(t *testing.T) {
		branch := []query.Clause{
			&query.Expression{
				Function: &query.GroundFunction{Value: 0},
				Binding:  datalog.NewSymbol("?count"),
			},
		}
		assert.False(t, isUncorrelatedBranch(branch),
			"ground-only branches have no SubqueryPattern")
	})

	t.Run("no_clauses", func(t *testing.T) {
		assert.False(t, isUncorrelatedBranch(nil))
	})
}

func TestBuildCachedBranch(t *testing.T) {
	e1 := datalog.NewIdentity("entity:1")
	e2 := datalog.NewIdentity("entity:2")
	e3 := datalog.NewIdentity("entity:3")

	symE := datalog.NewSymbol("?e")
	symCount := datalog.NewSymbol("?count")

	// Branch result: 3 groups from a decorrelated subquery
	branchResult := NewMaterializedRelation(
		[]query.Symbol{symE, symCount},
		[]Tuple{
			{e1, int64(10)},
			{e2, int64(20)},
			{e3, int64(30)},
		},
	)

	outerSyms := []query.Symbol{symE}

	t.Run("build_and_probe", func(t *testing.T) {
		cb := buildCachedBranch(branchResult, outerSyms, nil)
		require.NotNil(t, cb, "should build cache when shared symbols exist")

		// Probe for entity:1
		matches := cb.probe(Tuple{e1})
		require.Len(t, matches, 1)
		assert.Equal(t, e1, matches[0][0])
		assert.Equal(t, int64(10), matches[0][1])

		// Probe for entity:2
		matches = cb.probe(Tuple{e2})
		require.Len(t, matches, 1)
		assert.Equal(t, int64(20), matches[0][1])

		// Probe for entity:3
		matches = cb.probe(Tuple{e3})
		require.Len(t, matches, 1)
		assert.Equal(t, int64(30), matches[0][1])

		// Probe for non-existent entity
		matches = cb.probe(Tuple{datalog.NewIdentity("entity:999")})
		assert.Nil(t, matches)
	})

	t.Run("no_shared_symbols", func(t *testing.T) {
		unrelatedSyms := []query.Symbol{datalog.NewSymbol("?other")}
		cb := buildCachedBranch(branchResult, unrelatedSyms, nil)
		assert.Nil(t, cb, "no cache when no shared symbols")
	})

	t.Run("multiple_matches_per_key", func(t *testing.T) {
		// Two items for entity:1
		multiResult := NewMaterializedRelation(
			[]query.Symbol{symE, symCount},
			[]Tuple{
				{e1, int64(10)},
				{e1, int64(11)},
				{e2, int64(20)},
			},
		)

		cb := buildCachedBranch(multiResult, outerSyms, nil)
		require.NotNil(t, cb)

		matches := cb.probe(Tuple{e1})
		assert.Len(t, matches, 2, "should return both matches for entity:1")

		matches = cb.probe(Tuple{e2})
		assert.Len(t, matches, 1)
	})

	t.Run("probe_with_different_identity_instance", func(t *testing.T) {
		// The outer tuple's Identity and the branch result's Identity
		// may be different Go objects representing the same entity.
		// The cache must match on value equality, not pointer equality.
		e1copy := datalog.NewIdentity("entity:1") // different Go pointer, same hash
		cb := buildCachedBranch(branchResult, outerSyms, nil)
		require.NotNil(t, cb)

		matches := cb.probe(Tuple{e1copy})
		require.Len(t, matches, 1, "should match on Identity value, not pointer")
		assert.Equal(t, int64(10), matches[0][1])
	})

	t.Run("outer_tuple_with_extra_symbols", func(t *testing.T) {
		// Outer tuple has [?e, ?name] but cache only keys on ?e
		symName := datalog.NewSymbol("?name")
		outerWithExtra := []query.Symbol{symE, symName}

		cb := buildCachedBranch(branchResult, outerWithExtra, nil)
		require.NotNil(t, cb)

		// Probe with full outer tuple
		matches := cb.probe(Tuple{e1, "Alice"})
		require.Len(t, matches, 1)
		assert.Equal(t, int64(10), matches[0][1])
	})

	t.Run("decorrelated_subquery_result_shape", func(t *testing.T) {
		// Simulate the exact shape produced by a decorrelated or-join:
		// The SubqueryPattern binding maps inner ?s → outer ?project and
		// inner aggregates → outer binding vars.
		// outerSyms comes from the full outer relation (many columns).
		symProject := datalog.NewSymbol("?project")
		symLabel := datalog.NewSymbol("?label")
		symCreatedAt := datalog.NewSymbol("?createdAt")
		symItemCount := datalog.NewSymbol("?itemCount")

		p1 := datalog.NewIdentity("project:1")
		p2 := datalog.NewIdentity("project:2")

		// Branch result from decorrelated SubqueryPattern: [?project, ?itemCount]
		decorrelatedResult := NewMaterializedRelation(
			[]query.Symbol{symProject, symItemCount},
			[]Tuple{
				{p1, int64(5)},
				{p2, int64(3)},
			},
		)

		// Outer relation has many symbols, ?project is first
		outerSyms := []query.Symbol{symProject, symLabel, symCreatedAt}

		cb := buildCachedBranch(decorrelatedResult, outerSyms, nil)
		require.NotNil(t, cb)

		// Probe with outer tuple for project:1
		matches := cb.probe(Tuple{p1, "Project 1", "2026-01-01"})
		require.Len(t, matches, 1, "should find project:1 in cache")
		assert.Equal(t, int64(5), matches[0][1])

		// Probe with outer tuple for project:2
		matches = cb.probe(Tuple{p2, "Project 2", "2026-01-02"})
		require.Len(t, matches, 1, "should find project:2 in cache")
		assert.Equal(t, int64(3), matches[0][1])

		// Probe with non-existent project
		p3 := datalog.NewIdentity("project:3")
		matches = cb.probe(Tuple{p3, "Project 3", "2026-01-03"})
		assert.Nil(t, matches, "should return nil for missing project")
	})
}
