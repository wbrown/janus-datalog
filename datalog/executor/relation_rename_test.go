package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A relation renamed under a substitution yields the same tuples under the
// new symbols whatever its layout, leaves its input unchanged, carries its
// copy contract, renames its properties through the same substitution, and
// errors on a symbol the substitution names that the relation lacks
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §7).

func TestRenameAnswersTheSameTuplesUnderTheNewSymbolsWhateverTheLayout(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	subst, err := query.SubstitutionOf([]query.Symbol{x, y}, []query.Symbol{a, b})
	require.NoError(t, err)
	laidOut := []query.Symbol{y, x}
	tuples := []Tuple{{int64(1), "one"}, {int64(2), "two"}}
	kinds := map[string]func() Relation{
		"materialized": func() Relation {
			return NewMaterializedRelation(laidOut, tuples)
		},
		"streaming": func() Relation {
			return NewStreamingRelation(laidOut, NewMaterializedRelation(laidOut, tuples).Iterator())
		},
		"streaming over a reused workspace": func() Relation {
			return NewStreamingRelation(laidOut, newWorkspaceReusingIterator(len(laidOut), [][]interface{}{{int64(1), "one"}, {int64(2), "two"}}))
		},
		"lazy seq": func() Relation {
			return NewLazySeqRelation(NewTupleSeq(NewMaterializedRelation(laidOut, tuples).Iterator(), false), laidOut)
		},
		"limit": func() Relation {
			return NewLimitRelation(NewMaterializedRelation(laidOut, tuples), 2)
		},
	}
	for kind, build := range kinds {
		t.Run(kind, func(t *testing.T) {
			rel := build()
			renamed, err := rel.Rename(subst)
			require.NoError(t, err)
			require.Equal(t, []query.Symbol{b, a}, renamed.Symbols(), "each named symbol is replaced at its position and nothing moves")
			require.Equal(t, laidOut, rel.Symbols(), "the input is untouched")
			require.Equal(t, rel.RequiresCopy(), renamed.RequiresCopy(), "the copy contract is the input's")
			got, err := CollectTuples(renamed, nil)
			require.NoError(t, err)
			require.ElementsMatch(t, [][]interface{}{{int64(1), "one"}, {int64(2), "two"}}, got, "the tuples are the input's")
		})
	}
}

func TestRenameRenamesThePropertiesThroughTheSubstitution(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	subst, err := query.SubstitutionOf([]query.Symbol{x, y}, []query.Symbol{a, b})
	require.NoError(t, err)
	rel := NewMaterializedRelationWithProperties([]query.Symbol{y, x}, []Tuple{{int64(1), "one"}}, ExecutorOptions{}, RelationProperties{
		Keys:     [][]query.Symbol{{x}},
		Ordering: []query.OrderByClause{{Variable: y}},
	})

	renamed, err := rel.Rename(subst)
	require.NoError(t, err)

	props := renamed.Properties()
	require.Equal(t, [][]query.Symbol{{a}}, props.Keys, "a key names the renamed attribute")
	require.Equal(t, []query.OrderByClause{{Variable: b}}, props.Ordering, "an ordering names the renamed attribute")
}

func TestRenameErrorsOnASymbolTheRelationLacks(t *testing.T) {
	x := datalog.NewSymbol("?x")
	a := datalog.NewSymbol("?a")
	w := datalog.NewSymbol("?w")
	subst, err := query.SubstitutionOf([]query.Symbol{x}, []query.Symbol{a})
	require.NoError(t, err)
	rel := NewMaterializedRelation([]query.Symbol{w}, []Tuple{{int64(1)}})

	_, err = rel.Rename(subst)
	require.Error(t, err, "a symbol the substitution names that the relation lacks is an error")
	require.Contains(t, err.Error(), "?x")
}
