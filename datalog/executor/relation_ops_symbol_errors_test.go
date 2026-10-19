package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A union over a header is the query's error when a member lacks a header
// symbol, and a combination over symbols a relation lacks is the query's
// error (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §7).

func TestUnionRelationsErrorsOnAMemberLackingAUnionSymbol(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	whole := NewMaterializedRelation([]query.Symbol{x, y}, []Tuple{{int64(1), int64(2)}})
	lacking := NewMaterializedRelation([]query.Symbol{x}, []Tuple{{int64(3)}})

	result := unionRelations([]Relation{whole, lacking}, []query.Symbol{x, y}, ExecutorOptions{})

	err := driveErr(result)
	require.Error(t, err, "a member lacking ?y is the union's error")
	require.Contains(t, err.Error(), "?y")
}

func TestGetUniqueCombinationsErrorsOnAnAbsentSymbol(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	rel := NewMaterializedRelation([]query.Symbol{x}, []Tuple{{int64(1)}})

	_, err := getUniqueCombinations(rel, []query.Symbol{x, y})

	require.Error(t, err, "a combination over ?y, which the relation lacks, is an error")
	require.Contains(t, err.Error(), "?y")
}
