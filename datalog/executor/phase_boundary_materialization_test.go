package executor

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestMaterializePhaseBoundaryPreservesExactRelationContract(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	payload := datalog.NewSymbol("?payload")
	symbols := []query.Symbol{entity, payload}
	tuples := []Tuple{{int64(1), "alpha"}, {int64(2), "beta"}}
	properties := RelationProperties{
		Ordering: []query.OrderByClause{{Variable: entity, Descending: false}},
		Keys:     [][]query.Symbol{{entity}},
	}
	base := NewMaterializedRelationWithProperties(
		symbols,
		tuples,
		ExecutorOptions{EnableTrueStreaming: true},
		properties,
	)
	stream := NewStreamingRelationWithProperties(
		symbols,
		base.Iterator(),
		base.Options(),
		properties,
	)

	boundary := materializePhaseBoundary(stream)
	require.Equal(t, symbols, boundary.Symbols())
	require.Equal(t, properties, boundary.Properties())
	require.Equal(t, base.Options(), boundary.Options())
	actual, err := CollectTuples(boundary, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), "alpha"}, {int64(2), "beta"}}, actual)
}

func TestMaterializePhaseBoundaryPreservesIteratorAndCloseErrors(t *testing.T) {
	iterationFailure := materializePhaseBoundary(
		newFailingStream(1, Tuple{int64(1)}, Tuple{int64(2)}),
	)
	require.ErrorIs(t, driveErr(iterationFailure), errInjectedIterator)

	closeFailure := errors.New("phase boundary close failure")
	source := failingRelation{
		Relation: NewMaterializedRelation(
			testSymbols(),
			[]Tuple{{int64(1)}, {int64(2)}},
		),
		failAfter: 100,
		closeErr:  closeFailure,
	}
	require.ErrorIs(t, driveErr(materializePhaseBoundary(source)), closeFailure)
}
