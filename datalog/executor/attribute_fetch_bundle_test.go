package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

type iteratorCountingRelation struct {
	Relation
	iteratorCalls int
}

func (r *iteratorCountingRelation) Iterator() Iterator {
	r.iteratorCalls++
	return r.Relation.Iterator()
}

func TestAttributeFetchBundleTraversesInputOnce(t *testing.T) {
	entity := datalog.NewSymbol("?e")
	kind := datalog.NewSymbol("?kind")
	code := datalog.NewSymbol("?code")
	name := datalog.NewSymbol("?name")
	codeAttr := datalog.NewKeyword(":place/code")
	nameAttr := datalog.NewKeyword(":place/name")
	e1 := datalog.NewIdentity("bundle:e1")
	e2 := datalog.NewIdentity("bundle:e2")

	matcher := NewIndexedMemoryMatcher([]datalog.Datom{
		{E: e1, A: codeAttr, V: "R1"},
		{E: e1, A: nameAttr, V: "N1"},
		{E: e2, A: codeAttr, V: "R2"},
		{E: e2, A: nameAttr, V: "N2"},
	}).WithSchema(cardinalityOneSchema(t, ":place/code", ":place/name"))
	exec := newQueryExecutor(matcher, nil, ExecutorOptions{EnableAttributeFetchFusion: true})

	inputProperties := RelationProperties{Keys: [][]query.Symbol{{entity}}}
	input := &iteratorCountingRelation{Relation: NewMaterializedRelationWithProperties(
		[]query.Symbol{entity, kind},
		[]Tuple{{e1, "room"}, {e2, "room"}},
		ExecutorOptions{},
		inputProperties,
	)}
	clauses := []query.Clause{
		&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity},
			query.Constant{Value: codeAttr},
			query.Variable{Name: code},
		}},
		&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity},
			query.Constant{Value: nameAttr},
			query.Variable{Name: name},
		}},
	}

	groups, consumed, err := exec.tryFuseAttributeFetchBundle(
		NewContext(),
		clauses,
		[]Relation{input},
	)
	require.NoError(t, err)
	require.Equal(t, 2, consumed)
	require.Equal(t, 1, input.iteratorCalls, "bundle must traverse the input relation once")
	require.Len(t, groups, 1)
	require.Equal(t, inputProperties, groups[0].Properties())

	got, err := CollectTuples(groups[0], nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{
		{e1, "room", "R1", "N1"},
		{e2, "room", "R2", "N2"},
	}, got)
}
