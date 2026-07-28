package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The memory matcher emits raw datoms with no CRDT resolution, so a datom's
// full identity (E, A, V, Tx) is its stream's only candidate key: a pattern
// that drops part of it must restore set semantics at birth
// (patternCoversDatomIdentity; dedup in datomsToRelationWithOptions and the
// binding-driven path).

func TestPatternCoversDatomIdentity(t *testing.T) {
	eVar := query.Variable{Name: datalog.NewSymbol("?e")}
	aConst := query.Constant{Value: datalog.NewKeyword(":m/attr")}
	vVar := query.Variable{Name: datalog.NewSymbol("?v")}
	txVar := query.Variable{Name: datalog.NewSymbol("?tx")}

	require.True(t, patternCoversDatomIdentity(&query.DataPattern{
		Elements: []query.PatternElement{eVar, aConst, vVar, txVar},
	}))
	require.False(t, patternCoversDatomIdentity(&query.DataPattern{
		Elements: []query.PatternElement{eVar, aConst, vVar},
	}), "an absent Tx position drops a varying identity component")
	require.False(t, patternCoversDatomIdentity(&query.DataPattern{
		Elements: []query.PatternElement{query.Blank{}, aConst, vVar, txVar},
	}), "a wildcard drops a varying identity component")
}

func TestMemoryMatcherScanRestoresSetSemantics(t *testing.T) {
	attr := datalog.NewKeyword(":m/value")
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("m:1"), A: attr, V: "shared"},
		{E: datalog.NewIdentity("m:2"), A: attr, V: "shared"},
		{E: datalog.NewIdentity("m:3"), A: attr, V: "unique"},
	}
	matcher := NewMemoryPatternMatcher(datoms)

	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Blank{},
		query.Constant{Value: attr},
		query.Variable{Name: datalog.NewSymbol("?v")},
	}}
	rel, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)
	tuples, err := CollectTuples(rel, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{"shared"}, {"unique"}}, tuples,
		"a wildcard-E projection of raw datoms must dedup shared values")
}

func TestMemoryMatcherBoundScanIgnoresPassengerMultiplicity(t *testing.T) {
	attr := datalog.NewKeyword(":m/value")
	e1 := datalog.NewIdentity("m:1")
	datoms := []datalog.Datom{
		{E: e1, A: attr, V: "x"},
	}
	matcher := NewMemoryPatternMatcher(datoms)

	eSym := datalog.NewSymbol("?e")
	passenger := datalog.NewSymbol("?p")
	bindings := Relations{NewMaterializedRelation(
		[]query.Symbol{eSym, passenger},
		[]Tuple{{e1, int64(1)}, {e1, int64(2)}},
	)}

	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: eSym},
		query.Constant{Value: attr},
		query.Variable{Name: datalog.NewSymbol("?v")},
	}}
	rel, err := matcher.Match(query.PatternQuery(pattern), bindings)
	require.NoError(t, err)
	tuples, err := CollectTuples(rel, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{e1, "x"}}, tuples,
		"binding tuples differing only in passenger symbols must not duplicate the match")
}
