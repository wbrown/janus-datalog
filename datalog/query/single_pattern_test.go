package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

func TestQuerySingleDataPattern(t *testing.T) {
	pattern := &DataPattern{Elements: []PatternElement{
		Variable{Name: datalog.NewSymbol("?e")},
		Constant{Value: datalog.NewKeyword(":person/name")},
		Variable{Name: datalog.NewSymbol("?name")},
	}}

	got, err := (&Query{Where: []Clause{pattern}}).SingleDataPattern()
	require.NoError(t, err)
	require.Same(t, pattern, got)

	got, err = PatternQuery(pattern).SingleDataPattern()
	require.NoError(t, err)
	require.Same(t, pattern, got)

	_, err = (&Query{}).SingleDataPattern()
	require.Error(t, err)

	_, err = (&Query{Where: []Clause{pattern, pattern}}).SingleDataPattern()
	require.Error(t, err)

	_, err = (&Query{Where: []Clause{&Expression{}}}).SingleDataPattern()
	require.Error(t, err)
}
