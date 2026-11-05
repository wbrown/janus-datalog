package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// UnderCallSources reads an inner query under the sources its call passes:
// each clause's source renamed by the call's substitution, each :in source the
// source standing for it, declared once, and the call passing each under its
// own name. Its clauses are RenameSources over CallSources, the composition
// ReadSubqueryFrame and the roster read a call's clauses through. Neither
// renames the :in, which the rebuilt call declares.
func TestUnderCallSourcesRenamesTheInnerQueryToTheSourcesItsCallPasses(t *testing.T) {
	src := datalog.NewSymbol("$src")
	b := datalog.NewSymbol("$b")
	x := Variable{Name: datalog.NewSymbol("?x")}
	named := func(source Symbol) *DataPattern {
		return &DataPattern{Source: source, Elements: []PatternElement{
			x, Constant{Value: datalog.NewKeyword(":t/name")}, Blank{},
		}}
	}
	inner := &Query{
		Find:  []FindElement{FindVariable{Symbol: x.Name}},
		In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}, DatabaseInput{Name: b}},
		Where: []Clause{named(nil), named(b)},
	}
	call := &SubqueryPattern{
		Query: &Query{In: []InputSpec{
			DatabaseInput{Name: datalog.SymDollar}, DatabaseInput{Name: b}, ScalarInput{Symbol: x.Name},
		}},
		Inputs: []PatternElement{Constant{Value: src}, Constant{Value: src}, Variable{Name: datalog.NewSymbol("?e")}},
	}

	renamed, inputs := UnderCallSources(inner, CallSources(call),
		[]PatternElement{Constant{Value: src}, Constant{Value: src}})
	require.Equal(t, []InputSpec{DatabaseInput{Name: src}}, renamed.In,
		"the source the call passes at two places is declared once")
	require.Equal(t, []PatternElement{Constant{Value: src}}, inputs,
		"the call passes the source under its own name")
	require.Len(t, renamed.Where, 2)
	for _, clause := range renamed.Where {
		require.Equal(t, src, clause.(*DataPattern).Source, "every clause reads the source the call passes")
	}
}

// A call passes one argument per input position of its :in, and a tuple or a
// relation input spans one position per symbol. The rebuilt call passes, at
// each position of the rebuilt :in, the argument the call passed there: the
// source under its own name, and each symbol of the tuple and the relation
// input its argument.
func TestUnderCallSourcesPassesAnArgumentForEachInputPosition(t *testing.T) {
	src := datalog.NewSymbol("$src")
	inner := &Query{In: []InputSpec{
		DatabaseInput{Name: datalog.SymDollar},
		TupleInput{Symbols: []Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")}},
		RelationInput{Symbols: []Symbol{datalog.NewSymbol("?c"), datalog.NewSymbol("?d")}},
	}}
	args := []PatternElement{
		Constant{Value: src},
		Variable{Name: datalog.NewSymbol("?w")}, Variable{Name: datalog.NewSymbol("?x")},
		Variable{Name: datalog.NewSymbol("?y")}, Variable{Name: datalog.NewSymbol("?z")},
	}

	renamed, inputs := UnderCallSources(inner, CallSources(&SubqueryPattern{Query: inner, Inputs: args}), args)
	require.Equal(t, []InputSpec{DatabaseInput{Name: src}, inner.In[1], inner.In[2]}, renamed.In,
		"the source is declared under the name the call passes, and the tuple and the relation input keep their places")
	require.Equal(t, []PatternElement{Constant{Value: src}, args[1], args[2], args[3], args[4]}, inputs,
		"each input position of the rebuilt :in is passed the argument the call passed there")
}
