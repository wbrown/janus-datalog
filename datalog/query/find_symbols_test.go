package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// unknownFindElement is a FindElement of a kind FindSymbols does not name.
type unknownFindElement struct{}

func (unknownFindElement) String() string    { return "?unknown" }
func (unknownFindElement) IsAggregate() bool { return false }

// FindSymbols answers a :find's result symbols in written order: a variable
// as itself, an aggregate under its display symbol, and a pull under its
// entity variable, whatever order the kinds are written in. An empty :find
// answers no symbol, and an element of a kind it does not name panics.
func TestFindSymbolsAnswersAFindsResultSymbolsInWrittenOrder(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	sum := FindAggregate{Function: datalog.SymSum, Arg: value}
	count := FindAggregate{Function: datalog.SymCount, Arg: entity}
	pull := FindPull{Variable: entity, Pattern: &PullPattern{}}

	require.Equal(t,
		[]Symbol{datalog.NewSymbol("(sum ?value)"), value, entity, datalog.NewSymbol("(count ?entity)")},
		FindSymbols([]FindElement{sum, FindVariable{Symbol: value}, pull, count}),
		"each element's symbol, in the order the find writes it")
	require.Equal(t,
		[]Symbol{entity, datalog.NewSymbol("(sum ?value)"), entity},
		FindSymbols([]FindElement{FindVariable{Symbol: entity}, sum, pull}),
		"a variable and a pull of one entity each answer the entity's symbol")
	require.Empty(t, FindSymbols(nil), "an empty find answers no symbol")
	require.Panics(t, func() { FindSymbols([]FindElement{unknownFindElement{}}) },
		"an element of a kind FindSymbols does not name panics")
}
