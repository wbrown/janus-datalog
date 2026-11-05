package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// patternOn builds [source ?e attr ?v]; a nil source names the default.
func patternOn(source query.Symbol, attr string) *query.DataPattern {
	return &query.DataPattern{Source: source, Elements: []query.PatternElement{
		query.Variable{Name: datalog.NewSymbol("?e")},
		query.Constant{Value: datalog.NewKeyword(attr)},
		query.Variable{Name: datalog.NewSymbol("?v")},
	}}
}

func TestSourceRouterUnknownSource(t *testing.T) {
	router := NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.SymDollar: nil,
	})
	_, err := router.Match(query.PatternQuery(patternOn(datalog.NewSymbol("$nonexistent"), ":doc/title")), nil)
	if err == nil {
		t.Fatal("a pattern on a source the router does not hold is refused, got nil")
	}
}
