package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// An expression reading no symbol of the relation it extends, a constant
// argument or one the environment binds, has one value over the relation, so
// it is evaluated once however many tuples the relation holds. An expression
// reading the relation's symbol is evaluated on each tuple.
func TestAnExpressionReadingNoSymbolOfItsRelationIsEvaluatedOnce(t *testing.T) {
	calls := 0
	RegisterCustomFunction("test/counted", func(args []interface{}) (interface{}, error) {
		calls++
		return args[0], nil
	})
	a, k := datalog.NewSymbol("?a"), datalog.NewSymbol("?k")
	relation := func() Relation {
		return NewMaterializedRelation([]query.Symbol{a}, []Tuple{{int64(1)}, {int64(2)}, {int64(3)}})
	}
	expression := func(text string) *query.Expression {
		q, err := parser.ParseQuery(text)
		require.NoError(t, err)
		for _, clause := range q.Where {
			if expr, ok := clause.(*query.Expression); ok {
				return expr
			}
		}
		require.FailNow(t, "the query holds no expression", text)
		return nil
	}
	env := NewMaterializedRelation([]query.Symbol{k}, []Tuple{{int64(7)}})

	for _, c := range []struct {
		name  string
		expr  *query.Expression
		env   Relation
		calls int
	}{
		{"a constant argument", expression(`[:find ?c :where [(test/counted 7) ?c]]`), nil, 1},
		{"an argument the environment binds", expression(`[:find ?c :in $ ?k :where [(test/counted ?k) ?c]]`), env, 1},
		{"an argument the relation binds", expression(`[:find ?c :where [?x :t/a ?a] [(test/counted ?a) ?c]]`), nil, 3},
	} {
		t.Run(c.name, func(t *testing.T) {
			calls = 0
			tuples, err := CollectTuples(evaluateExpressionWithLookup(relation(), c.expr, nil, c.env), nil)
			require.NoError(t, err)
			require.Len(t, tuples, 3)
			require.Equal(t, c.calls, calls)
		})
	}
}
