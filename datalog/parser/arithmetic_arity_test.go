package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestParseArithmeticClojureArities(t *testing.T) {
	testCases := []struct {
		name       string
		expression string
		argCount   int
	}{
		{name: "unary subtraction", expression: "(- 5)", argCount: 1},
		{name: "unary division", expression: "(/ 4)", argCount: 1},
		{name: "variadic addition", expression: "(+ 1 2 3 4)", argCount: 4},
		{name: "variadic subtraction", expression: "(- 10 3 2)", argCount: 3},
		{name: "variadic multiplication", expression: "(* 2 3 4)", argCount: 3},
		{name: "variadic division", expression: "(/ 24 3 2)", argCount: 3},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			parsed, err := ParseQuery(
				"[:find ?result :where [" + testCase.expression + " ?result]]",
			)
			require.NoError(t, err)
			require.Len(t, parsed.Where, 1)
			expression, ok := parsed.Where[0].(*query.Expression)
			require.True(t, ok)
			arithmetic, ok := expression.Function.(*query.ArithmeticFunction)
			require.True(t, ok)
			require.Len(t, arithmetic.Args, testCase.argCount)
		})
	}
}

func TestParseArithmeticRejectsInvalidZeroArity(t *testing.T) {
	for _, expression := range []string{"(+)", "(-)", "(*)", "(/)"} {
		_, err := ParseQuery("[:find ?result :where [" + expression + " ?result]]")
		require.Error(t, err)
	}
}
