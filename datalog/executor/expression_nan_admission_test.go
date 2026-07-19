package executor

import (
	"math"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A constant-only expression (no required symbols, no groups) evaluates once
// on the executor's ground path rather than through the streaming function
// iterator. Its result crosses the same boundary into relational flow, so a
// NaN produced from ±Inf operands is rejected loudly before it can reach
// joins or comparisons.
func TestGroundExpressionProducingNaNIsError(t *testing.T) {
	r := datalog.NewSymbol("?r")
	q := &query.Query{
		Find: []query.FindElement{query.FindVariable{Symbol: r}},
		Where: []query.Clause{
			&query.Expression{
				Function: query.ArithmeticFunction{
					Op: datalog.SymSubtract,
					Args: []query.Term{
						query.ConstantTerm{Value: math.Inf(1)},
						query.ConstantTerm{Value: math.Inf(1)},
					},
				},
				Binding: r,
			},
		},
	}

	exec := NewExecutor(NewMemoryPatternMatcher(nil), nil)
	_, err := exec.Execute(q)
	if err == nil {
		t.Fatal("expected an error for a ground expression producing NaN, got none")
	}
	if !strings.Contains(err.Error(), "NaN") {
		t.Errorf("error should name NaN; got: %v", err)
	}
}
