package executor

import (
	"math"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A constant-only expression (no required symbols, no groups) stands over the
// unit relation and evaluates as its result is pulled. Its result crosses the
// boundary into relational flow there, so a NaN produced from ±Inf operands
// is rejected loudly before it can reach joins or comparisons: the query's
// failure, read where its result is driven.
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

	var matcher PatternMatcher
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
			result, err := exec.Execute(q)
			if err == nil {
				_, err = CollectTuples(result, nil)
			}
			if err == nil {
				t.Fatal("expected an error for a ground expression producing NaN, got none")
			}
			if !strings.Contains(err.Error(), "NaN") {
				t.Errorf("error should name NaN; got: %v", err)
			}
		})
	}
}
