package executor

import (
	"math"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// One aggregation algorithm: aggregateAccumulator is the incremental
// primitive, aggregateOps is the behavior resolved once per query from the
// interned function symbol (pointer equality, never string dispatch), and
// foldAggregateValues folds a batch through them. Sums preserve integer
// typing — an all-int64 group sums to an exact int64, never a float64
// round-trip that collapses adjacent values above 2^53 — and any float input
// promotes the sum to float64. Empty groups yield nil for sum/avg/min/max and
// 0 for count. Aggregation is a producer boundary for NaN (Inf and -Inf
// inputs cancel), so finalization errors rather than emitting a non-value.

func resolveAndFold(t *testing.T, fn datalog.Symbol, values []interface{}) (interface{}, error) {
	op, err := resolveAggregate(fn)
	if err != nil {
		t.Fatalf("resolveAggregate(%s): %v", fn, err)
	}
	return foldAggregateValues(op, values)
}

func TestAggregateSumPreservesIntegers(t *testing.T) {
	v, err := resolveAndFold(t, datalog.SymSum, []interface{}{int64(1 << 53), int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := v.(int64); !ok || got != int64(1<<53)+1 {
		t.Errorf("all-int sum must be exact int64, got %T %v", v, v)
	}

	v, err = resolveAndFold(t, datalog.SymSum, []interface{}{int64(2), 0.5})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := v.(float64); !ok || got != 2.5 {
		t.Errorf("mixed sum promotes to float64, got %T %v", v, v)
	}

	v, err = resolveAndFold(t, datalog.SymSum, []interface{}{0.5, int64(2)})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := v.(float64); !ok || got != 2.5 {
		t.Errorf("float-first mixed sum promotes to float64, got %T %v", v, v)
	}
}

func TestAggregateEmptyGroupResults(t *testing.T) {
	for _, fn := range []datalog.Symbol{datalog.SymSum, datalog.SymAvg, datalog.SymMin, datalog.SymMax} {
		v, err := resolveAndFold(t, fn, nil)
		if err != nil {
			t.Fatal(err)
		}
		if v != nil {
			t.Errorf("%s of empty must be nil, got %v", fn, v)
		}
	}
	v, err := resolveAndFold(t, datalog.SymCount, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := v.(int64); !ok || got != 0 {
		t.Errorf("count of empty must be int64 0, got %T %v", v, v)
	}
}

func TestAggregateInfCancellationIsError(t *testing.T) {
	for _, fn := range []datalog.Symbol{datalog.SymSum, datalog.SymAvg} {
		_, err := resolveAndFold(t, fn, []interface{}{math.Inf(1), math.Inf(-1)})
		if err == nil {
			t.Fatalf("%s of {+Inf, -Inf} must error: the result is NaN, not a value", fn)
		}
		if !strings.Contains(err.Error(), "NaN") {
			t.Errorf("%s error should name NaN; got: %v", fn, err)
		}
	}

	// Inf alone is a value and aggregates fine.
	v, err := resolveAndFold(t, datalog.SymSum, []interface{}{math.Inf(1), 1.0})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := v.(float64); !ok || !math.IsInf(got, 1) {
		t.Errorf("sum with +Inf input is +Inf, got %T %v", v, v)
	}
}

func TestResolveAggregateUnknownSymbolErrors(t *testing.T) {
	if _, err := resolveAggregate(datalog.NewSymbol("median")); err == nil {
		t.Fatal("unknown aggregate function must resolve to a loud error, not a silent nil aggregate")
	}
}

// The fold enforces the value domain: silently skipping a non-domain input
// manufactures a nil aggregate beside healthy siblings — how a qb-built
// get-else default of int(0) became a nil sum in a 2-row group
// (docs/bugs/resolved/BUG_BASELINE_ORDEFAULT_SUBQUERY_NIL_AGGREGATE.md). count keeps
// its documented SQL semantics (counts non-nil) and is not part of this pin.
func TestAggregateFoldRejectsNonDomainValues(t *testing.T) {
	for _, fn := range []datalog.Symbol{datalog.SymSum, datalog.SymAvg} {
		if _, err := resolveAndFold(t, fn, []interface{}{0}); err == nil {
			t.Errorf("%s over a Go int must error loudly, not skip to nil", fn)
		}
		if _, err := resolveAndFold(t, fn, []interface{}{nil}); err == nil {
			t.Errorf("%s over nil must error loudly, not skip to nil", fn)
		}
		if _, err := resolveAndFold(t, fn, []interface{}{"s"}); err == nil {
			t.Errorf("%s over a string must error loudly, not skip to nil", fn)
		}
	}
	for _, fn := range []datalog.Symbol{datalog.SymMin, datalog.SymMax} {
		if _, err := resolveAndFold(t, fn, []interface{}{nil}); err == nil {
			t.Errorf("%s over nil must error loudly, not skip", fn)
		}
	}
}
