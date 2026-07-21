package datalog

import (
	"math"
	"testing"
)

// NaN is not a datalog value. It is rejected at every boundary where a float
// enters relational flow (writes, query inputs, expression outputs), so the
// comparators treat it as an unreachable domain violation with no paid check:
// float-float comparison is structured so valid floats take the three
// ordinary arms and only NaN can reach the trailing panic, while the
// cross-type numeric comparators state NaN-freedom as a precondition. ±Inf
// remains a value — it is self-equal and totally ordered, breaking no
// container law.

func TestCompareValuesPanicsOnNaN(t *testing.T) {
	nan := math.NaN()

	assertPanics := func(name string, left, right interface{}) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s: CompareValues must panic on NaN, a non-value", name)
			}
		}()
		CompareValues(left, right)
	}

	assertPanics("float vs NaN", 1.0, nan)
	assertPanics("NaN vs float", nan, 1.0)
	assertPanics("NaN vs NaN", nan, nan)
}

func TestCompareValuesOrdersInfinity(t *testing.T) {
	posInf := math.Inf(1)
	negInf := math.Inf(-1)

	if CompareValues(negInf, posInf) != -1 {
		t.Error("-Inf < +Inf")
	}
	if CompareValues(posInf, 1.0) != 1 {
		t.Error("+Inf > 1.0")
	}
	if CompareValues(negInf, int64(1)) != -1 {
		t.Error("-Inf < 1")
	}
	if CompareValues(posInf, posInf) != 0 {
		t.Error("+Inf == +Inf")
	}
	if !ValuesEqual(posInf, posInf) {
		t.Error("+Inf equals itself")
	}
}
