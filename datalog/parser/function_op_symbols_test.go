package parser

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Arithmetic operators and time-extraction fields are interned symbols from
// the parse boundary onward: the parser resolves each name once to its
// pre-interned symbol, and all downstream dispatch is pointer equality. No
// string-typed operation identity survives parsing.

func parsedFunction(t *testing.T, src string) query.Function {
	t.Helper()
	q, err := ParseQuery(src)
	if err != nil {
		t.Fatalf("%s: %v", src, err)
	}
	for _, clause := range q.Where {
		if expr, ok := clause.(*query.Expression); ok {
			return expr.Function
		}
	}
	t.Fatalf("%s: no expression clause parsed", src)
	return nil
}

func TestArithmeticOperatorsResolveToInternedSymbols(t *testing.T) {
	cases := []struct {
		src string
		op  query.Symbol
	}{
		{`[:find ?y :where [?e :m/v ?x] [(+ ?x 1) ?y]]`, datalog.SymAdd},
		{`[:find ?y :where [?e :m/v ?x] [(- ?x 1) ?y]]`, datalog.SymSubtract},
		{`[:find ?y :where [?e :m/v ?x] [(* ?x 2) ?y]]`, datalog.SymMultiply},
		{`[:find ?y :where [?e :m/v ?x] [(/ ?x 2) ?y]]`, datalog.SymDivide},
	}
	for _, c := range cases {
		fn, ok := parsedFunction(t, c.src).(*query.ArithmeticFunction)
		if !ok {
			t.Fatalf("%s: expression function is not arithmetic", c.src)
		}
		if fn.Op != c.op {
			t.Errorf("%s: Op = %v, want the interned %v (pointer equality)", c.src, fn.Op, c.op)
		}
	}
}

func TestTimeExtractionFieldsResolveToInternedSymbols(t *testing.T) {
	cases := []struct {
		src   string
		field query.Symbol
	}{
		{`[:find ?y :where [?e :m/t ?t] [(year ?t) ?y]]`, datalog.SymYear},
		{`[:find ?y :where [?e :m/t ?t] [(month ?t) ?y]]`, datalog.SymMonth},
		{`[:find ?y :where [?e :m/t ?t] [(day ?t) ?y]]`, datalog.SymDay},
		{`[:find ?y :where [?e :m/t ?t] [(hour ?t) ?y]]`, datalog.SymHour},
		{`[:find ?y :where [?e :m/t ?t] [(minute ?t) ?y]]`, datalog.SymMinute},
		{`[:find ?y :where [?e :m/t ?t] [(second ?t) ?y]]`, datalog.SymSecond},
	}
	for _, c := range cases {
		fn, ok := parsedFunction(t, c.src).(*query.TimeExtractionFunction)
		if !ok {
			t.Fatalf("%s: expression function is not time extraction", c.src)
		}
		if fn.Field != c.field {
			t.Errorf("%s: Field = %v, want the interned %v (pointer equality)", c.src, fn.Field, c.field)
		}
	}
}
