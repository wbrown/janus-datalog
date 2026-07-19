package parser

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Comparison operators are interned symbols from the parse boundary onward:
// the parser resolves each operator name once to its pre-interned symbol, and
// all downstream dispatch is pointer equality. No string-typed operator
// survives parsing.
func TestComparisonOperatorsResolveToInternedSymbols(t *testing.T) {
	cases := []struct {
		src string
		op  query.Symbol
	}{
		{`[:find ?x :where [?e :m/v ?x] [(= ?x 1)]]`, datalog.SymEQ},
		{`[:find ?x :where [?e :m/v ?x] [(< ?x 1)]]`, datalog.SymLT},
		{`[:find ?x :where [?e :m/v ?x] [(<= ?x 1)]]`, datalog.SymLTE},
		{`[:find ?x :where [?e :m/v ?x] [(> ?x 1)]]`, datalog.SymGT},
		{`[:find ?x :where [?e :m/v ?x] [(>= ?x 1)]]`, datalog.SymGTE},
	}
	for _, c := range cases {
		q, err := ParseQuery(c.src)
		if err != nil {
			t.Fatalf("%s: %v", c.src, err)
		}
		found := false
		for _, clause := range q.Where {
			if cmp, ok := clause.(*query.Comparison); ok {
				found = true
				if cmp.Op != c.op {
					t.Errorf("%s: Op = %v, want the interned %v (pointer equality)", c.src, cmp.Op, c.op)
				}
			}
		}
		if !found {
			t.Fatalf("%s: no Comparison clause parsed", c.src)
		}
	}
}

func TestNotEqualResolvesToInternedSymbol(t *testing.T) {
	q, err := ParseQuery(`[:find ?x :where [?e :m/v ?x] [(!= ?x 1)]]`)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, clause := range q.Where {
		if ne, ok := clause.(*query.NotEqualPredicate); ok {
			found = true
			if ne.Op != datalog.SymEQ {
				t.Errorf("NotEqualPredicate embeds Op = %v, want the interned = symbol", ne.Op)
			}
		}
	}
	if !found {
		t.Fatal("no NotEqualPredicate clause parsed")
	}
}

func TestChainedComparisonResolvesToInternedSymbol(t *testing.T) {
	q, err := ParseQuery(`[:find ?x :where [?e :m/v ?x] [(< 0 ?x 100)]]`)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, clause := range q.Where {
		if chained, ok := clause.(*query.ChainedComparison); ok {
			found = true
			if chained.Op != datalog.SymLT {
				t.Errorf("ChainedComparison.Op = %v, want the interned < symbol", chained.Op)
			}
		}
	}
	if !found {
		t.Fatal("no ChainedComparison clause parsed")
	}
}
