package query

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestConstantAndIteratedInputSymbols(t *testing.T) {
	scalar := datalog.NewSymbol("?s")
	tupA := datalog.NewSymbol("?ta")
	tupB := datalog.NewSymbol("?tb")
	coll := datalog.NewSymbol("?c")
	relA := datalog.NewSymbol("?ra")
	relB := datalog.NewSymbol("?rb")

	specs := []InputSpec{
		DatabaseInput{Name: datalog.NewSymbol("$")},
		ScalarInput{Symbol: scalar},
		TupleInput{Symbols: []Symbol{tupA, tupB}},
		CollectionInput{Symbol: coll},
		RelationInput{Symbols: []Symbol{relA, relB}},
	}

	constants := ConstantInputSymbols(specs)
	for _, sym := range []Symbol{scalar, tupA, tupB} {
		if !constants[sym] {
			t.Errorf("expected %s to be a constant input", sym)
		}
	}
	for _, sym := range []Symbol{coll, relA, relB} {
		if constants[sym] {
			t.Errorf("%s must not be a constant input", sym)
		}
	}

	iterated := IteratedInputSymbols(specs)
	for _, sym := range []Symbol{coll, relA, relB} {
		if !iterated[sym] {
			t.Errorf("expected %s to be an iterated input", sym)
		}
	}
	for _, sym := range []Symbol{scalar, tupA, tupB} {
		if iterated[sym] {
			t.Errorf("%s must not be an iterated input", sym)
		}
	}
}

func TestEffectiveOrderByDropsConstantKeys(t *testing.T) {
	status := datalog.NewSymbol("?status")
	age := datalog.NewSymbol("?age")

	q := &Query{
		Find: []FindElement{FindVariable{Symbol: datalog.NewSymbol("?name")}},
		In: []InputSpec{
			DatabaseInput{Name: datalog.NewSymbol("$")},
			ScalarInput{Symbol: status},
		},
		OrderBy: []OrderByClause{
			{Variable: status, Direction: OrderAsc},
			{Variable: age, Direction: OrderDesc},
		},
	}

	effective := EffectiveOrderBy(q)
	if len(effective) != 1 || effective[0].Variable != age {
		t.Fatalf("expected only the ?age clause to survive, got %v", effective)
	}
}

func TestRetainedSortSymbols(t *testing.T) {
	name := datalog.NewSymbol("?name")
	age := datalog.NewSymbol("?age")
	status := datalog.NewSymbol("?status")

	t.Run("non-projected where-bound key is retained", func(t *testing.T) {
		q := &Query{
			Find:    []FindElement{FindVariable{Symbol: name}},
			OrderBy: []OrderByClause{{Variable: age, Direction: OrderAsc}},
		}
		retained := RetainedSortSymbols(q)
		if len(retained) != 1 || retained[0] != age {
			t.Fatalf("expected [?age], got %v", retained)
		}
	})

	t.Run("projected key is not retained", func(t *testing.T) {
		q := &Query{
			Find:    []FindElement{FindVariable{Symbol: name}},
			OrderBy: []OrderByClause{{Variable: name, Direction: OrderAsc}},
		}
		if retained := RetainedSortSymbols(q); len(retained) != 0 {
			t.Fatalf("expected no retention, got %v", retained)
		}
	})

	t.Run("constant input key is not retained", func(t *testing.T) {
		q := &Query{
			Find:    []FindElement{FindVariable{Symbol: name}},
			In:      []InputSpec{ScalarInput{Symbol: status}},
			OrderBy: []OrderByClause{{Variable: status, Direction: OrderAsc}},
		}
		if retained := RetainedSortSymbols(q); len(retained) != 0 {
			t.Fatalf("expected no retention, got %v", retained)
		}
	})

	t.Run("aggregate queries retain nothing", func(t *testing.T) {
		q := &Query{
			Find: []FindElement{
				FindVariable{Symbol: name},
				FindAggregate{Function: datalog.SymCount, Arg: age},
			},
			OrderBy: []OrderByClause{{Variable: age, Direction: OrderAsc}},
		}
		if retained := RetainedSortSymbols(q); len(retained) != 0 {
			t.Fatalf("aggregate find must never be augmented, got %v", retained)
		}
	})

	t.Run("pull variable counts as projected", func(t *testing.T) {
		q := &Query{
			Find:    []FindElement{FindPull{Variable: name}},
			OrderBy: []OrderByClause{{Variable: name, Direction: OrderAsc}},
		}
		if retained := RetainedSortSymbols(q); len(retained) != 0 {
			t.Fatalf("expected no retention for pull variable, got %v", retained)
		}
	})

	t.Run("duplicate keys are retained once", func(t *testing.T) {
		q := &Query{
			Find: []FindElement{FindVariable{Symbol: name}},
			OrderBy: []OrderByClause{
				{Variable: age, Direction: OrderAsc},
				{Variable: age, Direction: OrderDesc},
			},
		}
		if retained := RetainedSortSymbols(q); len(retained) != 1 {
			t.Fatalf("expected [?age] once, got %v", retained)
		}
	})
}
