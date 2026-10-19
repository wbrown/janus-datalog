package query

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// A substitution is what a call states about names: which symbol of a scope
// stands for which symbol of the enclosing scope, read once from the text
// where the grammar writes the two lists side by side
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.4, §7).

func TestSubstitutionOfPairsEachSymbolWithTheOneAtItsPlace(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")

	subst, err := SubstitutionOf([]Symbol{x, y}, []Symbol{a, b})
	if err != nil {
		t.Fatalf("two lists of one length pair, got error %v", err)
	}
	if got, ok := subst.Of(x); !ok || got != a {
		t.Errorf("?a stands for ?x, got %v %v", got, ok)
	}
	if got, ok := subst.Of(y); !ok || got != b {
		t.Errorf("?b stands for ?y, got %v %v", got, ok)
	}
	if _, ok := subst.Of(a); ok {
		t.Error("the substitution names ?x and ?y, not ?a")
	}
}

func TestSubstitutionOfRefusesListsOfDifferentLengths(t *testing.T) {
	x := datalog.NewSymbol("?x")
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")

	_, err := SubstitutionOf([]Symbol{x}, []Symbol{a, b})
	if err == nil {
		t.Fatal("a list of another length pairs nothing and is an error")
	}
}

func TestSubstitutionRenamesTheNamedSymbolsInPlaceAndKeepsTheRest(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	w := datalog.NewSymbol("?w")
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	subst, err := SubstitutionOf([]Symbol{x, y}, []Symbol{a, b})
	if err != nil {
		t.Fatal(err)
	}

	renamed, err := subst.Rename([]Symbol{y, w, x})
	if err != nil {
		t.Fatalf("every named symbol is present, got error %v", err)
	}
	want := []Symbol{b, w, a}
	if len(renamed) != len(want) {
		t.Fatalf("renamed = %v, want %v", renamed, want)
	}
	for i := range want {
		if renamed[i] != want[i] {
			t.Errorf("renamed[%d] = %v, want %v: a rename keeps every position and changes the names alone", i, renamed[i], want[i])
		}
	}
}

func TestSubstitutionRenameErrorsOnANamedSymbolTheListLacks(t *testing.T) {
	x := datalog.NewSymbol("?x")
	w := datalog.NewSymbol("?w")
	a := datalog.NewSymbol("?a")
	subst, err := SubstitutionOf([]Symbol{x}, []Symbol{a})
	if err != nil {
		t.Fatal(err)
	}

	_, err = subst.Rename([]Symbol{w})
	if err == nil {
		t.Fatal("a named symbol the list lacks is an error, never a rename of nothing")
	}
	if !strings.Contains(err.Error(), "?x") || !strings.Contains(err.Error(), "?w") {
		t.Errorf("the error names the absent symbol and the symbols searched, got %q", err.Error())
	}
}

func TestSubstitutionOverKeepsThePairsNamingTheGivenSymbols(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	w := datalog.NewSymbol("?w")
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	subst, err := SubstitutionOf([]Symbol{x, y}, []Symbol{a, b})
	if err != nil {
		t.Fatal(err)
	}

	over := subst.Over([]Symbol{y, w})
	if got, ok := over.Of(y); !ok || got != b {
		t.Errorf("the pair naming ?y, among the given symbols, is kept, got %v %v", got, ok)
	}
	if _, ok := over.Of(x); ok {
		t.Error("the pair naming ?x, not among the given symbols, is left out")
	}
	renamed, err := over.Rename([]Symbol{y, w})
	if err != nil {
		t.Fatalf("a part over the given symbols renames them, got error %v", err)
	}
	if renamed[0] != b || renamed[1] != w {
		t.Errorf("renamed = %v, want [?b ?w]", renamed)
	}
}

func TestSubstitutionRenameErrorsOnTwoAttributesUnderOneName(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	a := datalog.NewSymbol("?a")
	subst, err := SubstitutionOf([]Symbol{x, y}, []Symbol{a, a})
	if err != nil {
		t.Fatal(err)
	}

	_, err = subst.Rename([]Symbol{x, y})
	if err == nil {
		t.Fatal("one name standing for two attributes is a selection on their equality, not a rename, and is an error here")
	}
	if !strings.Contains(err.Error(), "?a") {
		t.Errorf("the error names the shared name, got %q", err.Error())
	}
}
