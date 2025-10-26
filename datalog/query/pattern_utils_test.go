package query

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestPatternExtractor_Constants(t *testing.T) {
	// Pattern with all constants: [alice :user/name "Alice"]
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")

	pattern := &DataPattern{
		Elements: []PatternElement{
			Constant{Value: alice},
			Constant{Value: nameAttr},
			Constant{Value: "Alice"},
		},
	}

	extractor := NewPatternExtractor(pattern, []Symbol{})
	values := extractor.Extract(Tuple{})

	if values.E == nil || !values.E.(datalog.Identity).Equal(alice) {
		t.Errorf("Expected E=%v, got %v", alice, values.E)
	}
	if values.A == nil || values.A.(datalog.Keyword) != nameAttr {
		t.Errorf("Expected A=%v, got %v", nameAttr, values.A)
	}
	if values.V != "Alice" {
		t.Errorf("Expected V=Alice, got %v", values.V)
	}
	if values.T != nil {
		t.Errorf("Expected T=nil, got %v", values.T)
	}
}

func TestPatternExtractor_Variables(t *testing.T) {
	// Pattern with all variables: [?e ?a ?v]
	pattern := &DataPattern{
		Elements: []PatternElement{
			Variable{Name: "?e"},
			Variable{Name: "?a"},
			Variable{Name: "?v"},
		},
	}

	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")

	columns := []Symbol{"?e", "?a", "?v"}
	bindingTuple := Tuple{alice, nameAttr, "Alice"}

	extractor := NewPatternExtractor(pattern, columns)
	values := extractor.Extract(bindingTuple)

	if values.E == nil || !values.E.(datalog.Identity).Equal(alice) {
		t.Errorf("Expected E=%v, got %v", alice, values.E)
	}
	if values.A == nil || values.A.(datalog.Keyword) != nameAttr {
		t.Errorf("Expected A=%v, got %v", nameAttr, values.A)
	}
	if values.V != "Alice" {
		t.Errorf("Expected V=Alice, got %v", values.V)
	}
	if values.T != nil {
		t.Errorf("Expected T=nil, got %v", values.T)
	}
}

func TestPatternExtractor_MixedConstantsAndVariables(t *testing.T) {
	// Pattern: [?e :user/name ?v]
	nameAttr := datalog.NewKeyword(":user/name")

	pattern := &DataPattern{
		Elements: []PatternElement{
			Variable{Name: "?e"},
			Constant{Value: nameAttr},
			Variable{Name: "?v"},
		},
	}

	alice := datalog.NewIdentity("user:alice")
	columns := []Symbol{"?e", "?v"}
	bindingTuple := Tuple{alice, "Alice"}

	extractor := NewPatternExtractor(pattern, columns)
	values := extractor.Extract(bindingTuple)

	if values.E == nil || !values.E.(datalog.Identity).Equal(alice) {
		t.Errorf("Expected E=%v, got %v", alice, values.E)
	}
	if values.A == nil || values.A.(datalog.Keyword) != nameAttr {
		t.Errorf("Expected A=%v, got %v", nameAttr, values.A)
	}
	if values.V != "Alice" {
		t.Errorf("Expected V=Alice, got %v", values.V)
	}
}

func TestPatternExtractor_Blanks(t *testing.T) {
	// Pattern: [_ :user/name ?v]
	nameAttr := datalog.NewKeyword(":user/name")

	pattern := &DataPattern{
		Elements: []PatternElement{
			Blank{},
			Constant{Value: nameAttr},
			Variable{Name: "?v"},
		},
	}

	columns := []Symbol{"?v"}
	bindingTuple := Tuple{"Alice"}

	extractor := NewPatternExtractor(pattern, columns)
	values := extractor.Extract(bindingTuple)

	if values.E != nil {
		t.Errorf("Expected E=nil for blank, got %v", values.E)
	}
	if values.A == nil || values.A.(datalog.Keyword) != nameAttr {
		t.Errorf("Expected A=%v, got %v", nameAttr, values.A)
	}
	if values.V != "Alice" {
		t.Errorf("Expected V=Alice, got %v", values.V)
	}
}

func TestPatternExtractor_VariableNotInBinding(t *testing.T) {
	// Pattern: [?e ?a ?v] but binding only has ?e
	pattern := &DataPattern{
		Elements: []PatternElement{
			Variable{Name: "?e"},
			Variable{Name: "?a"},
			Variable{Name: "?v"},
		},
	}

	alice := datalog.NewIdentity("user:alice")
	columns := []Symbol{"?e"}
	bindingTuple := Tuple{alice}

	extractor := NewPatternExtractor(pattern, columns)
	values := extractor.Extract(bindingTuple)

	if values.E == nil || !values.E.(datalog.Identity).Equal(alice) {
		t.Errorf("Expected E=%v, got %v", alice, values.E)
	}
	if values.A != nil {
		t.Errorf("Expected A=nil (variable not in binding), got %v", values.A)
	}
	if values.V != nil {
		t.Errorf("Expected V=nil (variable not in binding), got %v", values.V)
	}
}

func TestPatternExtractor_WithTransaction(t *testing.T) {
	// Pattern with 4 elements: [?e ?a ?v ?t]
	pattern := &DataPattern{
		Elements: []PatternElement{
			Variable{Name: "?e"},
			Variable{Name: "?a"},
			Variable{Name: "?v"},
			Variable{Name: "?t"},
		},
	}

	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")

	columns := []Symbol{"?e", "?a", "?v", "?t"}
	bindingTuple := Tuple{alice, nameAttr, "Alice", uint64(123)}

	extractor := NewPatternExtractor(pattern, columns)
	values := extractor.Extract(bindingTuple)

	if values.E == nil || !values.E.(datalog.Identity).Equal(alice) {
		t.Errorf("Expected E=%v, got %v", alice, values.E)
	}
	if values.A == nil || values.A.(datalog.Keyword) != nameAttr {
		t.Errorf("Expected A=%v, got %v", nameAttr, values.A)
	}
	if values.V != "Alice" {
		t.Errorf("Expected V=Alice, got %v", values.V)
	}
	if values.T == nil || values.T.(uint64) != 123 {
		t.Errorf("Expected T=123, got %v", values.T)
	}
}

func TestPatternExtractor_IndividualExtractors(t *testing.T) {
	// Test ExtractE, ExtractA, ExtractV, ExtractT individually
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")

	pattern := &DataPattern{
		Elements: []PatternElement{
			Variable{Name: "?e"},
			Constant{Value: nameAttr},
			Variable{Name: "?v"},
		},
	}

	columns := []Symbol{"?e", "?v"}
	bindingTuple := Tuple{alice, "Alice"}

	extractor := NewPatternExtractor(pattern, columns)

	e := extractor.ExtractE(bindingTuple)
	if e == nil || !e.(datalog.Identity).Equal(alice) {
		t.Errorf("ExtractE: Expected %v, got %v", alice, e)
	}

	a := extractor.ExtractA(bindingTuple)
	if a == nil || a.(datalog.Keyword) != nameAttr {
		t.Errorf("ExtractA: Expected %v, got %v", nameAttr, a)
	}

	v := extractor.ExtractV(bindingTuple)
	if v != "Alice" {
		t.Errorf("ExtractV: Expected Alice, got %v", v)
	}

	tx := extractor.ExtractT(bindingTuple)
	if tx != nil {
		t.Errorf("ExtractT: Expected nil, got %v", tx)
	}
}

func TestBuildColumnIndexMap(t *testing.T) {
	columns := []Symbol{"?e", "?a", "?v"}
	colMap := BuildColumnIndexMap(columns)

	if colMap["?e"] != 0 {
		t.Errorf("Expected ?e at index 0, got %d", colMap["?e"])
	}
	if colMap["?a"] != 1 {
		t.Errorf("Expected ?a at index 1, got %d", colMap["?a"])
	}
	if colMap["?v"] != 2 {
		t.Errorf("Expected ?v at index 2, got %d", colMap["?v"])
	}
}

func TestExtractPatternValues_ConvenienceFunction(t *testing.T) {
	// Test the convenience function
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")

	pattern := &DataPattern{
		Elements: []PatternElement{
			Variable{Name: "?e"},
			Constant{Value: nameAttr},
			Variable{Name: "?v"},
		},
	}

	columns := []Symbol{"?e", "?v"}
	bindingTuple := Tuple{alice, "Alice"}

	e, a, v, tx := ExtractPatternValues(pattern, columns, bindingTuple)

	if e == nil || !e.(datalog.Identity).Equal(alice) {
		t.Errorf("Expected E=%v, got %v", alice, e)
	}
	if a == nil || a.(datalog.Keyword) != nameAttr {
		t.Errorf("Expected A=%v, got %v", nameAttr, a)
	}
	if v != "Alice" {
		t.Errorf("Expected V=Alice, got %v", v)
	}
	if tx != nil {
		t.Errorf("Expected T=nil, got %v", tx)
	}
}

func TestPatternExtractor_OutOfBoundsTuple(t *testing.T) {
	// Pattern expects more columns than the binding tuple has
	pattern := &DataPattern{
		Elements: []PatternElement{
			Variable{Name: "?e"},
			Variable{Name: "?a"},
			Variable{Name: "?v"},
		},
	}

	alice := datalog.NewIdentity("user:alice")
	columns := []Symbol{"?e", "?a", "?v"}
	bindingTuple := Tuple{alice} // Only has 1 value instead of 3

	extractor := NewPatternExtractor(pattern, columns)
	values := extractor.Extract(bindingTuple)

	if values.E == nil || !values.E.(datalog.Identity).Equal(alice) {
		t.Errorf("Expected E=%v, got %v", alice, values.E)
	}
	if values.A != nil {
		t.Errorf("Expected A=nil (out of bounds), got %v", values.A)
	}
	if values.V != nil {
		t.Errorf("Expected V=nil (out of bounds), got %v", values.V)
	}
}

func TestPatternExtractor_EmptyPattern(t *testing.T) {
	// Edge case: empty pattern
	pattern := &DataPattern{
		Elements: []PatternElement{},
	}

	extractor := NewPatternExtractor(pattern, []Symbol{})
	values := extractor.Extract(Tuple{})

	if values.E != nil {
		t.Errorf("Expected E=nil for empty pattern, got %v", values.E)
	}
	if values.A != nil {
		t.Errorf("Expected A=nil for empty pattern, got %v", values.A)
	}
	if values.V != nil {
		t.Errorf("Expected V=nil for empty pattern, got %v", values.V)
	}
	if values.T != nil {
		t.Errorf("Expected T=nil for empty pattern, got %v", values.T)
	}
}

func TestPatternExtractor_ColumnsInDifferentOrder(t *testing.T) {
	// Test that column ordering doesn't matter - it's the symbol name that counts
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")

	pattern := &DataPattern{
		Elements: []PatternElement{
			Variable{Name: "?e"},
			Variable{Name: "?a"},
			Variable{Name: "?v"},
		},
	}

	// Columns in different order: ?v, ?e, ?a
	columns := []Symbol{"?v", "?e", "?a"}
	bindingTuple := Tuple{"Alice", alice, nameAttr}

	extractor := NewPatternExtractor(pattern, columns)
	values := extractor.Extract(bindingTuple)

	if values.E == nil || !values.E.(datalog.Identity).Equal(alice) {
		t.Errorf("Expected E=%v, got %v", alice, values.E)
	}
	if values.A == nil || values.A.(datalog.Keyword) != nameAttr {
		t.Errorf("Expected A=%v, got %v", nameAttr, values.A)
	}
	if values.V != "Alice" {
		t.Errorf("Expected V=Alice, got %v", values.V)
	}
}
