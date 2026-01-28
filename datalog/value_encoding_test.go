package datalog

import (
	"testing"
)

func TestSymbolValueEncoding_RoundTrip(t *testing.T) {
	tests := []string{
		"my-function",
		"concat",
		"rule-name",
		"$source",
		"?var",
	}

	for _, name := range tests {
		sym := NewSymbol(name)

		// Check type
		vt := Type(sym)
		if vt != TypeSymbol {
			t.Errorf("Type(NewSymbol(%q)) = %d, want TypeSymbol (%d)", name, vt, TypeSymbol)
		}

		// Encode
		data := ValueBytes(sym)
		if string(data) != name {
			t.Errorf("ValueBytes(NewSymbol(%q)) = %q, want %q", name, string(data), name)
		}

		// Decode
		val, err := ValueFromBytes(TypeSymbol, data)
		if err != nil {
			t.Fatalf("ValueFromBytes(TypeSymbol, %q): %v", name, err)
		}

		got, ok := val.(Symbol)
		if !ok {
			t.Fatalf("ValueFromBytes(TypeSymbol, %q) returned %T, want Symbol", name, val)
		}

		// Interning: round-tripped symbol should be pointer-equal to original
		if got != sym {
			t.Errorf("round-tripped symbol %q not pointer-equal to original", name)
		}
	}
}

func TestSymbolValueType_Distinct(t *testing.T) {
	// Symbol, Keyword, and String should have distinct type tags
	sym := NewSymbol("foo")
	kw := NewKeyword(":foo")
	str := "foo"

	symType := Type(sym)
	kwType := Type(kw)
	strType := Type(str)

	if symType == kwType {
		t.Errorf("Symbol and Keyword have same type tag: %d", symType)
	}
	if symType == strType {
		t.Errorf("Symbol and String have same type tag: %d", symType)
	}
	if symType != TypeSymbol {
		t.Errorf("Symbol type = %d, want %d (TypeSymbol)", symType, TypeSymbol)
	}
}
