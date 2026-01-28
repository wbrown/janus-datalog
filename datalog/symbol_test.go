package datalog

import "testing"

func TestNewSymbol_Interning(t *testing.T) {
	// Same string must return same pointer
	s1 := NewSymbol("?x")
	s2 := NewSymbol("?x")
	if s1 != s2 {
		t.Errorf("NewSymbol(%q) returned different pointers: %p vs %p", "?x", s1, s2)
	}

	// Different strings must return different pointers
	s3 := NewSymbol("?y")
	if s1 == s3 {
		t.Errorf("NewSymbol(%q) and NewSymbol(%q) returned same pointer", "?x", "?y")
	}
}

func TestSymbol_String(t *testing.T) {
	tests := []struct {
		input  string
		expect string
	}{
		{"?x", "?x"},
		{"?name", "?name"},
		{"$users", "$users"},
		{"$", "$"},
		{"foo", "foo"},
	}

	for _, tt := range tests {
		s := NewSymbol(tt.input)
		if got := s.String(); got != tt.expect {
			t.Errorf("NewSymbol(%q).String() = %q, want %q", tt.input, got, tt.expect)
		}
	}
}

func TestSymbol_String_Nil(t *testing.T) {
	var s Symbol
	if got := s.String(); got != "" {
		t.Errorf("nil Symbol.String() = %q, want %q", got, "")
	}
}

func TestSymbol_IsVariable(t *testing.T) {
	tests := []struct {
		input string
		isVar bool
	}{
		{"?x", true},
		{"?name", true},
		{"?", true},
		{"$users", false},
		{"$", false},
		{"foo", false},
	}

	for _, tt := range tests {
		s := NewSymbol(tt.input)
		if got := s.IsVariable(); got != tt.isVar {
			t.Errorf("NewSymbol(%q).IsVariable() = %v, want %v", tt.input, got, tt.isVar)
		}
	}
}

func TestSymbol_IsVariable_Nil(t *testing.T) {
	var s Symbol
	if s.IsVariable() {
		t.Error("nil Symbol.IsVariable() should be false")
	}
}

func TestSymbol_IsSource(t *testing.T) {
	tests := []struct {
		input    string
		isSource bool
	}{
		{"$users", true},
		{"$perms", true},
		{"$", true},
		{"?x", false},
		{"foo", false},
	}

	for _, tt := range tests {
		s := NewSymbol(tt.input)
		if got := s.IsSource(); got != tt.isSource {
			t.Errorf("NewSymbol(%q).IsSource() = %v, want %v", tt.input, got, tt.isSource)
		}
	}
}

func TestSymbol_IsSource_Nil(t *testing.T) {
	var s Symbol
	if s.IsSource() {
		t.Error("nil Symbol.IsSource() should be false")
	}
}

func TestSymbol_Compare(t *testing.T) {
	a := NewSymbol("?a")
	b := NewSymbol("?b")
	a2 := NewSymbol("?a")

	// Same pointer (interned)
	if cmp := a.Compare(a2); cmp != 0 {
		t.Errorf("?a.Compare(?a) = %d, want 0", cmp)
	}

	// Different values
	if cmp := a.Compare(b); cmp >= 0 {
		t.Errorf("?a.Compare(?b) = %d, want < 0", cmp)
	}
	if cmp := b.Compare(a); cmp <= 0 {
		t.Errorf("?b.Compare(?a) = %d, want > 0", cmp)
	}
}

func TestSymbol_Compare_Nil(t *testing.T) {
	s := NewSymbol("?x")
	var nilSym Symbol

	if cmp := nilSym.Compare(nilSym); cmp != 0 {
		t.Errorf("nil.Compare(nil) = %d, want 0", cmp)
	}
	if cmp := nilSym.Compare(s); cmp >= 0 {
		t.Errorf("nil.Compare(?x) = %d, want < 0", cmp)
	}
	if cmp := s.Compare(nilSym); cmp <= 0 {
		t.Errorf("?x.Compare(nil) = %d, want > 0", cmp)
	}
}

func TestSymbol_Equal(t *testing.T) {
	a := NewSymbol("?a")
	b := NewSymbol("?b")
	a2 := NewSymbol("?a")

	if !a.Equal(a2) {
		t.Error("?a.Equal(?a) should be true (interned)")
	}
	if a.Equal(b) {
		t.Error("?a.Equal(?b) should be false")
	}
}

func TestSymbol_Equal_Nil(t *testing.T) {
	s := NewSymbol("?x")
	var nilSym Symbol

	if !nilSym.Equal(nilSym) {
		t.Error("nil.Equal(nil) should be true")
	}
	if nilSym.Equal(s) {
		t.Error("nil.Equal(?x) should be false")
	}
	if s.Equal(nilSym) {
		t.Error("?x.Equal(nil) should be false")
	}
}

func TestSymbol_PointerEquality(t *testing.T) {
	// Interning guarantees == works for same-string symbols
	s1 := NewSymbol("$cache")
	s2 := NewSymbol("$cache")
	s3 := NewSymbol("$other")

	if s1 != s2 {
		t.Error("same-string symbols should be pointer-equal")
	}
	if s1 == s3 {
		t.Error("different-string symbols should not be pointer-equal")
	}
}

func TestSymbol_MapKey(t *testing.T) {
	// Pointer-interned symbols work as map keys
	m := map[Symbol]string{}
	s1 := NewSymbol("?e")
	s2 := NewSymbol("?e")

	m[s1] = "entity"
	if m[s2] != "entity" {
		t.Error("interned symbol should work as map key")
	}
}

func TestNewSymbol_EmptyStringPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("NewSymbol(\"\") should panic")
		}
	}()
	NewSymbol("")
}

func TestClearInterns_Symbol(t *testing.T) {
	s1 := NewSymbol("?cleartest")
	ClearInterns()
	s2 := NewSymbol("?cleartest")

	// After clearing, same string gets a new pointer
	if s1 == s2 {
		t.Error("after ClearInterns, new symbol should be a different pointer")
	}
	// But values are still equal
	if s1.String() != s2.String() {
		t.Errorf("values should match: %q vs %q", s1.String(), s2.String())
	}
}
