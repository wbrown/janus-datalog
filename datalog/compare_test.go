package datalog

import (
	"testing"
)

func TestValuesEqualWithPointers(t *testing.T) {
	// Test with interned values
	id1 := NewIdentity("test")
	id2 := NewIdentity("test")

	// Test direct values
	if !ValuesEqual(id1, id2) {
		t.Error("Expected equal identities to be equal")
	}

	// Test pointers
	ptr1 := InternIdentity(id1)
	ptr2 := InternIdentity(id2)

	if !ValuesEqual(ptr1, ptr2) {
		t.Error("Expected pointers to equal identities to be equal")
	}

	// Test mixed
	if !ValuesEqual(ptr1, id1) {
		t.Error("Expected pointer and value to be equal")
	}

	// Test keywords
	kw1 := NewKeyword(":test")
	kw2 := NewKeyword(":test")

	if !ValuesEqual(kw1, kw2) {
		t.Error("Expected equal keywords to be equal")
	}

	kwPtr1 := InternKeyword(":test")
	kwPtr2 := InternKeyword(":test")

	if !ValuesEqual(kwPtr1, kwPtr2) {
		t.Error("Expected keyword pointers to be equal")
	}
}

// TestNilIdentityHandling tests that nil Identity values don't cause panics
func TestNilIdentityHandling(t *testing.T) {
	var nilId Identity // nil

	// Test nil methods don't panic
	t.Run("Hash", func(t *testing.T) {
		hash := nilId.Hash()
		if hash != [20]byte{} {
			t.Error("Expected zero hash for nil Identity")
		}
	})

	t.Run("L85", func(t *testing.T) {
		l85 := nilId.L85()
		if l85 != "" {
			t.Error("Expected empty L85 for nil Identity")
		}
	})

	t.Run("String", func(t *testing.T) {
		str := nilId.String()
		if str != "" {
			t.Error("Expected empty string for nil Identity")
		}
	})

	t.Run("ID", func(t *testing.T) {
		id := nilId.ID()
		if id != 0 {
			t.Error("Expected zero ID for nil Identity")
		}
	})

	t.Run("Bytes", func(t *testing.T) {
		b := nilId.Bytes()
		if b != nil {
			t.Error("Expected nil bytes for nil Identity")
		}
	})

	t.Run("Equal_nil_nil", func(t *testing.T) {
		var other Identity
		if !nilId.Equal(other) {
			t.Error("Expected nil == nil for Identity")
		}
	})

	t.Run("Equal_nil_nonnil", func(t *testing.T) {
		nonNil := NewIdentity("test")
		if nilId.Equal(nonNil) {
			t.Error("Expected nil != non-nil for Identity")
		}
		if nonNil.Equal(nilId) {
			t.Error("Expected non-nil != nil for Identity")
		}
	})

	t.Run("Compare_nil_nil", func(t *testing.T) {
		var other Identity
		if nilId.Compare(other) != 0 {
			t.Error("Expected Compare(nil, nil) == 0")
		}
	})

	t.Run("Compare_nil_nonnil", func(t *testing.T) {
		nonNil := NewIdentity("test")
		if nilId.Compare(nonNil) >= 0 {
			t.Error("Expected nil < non-nil for Identity")
		}
		if nonNil.Compare(nilId) <= 0 {
			t.Error("Expected non-nil > nil for Identity")
		}
	})

	t.Run("ValuesEqual_nil", func(t *testing.T) {
		var other Identity
		if !ValuesEqual(nilId, other) {
			t.Error("Expected ValuesEqual(nil, nil) == true")
		}
		nonNil := NewIdentity("test")
		if ValuesEqual(nilId, nonNil) {
			t.Error("Expected ValuesEqual(nil, non-nil) == false")
		}
	})
}

// TestNilKeywordHandling tests that nil Keyword values don't cause panics
func TestNilKeywordHandling(t *testing.T) {
	var nilKw Keyword // nil

	// Test nil methods don't panic
	t.Run("String", func(t *testing.T) {
		str := nilKw.String()
		if str != "" {
			t.Error("Expected empty string for nil Keyword")
		}
	})

	t.Run("Bytes", func(t *testing.T) {
		b := nilKw.Bytes()
		if b != nil {
			t.Error("Expected nil bytes for nil Keyword")
		}
	})

	t.Run("Namespace", func(t *testing.T) {
		ns := nilKw.Namespace()
		if ns != "" {
			t.Error("Expected empty namespace for nil Keyword")
		}
	})

	t.Run("Name", func(t *testing.T) {
		name := nilKw.Name()
		if name != "" {
			t.Error("Expected empty name for nil Keyword")
		}
	})

	t.Run("IsQualified", func(t *testing.T) {
		if nilKw.IsQualified() {
			t.Error("Expected nil Keyword to not be qualified")
		}
	})

	t.Run("Equal_nil_nil", func(t *testing.T) {
		var other Keyword
		if !nilKw.Equal(other) {
			t.Error("Expected nil == nil for Keyword")
		}
	})

	t.Run("Equal_nil_nonnil", func(t *testing.T) {
		nonNil := NewKeyword(":test")
		if nilKw.Equal(nonNil) {
			t.Error("Expected nil != non-nil for Keyword")
		}
		if nonNil.Equal(nilKw) {
			t.Error("Expected non-nil != nil for Keyword")
		}
	})

	t.Run("Compare_nil_nil", func(t *testing.T) {
		var other Keyword
		if nilKw.Compare(other) != 0 {
			t.Error("Expected Compare(nil, nil) == 0")
		}
	})

	t.Run("Compare_nil_nonnil", func(t *testing.T) {
		nonNil := NewKeyword(":test")
		if nilKw.Compare(nonNil) >= 0 {
			t.Error("Expected nil < non-nil for Keyword")
		}
		if nonNil.Compare(nilKw) <= 0 {
			t.Error("Expected non-nil > nil for Keyword")
		}
	})

	t.Run("ValuesEqual_nil", func(t *testing.T) {
		var other Keyword
		if !ValuesEqual(nilKw, other) {
			t.Error("Expected ValuesEqual(nil, nil) == true")
		}
		nonNil := NewKeyword(":test")
		if ValuesEqual(nilKw, nonNil) {
			t.Error("Expected ValuesEqual(nil, non-nil) == false")
		}
	})

	t.Run("Matches_nil", func(t *testing.T) {
		var other Keyword
		if !nilKw.Matches(other) {
			t.Error("Expected nil.Matches(nil) == true")
		}
		nonNil := NewKeyword(":test")
		if nilKw.Matches(nonNil) {
			t.Error("Expected nil.Matches(non-nil) == false")
		}
	})
}
