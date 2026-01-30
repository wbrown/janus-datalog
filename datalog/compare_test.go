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

func TestSymbolCompareValues(t *testing.T) {
	a := NewSymbol("alpha")
	b := NewSymbol("beta")
	a2 := NewSymbol("alpha")

	// Same symbol (interned pointer equality)
	if cmp := CompareValues(a, a2); cmp != 0 {
		t.Errorf("CompareValues(alpha, alpha) = %d, want 0", cmp)
	}

	// Different symbols
	if cmp := CompareValues(a, b); cmp >= 0 {
		t.Errorf("CompareValues(alpha, beta) = %d, want < 0", cmp)
	}
	if cmp := CompareValues(b, a); cmp <= 0 {
		t.Errorf("CompareValues(beta, alpha) = %d, want > 0", cmp)
	}

	// Symbol vs non-Symbol
	if cmp := CompareValues(a, "alpha"); cmp >= 0 {
		t.Errorf("CompareValues(symbol, string) = %d, want < 0 (type mismatch)", cmp)
	}
}

func TestSymbolValuesEqual(t *testing.T) {
	a := NewSymbol("test-fn")
	b := NewSymbol("test-fn")
	c := NewSymbol("other-fn")

	if !ValuesEqual(a, b) {
		t.Error("ValuesEqual(same symbol, same symbol) should be true")
	}
	if ValuesEqual(a, c) {
		t.Error("ValuesEqual(different symbols) should be false")
	}
	if ValuesEqual(a, "test-fn") {
		t.Error("ValuesEqual(symbol, string) should be false")
	}
}

// TestElementIDCompareValues tests CompareValues with ElementID values
func TestElementIDCompareValues(t *testing.T) {
	a := ElementID{Lamport: 100, ReplicaID: 5}
	b := ElementID{Lamport: 100, ReplicaID: 5}
	c := ElementID{Lamport: 200, ReplicaID: 5}
	d := ElementID{Lamport: 100, ReplicaID: 10}

	// Same ElementID
	if cmp := CompareValues(a, b); cmp != 0 {
		t.Errorf("CompareValues(equal ElementIDs) = %d, want 0", cmp)
	}

	// Different Lamport
	if cmp := CompareValues(a, c); cmp >= 0 {
		t.Errorf("CompareValues(lower Lamport, higher Lamport) = %d, want < 0", cmp)
	}
	if cmp := CompareValues(c, a); cmp <= 0 {
		t.Errorf("CompareValues(higher Lamport, lower Lamport) = %d, want > 0", cmp)
	}

	// Same Lamport, different ReplicaID
	if cmp := CompareValues(a, d); cmp >= 0 {
		t.Errorf("CompareValues(lower ReplicaID, higher ReplicaID) = %d, want < 0", cmp)
	}
	if cmp := CompareValues(d, a); cmp <= 0 {
		t.Errorf("CompareValues(higher ReplicaID, lower ReplicaID) = %d, want > 0", cmp)
	}

	// ElementID vs non-ElementID
	if cmp := CompareValues(a, int64(100)); cmp >= 0 {
		t.Errorf("CompareValues(ElementID, int64) = %d, want < 0 (type mismatch)", cmp)
	}
}

// TestElementIDValuesEqual tests ValuesEqual with ElementID values
func TestElementIDValuesEqual(t *testing.T) {
	a := ElementID{Lamport: 100, ReplicaID: 5}
	b := ElementID{Lamport: 100, ReplicaID: 5}
	c := ElementID{Lamport: 200, ReplicaID: 5}
	d := ElementID{Lamport: 100, ReplicaID: 10}

	// Same ElementID
	if !ValuesEqual(a, b) {
		t.Error("ValuesEqual(equal ElementIDs) should be true")
	}

	// Different Lamport
	if ValuesEqual(a, c) {
		t.Error("ValuesEqual(different Lamport) should be false")
	}

	// Different ReplicaID
	if ValuesEqual(a, d) {
		t.Error("ValuesEqual(different ReplicaID) should be false")
	}

	// ElementID vs non-ElementID
	if ValuesEqual(a, int64(100)) {
		t.Error("ValuesEqual(ElementID, int64) should be false")
	}
}

// TestElementIDZeroValues tests comparison with zero/HEAD ElementID
func TestElementIDZeroValues(t *testing.T) {
	zero := ElementIDZero
	nonZero := ElementID{Lamport: 1, ReplicaID: 0}

	// Zero equals zero
	if cmp := CompareValues(zero, ElementIDZero); cmp != 0 {
		t.Errorf("CompareValues(zero, zero) = %d, want 0", cmp)
	}
	if !ValuesEqual(zero, ElementIDZero) {
		t.Error("ValuesEqual(zero, zero) should be true")
	}

	// Zero < non-zero
	if cmp := CompareValues(zero, nonZero); cmp >= 0 {
		t.Errorf("CompareValues(zero, nonZero) = %d, want < 0", cmp)
	}
	if cmp := CompareValues(nonZero, zero); cmp <= 0 {
		t.Errorf("CompareValues(nonZero, zero) = %d, want > 0", cmp)
	}
}
