package datalog

import (
	"strings"
	"testing"
)

// Reproduction for BUG_ATTRIBUTE_KEY_TRUNCATION_COLLISION.md
//
// InternKeyword keys the intern cache by a [32]byte buffer filled with
// copy(key[:], s), which silently truncates strings longer than 32 bytes.
// Two distinct keyword strings that share their first 32 bytes therefore
// intern to the same *keyword pointer.
func TestKeywordInterning_LongNamesDoNotCollide(t *testing.T) {
	// 42 bytes of shared prefix — well past the 32-byte intern key — so the
	// distinguishing suffix lives entirely beyond the truncation boundary.
	shared := ":collintern/" + strings.Repeat("x", 30)
	n1 := shared + "-one"
	n2 := shared + "-two"

	a1 := NewKeyword(n1)
	a2 := NewKeyword(n2)

	if a1 == a2 {
		t.Fatalf("distinct keywords interned to the same pointer: %q and %q share their first 32 bytes and were truncated", n1, n2)
	}
	if got := a1.String(); got != n1 {
		t.Errorf("a1.String() = %q, want %q", got, n1)
	}
	if got := a2.String(); got != n2 {
		t.Errorf("a2.String() = %q, want %q", got, n2)
	}
}
