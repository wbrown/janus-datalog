package datalog

import "testing"

// TestClearInternsResetsKeywordByteCache pins the byte-keyed keyword cache to
// the string-keyed authority across ClearInterns: after a clear, a keyword
// decoded from storage bytes must share the interned pointer of the same
// keyword created from its string, or interned Equal/Compare panic on two
// pointers carrying one value.
func TestClearInternsResetsKeywordByteCache(t *testing.T) {
	var key [32]byte
	copy(key[:], ":clear/keyword")

	before := InternKeywordFromBytes(key)
	if got := before.String(); got != ":clear/keyword" {
		t.Fatalf("decoded keyword = %q, want %q", got, ":clear/keyword")
	}

	ClearInterns()

	fromString := InternKeyword(":clear/keyword")
	fromBytes := InternKeywordFromBytes(key)
	if fromBytes != fromString {
		t.Fatalf("post-clear byte-interned keyword is a stale pointer: "+
			"fromBytes=%p fromString=%p", fromBytes, fromString)
	}
	// The panic the stale pointer causes downstream: interned comparison.
	if !fromBytes.Equal(fromString) {
		t.Fatal("equal keywords must compare equal")
	}
}

// TestClearInternsPreservesWellKnownIdentity pins the contract a package-level
// variable depends on: the instance it bound at init is still the canonical
// one after a clear.
//
// Re-interning would not do. It mints a new pointer, so the variable in *this*
// package could be reassigned but a variable in a package this one cannot name
// could not — and neither could a copy already stored in someone's schema or
// AST. Preserving the instance is what reaches those.
//
// The teeth are in Equal: two pointers carrying one string panic, so a
// well-known value orphaned by a clear does not degrade to a mismatch, it
// crashes the next comparison against a fresh intern of the same text.
func TestClearInternsPreservesWellKnownIdentity(t *testing.T) {
	wellKnownKw := WellKnownKeyword(":datalog.test/well-known")
	symbol := SymCount

	// An ordinary keyword, registered by nobody, to pin the other half: the
	// clear still clears. A registry that preserved everything would pass the
	// assertions above and defeat the function.
	ordinary := NewKeyword(":datalog.test/ordinary")

	ClearInterns()

	if got := InternKeyword(":datalog.test/well-known"); got != wellKnownKw {
		t.Fatalf("well-known keyword lost its identity across the clear: "+
			"held=%p canonical=%p", wellKnownKw, got)
	}
	if !wellKnownKw.Equal(InternKeyword(":datalog.test/well-known")) {
		t.Fatal("a well-known keyword must compare equal to its fresh intern")
	}
	if got := internSymbol("count"); got != symbol {
		t.Fatalf("well-known symbol lost its identity across the clear: "+
			"held=%p canonical=%p", symbol, got)
	}

	if NewKeyword(":datalog.test/ordinary") == ordinary {
		t.Fatal("an unregistered keyword must not survive the clear; " +
			"ClearInterns reclaims what nothing holds")
	}
}
