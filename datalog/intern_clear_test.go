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
