package datalog

import (
	"hash/fnv"
	"testing"
)

// TestAKeywordsHash64IsTheFNV1aHashOfItsText pins that an interned keyword
// carries the 64-bit FNV-1a hash of its text, and that every way of interning
// the text yields a keyword carrying that hash.
func TestAKeywordsHash64IsTheFNV1aHashOfItsText(t *testing.T) {
	for _, text := range []string{":keyword-hash/a", ":keyword-hash/name", ":keyword-hash/a-longer-attribute"} {
		h := fnv.New64a()
		if _, err := h.Write([]byte(text)); err != nil {
			t.Fatal(err)
		}
		want := h.Sum64()

		var stored [32]byte
		copy(stored[:], text)
		for _, interned := range []struct {
			how string
			k   Keyword
		}{
			{"NewKeyword", NewKeyword(text)},
			{"InternKeyword", InternKeyword(text)},
			{"InternKeywordFromBytes", InternKeywordFromBytes(stored)},
		} {
			if got := interned.k.Hash64(); got != want {
				t.Errorf("%s(%q).Hash64() = %#x, want the FNV-1a hash %#x", interned.how, text, got, want)
			}
		}
	}
}
