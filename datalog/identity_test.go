package datalog

import (
	"crypto/sha1"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/codec"
)

// TestIdentityStringIsCanonicalL85 pins the hash-only contract: String() is the
// L85 encoding of the content-address hash, never the seed string that produced
// it. The seed is consumed by NewIdentity and discarded; recovering a human
// name requires storing it as an attribute.
func TestIdentityStringIsCanonicalL85(t *testing.T) {
	seed := "user:alice"
	hash := sha1.Sum([]byte(seed))
	want := codec.EncodeL85(hash[:])

	id := NewIdentity(seed)
	if got := id.String(); got != want {
		t.Errorf("String() = %q, want canonical L85 %q", got, want)
	}
	if id.String() != id.L85() {
		t.Errorf("String() = %q diverges from L85() = %q", id.String(), id.L85())
	}
}

// TestIdentityStringProvenanceIndependent pins that rendering is a pure
// function of the hash: the same entity renders identically whether it was
// first interned from a seed string or from a storage-decoded hash, in either
// interning order. (The pre-fix defect: the intern cache is keyed by hash and
// the first constructor to run won the pointer, so String() output depended on
// construction order within a process.)
func TestIdentityStringProvenanceIndependent(t *testing.T) {
	seed := "user:provenance"
	hash := sha1.Sum([]byte(seed))
	want := codec.EncodeL85(hash[:])

	ClearInterns()
	seedFirst := NewIdentity(seed)
	decoded := NewIdentityFromHash(hash)
	if seedFirst != decoded {
		t.Fatal("interning broken: seed and hash constructors returned different pointers")
	}
	if got := decoded.String(); got != want {
		t.Errorf("seed-first interning: String() = %q, want %q", got, want)
	}

	ClearInterns()
	hashFirst := NewIdentityFromHash(hash)
	reSeeded := NewIdentity(seed)
	if hashFirst != reSeeded {
		t.Fatal("interning broken: hash and seed constructors returned different pointers")
	}
	if got := reSeeded.String(); got != want {
		t.Errorf("hash-first interning: String() = %q, want %q", got, want)
	}
}
