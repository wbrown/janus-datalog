package datalog

import (
	"crypto/sha1"
	"encoding/binary"

	"github.com/wbrown/janus-datalog/datalog/codec"
)

// Identity represents an entity identifier
// Like C++ Reference and Clojure Identity, it contains both the hash and cached encodings
type Identity struct {
	value       [20]byte // SHA1 hash
	l85         string   // Lazily computed L85 encoding
	str         string   // Original string (if known)
	l85Computed bool     // Whether l85 has been computed
}

// NewIdentity creates an identity from a string
func NewIdentity(s string) Identity {
	hash := sha1.Sum([]byte(s))
	return Identity{
		value: hash,
		str:   s,
		// l85 will be computed lazily when needed
	}
}

// NewIdentityFromHash creates an identity from a hash
func NewIdentityFromHash(hash [20]byte) Identity {
	// CRITICAL: Compute L85 eagerly so String() returns it
	// Otherwise comparisons fail because identities from storage
	// have empty str/l85 fields and don't match the original
	return Identity{
		value:       hash,
		l85:         codec.EncodeL85(hash[:]),
		str:         "", // Unknown - use L85 representation
		l85Computed: true,
	}
}

// Hash returns the raw hash value
func (i Identity) Hash() [20]byte {
	return i.value
}

// L85 returns the L85-encoded representation
func (i *Identity) L85() string {
	if !i.l85Computed {
		i.l85 = codec.EncodeL85(i.value[:])
		i.l85Computed = true
	}
	return i.l85
}

// String returns a string representation
func (i Identity) String() string {
	if i.str != "" {
		return i.str
	}
	// If we don't know the original string, show the L85
	return i.l85
}

// ID returns a numeric ID (first 8 bytes as uint64, like Clojure)
func (i Identity) ID() uint64 {
	return binary.BigEndian.Uint64(i.value[:8])
}

// Compare compares two identities
func (i Identity) Compare(other Identity) int {
	// Compare using L85 for sort order
	if i.l85 < other.l85 {
		return -1
	} else if i.l85 > other.l85 {
		return 1
	}
	return 0
}

// Equal checks if two identities are equal
func (i Identity) Equal(other Identity) bool {
	return i.value == other.value
}

// Bytes returns the raw hash bytes
func (i Identity) Bytes() []byte {
	return i.value[:]
}
