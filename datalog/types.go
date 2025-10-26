package datalog

import (
	"fmt"
)

// Datom is the fundamental unit of data in a Datalog system
// It represents a single fact: Entity-Attribute-Value-Transaction
type Datom struct {
	E  Identity // Entity identifier
	A  Keyword  // Attribute keyword
	V  Value    // Any value (see value.go for valid types)
	Tx uint64   // Transaction ID
}

// Keyword represents an attribute keyword
// Unlike entities, keywords are interned strings, not hashes
type Keyword struct {
	value string // The keyword string (e.g., ":user/name")
}

// NewKeyword creates a keyword
func NewKeyword(s string) Keyword {
	// TODO: Add interning/caching for performance
	return Keyword{value: s}
}

// String returns the keyword string
func (k Keyword) String() string {
	return k.value
}

// Compare compares two keywords
func (k Keyword) Compare(other Keyword) int {
	if k.value < other.value {
		return -1
	} else if k.value > other.value {
		return 1
	}
	return 0
}

// Bytes returns the keyword as bytes
func (k Keyword) Bytes() []byte {
	return []byte(k.value)
}

// String returns a string representation of the Datom
func (d Datom) String() string {
	return fmt.Sprintf("[%s %s %v %d]", d.E, d.A, d.V, d.Tx)
}
