package datalog

import (
	"fmt"
	"strings"
)

// CRDTOp represents the operation type for CRDT semantics.
// Used for cardinality-many (add-wins sets) and cardinality-vector (RGA).
type CRDTOp uint8

const (
	// OpNone indicates no CRDT operation (used for cardinality-one)
	OpNone CRDTOp = 0
	// OpCRDTAdd indicates a set add operation (cardinality-many)
	OpCRDTAdd CRDTOp = 1
	// OpCRDTRemove indicates a set remove operation (cardinality-many tombstone)
	OpCRDTRemove CRDTOp = 2
	// OpRGAInsert indicates an RGA insert operation (cardinality-vector)
	// When this Op is used, the key includes AfterRef to indicate position
	OpRGAInsert CRDTOp = 3
	// OpRGATombstone indicates an RGA tombstone operation (cardinality-vector deletion)
	// When this Op is used, the key includes AfterRef to identify the deleted element
	OpRGATombstone CRDTOp = 4
)

// HasAfterRef returns true if this operation type includes an AfterRef in the key.
// This makes the key format self-describing - the Op value determines whether
// AfterRef bytes follow in the encoded key.
func (op CRDTOp) HasAfterRef() bool {
	return op == OpRGAInsert || op == OpRGATombstone
}

// Datom is the fundamental unit of data in a Datalog system
// It represents a single fact: Entity-Attribute-Value-Transaction
type Datom struct {
	E        Identity  // Entity identifier
	A        Keyword   // Attribute keyword (interned pointer)
	V        Value     // Any value (see value.go for valid types)
	Tx       ElementID // Transaction/CRDT version (Lamport + ReplicaID)
	Op       CRDTOp    // CRDT operation type (0-4, see CRDTOp constants)
	AfterRef ElementID // RGA position reference (only used when Op.HasAfterRef() is true)
}

// keyword is the unexported base type for attribute keywords.
// Unlike entities, keywords are interned strings, not hashes.
type keyword struct {
	value string // The keyword string (e.g., ":user/name")
}

// Keyword is the exported pointer type, always interned.
type Keyword = *keyword

// MaxAttributeBytes is the maximum length, in bytes, of an attribute keyword's
// string form that can be stored. The storage layer encodes an attribute into a
// fixed-size key component (storage.Attribute, a [32]byte); longer names would
// be silently truncated and alias each other, so writes and schema definitions
// of longer attributes are rejected. Keep this in sync with len(storage.Attribute{})
// (storage enforces the match with a compile-time assertion).
const MaxAttributeBytes = 32

// NewKeyword creates an interned keyword.
// Accepts both ":foo/bar" and "foo/bar" formats (auto-prefixes colon).
func NewKeyword(s string) Keyword {
	if len(s) == 0 || s[0] != ':' {
		s = ":" + s
	}
	return InternKeyword(s)
}

// String returns the keyword string
func (k Keyword) String() string {
	if k == nil {
		return ""
	}
	return k.value
}

// Compare compares two keywords using pointer equality first.
// Since all Keywords are interned, pointer equality implies value equality.
// Panics if two different pointers have the same string (indicates interning bug).
func (k Keyword) Compare(other Keyword) int {
	if k == nil && other == nil {
		return 0
	}
	if k == nil {
		return -1
	}
	if other == nil {
		return 1
	}
	// Pointer equality for interned keywords
	if k == other {
		return 0
	}
	// Different pointers - compare strings for ordering
	cmp := strings.Compare(k.value, other.value)
	if cmp == 0 {
		panic(fmt.Sprintf("BUG: two Keyword pointers with same value %q in Compare - interning failed", k.value))
	}
	return cmp
}

// Equal checks if two keywords are equal using pointer comparison.
// Since all Keywords are interned, pointer equality implies value equality.
// Panics if two different pointers have the same string (indicates interning bug).
func (k Keyword) Equal(other Keyword) bool {
	if k == other {
		return true
	}
	if k == nil || other == nil {
		return false
	}
	// Different pointers - if they have the same value, interning is broken
	if k.value == other.value {
		panic(fmt.Sprintf("BUG: two Keyword pointers with same value %q in Equal - interning failed", k.value))
	}
	return false
}

// Bytes returns the keyword as bytes
func (k Keyword) Bytes() []byte {
	if k == nil {
		return nil
	}
	return []byte(k.value)
}

// Namespace returns the namespace part of a qualified keyword.
// ":scenario/premise" → "scenario"
// ":foo" → "" (unqualified)
// ":/" → "" (slash is the name, not separator)
func (k Keyword) Namespace() string {
	if k == nil {
		return ""
	}
	s := k.value
	if len(s) > 0 && s[0] == ':' {
		s = s[1:]
	}
	// Edge case: just "/" means no namespace (slash is the name)
	if s == "/" {
		return ""
	}
	idx := strings.Index(s, "/")
	if idx == -1 {
		return ""
	}
	return s[:idx]
}

// Name returns the local name part of a keyword.
// ":scenario/premise" → "premise"
// ":scenario/characterEval:Alice" → "characterEval:Alice"
// ":foo" → "foo"
// ":/" → "/" (slash is the name)
func (k Keyword) Name() string {
	if k == nil {
		return ""
	}
	s := k.value
	if len(s) > 0 && s[0] == ':' {
		s = s[1:]
	}
	// Edge case: just "/" means name is "/"
	if s == "/" {
		return "/"
	}
	idx := strings.Index(s, "/")
	if idx == -1 {
		return s
	}
	return s[idx+1:]
}

// IsQualified returns true if the keyword has a namespace.
func (k Keyword) IsQualified() bool {
	if k == nil {
		return false
	}
	s := k.value
	if len(s) > 0 && s[0] == ':' {
		s = s[1:]
	}
	// Edge case: just "/" is not qualified (slash is the name)
	if s == "/" {
		return false
	}
	return strings.Contains(s, "/")
}

// InNamespace returns true if the keyword's namespace matches ns.
func (k Keyword) InNamespace(ns string) bool {
	return k.Namespace() == ns
}

// Matches returns true if keyword matches pattern.
// Wildcard "*" matches any namespace or name part.
// k.Matches(Kw("scenario", "*")) - any name in scenario namespace
// k.Matches(Kw("*", "premise")) - premise in any namespace
func (k Keyword) Matches(pattern Keyword) bool {
	if k == nil || pattern == nil {
		return k == pattern
	}
	pns := pattern.Namespace()
	pname := pattern.Name()
	if pns != "*" && pns != k.Namespace() {
		return false
	}
	if pname != "*" && pname != k.Name() {
		return false
	}
	return true
}

// symbol is the unexported base type for query symbols.
// Symbols represent names that resolve to something: variables (?x),
// data sources ($users), function references, etc.
// Unlike keywords, symbols are not persisted to storage — they exist only
// in query ASTs and execution contexts.
type symbol struct {
	value string // The symbol string (e.g., "?x", "$users")
}

// Symbol is the exported pointer type, always interned.
type Symbol = *symbol

// NewSymbol creates an interned symbol.
// Panics on empty string — use nil for absent symbols.
func NewSymbol(s string) Symbol {
	if s == "" {
		panic("NewSymbol: empty string is not a valid symbol (use nil for absent)")
	}
	return internSymbol(s)
}

// String returns the symbol string
func (s *symbol) String() string {
	if s == nil {
		return ""
	}
	return s.value
}

// IsVariable returns true if this symbol represents a query variable (starts with ?)
func (s *symbol) IsVariable() bool {
	return s != nil && len(s.value) > 0 && s.value[0] == '?'
}

// IsSource returns true if this symbol represents a data source (starts with $)
func (s *symbol) IsSource() bool {
	return s != nil && len(s.value) > 0 && s.value[0] == '$'
}

// Compare compares two symbols using pointer equality first.
// Since all Symbols are interned, pointer equality implies value equality.
// Panics if two different pointers have the same string (indicates interning bug).
func (s *symbol) Compare(other Symbol) int {
	if s == nil && other == nil {
		return 0
	}
	if s == nil {
		return -1
	}
	if other == nil {
		return 1
	}
	// Pointer equality for interned symbols
	if s == other {
		return 0
	}
	// Different pointers - compare strings for ordering
	cmp := strings.Compare(s.value, other.value)
	if cmp == 0 {
		panic(fmt.Sprintf("BUG: two Symbol pointers with same value %q in Compare - interning failed", s.value))
	}
	return cmp
}

// Equal checks if two symbols are equal using pointer comparison.
// Since all Symbols are interned, pointer equality implies value equality.
// Panics if two different pointers have the same string (indicates interning bug).
func (s *symbol) Equal(other Symbol) bool {
	if s == other {
		return true
	}
	if s == nil || other == nil {
		return false
	}
	// Different pointers - if they have the same value, interning is broken
	if s.value == other.value {
		panic(fmt.Sprintf("BUG: two Symbol pointers with same value %q in Equal - interning failed", s.value))
	}
	return false
}

// String returns a string representation of the Datom
func (d Datom) String() string {
	return fmt.Sprintf("[%s %s %v %s]", d.E, d.A, d.V, d.Tx)
}
