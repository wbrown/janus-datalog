package schema

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// ValueType represents the allowed types for attribute values
// Uses Datomic-compatible naming conventions
type ValueType string

const (
	TypeString  ValueType = "db.type/string"
	TypeLong    ValueType = "db.type/long"    // int64
	TypeDouble  ValueType = "db.type/double"  // float64
	TypeBoolean ValueType = "db.type/boolean" // bool
	TypeInstant ValueType = "db.type/instant" // time.Time
	TypeBytes   ValueType = "db.type/bytes"   // []byte
	TypeRef     ValueType = "db.type/ref"     // datalog.Identity
	TypeKeyword ValueType = "db.type/keyword" // datalog.Keyword
)

// Cardinality represents how many values an attribute can have
type Cardinality string

const (
	CardinalityOne    Cardinality = "db.cardinality/one"
	CardinalityMany   Cardinality = "db.cardinality/many"
	CardinalityVector Cardinality = "db.cardinality/vector" // Ordered collection (RGA)
)

// Unique represents uniqueness constraints on attribute values
type Unique string

const (
	UniqueNone     Unique = ""                   // No uniqueness (default)
	UniqueValue    Unique = "db.unique/value"    // Value must be globally unique
	UniqueIdentity Unique = "db.unique/identity" // Upsert behavior on conflict
)

// Ordering determines how elements are arranged in a collection.
// Currently derived from Cardinality; will become primary in future toolkit.
type Ordering int

const (
	OrderingNone Ordering = iota // Unordered (registers, sets)
	OrderingRGA                  // Chained positions via AfterRef (Vector)
	// Future: OrderingLSeq (independent positions), OrderingTimestamped
)

// Conflict determines how concurrent writes are resolved.
// Currently derived from Cardinality; will become primary in future toolkit.
type Conflict int

const (
	ConflictLWW     Conflict = iota // Last Writer Wins (highest ElementID)
	ConflictAddWins                 // Add beats concurrent remove at same Lamport
	// Future: ConflictRemoveWins, ConflictMV
)

// AttributeDefinition defines schema for a single attribute
type AttributeDefinition struct {
	Ident          datalog.Keyword // The attribute keyword (e.g., :person/name), interned
	ValueType      ValueType       // Required for type validation
	Cardinality    Cardinality     // Required for Pull API (default: one)
	Unique         Unique          // Optional uniqueness constraint (db.unique/identity, db.unique/value)
	UniqueElements bool            // If true, collection has set semantics (no duplicate values)
	Doc            string          // Optional documentation
}

// GetOrdering returns the ordering strategy for this attribute.
// Currently derived from Cardinality.
func (a *AttributeDefinition) GetOrdering() Ordering {
	if a == nil {
		return OrderingNone
	}
	switch a.Cardinality {
	case CardinalityVector:
		return OrderingRGA
	default:
		return OrderingNone
	}
}

// GetConflict returns the conflict resolution strategy for this attribute.
// Currently derived from Cardinality.
func (a *AttributeDefinition) GetConflict() Conflict {
	if a == nil {
		return ConflictLWW
	}
	switch a.Cardinality {
	case CardinalityOne:
		return ConflictLWW
	default:
		return ConflictAddWins
	}
}

// IsUniqueElements returns true if this attribute enforces element uniqueness.
// True for CardinalityMany (sets are inherently unique), and for Vector with UniqueElements flag.
func (a *AttributeDefinition) IsUniqueElements() bool {
	if a == nil {
		return false
	}
	return a.Cardinality == CardinalityMany || a.UniqueElements
}

// IsOrdered returns true if this attribute maintains element ordering.
func (a *AttributeDefinition) IsOrdered() bool {
	if a == nil {
		return false
	}
	return a.Cardinality == CardinalityVector
}

// IsSet returns true if this attribute has set semantics (unordered, unique).
func (a *AttributeDefinition) IsSet() bool {
	if a == nil {
		return false
	}
	return a.Cardinality == CardinalityMany
}

// IsRegister returns true if this attribute has register semantics (single value, LWW).
func (a *AttributeDefinition) IsRegister() bool {
	if a == nil {
		return false
	}
	return a.Cardinality == CardinalityOne
}

// IsOrderedSet returns true if this attribute has ordered set semantics (ordered + unique).
// This is Vector with UniqueElements enabled.
func (a *AttributeDefinition) IsOrderedSet() bool {
	if a == nil {
		return false
	}
	return a.Cardinality == CardinalityVector && a.UniqueElements
}

// Schema holds all attribute definitions
// Thread-safe for concurrent reads after construction
type Schema struct {
	attributes map[datalog.Keyword]*AttributeDefinition
}

// NewSchema creates an empty schema
func NewSchema() *Schema {
	return &Schema{
		attributes: make(map[datalog.Keyword]*AttributeDefinition),
	}
}

// Add adds an attribute definition to the schema
// Returns the schema for chaining
func (s *Schema) Add(def *AttributeDefinition) *Schema {
	if def == nil {
		return s
	}
	// Set default cardinality if not specified
	if def.Cardinality == "" {
		def.Cardinality = CardinalityOne
	}
	s.attributes[def.Ident] = def
	return s
}

// GetAttribute returns the definition for an attribute, or nil if unknown
func (s *Schema) GetAttribute(attr datalog.Keyword) *AttributeDefinition {
	if s == nil {
		return nil
	}
	return s.attributes[attr]
}

// HasSchema returns true if a schema is available with at least one attribute
func (s *Schema) HasSchema() bool {
	return s != nil && len(s.attributes) > 0
}

// IsRef returns true if the attribute is a reference type
func (s *Schema) IsRef(attr datalog.Keyword) bool {
	def := s.GetAttribute(attr)
	return def != nil && def.ValueType == TypeRef
}

// IsMany returns true if the attribute has cardinality many
func (s *Schema) IsMany(attr datalog.Keyword) bool {
	def := s.GetAttribute(attr)
	return def != nil && def.Cardinality == CardinalityMany
}

// IsVector returns true if the attribute has cardinality vector (ordered collection)
func (s *Schema) IsVector(attr datalog.Keyword) bool {
	def := s.GetAttribute(attr)
	return def != nil && def.Cardinality == CardinalityVector
}

// Cardinality returns the cardinality of an attribute, defaulting to CardinalityOne
func (s *Schema) Cardinality(attr datalog.Keyword) Cardinality {
	def := s.GetAttribute(attr)
	if def == nil {
		return CardinalityOne
	}
	return def.Cardinality
}

// Attributes returns all attribute definitions in the schema
func (s *Schema) Attributes() []*AttributeDefinition {
	if s == nil {
		return nil
	}
	result := make([]*AttributeDefinition, 0, len(s.attributes))
	for _, def := range s.attributes {
		result = append(result, def)
	}
	return result
}

// Count returns the number of attributes in the schema
func (s *Schema) Count() int {
	if s == nil {
		return 0
	}
	return len(s.attributes)
}
