package schema

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// Builder provides fluent schema construction
//
// Example usage:
//
//	schema, err := schema.NewBuilder().
//	    Attribute(":person/name").Type(schema.TypeString).Add().
//	    Attribute(":person/friends").Type(schema.TypeRef).Many().Add().
//	    Attribute(":person/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
//	    Build()
type Builder struct {
	schema *Schema
	errors []error
}

// NewBuilder creates a new schema builder
func NewBuilder() *Builder {
	return &Builder{
		schema: NewSchema(),
	}
}

// Attribute starts defining a new attribute
func (b *Builder) Attribute(ident string) *AttributeBuilder {
	return &AttributeBuilder{
		parent: b,
		def: &AttributeDefinition{
			Ident:       datalog.NewKeyword(ident),
			Cardinality: CardinalityOne, // Default
		},
	}
}

// Build finalizes and returns the schema
// Returns error if any attribute definitions were invalid
func (b *Builder) Build() (*Schema, error) {
	if len(b.errors) > 0 {
		return nil, fmt.Errorf("schema errors: %v", b.errors)
	}
	return b.schema, nil
}

// MustBuild is like Build but panics on error
// Useful for static schema definitions
func (b *Builder) MustBuild() *Schema {
	s, err := b.Build()
	if err != nil {
		panic(err)
	}
	return s
}

// AttributeBuilder provides fluent attribute construction
type AttributeBuilder struct {
	parent *Builder
	def    *AttributeDefinition
}

// Type sets the value type for the attribute
func (ab *AttributeBuilder) Type(t ValueType) *AttributeBuilder {
	ab.def.ValueType = t
	return ab
}

// One sets cardinality to one (default)
func (ab *AttributeBuilder) One() *AttributeBuilder {
	ab.def.Cardinality = CardinalityOne
	return ab
}

// Many sets cardinality to many
func (ab *AttributeBuilder) Many() *AttributeBuilder {
	ab.def.Cardinality = CardinalityMany
	return ab
}

// Vector sets cardinality to vector (ordered collection using RGA CRDT)
func (ab *AttributeBuilder) Vector() *AttributeBuilder {
	ab.def.Cardinality = CardinalityVector
	return ab
}

// OrderedSet sets cardinality to vector with unique elements (ordered set using RGA CRDT).
// This is a convenience method equivalent to Vector().UniqueElements(true).
func (ab *AttributeBuilder) OrderedSet() *AttributeBuilder {
	ab.def.Cardinality = CardinalityVector
	ab.def.UniqueElements = true
	return ab
}

// UniqueElements sets whether the collection enforces element uniqueness.
// When true, duplicate values are rejected (set semantics).
// Only meaningful for Vector cardinality; Many is inherently unique.
func (ab *AttributeBuilder) UniqueElements(unique bool) *AttributeBuilder {
	ab.def.UniqueElements = unique
	return ab
}

// Unique sets the uniqueness constraint (db.unique/identity or db.unique/value)
func (ab *AttributeBuilder) Unique(u Unique) *AttributeBuilder {
	ab.def.Unique = u
	return ab
}

// Doc sets the documentation string
func (ab *AttributeBuilder) Doc(doc string) *AttributeBuilder {
	ab.def.Doc = doc
	return ab
}

// Add finalizes this attribute and adds it to the schema
// Returns the builder for chaining more attributes
func (ab *AttributeBuilder) Add() *Builder {
	// Validate attribute definition
	if ab.def.Ident.String() == "" {
		ab.parent.errors = append(ab.parent.errors, fmt.Errorf("attribute ident is required"))
		return ab.parent
	}

	// Reject attribute names too long to store without truncation/aliasing.
	if n := len(ab.def.Ident.String()); n > datalog.MaxAttributeBytes {
		ab.parent.errors = append(ab.parent.errors, fmt.Errorf("attribute %q is %d bytes, exceeds the %d-byte storage limit", ab.def.Ident.String(), n, datalog.MaxAttributeBytes))
		return ab.parent
	}

	ab.parent.schema.Add(ab.def)
	return ab.parent
}
