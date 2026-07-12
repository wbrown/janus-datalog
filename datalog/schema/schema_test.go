package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

func kw(s string) datalog.Keyword {
	return datalog.NewKeyword(s)
}

func TestSchemaBasic(t *testing.T) {
	s := NewSchema()
	assert.False(t, s.HasSchema())
	assert.Equal(t, 0, s.Count())

	s.Add(&AttributeDefinition{
		Ident:       kw(":person/name"),
		ValueType:   TypeString,
		Cardinality: CardinalityOne,
	})

	assert.True(t, s.HasSchema())
	assert.Equal(t, 1, s.Count())

	def := s.GetAttribute(kw(":person/name"))
	require.NotNil(t, def)
	assert.Equal(t, TypeString, def.ValueType)
	assert.Equal(t, CardinalityOne, def.Cardinality)
}

func TestSchemaIsRef(t *testing.T) {
	s := NewSchema()
	s.Add(&AttributeDefinition{
		Ident:     kw(":person/friend"),
		ValueType: TypeRef,
	})
	s.Add(&AttributeDefinition{
		Ident:     kw(":person/name"),
		ValueType: TypeString,
	})

	assert.True(t, s.IsRef(kw(":person/friend")))
	assert.False(t, s.IsRef(kw(":person/name")))
	assert.False(t, s.IsRef(kw(":unknown/attr")))
}

func TestSchemaIsMany(t *testing.T) {
	s := NewSchema()
	s.Add(&AttributeDefinition{
		Ident:       kw(":person/friends"),
		ValueType:   TypeRef,
		Cardinality: CardinalityMany,
	})
	s.Add(&AttributeDefinition{
		Ident:       kw(":person/name"),
		ValueType:   TypeString,
		Cardinality: CardinalityOne,
	})

	assert.True(t, s.IsMany(kw(":person/friends")))
	assert.False(t, s.IsMany(kw(":person/name")))
	assert.False(t, s.IsMany(kw(":unknown/attr")))
}

func TestSchemaIsVector(t *testing.T) {
	s := NewSchema()
	s.Add(&AttributeDefinition{
		Ident:       kw(":character/skills"),
		ValueType:   TypeString,
		Cardinality: CardinalityVector,
	})
	s.Add(&AttributeDefinition{
		Ident:       kw(":person/name"),
		ValueType:   TypeString,
		Cardinality: CardinalityOne,
	})
	s.Add(&AttributeDefinition{
		Ident:       kw(":person/tags"),
		ValueType:   TypeString,
		Cardinality: CardinalityMany,
	})

	assert.True(t, s.IsVector(kw(":character/skills")))
	assert.False(t, s.IsVector(kw(":person/name")))
	assert.False(t, s.IsVector(kw(":person/tags")))
	assert.False(t, s.IsVector(kw(":unknown/attr")))
}

func TestSchemaCardinality(t *testing.T) {
	s := NewSchema()
	s.Add(&AttributeDefinition{
		Ident:       kw(":attr/one"),
		ValueType:   TypeString,
		Cardinality: CardinalityOne,
	})
	s.Add(&AttributeDefinition{
		Ident:       kw(":attr/many"),
		ValueType:   TypeString,
		Cardinality: CardinalityMany,
	})
	s.Add(&AttributeDefinition{
		Ident:       kw(":attr/vector"),
		ValueType:   TypeString,
		Cardinality: CardinalityVector,
	})

	assert.Equal(t, CardinalityOne, s.Cardinality(kw(":attr/one")))
	assert.Equal(t, CardinalityMany, s.Cardinality(kw(":attr/many")))
	assert.Equal(t, CardinalityVector, s.Cardinality(kw(":attr/vector")))
	// Unknown attributes default to CardinalityOne
	assert.Equal(t, CardinalityOne, s.Cardinality(kw(":unknown/attr")))
}

func TestSchemaDefaultCardinality(t *testing.T) {
	s := NewSchema()
	// Add without explicit cardinality
	s.Add(&AttributeDefinition{
		Ident:     kw(":person/name"),
		ValueType: TypeString,
	})

	def := s.GetAttribute(kw(":person/name"))
	require.NotNil(t, def)
	assert.Equal(t, CardinalityOne, def.Cardinality)
}

func TestSchemaNil(t *testing.T) {
	var s *Schema = nil

	assert.False(t, s.HasSchema())
	assert.Nil(t, s.GetAttribute(kw(":person/name")))
	assert.False(t, s.IsRef(kw(":person/name")))
	assert.False(t, s.IsMany(kw(":person/name")))
	assert.Equal(t, 0, s.Count())
	assert.Nil(t, s.Attributes())
}

func TestBuilderBasic(t *testing.T) {
	schema, err := NewBuilder().
		Attribute(":person/name").Type(TypeString).Add().
		Attribute(":person/age").Type(TypeLong).Add().
		Build()

	require.NoError(t, err)
	require.NotNil(t, schema)
	assert.Equal(t, 2, schema.Count())

	name := schema.GetAttribute(kw(":person/name"))
	require.NotNil(t, name)
	assert.Equal(t, TypeString, name.ValueType)
	assert.Equal(t, CardinalityOne, name.Cardinality)

	age := schema.GetAttribute(kw(":person/age"))
	require.NotNil(t, age)
	assert.Equal(t, TypeLong, age.ValueType)
}

func TestBuilderWithCardinality(t *testing.T) {
	schema, err := NewBuilder().
		Attribute(":person/name").Type(TypeString).One().Add().
		Attribute(":person/friends").Type(TypeRef).Many().Add().
		Attribute(":character/skills").Type(TypeString).Vector().Add().
		Build()

	require.NoError(t, err)

	assert.False(t, schema.IsMany(kw(":person/name")))
	assert.True(t, schema.IsMany(kw(":person/friends")))
	assert.True(t, schema.IsVector(kw(":character/skills")))
	assert.Equal(t, CardinalityVector, schema.Cardinality(kw(":character/skills")))
}

func TestBuilderWithUnique(t *testing.T) {
	schema, err := NewBuilder().
		Attribute(":person/email").Type(TypeString).Unique(UniqueValue).Add().
		Attribute(":person/id").Type(TypeString).Unique(UniqueIdentity).Add().
		Build()

	require.NoError(t, err)

	email := schema.GetAttribute(kw(":person/email"))
	require.NotNil(t, email)
	assert.Equal(t, UniqueValue, email.Unique)

	id := schema.GetAttribute(kw(":person/id"))
	require.NotNil(t, id)
	assert.Equal(t, UniqueIdentity, id.Unique)
}

func TestBuilderWithDoc(t *testing.T) {
	schema, err := NewBuilder().
		Attribute(":person/name").Type(TypeString).Doc("The person's full name").Add().
		Build()

	require.NoError(t, err)

	name := schema.GetAttribute(kw(":person/name"))
	require.NotNil(t, name)
	assert.Equal(t, "The person's full name", name.Doc)
}

func TestBuilderMustBuild(t *testing.T) {
	schema := NewBuilder().
		Attribute(":person/name").Type(TypeString).Add().
		MustBuild()

	require.NotNil(t, schema)
	assert.Equal(t, 1, schema.Count())
}

func TestBuilderAllTypes(t *testing.T) {
	schema, err := NewBuilder().
		Attribute(":test/string").Type(TypeString).Add().
		Attribute(":test/long").Type(TypeLong).Add().
		Attribute(":test/double").Type(TypeDouble).Add().
		Attribute(":test/boolean").Type(TypeBoolean).Add().
		Attribute(":test/instant").Type(TypeInstant).Add().
		Attribute(":test/bytes").Type(TypeBytes).Add().
		Attribute(":test/ref").Type(TypeRef).Add().
		Attribute(":test/keyword").Type(TypeKeyword).Add().
		Attribute(":test/symbol").Type(TypeSymbol).Add().
		Build()

	require.NoError(t, err)
	assert.Equal(t, 9, schema.Count())

	// Verify each type
	assert.Equal(t, TypeString, schema.GetAttribute(kw(":test/string")).ValueType)
	assert.Equal(t, TypeLong, schema.GetAttribute(kw(":test/long")).ValueType)
	assert.Equal(t, TypeDouble, schema.GetAttribute(kw(":test/double")).ValueType)
	assert.Equal(t, TypeBoolean, schema.GetAttribute(kw(":test/boolean")).ValueType)
	assert.Equal(t, TypeInstant, schema.GetAttribute(kw(":test/instant")).ValueType)
	assert.Equal(t, TypeBytes, schema.GetAttribute(kw(":test/bytes")).ValueType)
	assert.Equal(t, TypeRef, schema.GetAttribute(kw(":test/ref")).ValueType)
	assert.Equal(t, TypeKeyword, schema.GetAttribute(kw(":test/keyword")).ValueType)
	assert.Equal(t, TypeSymbol, schema.GetAttribute(kw(":test/symbol")).ValueType)
}

func TestBuilderChaining(t *testing.T) {
	// Complex real-world example
	schema, err := NewBuilder().
		Attribute(":user/id").Type(TypeString).Unique(UniqueIdentity).Doc("User identifier").Add().
		Attribute(":user/name").Type(TypeString).One().Add().
		Attribute(":user/email").Type(TypeString).Unique(UniqueValue).Add().
		Attribute(":user/friends").Type(TypeRef).Many().Doc("User's friends").Add().
		Attribute(":user/created-at").Type(TypeInstant).Add().
		Attribute(":user/settings").Type(TypeBytes).Add().
		Build()

	require.NoError(t, err)
	assert.Equal(t, 6, schema.Count())

	// Check specific attributes
	id := schema.GetAttribute(kw(":user/id"))
	assert.Equal(t, UniqueIdentity, id.Unique)
	assert.Equal(t, "User identifier", id.Doc)

	friends := schema.GetAttribute(kw(":user/friends"))
	assert.True(t, schema.IsRef(kw(":user/friends")))
	assert.True(t, schema.IsMany(kw(":user/friends")))
	assert.Equal(t, "User's friends", friends.Doc)
}

func TestSchemaAttributes(t *testing.T) {
	schema, _ := NewBuilder().
		Attribute(":person/name").Type(TypeString).Add().
		Attribute(":person/age").Type(TypeLong).Add().
		Build()

	attrs := schema.Attributes()
	assert.Len(t, attrs, 2)

	// Verify both attributes are present (order not guaranteed due to map)
	idents := make(map[string]bool)
	for _, attr := range attrs {
		idents[attr.Ident.String()] = true
	}
	assert.True(t, idents[":person/name"])
	assert.True(t, idents[":person/age"])
}
