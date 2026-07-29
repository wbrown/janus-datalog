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

// TestBuilderRejectsKeywordFromTheWrongVocabulary pins the builder's membership
// check, which is the only thing standing between a caller and a definition
// carrying a keyword from the wrong vocabulary.
//
// The three schema vocabularies are all datalog.Keyword, so nothing stops
// Type(CardinalityOne) from compiling — a distinct Go type would have, but Go
// forbids methods on a defined type whose underlying type is a pointer, and a
// struct wrapping a keyword is a wrapper type this codebase does not have. So
// each vocabulary keeps a closed set and the builder checks membership. Without
// this test that check is unpinned, and deleting it leaves the suite green
// while a definition silently carries :db.cardinality/one as its value type.
//
// The last case is the control: a builder that rejected everything would pass
// every case above it.
func TestBuilderRejectsKeywordFromTheWrongVocabulary(t *testing.T) {
	for _, tc := range []struct {
		name    string
		build   func(*AttributeBuilder) *AttributeBuilder
		wantErr string
	}{
		{"cardinality as a value type",
			func(ab *AttributeBuilder) *AttributeBuilder { return ab.Type(CardinalityOne) },
			":db.cardinality/one is not a value type"},
		{"unique as a value type",
			func(ab *AttributeBuilder) *AttributeBuilder { return ab.Type(UniqueValue) },
			":db.unique/value is not a value type"},
		{"an unrelated keyword as a value type",
			func(ab *AttributeBuilder) *AttributeBuilder { return ab.Type(datalog.NewKeyword(":not/a-type")) },
			":not/a-type is not a value type"},
		{"value type as a uniqueness constraint",
			func(ab *AttributeBuilder) *AttributeBuilder { return ab.Type(TypeString).Unique(TypeString) },
			":db.type/string is not a uniqueness constraint"},
		{"cardinality as a uniqueness constraint",
			func(ab *AttributeBuilder) *AttributeBuilder { return ab.Type(TypeString).Unique(CardinalityMany) },
			":db.cardinality/many is not a uniqueness constraint"},

		// Absence is written by omitting the call, not by naming it. Accepting
		// nil here would make Unique(nil) and leaving Unique out two ways to
		// say one thing.
		{"nil as a uniqueness constraint",
			func(ab *AttributeBuilder) *AttributeBuilder { return ab.Type(TypeString).Unique(nil) },
			"is not a uniqueness constraint"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBuilder()
			_, err := tc.build(b.Attribute(":person/x")).Add().Build()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr,
				"the error must name the keyword that was rejected")
		})
	}

	t.Run("the right vocabularies build clean", func(t *testing.T) {
		s, err := NewBuilder().
			Attribute(":person/x").Type(TypeString).Unique(UniqueValue).Add().
			Build()
		require.NoError(t, err)
		def := s.GetAttribute(kw(":person/x"))
		require.NotNil(t, def)
		assert.Equal(t, TypeString, def.ValueType)
		assert.Equal(t, UniqueValue, def.Unique)
	})
}

// TestSchemaAddPanicsOnKeywordFromTheWrongVocabulary pins the third entry
// point. ParseSchema and the builder check membership; Add did not, and it is
// the one every definition passes through — it holds the only write into
// s.attributes, so parser and builder both reach the map through it.
//
// It panics rather than returning an error, on the registration convention:
// sql.Register and http.Handle panic with no Must prefix and no error sibling,
// because they declare something during initialization from source literals,
// where a failure is a programmer's mistake and there is no recovery a caller
// could perform. Add declares an attribute and keeps its chaining signature.
// The parsing convention — regexp.MustCompile paired with regexp.Compile — does
// not apply, because Add converts no input.
//
// What reaches the dispatch if this check is absent is not a bad definition
// sitting inert: crdt_resolving_iterator's cardinality switch has no arm for a
// wrong-vocabulary keyword, so every datom in the group is skipped and the
// query returns no tuples with a nil error.
//
// nil is absence and stays legal for ValueType and Unique — a definition that
// has not said which it is. Cardinality's nil is filled with CardinalityOne, as
// it was before.
func TestSchemaAddPanicsOnKeywordFromTheWrongVocabulary(t *testing.T) {
	for _, tc := range []struct {
		name string
		def  *AttributeDefinition
		want string
	}{
		{"cardinality as a value type",
			&AttributeDefinition{Ident: kw(":person/x"), ValueType: CardinalityOne},
			"attribute :person/x: :db.cardinality/one is not a value type"},
		{"unique as a value type",
			&AttributeDefinition{Ident: kw(":person/x"), ValueType: UniqueValue},
			"attribute :person/x: :db.unique/value is not a value type"},
		{"value type as a cardinality",
			&AttributeDefinition{Ident: kw(":person/x"), Cardinality: TypeString},
			"attribute :person/x: :db.type/string is not a cardinality"},
		{"unknown as a declared cardinality",
			&AttributeDefinition{Ident: kw(":person/x"), Cardinality: CardinalityUnknown},
			"attribute :person/x: :db.cardinality/unknown is not a cardinality"},
		{"value type as a uniqueness constraint",
			&AttributeDefinition{Ident: kw(":person/x"), Unique: TypeString},
			"attribute :person/x: :db.type/string is not a uniqueness constraint"},
		{"an unrelated keyword as a cardinality",
			&AttributeDefinition{Ident: kw(":person/x"), Cardinality: datalog.NewKeyword(":not/a-cardinality")},
			"attribute :person/x: :not/a-cardinality is not a cardinality"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.PanicsWithError(t, tc.want, func() {
				NewSchema().Add(tc.def)
			})
		})
	}

	// Controls. A schema that panicked on everything would pass every case above.
	t.Run("a well-formed definition is stored", func(t *testing.T) {
		s := NewSchema().Add(&AttributeDefinition{
			Ident:       kw(":person/x"),
			ValueType:   TypeString,
			Cardinality: CardinalityMany,
			Unique:      UniqueValue,
		})
		def := s.GetAttribute(kw(":person/x"))
		require.NotNil(t, def)
		assert.Equal(t, CardinalityMany, def.Cardinality)
	})

	t.Run("absence is legal and cardinality still defaults", func(t *testing.T) {
		s := NewSchema().Add(&AttributeDefinition{Ident: kw(":person/x")})
		def := s.GetAttribute(kw(":person/x"))
		require.NotNil(t, def)
		assert.Nil(t, def.ValueType, "a definition may decline to name a value type")
		assert.Nil(t, def.Unique, "absence of a uniqueness constraint is written as nil")
		assert.Equal(t, CardinalityOne, def.Cardinality)
	})
}

// TestVocabularyMembershipSurvivesClearInterns pins vocabulary membership across
// ClearInterns.
//
// The three closed sets are map[datalog.Keyword]struct{}, keyed by the pointers
// the define* functions captured at init. Orphan those and the maps still hold
// them while every keyword minted afterwards is a different pointer, so
// membership misses and Add rejects a valid definition.
//
// The keywords are interned fresh from text, the way a parsed schema's arrive.
// Taking them from the package variables would pass on identity alone even with
// fresh interns diverging, which is the case that breaks.
func TestVocabularyMembershipSurvivesClearInterns(t *testing.T) {
	datalog.ClearInterns()

	require.NotPanics(t, func() {
		NewSchema().Add(&AttributeDefinition{
			Ident:       datalog.NewKeyword(":person/tag"),
			ValueType:   datalog.NewKeyword(":db.type/string"),
			Cardinality: datalog.NewKeyword(":db.cardinality/many"),
			Unique:      datalog.NewKeyword(":db.unique/value"),
		})
	}, "the closed sets are keyed by the well-known pointers, so an orphaned "+
		"registry rejects every valid schema")

	// require.Same, not require.Equal: Equal reflect-compares the pointed-to
	// structs and so passes for two distinct pointers carrying one string, which
	// is the orphan case.
	for _, v := range []struct {
		held datalog.Keyword
		name string
	}{
		{CardinalityMany, ":db.cardinality/many"},
		{TypeString, ":db.type/string"},
		{UniqueValue, ":db.unique/value"},
	} {
		require.Same(t, v.held, datalog.NewKeyword(v.name),
			"%s must be the instance its closed set is keyed by", v.name)
	}
}

// TestCardinalitySetIsClosedAtThree reds when defineCardinality gains a member.
// It pins the set rather than the switches that dispatch on it, because
// defineCardinality is the single registration point.
func TestCardinalitySetIsClosedAtThree(t *testing.T) {
	const sweep = "give every switch that dispatches on cardinality an arm for " +
		"the new member first: one with no arm is skipped silently, and the query " +
		"returns zero rows with a nil error."

	require.Len(t, cardinalities, 3, sweep)
	for _, c := range []datalog.Keyword{CardinalityOne, CardinalityMany, CardinalityVector} {
		require.Contains(t, cardinalities, c, sweep)
	}
	require.NotContains(t, cardinalities, CardinalityUnknown,
		"CardinalityUnknown marks an attribute with no definition; it is not "+
			"declarable and must stay outside the parseable set")
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
