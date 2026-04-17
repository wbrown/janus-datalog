package schema

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSchemaBasic(t *testing.T) {
	input := `{:person/name {:db/valueType :db.type/string}}`

	schema, err := ParseSchema(input)
	require.NoError(t, err)
	require.NotNil(t, schema)

	def := schema.GetAttribute(kw(":person/name"))
	require.NotNil(t, def)
	assert.Equal(t, TypeString, def.ValueType)
	assert.Equal(t, CardinalityOne, def.Cardinality) // Default
}

func TestParseSchemaMultipleAttributes(t *testing.T) {
	input := `{:person/name   {:db/valueType   :db.type/string
                  :db/cardinality :db.cardinality/one}
 :person/age    {:db/valueType   :db.type/long
                  :db/cardinality :db.cardinality/one}
 :person/friends {:db/valueType   :db.type/ref
                   :db/cardinality :db.cardinality/many}}`

	schema, err := ParseSchema(input)
	require.NoError(t, err)
	assert.Equal(t, 3, schema.Count())

	// Check name
	name := schema.GetAttribute(kw(":person/name"))
	require.NotNil(t, name)
	assert.Equal(t, TypeString, name.ValueType)
	assert.Equal(t, CardinalityOne, name.Cardinality)

	// Check age
	age := schema.GetAttribute(kw(":person/age"))
	require.NotNil(t, age)
	assert.Equal(t, TypeLong, age.ValueType)

	// Check friends
	friends := schema.GetAttribute(kw(":person/friends"))
	require.NotNil(t, friends)
	assert.Equal(t, TypeRef, friends.ValueType)
	assert.Equal(t, CardinalityMany, friends.Cardinality)
	assert.True(t, schema.IsRef(kw(":person/friends")))
	assert.True(t, schema.IsMany(kw(":person/friends")))
}

func TestParseSchemaAllTypes(t *testing.T) {
	input := `{:test/string  {:db/valueType :db.type/string}
 :test/long    {:db/valueType :db.type/long}
 :test/double  {:db/valueType :db.type/double}
 :test/boolean {:db/valueType :db.type/boolean}
 :test/instant {:db/valueType :db.type/instant}
 :test/bytes   {:db/valueType :db.type/bytes}
 :test/ref     {:db/valueType :db.type/ref}
 :test/keyword {:db/valueType :db.type/keyword}}`

	schema, err := ParseSchema(input)
	require.NoError(t, err)
	assert.Equal(t, 8, schema.Count())

	assert.Equal(t, TypeString, schema.GetAttribute(kw(":test/string")).ValueType)
	assert.Equal(t, TypeLong, schema.GetAttribute(kw(":test/long")).ValueType)
	assert.Equal(t, TypeDouble, schema.GetAttribute(kw(":test/double")).ValueType)
	assert.Equal(t, TypeBoolean, schema.GetAttribute(kw(":test/boolean")).ValueType)
	assert.Equal(t, TypeInstant, schema.GetAttribute(kw(":test/instant")).ValueType)
	assert.Equal(t, TypeBytes, schema.GetAttribute(kw(":test/bytes")).ValueType)
	assert.Equal(t, TypeRef, schema.GetAttribute(kw(":test/ref")).ValueType)
	assert.Equal(t, TypeKeyword, schema.GetAttribute(kw(":test/keyword")).ValueType)
}

func TestParseSchemaWithUnique(t *testing.T) {
	input := `{:user/email {:db/valueType :db.type/string
                :db/unique :db.unique/value}
 :user/id    {:db/valueType :db.type/string
               :db/unique :db.unique/identity}}`

	schema, err := ParseSchema(input)
	require.NoError(t, err)

	email := schema.GetAttribute(kw(":user/email"))
	require.NotNil(t, email)
	assert.Equal(t, UniqueValue, email.Unique)

	id := schema.GetAttribute(kw(":user/id"))
	require.NotNil(t, id)
	assert.Equal(t, UniqueIdentity, id.Unique)
}

func TestParseSchemaWithDoc(t *testing.T) {
	input := `{:person/name {:db/valueType :db.type/string
                :db/doc "The person's full name"}}`

	schema, err := ParseSchema(input)
	require.NoError(t, err)

	name := schema.GetAttribute(kw(":person/name"))
	require.NotNil(t, name)
	assert.Equal(t, "The person's full name", name.Doc)
}

func TestParseSchemaComplex(t *testing.T) {
	// Real-world style schema
	input := `{:user/id       {:db/valueType   :db.type/string
                  :db/unique      :db.unique/identity
                  :db/cardinality :db.cardinality/one
                  :db/doc         "User identifier"}
 :user/name     {:db/valueType   :db.type/string
                  :db/cardinality :db.cardinality/one}
 :user/email    {:db/valueType   :db.type/string
                  :db/unique      :db.unique/value}
 :user/friends  {:db/valueType   :db.type/ref
                  :db/cardinality :db.cardinality/many
                  :db/doc         "References to friend users"}
 :user/created  {:db/valueType   :db.type/instant}}`

	schema, err := ParseSchema(input)
	require.NoError(t, err)
	assert.Equal(t, 5, schema.Count())

	id := schema.GetAttribute(kw(":user/id"))
	assert.Equal(t, TypeString, id.ValueType)
	assert.Equal(t, UniqueIdentity, id.Unique)
	assert.Equal(t, "User identifier", id.Doc)

	friends := schema.GetAttribute(kw(":user/friends"))
	assert.True(t, schema.IsRef(kw(":user/friends")))
	assert.True(t, schema.IsMany(kw(":user/friends")))
	assert.Equal(t, "References to friend users", friends.Doc)
}

func TestParseSchemaErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "not a map",
			input: `[:person/name]`,
			want:  "must be a map",
		},
		{
			name:  "non-keyword ident",
			input: `{"person/name" {:db/valueType :db.type/string}}`,
			want:  "must be keyword",
		},
		{
			name:  "invalid value type",
			input: `{:person/name {:db/valueType :db.type/invalid}}`,
			want:  "unknown value type",
		},
		{
			name:  "invalid cardinality",
			input: `{:person/name {:db/cardinality :db.cardinality/invalid}}`,
			want:  "unknown cardinality",
		},
		{
			name:  "invalid unique",
			input: `{:person/name {:db/unique :db.unique/invalid}}`,
			want:  "unknown unique",
		},
		{
			name:  "doc not string",
			input: `{:person/name {:db/doc 123}}`,
			want:  "must be string",
		},
		{
			name:  "valueType not keyword",
			input: `{:person/name {:db/valueType "string"}}`,
			want:  "must be keyword",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseSchema(tt.input)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestParseSchemaFile(t *testing.T) {
	// Create temp file
	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "schema.edn")

	content := `{:person/name {:db/valueType :db.type/string}
 :person/age  {:db/valueType :db.type/long}}`

	err := os.WriteFile(schemaFile, []byte(content), 0644)
	require.NoError(t, err)

	schema, err := ParseSchemaFile(schemaFile)
	require.NoError(t, err)
	assert.Equal(t, 2, schema.Count())
	assert.Equal(t, TypeString, schema.GetAttribute(kw(":person/name")).ValueType)
}

func TestParseSchemaFileNotFound(t *testing.T) {
	_, err := ParseSchemaFile("/nonexistent/path/schema.edn")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read schema file")
}

func TestParseSchemaEmptyMap(t *testing.T) {
	schema, err := ParseSchema(`{}`)
	require.NoError(t, err)
	assert.False(t, schema.HasSchema())
	assert.Equal(t, 0, schema.Count())
}

// TestParseSchema_CardinalityVector verifies the EDN parser accepts
// :db.cardinality/vector, matching the Go builder API's Vector()
// method. Regression for EXTERNAL_REVIEW_2026_04 item 6 — prior to
// the fix, the EDN path rejected :db.cardinality/vector with
// "unknown cardinality", despite the schema supporting vector
// cardinality everywhere else.
func TestParseSchema_CardinalityVector(t *testing.T) {
	input := `{:person/skills {:db/valueType :db.type/string
	                          :db/cardinality :db.cardinality/vector}}`

	schema, err := ParseSchema(input)
	require.NoError(t, err)

	skills := schema.GetAttribute(kw(":person/skills"))
	require.NotNil(t, skills)
	assert.Equal(t, CardinalityVector, skills.Cardinality)
	assert.Equal(t, TypeString, skills.ValueType)
}

// TestParseSchema_UniqueElements verifies :db/unique-elements true
// round-trips through the EDN parser, matching the Go builder's
// UniqueElements() / OrderedSet() semantics. Required for OrderedSet
// attributes to be expressible in EDN schema files.
func TestParseSchema_UniqueElements(t *testing.T) {
	input := `{:character/prefs {:db/valueType :db.type/string
	                             :db/cardinality :db.cardinality/vector
	                             :db/unique-elements true}}`

	schema, err := ParseSchema(input)
	require.NoError(t, err)

	prefs := schema.GetAttribute(kw(":character/prefs"))
	require.NotNil(t, prefs)
	assert.Equal(t, CardinalityVector, prefs.Cardinality)
	assert.True(t, prefs.UniqueElements,
		":db/unique-elements true should populate AttributeDefinition.UniqueElements")
}
