package schema

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

func TestIsZeroValue(t *testing.T) {
	tests := []struct {
		name      string
		valueType datalog.Keyword
		zero      interface{}
		nonzero   []interface{}
	}{
		{"string", TypeString, "", []interface{}{"x", " "}},
		{"long int64", TypeLong, int64(0), []interface{}{int64(1), int64(-1)}},
		{"long int", TypeLong, int(0), []interface{}{int(1)}},
		{"double", TypeDouble, 0.0, []interface{}{1.0, -1.0}},
		{"boolean", TypeBoolean, false, []interface{}{true}},
		{"instant", TypeInstant, time.Time{}, []interface{}{time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}},
		{"bytes empty", TypeBytes, []byte{}, []interface{}{[]byte{0}}},
		{"bytes nil", TypeBytes, []byte(nil), []interface{}{[]byte{1}}},
		{"ref", TypeRef, datalog.Identity(nil), []interface{}{datalog.NewIdentity("e")}},
		{"keyword", TypeKeyword, datalog.Keyword(nil), []interface{}{datalog.NewKeyword(":k")}},
		{"symbol", TypeSymbol, datalog.Symbol(nil), []interface{}{datalog.NewSymbol("s")}},
		{"tx", TypeTx, datalog.ElementID{}, []interface{}{datalog.ElementID{Lamport: 1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.True(t, IsZeroValue(tt.zero, tt.valueType), "zero value")
			for _, v := range tt.nonzero {
				assert.False(t, IsZeroValue(v, tt.valueType), "non-zero %#v", v)
			}
		})
	}
	assert.True(t, IsZeroValue(nil, TypeString))
}

func TestValidateDatomNeverZeroValue(t *testing.T) {
	s, err := NewBuilder().
		Attribute(":person/bio").Type(TypeString).NeverZeroValue().Add().
		Attribute(":person/age").Type(TypeLong).NeverZeroValue().Add().
		Attribute(":person/name").Type(TypeString).Add().
		Build()
	require.NoError(t, err)

	require.Error(t, ValidateDatom(s, kw(":person/bio"), ""))
	require.NoError(t, ValidateDatom(s, kw(":person/bio"), "hello"))
	require.NoError(t, ValidateDatom(s, kw(":person/bio"), " "))

	require.Error(t, ValidateDatom(s, kw(":person/age"), int64(0)))
	require.NoError(t, ValidateDatom(s, kw(":person/age"), int64(1)))

	// Undeclared: empty string remains a value.
	require.NoError(t, ValidateDatom(s, kw(":person/name"), ""))
}

func TestBuilderNeverZeroValue(t *testing.T) {
	s, err := NewBuilder().
		Attribute(":person/bio").Type(TypeString).NeverZeroValue().Add().
		Build()
	require.NoError(t, err)
	def := s.GetAttribute(kw(":person/bio"))
	require.NotNil(t, def)
	assert.True(t, def.NeverZeroValue)
	assert.Equal(t, TypeString, def.ValueType)
}

func TestBuilderNeverZeroValueRequiresValueType(t *testing.T) {
	_, err := NewBuilder().
		Attribute(":person/bio").NeverZeroValue().Add().
		Build()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NeverZeroValue requires a value type")
}

func TestSchemaAddPanicsOnNeverZeroValueWithoutValueType(t *testing.T) {
	require.PanicsWithError(t,
		"attribute :person/bio: NeverZeroValue requires a value type",
		func() {
			NewSchema().Add(&AttributeDefinition{
				Ident:          kw(":person/bio"),
				NeverZeroValue: true,
			})
		})
}

func TestParseSchemaNeverZeroValue(t *testing.T) {
	s, err := ParseSchema(`{:person/bio {:db/valueType :db.type/string
	                                       :db/neverZeroValue true}}`)
	require.NoError(t, err)
	def := s.GetAttribute(kw(":person/bio"))
	require.NotNil(t, def)
	assert.True(t, def.NeverZeroValue)
	assert.Equal(t, TypeString, def.ValueType)
}

func TestParseSchemaAndBuilderNeverZeroValueMatch(t *testing.T) {
	ednSchema, err := ParseSchema(`{:person/bio {:db/valueType :db.type/string
	                                               :db/cardinality :db.cardinality/one
	                                               :db/neverZeroValue true}}`)
	require.NoError(t, err)
	built, err := NewBuilder().
		Attribute(":person/bio").Type(TypeString).One().NeverZeroValue().Add().
		Build()
	require.NoError(t, err)

	want := ednSchema.GetAttribute(kw(":person/bio"))
	got := built.GetAttribute(kw(":person/bio"))
	require.NotNil(t, want)
	require.NotNil(t, got)
	assert.True(t, want.Ident.Equal(got.Ident))
	assert.True(t, want.ValueType.Equal(got.ValueType))
	assert.True(t, want.Cardinality.Equal(got.Cardinality))
	assert.Equal(t, want.Unique, got.Unique)
	assert.Equal(t, want.UniqueElements, got.UniqueElements)
	assert.Equal(t, want.NeverZeroValue, got.NeverZeroValue)
	assert.Equal(t, want.Doc, got.Doc)
}

func TestParseSchemaNeverZeroValueRequiresValueType(t *testing.T) {
	_, err := ParseSchema(`{:person/bio {:db/neverZeroValue true}}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), ":db/neverZeroValue requires :db/valueType")
}

func TestParseSchemaUnknownKeyIsError(t *testing.T) {
	_, err := ParseSchema(`{:person/name {:db/valueType :db.type/string
	                                      :db/notAKey true}}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown schema key")
	assert.Contains(t, err.Error(), ":db/notAKey")
}

func TestParseSchemaNonKeywordDefinitionKeyIsError(t *testing.T) {
	_, err := ParseSchema(`{:person/name {"db/valueType" :db.type/string}}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "attribute definition key must be keyword")
}

func TestParseSchemaUniqueElementsKebabIsUnknownKey(t *testing.T) {
	_, err := ParseSchema(`{:character/prefs {:db/valueType :db.type/string
	                                          :db/cardinality :db.cardinality/vector
	                                          :db/unique-elements true}}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown schema key")
	assert.Contains(t, err.Error(), ":db/unique-elements")
}

func TestParseSchemaUniqueElementsCamelCase(t *testing.T) {
	s, err := ParseSchema(`{:character/prefs {:db/valueType :db.type/string
	                                          :db/cardinality :db.cardinality/vector
	                                          :db/uniqueElements true}}`)
	require.NoError(t, err)
	prefs := s.GetAttribute(kw(":character/prefs"))
	require.NotNil(t, prefs)
	assert.True(t, prefs.UniqueElements)
}
