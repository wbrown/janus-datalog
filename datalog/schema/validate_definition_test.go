package schema

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// What the schema admits as a definition is one statement: each keyword the
// definition carries drawn from its vocabulary, an ident within the width
// storage holds, and NeverZeroValue only beside a value type.
// ValidateDefinition states it, Add panics with its error, and the builder's
// Build returns its error.
func TestValidateDefinition_StatesWhatAddPanicsOnAndBuildReturns(t *testing.T) {
	const long = ":order/shipping-address-line-two2"
	require.Greater(t, len(long), datalog.MaxAttributeBytes)
	tooLong := fmt.Sprintf("attribute %s is %d bytes, exceeds the %d-byte storage limit", long, len(long), datalog.MaxAttributeBytes)
	refused := []struct {
		name    string
		def     *AttributeDefinition
		message string
	}{
		{"a cardinality keyword as the value type", &AttributeDefinition{Ident: datalog.NewKeyword(":t/a"), ValueType: CardinalityMany, Cardinality: CardinalityOne}, "attribute :t/a: :db.cardinality/many is not a value type"},
		{"a value-type keyword as the cardinality", &AttributeDefinition{Ident: datalog.NewKeyword(":t/a"), ValueType: TypeString, Cardinality: TypeString}, "attribute :t/a: :db.type/string is not a cardinality"},
		{"a keyword outside the vocabulary as the cardinality", &AttributeDefinition{Ident: datalog.NewKeyword(":t/a"), ValueType: TypeString, Cardinality: datalog.NewKeyword(":db.cardinality/bogus")}, "attribute :t/a: :db.cardinality/bogus is not a cardinality"},
		{"a cardinality keyword as the uniqueness constraint", &AttributeDefinition{Ident: datalog.NewKeyword(":t/a"), ValueType: TypeString, Cardinality: CardinalityOne, Unique: CardinalityOne}, "attribute :t/a: :db.cardinality/one is not a uniqueness constraint"},
		{"an ident longer than storable", &AttributeDefinition{Ident: datalog.NewKeyword(long), ValueType: TypeString, Cardinality: CardinalityOne}, tooLong},
		{"NeverZeroValue with no value type", &AttributeDefinition{Ident: datalog.NewKeyword(":t/a"), Cardinality: CardinalityOne, NeverZeroValue: true}, "attribute :t/a: NeverZeroValue requires a value type"},
	}
	for _, c := range refused {
		t.Run(c.name, func(t *testing.T) {
			require.EqualError(t, ValidateDefinition(c.def), c.message)
			require.PanicsWithError(t, c.message, func() { NewSchema().Add(c.def) }, "Add panics with the same error")
		})
	}
	admitted := []*AttributeDefinition{
		{Ident: datalog.NewKeyword(":t/a"), ValueType: TypeString, Cardinality: CardinalityOne},
		{Ident: datalog.NewKeyword(":t/b"), Cardinality: CardinalityMany},
		{Ident: datalog.NewKeyword(":t/c"), ValueType: TypeRef, Cardinality: CardinalityVector, Unique: UniqueValue, NeverZeroValue: true, UniqueElements: true},
	}
	for _, def := range admitted {
		require.NoError(t, ValidateDefinition(def), def.Ident)
		require.NotPanics(t, func() { NewSchema().Add(def) }, def.Ident)
	}
	built := []struct {
		name    string
		build   func() (*Schema, error)
		message string
	}{
		{"an ident longer than storable", func() (*Schema, error) { return NewBuilder().Attribute(long).Type(TypeString).Add().Build() }, tooLong},
		{"NeverZeroValue with no value type", func() (*Schema, error) { return NewBuilder().Attribute(":t/a").NeverZeroValue().Add().Build() }, "attribute :t/a: NeverZeroValue requires a value type"},
	}
	for _, c := range built {
		t.Run("the builder returns the same error for "+c.name, func(t *testing.T) {
			_, err := c.build()
			require.ErrorContains(t, err, c.message)
		})
	}
}
