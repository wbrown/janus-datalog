package schema

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

func TestValidateValueString(t *testing.T) {
	assert.NoError(t, ValidateValue("hello", TypeString))
	assert.NoError(t, ValidateValue("", TypeString))
	assert.Error(t, ValidateValue(123, TypeString))
	assert.Error(t, ValidateValue(true, TypeString))
}

func TestValidateValueLong(t *testing.T) {
	assert.NoError(t, ValidateValue(int64(42), TypeLong))
	assert.NoError(t, ValidateValue(int(42), TypeLong))
	assert.NoError(t, ValidateValue(int32(42), TypeLong))
	assert.Error(t, ValidateValue("42", TypeLong))
	assert.Error(t, ValidateValue(42.5, TypeLong))
}

func TestValidateValueDouble(t *testing.T) {
	assert.NoError(t, ValidateValue(float64(3.14), TypeDouble))
	assert.NoError(t, ValidateValue(float32(3.14), TypeDouble))
	assert.Error(t, ValidateValue(int64(42), TypeDouble))
	assert.Error(t, ValidateValue("3.14", TypeDouble))
}

func TestValidateValueBoolean(t *testing.T) {
	assert.NoError(t, ValidateValue(true, TypeBoolean))
	assert.NoError(t, ValidateValue(false, TypeBoolean))
	assert.Error(t, ValidateValue(1, TypeBoolean))
	assert.Error(t, ValidateValue("true", TypeBoolean))
}

func TestValidateValueInstant(t *testing.T) {
	assert.NoError(t, ValidateValue(time.Now(), TypeInstant))
	assert.NoError(t, ValidateValue(time.Time{}, TypeInstant))
	assert.Error(t, ValidateValue("2023-01-01", TypeInstant))
	assert.Error(t, ValidateValue(int64(1234567890), TypeInstant))
}

func TestValidateValueBytes(t *testing.T) {
	assert.NoError(t, ValidateValue([]byte{1, 2, 3}, TypeBytes))
	assert.NoError(t, ValidateValue([]byte{}, TypeBytes))
	assert.Error(t, ValidateValue("bytes", TypeBytes))
	assert.Error(t, ValidateValue([]int{1, 2, 3}, TypeBytes))
}

func TestValidateValueRef(t *testing.T) {
	id := datalog.NewIdentity("test:entity")
	assert.NoError(t, ValidateValue(id, TypeRef))
	assert.NoError(t, ValidateValue(&id, TypeRef))
	assert.Error(t, ValidateValue("test:entity", TypeRef))
	assert.Error(t, ValidateValue(12345, TypeRef))
}

func TestValidateValueKeyword(t *testing.T) {
	kw := datalog.NewKeyword(":test/keyword")
	assert.NoError(t, ValidateValue(kw, TypeKeyword))
	assert.NoError(t, ValidateValue(&kw, TypeKeyword))
	assert.Error(t, ValidateValue(":test/keyword", TypeKeyword))
	assert.Error(t, ValidateValue("keyword", TypeKeyword))
}

func TestValidateValueEmptyType(t *testing.T) {
	// Empty type means no constraint
	assert.NoError(t, ValidateValue("anything", ""))
	assert.NoError(t, ValidateValue(123, ""))
	assert.NoError(t, ValidateValue(nil, ""))
}

func TestValidateDatom(t *testing.T) {
	schema, err := NewBuilder().
		Attribute(":person/name").Type(TypeString).Add().
		Attribute(":person/age").Type(TypeLong).Add().
		Attribute(":person/friend").Type(TypeRef).Add().
		Build()
	require.NoError(t, err)

	// Valid values
	assert.NoError(t, ValidateDatom(schema, kw(":person/name"), "Alice"))
	assert.NoError(t, ValidateDatom(schema, kw(":person/age"), int64(30)))
	assert.NoError(t, ValidateDatom(schema, kw(":person/friend"), datalog.NewIdentity("bob")))

	// Invalid values
	err = ValidateDatom(schema, kw(":person/name"), 123)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected db.type/string")

	err = ValidateDatom(schema, kw(":person/age"), "thirty")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected db.type/long")

	// Unknown attribute - should pass (additive schema)
	assert.NoError(t, ValidateDatom(schema, kw(":person/unknown"), "anything"))
}

func TestValidateDatomNilSchema(t *testing.T) {
	// nil schema should allow everything
	assert.NoError(t, ValidateDatom(nil, kw(":any/attr"), "any value"))
	assert.NoError(t, ValidateDatom(nil, kw(":any/attr"), 123))
}

func TestValidateDatomEmptySchema(t *testing.T) {
	schema := NewSchema()
	// Empty schema should allow everything
	assert.NoError(t, ValidateDatom(schema, kw(":any/attr"), "any value"))
}

func TestValidateValueErrorMessage(t *testing.T) {
	err := ValidateValue(123, TypeString)
	require.Error(t, err)
	// Error message should include both expected type and actual type
	assert.Contains(t, err.Error(), "db.type/string")
	assert.Contains(t, err.Error(), "int")
}
