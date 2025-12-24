package schema

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

// ValidateValue checks if a value matches the expected type
// Returns nil if valid, error if type mismatch
func ValidateValue(value interface{}, expected ValueType) error {
	if expected == "" {
		return nil // No type constraint
	}

	var ok bool
	var actualType string

	switch expected {
	case TypeString:
		_, ok = value.(string)
		actualType = "string"

	case TypeLong:
		switch value.(type) {
		case int64, int, int32, int16, int8:
			ok = true
		}
		actualType = "int64"

	case TypeDouble:
		switch value.(type) {
		case float64, float32:
			ok = true
		}
		actualType = "float64"

	case TypeBoolean:
		_, ok = value.(bool)
		actualType = "bool"

	case TypeInstant:
		_, ok = value.(time.Time)
		actualType = "time.Time"

	case TypeBytes:
		_, ok = value.([]byte)
		actualType = "[]byte"

	case TypeRef:
		// Identity is always a pointer type now
		_, ok = value.(datalog.Identity)
		actualType = "Identity"

	case TypeKeyword:
		_, ok = value.(datalog.Keyword)
		if !ok {
			// Also check for pointer type
			_, ok = value.(datalog.Keyword)
		}
		actualType = "Keyword"

	default:
		// Unknown type constraint, allow anything
		return nil
	}

	if !ok {
		return fmt.Errorf("expected %s (%s), got %T", expected, actualType, value)
	}
	return nil
}

// ValidateDatom validates a single attribute-value pair against schema
// Returns nil if valid or if schema doesn't define the attribute
func ValidateDatom(s SchemaProvider, attr datalog.Keyword, value interface{}) error {
	if s == nil || !s.HasSchema() {
		return nil // No schema = no validation
	}

	def := s.GetAttribute(attr)
	if def == nil {
		return nil // Unknown attribute = allow (additive schema)
	}

	// Type validation
	if def.ValueType != "" {
		if err := ValidateValue(value, def.ValueType); err != nil {
			return err
		}
	}

	return nil
}
