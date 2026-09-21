package schema

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

// ValidateValue checks if a value matches the expected type
// Returns nil if valid, error if type mismatch
func ValidateValue(value interface{}, expected datalog.Keyword) error {
	if expected == nil {
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

	case TypeSymbol:
		_, ok = value.(datalog.Symbol)
		actualType = "Symbol"

	case TypeTx:
		switch value.(type) {
		case datalog.ElementID:
			ok = true
		case *datalog.ElementID:
			ok = true
		}
		actualType = "ElementID"

	default:
		// Unknown type constraint, allow anything
		return nil
	}

	if !ok {
		return fmt.Errorf("expected %s (%s), got %T", expected, actualType, value)
	}
	return nil
}

// IsZeroValue reports whether value is the zero value of valueType: the Go
// zero of the type the store maps that value type to. A nil value is zero for
// every type. For TypeBytes a zero-length slice is zero as well as a nil one,
// since both carry the same nothing. Integers and floats of every width
// ValidateValue admits are judged by magnitude, and a *ElementID by what it
// points to. A value of the wrong Go type is not the zero value; ValidateValue
// is where it is rejected.
//
// It panics on a keyword outside the value-type vocabulary. Schema.Add admits
// only the closed set, so through a schema the arm is unreachable; a direct
// caller reaching it has passed a keyword from another vocabulary.
func IsZeroValue(value interface{}, valueType datalog.Keyword) bool {
	if value == nil {
		return true
	}
	switch valueType {
	case TypeString:
		s, ok := value.(string)
		return ok && s == ""

	case TypeLong:
		switch n := value.(type) {
		case int64:
			return n == 0
		case int:
			return n == 0
		case int32:
			return n == 0
		case int16:
			return n == 0
		case int8:
			return n == 0
		}
		return false

	case TypeDouble:
		switch f := value.(type) {
		case float64:
			return f == 0
		case float32:
			return f == 0
		}
		return false

	case TypeBoolean:
		b, ok := value.(bool)
		return ok && !b

	case TypeInstant:
		t, ok := value.(time.Time)
		return ok && t.IsZero()

	case TypeBytes:
		b, ok := value.([]byte)
		return ok && len(b) == 0

	case TypeRef:
		id, ok := value.(datalog.Identity)
		return ok && id == nil

	case TypeKeyword:
		kw, ok := value.(datalog.Keyword)
		return ok && kw == nil

	case TypeSymbol:
		sym, ok := value.(datalog.Symbol)
		return ok && sym == nil

	case TypeTx:
		switch id := value.(type) {
		case datalog.ElementID:
			return id.IsZero()
		case *datalog.ElementID:
			return id == nil || id.IsZero()
		}
		return false
	}
	panic(fmt.Errorf("IsZeroValue: %s is not a value type", valueType))
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
	if def.ValueType != nil {
		if err := ValidateValue(value, def.ValueType); err != nil {
			return err
		}
	}

	// After the type check, so the value judged is one of the declared type.
	// Schema.Add guarantees a ValueType wherever the flag is set.
	if def.NeverZeroValue && IsZeroValue(value, def.ValueType) {
		return fmt.Errorf("the %s zero value is not a value of a NeverZeroValue attribute", def.ValueType)
	}

	return nil
}
