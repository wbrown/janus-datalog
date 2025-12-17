package reflect

import (
	"fmt"
	"reflect"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Well-known types for comparison
var (
	timeType     = reflect.TypeOf(time.Time{})
	identityType = reflect.TypeOf(datalog.Identity{})
	keywordType  = reflect.TypeOf(datalog.Keyword{})
)

// GoTypeToSchemaType maps a Go reflect.Type to a schema.ValueType
func GoTypeToSchemaType(t reflect.Type) (schema.ValueType, error) {
	// Handle pointers by dereferencing
	if t.Kind() == reflect.Ptr {
		return GoTypeToSchemaType(t.Elem())
	}

	switch t.Kind() {
	case reflect.String:
		return schema.TypeString, nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return schema.TypeLong, nil

	case reflect.Float32, reflect.Float64:
		return schema.TypeDouble, nil

	case reflect.Bool:
		return schema.TypeBoolean, nil

	case reflect.Slice:
		// []byte is TypeBytes
		if t.Elem().Kind() == reflect.Uint8 {
			return schema.TypeBytes, nil
		}
		// Slice of other types = cardinality-many, recurse on element type
		return GoTypeToSchemaType(t.Elem())

	case reflect.Struct:
		// Check well-known types
		if t == timeType {
			return schema.TypeInstant, nil
		}
		if t == identityType {
			return schema.TypeRef, nil
		}
		if t == keywordType {
			return schema.TypeKeyword, nil
		}
		// Other struct = nested reference
		return schema.TypeRef, nil
	}

	return "", fmt.Errorf("unsupported type: %s", t)
}

// InferCardinality determines cardinality from a Go type
// Slices (except []byte) are cardinality-many, everything else is one
func InferCardinality(t reflect.Type) schema.Cardinality {
	// Handle pointers
	if t.Kind() == reflect.Ptr {
		return InferCardinality(t.Elem())
	}

	if t.Kind() == reflect.Slice && t.Elem().Kind() != reflect.Uint8 {
		return schema.CardinalityMany
	}
	return schema.CardinalityOne
}

// IsRefType checks if a Go type represents a reference to another entity
func IsRefType(t reflect.Type) bool {
	// Handle pointers
	if t.Kind() == reflect.Ptr {
		return IsRefType(t.Elem())
	}

	// Direct Identity type
	if t == identityType {
		return true
	}

	// Slice of Identity
	if t.Kind() == reflect.Slice && t.Elem() == identityType {
		return true
	}

	// Struct (other than well-known types) = nested entity reference
	if t.Kind() == reflect.Struct {
		if t == timeType || t == keywordType {
			return false
		}
		return true
	}

	// Slice of structs (nested entity references)
	if t.Kind() == reflect.Slice {
		elem := t.Elem()
		if elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		if elem.Kind() == reflect.Struct {
			if elem == timeType || elem == keywordType || elem == identityType {
				return false
			}
			return true
		}
	}

	return false
}

// ElementType returns the element type for slices/pointers, or the type itself
func ElementType(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		return ElementType(t.Elem())
	}
	if t.Kind() == reflect.Slice {
		return t.Elem()
	}
	return t
}

// IsPointerType checks if a type is a pointer (indicating optional)
func IsPointerType(t reflect.Type) bool {
	return t.Kind() == reflect.Ptr
}

// IsSliceType checks if a type is a slice (indicating cardinality-many)
func IsSliceType(t reflect.Type) bool {
	if t.Kind() == reflect.Ptr {
		return IsSliceType(t.Elem())
	}
	return t.Kind() == reflect.Slice && t.Elem().Kind() != reflect.Uint8
}
