package reflect

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Well-known types for comparison
var (
	timeType      = reflect.TypeOf(time.Time{})
	identityType  = reflect.TypeOf((datalog.Identity)(nil))
	keywordType   = reflect.TypeOf((datalog.Keyword)(nil)) // Keyword is *keyword, no .Elem()
	elementIDType = reflect.TypeOf(datalog.ElementID{})
)

// isOrderedSetType checks if the type is datalog.OrderedSet[T]
func isOrderedSetType(t reflect.Type) bool {
	// OrderedSet is a struct type with a name starting with "OrderedSet["
	// Full type string is "datalog.OrderedSet[T]" where T is the element type
	typeName := t.String()
	return strings.HasPrefix(typeName, "datalog.OrderedSet[")
}

// getOrderedSetElementType returns the element type of an OrderedSet[T].
// Returns nil if not an OrderedSet type.
func getOrderedSetElementType(t reflect.Type) reflect.Type {
	if !isOrderedSetType(t) {
		return nil
	}
	// OrderedSet has an 'items' field which is []T
	itemsField, ok := t.FieldByName("Items")
	if !ok {
		return nil
	}
	if itemsField.Type.Kind() != reflect.Slice {
		return nil
	}
	return itemsField.Type.Elem()
}

// GoTypeToSchemaType maps a Go reflect.Type to a schema.ValueType
func GoTypeToSchemaType(t reflect.Type) (schema.ValueType, error) {
	// Check pointer type aliases BEFORE dereferencing
	// Identity and Keyword are pointer type aliases (*identity, *keyword)
	if t == identityType {
		return schema.TypeRef, nil
	}
	if t == keywordType {
		return schema.TypeKeyword, nil
	}

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
		if t == elementIDType {
			return schema.TypeTx, nil
		}
		// Check for OrderedSet[T] - get element type
		if isOrderedSetType(t) {
			elemType := getOrderedSetElementType(t)
			if elemType != nil {
				return GoTypeToSchemaType(elemType)
			}
		}
		// Other struct = nested reference
		return schema.TypeRef, nil
	}

	return "", fmt.Errorf("unsupported type: %s", t)
}

// InferCardinality determines cardinality from a Go type
// Slices (except []byte) are cardinality-many, everything else is one
// OrderedSet[T] is cardinality-vector (ordered with unique elements)
func InferCardinality(t reflect.Type) schema.Cardinality {
	// Check pointer type aliases BEFORE dereferencing
	// Identity and Keyword are pointer type aliases (*identity, *keyword)
	if t == identityType || t == keywordType {
		return schema.CardinalityOne
	}

	// Handle pointers
	if t.Kind() == reflect.Ptr {
		return InferCardinality(t.Elem())
	}

	// Check for OrderedSet[T] - this is vector cardinality
	if t.Kind() == reflect.Struct && isOrderedSetType(t) {
		return schema.CardinalityVector
	}

	if t.Kind() == reflect.Slice && t.Elem().Kind() != reflect.Uint8 {
		return schema.CardinalityMany
	}
	return schema.CardinalityOne
}

// InferUniqueElements determines if the type should have unique element enforcement.
// OrderedSet[T] returns true; slices and other types return false.
func InferUniqueElements(t reflect.Type) bool {
	// Handle pointers
	if t.Kind() == reflect.Ptr {
		return InferUniqueElements(t.Elem())
	}

	// OrderedSet[T] has unique elements
	if t.Kind() == reflect.Struct && isOrderedSetType(t) {
		return true
	}

	return false
}

// IsRefType checks if a Go type represents a reference to another entity
func IsRefType(t reflect.Type) bool {
	// Check pointer type aliases BEFORE dereferencing
	// Identity and Keyword are pointer type aliases (*identity, *keyword)
	if t == identityType {
		return true
	}
	if t == keywordType {
		return false // Keyword is not a reference type
	}

	// Handle pointers
	if t.Kind() == reflect.Ptr {
		return IsRefType(t.Elem())
	}

	// Slice of Identity
	if t.Kind() == reflect.Slice && t.Elem() == identityType {
		return true
	}

	// Struct (other than well-known types) = nested entity reference
	if t.Kind() == reflect.Struct {
		if t == timeType || t == keywordType || t == elementIDType {
			return false
		}
		return true
	}

	// Slice of structs (nested entity references)
	if t.Kind() == reflect.Slice {
		elem := t.Elem()
		// Check pointer type aliases BEFORE dereferencing
		// Identity and Keyword are pointer type aliases (*identity, *keyword)
		if elem == identityType || elem == keywordType {
			return false
		}
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
	// Check pointer type aliases BEFORE dereferencing
	// Identity and Keyword are pointer type aliases (*identity, *keyword)
	if t == identityType || t == keywordType {
		return t
	}
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
	// Check pointer type aliases BEFORE dereferencing
	// Identity and Keyword are pointer type aliases (*identity, *keyword)
	if t == identityType || t == keywordType {
		return false
	}
	if t.Kind() == reflect.Ptr {
		return IsSliceType(t.Elem())
	}
	return t.Kind() == reflect.Slice && t.Elem().Kind() != reflect.Uint8
}
