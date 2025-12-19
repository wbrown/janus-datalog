// Package reflect provides reflection-based utilities for converting
// Go structs to/from datoms and query results.
package reflect

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

// Errors for query result mapping
var (
	ErrNotPointerToSlice  = errors.New("dest must be pointer to slice of structs")
	ErrNotPointerToStruct = errors.New("dest must be pointer to struct")
	ErrNotFound           = errors.New("query returned no results")
	ErrMultipleResults    = errors.New("query returned multiple results, expected one")
	ErrMixedTags          = errors.New("struct has mixed tagged and untagged query fields")
	ErrSymbolNotFound     = errors.New("tagged symbol not found in query results")
)

// QueryFieldMapping maps a struct field to a query result column
type QueryFieldMapping struct {
	FieldIndex int          // Index in struct
	FieldName  string       // Go field name
	FieldType  reflect.Type // Go type
	Tag        string       // The datalog tag value (e.g., "?name" or "(sum ?x)")
	ColIndex   int          // Column index in result tuple (-1 if unmapped)
}

// QueryResultMapper handles query result -> struct conversion
type QueryResultMapper struct {
	elemType reflect.Type
	mappings []QueryFieldMapping
}

// NewQueryResultMapper creates a mapper from struct type and :find column names.
// The findColumns should be the string representations of FindElements
// (e.g., "?name", "(sum ?salary)").
func NewQueryResultMapper(elemType reflect.Type, findColumns []string) (*QueryResultMapper, error) {
	// Handle pointer to struct
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}

	if elemType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct type, got %s", elemType.Kind())
	}

	// Build column index map
	colIndex := make(map[string]int)
	for i, col := range findColumns {
		colIndex[col] = i
	}

	// Parse struct fields
	mappings := make([]QueryFieldMapping, 0)
	hasTagged := false
	hasUntagged := false

	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		tag := field.Tag.Get("datalog")

		// Skip fields marked with "-" or ID fields
		if tag == "-" || strings.HasSuffix(tag, ",id") {
			continue
		}

		mapping := QueryFieldMapping{
			FieldIndex: i,
			FieldName:  field.Name,
			FieldType:  field.Type,
			ColIndex:   -1,
		}

		// Check if this is a query symbol tag (starts with ? or ()
		if isQueryTag(tag) {
			mapping.Tag = tag
			hasTagged = true

			// Look up column index
			idx, found := colIndex[tag]
			if !found {
				return nil, fmt.Errorf("%w: field %s tagged with %q not found in query results %v",
					ErrSymbolNotFound, field.Name, tag, findColumns)
			}
			mapping.ColIndex = idx
		} else if tag == "" {
			// No tag - will use positional mapping
			hasUntagged = true
		} else {
			// Has a tag but it's an attribute tag (e.g., "person/name"), not a query tag
			// Skip this field for query mapping
			continue
		}

		mappings = append(mappings, mapping)
	}

	// Check for mixed tags
	if hasTagged && hasUntagged {
		return nil, fmt.Errorf("%w: either all fields should have query tags (e.g., `datalog:\"?name\"`) or none",
			ErrMixedTags)
	}

	// Apply positional mapping if no tags
	if hasUntagged {
		for i := range mappings {
			if i < len(findColumns) {
				mappings[i].ColIndex = i
				mappings[i].Tag = findColumns[i] // For error messages
			}
		}
	}

	return &QueryResultMapper{
		elemType: elemType,
		mappings: mappings,
	}, nil
}

// isQueryTag returns true if the tag looks like a query symbol or aggregate
func isQueryTag(tag string) bool {
	if tag == "" {
		return false
	}
	// Query variables start with ?
	if strings.HasPrefix(tag, "?") {
		return true
	}
	// Aggregates are wrapped in parens: (sum ?x)
	if strings.HasPrefix(tag, "(") && strings.HasSuffix(tag, ")") {
		return true
	}
	return false
}

// MapTuple populates a struct from a single result tuple
func (m *QueryResultMapper) MapTuple(tuple []interface{}, dest reflect.Value) error {
	// Handle pointer to struct
	if dest.Kind() == reflect.Ptr {
		if dest.IsNil() {
			dest.Set(reflect.New(dest.Type().Elem()))
		}
		dest = dest.Elem()
	}

	if dest.Kind() != reflect.Struct {
		return fmt.Errorf("expected struct, got %s", dest.Kind())
	}

	for _, mapping := range m.mappings {
		if mapping.ColIndex < 0 || mapping.ColIndex >= len(tuple) {
			continue // Skip unmapped or out-of-range columns
		}

		value := tuple[mapping.ColIndex]
		fieldVal := dest.Field(mapping.FieldIndex)

		if err := setQueryValue(fieldVal, mapping.FieldType, value); err != nil {
			return fmt.Errorf("field %s (tag %s): %w", mapping.FieldName, mapping.Tag, err)
		}
	}

	return nil
}

// MapAll populates a slice of structs from multiple result tuples
func (m *QueryResultMapper) MapAll(tuples [][]interface{}, destSlice reflect.Value) error {
	if destSlice.Kind() != reflect.Slice {
		return fmt.Errorf("expected slice, got %s", destSlice.Kind())
	}

	// Pre-allocate slice
	newSlice := reflect.MakeSlice(destSlice.Type(), 0, len(tuples))

	for i, tuple := range tuples {
		elem := reflect.New(m.elemType).Elem()
		if err := m.MapTuple(tuple, elem); err != nil {
			return fmt.Errorf("row %d: %w", i, err)
		}
		newSlice = reflect.Append(newSlice, elem)
	}

	destSlice.Set(newSlice)
	return nil
}

// setQueryValue sets a struct field from a query result value.
// This handles type coercion similar to setSingleValue in reader.go.
func setQueryValue(fieldVal reflect.Value, fieldType reflect.Type, value interface{}) error {
	// Handle nil values
	if value == nil {
		if fieldType.Kind() == reflect.Ptr {
			// Pointer field - leave as nil
			return nil
		}
		// Non-pointer field with nil value - this is an error for required fields
		// But we'll be lenient and leave as zero value
		return nil
	}

	// Handle pointer fields
	if fieldType.Kind() == reflect.Ptr {
		// Create new value and set pointer
		newVal := reflect.New(fieldType.Elem())
		if err := setQueryValue(newVal.Elem(), fieldType.Elem(), value); err != nil {
			return err
		}
		fieldVal.Set(newVal)
		return nil
	}

	// Handle specific types
	switch fieldType {
	case timeType:
		t, ok := value.(time.Time)
		if !ok {
			return fmt.Errorf("expected time.Time, got %T", value)
		}
		fieldVal.Set(reflect.ValueOf(t))
		return nil

	case identityType:
		switch v := value.(type) {
		case datalog.Identity:
			fieldVal.Set(reflect.ValueOf(v))
		case *datalog.Identity:
			if v != nil {
				fieldVal.Set(reflect.ValueOf(*v))
			}
		default:
			return fmt.Errorf("expected Identity, got %T", value)
		}
		return nil

	case keywordType:
		kw, ok := value.(datalog.Keyword)
		if !ok {
			return fmt.Errorf("expected Keyword, got %T", value)
		}
		fieldVal.Set(reflect.ValueOf(kw))
		return nil
	}

	// Handle basic types
	switch fieldType.Kind() {
	case reflect.String:
		s, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
		fieldVal.SetString(s)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var i int64
		switch v := value.(type) {
		case int64:
			i = v
		case int:
			i = int64(v)
		case int32:
			i = int64(v)
		case float64:
			i = int64(v)
		case uint64:
			i = int64(v)
		default:
			return fmt.Errorf("expected integer, got %T", value)
		}
		fieldVal.SetInt(i)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		var u uint64
		switch v := value.(type) {
		case uint64:
			u = v
		case int64:
			u = uint64(v)
		case int:
			u = uint64(v)
		case float64:
			u = uint64(v)
		default:
			return fmt.Errorf("expected unsigned integer, got %T", value)
		}
		fieldVal.SetUint(u)

	case reflect.Float32, reflect.Float64:
		var f float64
		switch v := value.(type) {
		case float64:
			f = v
		case float32:
			f = float64(v)
		case int64:
			f = float64(v)
		case int:
			f = float64(v)
		default:
			return fmt.Errorf("expected float, got %T", value)
		}
		fieldVal.SetFloat(f)

	case reflect.Bool:
		b, ok := value.(bool)
		if !ok {
			return fmt.Errorf("expected bool, got %T", value)
		}
		fieldVal.SetBool(b)

	case reflect.Slice:
		// []byte
		if fieldType.Elem().Kind() == reflect.Uint8 {
			b, ok := value.([]byte)
			if !ok {
				return fmt.Errorf("expected []byte, got %T", value)
			}
			fieldVal.SetBytes(b)
		} else {
			return fmt.Errorf("unsupported slice type: %s", fieldType)
		}

	default:
		return fmt.Errorf("unsupported type: %s", fieldType)
	}

	return nil
}
