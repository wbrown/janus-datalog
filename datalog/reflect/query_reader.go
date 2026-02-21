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
	"github.com/wbrown/janus-datalog/datalog/query"
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
	elemType      reflect.Type
	mappings      []QueryFieldMapping // Query variable mappings (with ColIndex set)
	pullMappings  []QueryFieldMapping // Attribute tag mappings (for pull result maps)
	isPullMapping bool                // true if struct uses ONLY attribute-style tags
}

// NewQueryResultMapper creates a mapper from struct type and :find column names.
// The findColumns should be the string representations of FindElements
// (e.g., "?name", "(sum ?salary)").
//
// The mapper supports three modes:
// 1. Query-style tags only (`datalog:"?name"`) - maps query result columns to struct fields
// 2. Attribute-style tags only (`datalog:"person/name"`) - maps pull result maps to struct fields
// 3. Mixed mode - both query-style AND attribute-style tags in the same struct
//
// For pure attribute-style structs, the query should return a pull expression column
// like `[:find (pull ?e [*]) :where ...]`.
//
// For mixed mode, use queries like `[:find ?name (pull ?e [:person/age]) :where ...]`
// where query variables map to their columns and attribute tags map from the pull result.
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

	// Parse struct fields into query mappings and attribute (pull) mappings
	queryMappings := make([]QueryFieldMapping, 0)
	pullMappings := make([]QueryFieldMapping, 0)
	untaggedMappings := make([]QueryFieldMapping, 0)

	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		tag := field.Tag.Get("datalog")

		// Skip fields marked with "-" or starting with "-," (e.g., "-,id" for legacy compat)
		if tag == "-" || strings.HasPrefix(tag, "-,") {
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

			// Look up column index
			idx, found := colIndex[tag]
			if !found {
				return nil, fmt.Errorf("%w: field %s tagged with %q not found in query results %v",
					ErrSymbolNotFound, field.Name, tag, findColumns)
			}
			mapping.ColIndex = idx
			queryMappings = append(queryMappings, mapping)
		} else if tag == "" {
			// No tag - will use positional mapping
			untaggedMappings = append(untaggedMappings, mapping)
		} else {
			// Has an attribute-style tag (e.g., "person/name", "db/id")
			// These are used for pull result mapping
			// Strip modifiers (e.g., "db/id,id" -> "db/id") since the pull map
			// keys only contain the attribute name without modifiers
			attrName := tag
			if idx := strings.Index(tag, ","); idx > 0 {
				attrName = tag[:idx]
			}
			mapping.Tag = attrName
			pullMappings = append(pullMappings, mapping)
		}
	}

	hasQueryTags := len(queryMappings) > 0
	hasAttributeTags := len(pullMappings) > 0
	hasUntagged := len(untaggedMappings) > 0

	// Check for mixed query tags and untagged fields (not allowed)
	if hasQueryTags && hasUntagged {
		return nil, fmt.Errorf("%w: either all fields should have query tags (e.g., `datalog:\"?name\"`) or none",
			ErrMixedTags)
	}

	// Pure pull mapping mode: only attribute-style tags
	if hasAttributeTags && !hasQueryTags && !hasUntagged {
		return &QueryResultMapper{
			elemType:      elemType,
			mappings:      nil,
			pullMappings:  pullMappings,
			isPullMapping: true,
		}, nil
	}

	// Mixed mode: query tags AND attribute tags
	// Query variables map from tuple columns, attribute tags map from pull result in tuple
	if hasQueryTags && hasAttributeTags {
		return &QueryResultMapper{
			elemType:      elemType,
			mappings:      queryMappings,
			pullMappings:  pullMappings,
			isPullMapping: false,
		}, nil
	}

	// Pure query mode: only query tags
	if hasQueryTags {
		return &QueryResultMapper{
			elemType:      elemType,
			mappings:      queryMappings,
			pullMappings:  nil,
			isPullMapping: false,
		}, nil
	}

	// Positional mapping: no tags at all
	if hasUntagged {
		for i := range untaggedMappings {
			if i < len(findColumns) {
				untaggedMappings[i].ColIndex = i
				untaggedMappings[i].Tag = findColumns[i] // For error messages
			}
		}
		return &QueryResultMapper{
			elemType:      elemType,
			mappings:      untaggedMappings,
			pullMappings:  nil,
			isPullMapping: false,
		}, nil
	}

	// No fields to map
	return &QueryResultMapper{
		elemType:      elemType,
		mappings:      nil,
		pullMappings:  nil,
		isPullMapping: false,
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

	// Pure pull mapping mode: expect a map value in the tuple
	if m.isPullMapping {
		if len(tuple) == 0 {
			return fmt.Errorf("pull mapping expects at least one column, got empty tuple")
		}
		pullMap := m.findPullMap(tuple)
		if pullMap == nil {
			return fmt.Errorf("pull mapping expects map[string]interface{} in tuple, got %T", tuple[0])
		}
		return m.mapPullResult(pullMap, dest)
	}

	// Map query variable fields from tuple columns
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

	// Mixed mode: also map attribute-tagged fields from pull result in tuple
	if len(m.pullMappings) > 0 {
		pullMap := m.findPullMap(tuple)
		if pullMap != nil {
			if err := m.mapPullResult(pullMap, dest); err != nil {
				return err
			}
		}
		// If no pull map found but we have pull mappings, leave those fields as zero values
		// This is lenient - user might have attribute tags but no pull in this particular query
	}

	return nil
}

// findPullMap searches for the first map[string]interface{} in the tuple
func (m *QueryResultMapper) findPullMap(tuple []interface{}) map[string]interface{} {
	for _, val := range tuple {
		if pm, ok := val.(map[string]interface{}); ok {
			return pm
		}
	}
	return nil
}

// mapPullResult populates a struct from a pull result map using attribute-style tags.
// All attributes are treated uniformly - the entity ID uses tag "db/id" or ":db/id"
// just like any other attribute (e.g., "person/name" or ":person/name").
func (m *QueryResultMapper) mapPullResult(pullMap map[string]interface{}, dest reflect.Value) error {
	for _, mapping := range m.pullMappings {
		// Normalize the tag to map key format (strip leading colon if present)
		// This matches how pull results store keys via query.KeyName()
		attrKey := query.KeyNameFromString(mapping.Tag)

		// Look up value in pull map
		val, ok := pullMap[attrKey]
		if !ok {
			// Field not in pull result - leave as zero value
			continue
		}

		fieldVal := dest.Field(mapping.FieldIndex)
		if err := setQueryValue(fieldVal, mapping.FieldType, val); err != nil {
			return fmt.Errorf("field %s (attr %s): %w", mapping.FieldName, attrKey, err)
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

	// Handle Identity specially BEFORE generic pointer handling
	// Identity is always a pointer type (*identity)
	if fieldType == identityType {
		if v, ok := value.(datalog.Identity); ok {
			fieldVal.Set(reflect.ValueOf(v))
		} else {
			return fmt.Errorf("expected Identity, got %T", value)
		}
		return nil
	}

	// Handle *Keyword specially BEFORE generic pointer handling
	// Handle Keyword type - always a pointer now
	if fieldType == keywordType {
		switch v := value.(type) {
		case datalog.Keyword:
			fieldVal.Set(reflect.ValueOf(v))
		default:
			return fmt.Errorf("expected Keyword, got %T", value)
		}
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

	case elementIDType:
		switch v := value.(type) {
		case datalog.ElementID:
			fieldVal.Set(reflect.ValueOf(v))
		case *datalog.ElementID:
			fieldVal.Set(reflect.ValueOf(*v))
		default:
			return fmt.Errorf("expected ElementID, got %T", value)
		}
		return nil

	case identityType:
		// Identity is now always a pointer type (*identity)
		if v, ok := value.(datalog.Identity); ok {
			fieldVal.Set(reflect.ValueOf(v))
		} else {
			return fmt.Errorf("expected Identity, got %T", value)
		}
		return nil

		// Note: keywordType is handled above before generic pointer handling
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
			return nil
		}

		// Handle slice values from pull results (cardinality-many attributes)
		// Pull returns []interface{} that we need to convert to the target type
		srcSlice, ok := value.([]interface{})
		if !ok {
			// Value might already be the correct slice type
			srcVal := reflect.ValueOf(value)
			if srcVal.Kind() == reflect.Slice {
				// Try direct assignment if types match
				if srcVal.Type().AssignableTo(fieldType) {
					fieldVal.Set(srcVal)
					return nil
				}
				// Convert element by element
				srcSlice = make([]interface{}, srcVal.Len())
				for i := 0; i < srcVal.Len(); i++ {
					srcSlice[i] = srcVal.Index(i).Interface()
				}
			} else {
				// Wildcard pull may return single value for cardinality-many
				// attributes that have only one value - wrap it in a slice
				srcSlice = []interface{}{value}
			}
		}

		// Create new slice and populate
		elemType := fieldType.Elem()
		newSlice := reflect.MakeSlice(fieldType, len(srcSlice), len(srcSlice))
		for i, elem := range srcSlice {
			if err := setQueryValue(newSlice.Index(i), elemType, elem); err != nil {
				return fmt.Errorf("slice element %d: %w", i, err)
			}
		}
		fieldVal.Set(newSlice)

	case reflect.Interface:
		// For interface{}/any fields, assign the value directly.
		// The caller accepts responsibility for type assertions.
		fieldVal.Set(reflect.ValueOf(value))

	default:
		return fmt.Errorf("unsupported type: %s", fieldType)
	}

	return nil
}
