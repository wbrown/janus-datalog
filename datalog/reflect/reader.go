package reflect

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// StructReader handles pull result → struct conversion
type StructReader struct {
	info   *StructInfo
	schema schema.SchemaProvider
	ctx    ReflectContext
}

// NewStructReader creates a reader for a struct type
func NewStructReader(v interface{}, s schema.SchemaProvider) (*StructReader, error) {
	info, err := GetStructInfoFromValue(v)
	if err != nil {
		return nil, err
	}
	return &StructReader{
		info:   info,
		schema: s,
		ctx:    &BaseReflectContext{},
	}, nil
}

// NewStructReaderWithHandler creates a reader with annotation support
func NewStructReaderWithHandler(v interface{}, s schema.SchemaProvider, handler annotations.Handler) (*StructReader, error) {
	info, err := GetStructInfoFromValue(v)
	if err != nil {
		return nil, err
	}
	return &StructReader{
		info:   info,
		schema: s,
		ctx:    NewReflectContext(handler),
	}, nil
}

// SetHandler configures the annotation handler
func (sr *StructReader) SetHandler(handler annotations.Handler) {
	sr.ctx = NewReflectContext(handler)
}

// Read populates struct from pull result map
func (sr *StructReader) Read(result map[string]interface{}, v interface{}) error {
	sr.ctx.ReadBegin(sr.info.Name, len(sr.info.Fields), len(result))

	err := sr.ReadWithDepth(result, v, 10) // Default max depth

	sr.ctx.ReadComplete(sr.info.Name, err)

	return err
}

// ReadWithDepth limits nested struct recursion
func (sr *StructReader) ReadWithDepth(result map[string]interface{}, v interface{}, maxDepth int) error {
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Ptr {
		return fmt.Errorf("Read requires pointer to struct")
	}
	if val.IsNil() {
		return fmt.Errorf("cannot read into nil pointer")
	}

	structVal := val.Elem()
	if structVal.Kind() != reflect.Struct {
		return fmt.Errorf("expected pointer to struct, got pointer to %s", structVal.Kind())
	}

	return sr.readIntoStruct(result, structVal, 0, maxDepth)
}

// readIntoStruct populates a struct value from a result map
func (sr *StructReader) readIntoStruct(result map[string]interface{}, structVal reflect.Value, depth, maxDepth int) error {
	if depth >= maxDepth {
		return nil
	}

	info, err := GetStructInfo(structVal.Type())
	if err != nil {
		return err
	}

	for _, field := range info.Fields {
		// Pull results use keys without colon: "person/name" not ":person/name"
		key := strings.TrimPrefix(field.FullAttr, ":")

		value, ok := result[key]
		if !ok {
			// Field not in result, leave as zero value
			continue
		}

		fieldVal := structVal.Field(field.Index)
		if err := sr.setFieldValue(fieldVal, field, value, depth, maxDepth); err != nil {
			return fmt.Errorf("field %s: %w", field.FieldName, err)
		}
	}

	return nil
}

// setFieldValue sets a struct field from a pull result value
func (sr *StructReader) setFieldValue(fieldVal reflect.Value, field *FieldInfo, value interface{}, depth, maxDepth int) error {
	if value == nil {
		return nil
	}

	fieldType := field.GoType

	// Handle pointer type aliases (Identity, Keyword) BEFORE general pointer handling
	// These are pointer types that should be assigned directly, not dereferenced
	if fieldType == identityType {
		if id, ok := value.(datalog.Identity); ok {
			fieldVal.Set(reflect.ValueOf(id))
			return nil
		}
		return fmt.Errorf("expected Identity, got %T", value)
	}
	if fieldType == keywordType {
		if kw, ok := value.(datalog.Keyword); ok {
			fieldVal.Set(reflect.ValueOf(kw))
			return nil
		}
		return fmt.Errorf("expected Keyword, got %T", value)
	}

	// Handle ElementID (value struct, not a reference)
	if fieldType == elementIDType {
		switch v := value.(type) {
		case datalog.ElementID:
			fieldVal.Set(reflect.ValueOf(v))
			return nil
		case *datalog.ElementID:
			fieldVal.Set(reflect.ValueOf(*v))
			return nil
		default:
			return fmt.Errorf("expected ElementID, got %T", value)
		}
	}

	// Handle pointer fields (but not Identity/Keyword which are handled above)
	if fieldType.Kind() == reflect.Ptr {
		// Create new value and set pointer
		newVal := reflect.New(fieldType.Elem())
		if err := sr.setSingleValue(newVal.Elem(), fieldType.Elem(), value, depth, maxDepth); err != nil {
			return err
		}
		fieldVal.Set(newVal)
		return nil
	}

	// Handle slice fields (cardinality-many)
	if IsSliceType(fieldType) {
		return sr.setSliceValue(fieldVal, field, value, depth, maxDepth)
	}

	// Handle OrderedSet fields (cardinality-vector with unique elements)
	if isOrderedSetType(fieldType) {
		return sr.setOrderedSetValue(fieldVal, field, value, depth, maxDepth)
	}

	// Single value
	return sr.setSingleValue(fieldVal, fieldType, value, depth, maxDepth)
}

// setOrderedSetValue handles OrderedSet[T] fields (cardinality-vector with unique elements)
func (sr *StructReader) setOrderedSetValue(fieldVal reflect.Value, field *FieldInfo, value interface{}, depth, maxDepth int) error {
	// Value should be a slice from the pull result
	valueSlice, ok := value.([]interface{})
	if !ok {
		// Check if value is already a typed slice (e.g., []string from typed vector returns)
		rv := reflect.ValueOf(value)
		if rv.Kind() == reflect.Slice {
			valueSlice = make([]interface{}, rv.Len())
			for i := range valueSlice {
				valueSlice[i] = rv.Index(i).Interface()
			}
		} else {
			// Single value - wrap in slice
			valueSlice = []interface{}{value}
		}
	}

	elemType := getOrderedSetElementType(field.GoType)
	if elemType == nil {
		return fmt.Errorf("cannot determine OrderedSet element type for %s", field.GoType)
	}

	// Create new OrderedSet instance
	// OrderedSet has Items (exported []T) and seen (unexported map[T]struct{}) fields
	// We only set the exported Items field; the seen map is initialized lazily
	// by OrderedSet methods when needed (Append, Contains, etc.)
	newSet := reflect.New(field.GoType).Elem()
	itemsField := newSet.FieldByName("Items")

	if !itemsField.IsValid() {
		return fmt.Errorf("OrderedSet missing Items field")
	}

	// Initialize the slice
	itemsSlice := reflect.MakeSlice(itemsField.Type(), 0, len(valueSlice))

	for _, elem := range valueSlice {
		var newElem reflect.Value

		// Handle different element types
		if elemType == identityType {
			if id, ok := elem.(datalog.Identity); ok {
				newElem = reflect.ValueOf(id)
			} else {
				return fmt.Errorf("expected Identity, got %T", elem)
			}
		} else if elemType == keywordType {
			if kw, ok := elem.(datalog.Keyword); ok {
				newElem = reflect.ValueOf(kw)
			} else {
				return fmt.Errorf("expected Keyword, got %T", elem)
			}
		} else {
			// Regular value type
			newElem = reflect.New(elemType).Elem()
			if err := sr.setSingleValue(newElem, elemType, elem, depth, maxDepth); err != nil {
				return err
			}
		}

		// Add to items slice
		itemsSlice = reflect.Append(itemsSlice, newElem)
	}

	itemsField.Set(itemsSlice)
	fieldVal.Set(newSet)

	return nil
}

// setSliceValue handles cardinality-many fields
func (sr *StructReader) setSliceValue(fieldVal reflect.Value, field *FieldInfo, value interface{}, depth, maxDepth int) error {
	// Value should be a slice
	valueSlice, ok := value.([]interface{})
	if !ok {
		// Check if value is already a typed slice (e.g., []string from typed vector returns)
		rv := reflect.ValueOf(value)
		if rv.Kind() == reflect.Slice && rv.Type().AssignableTo(field.GoType) {
			fieldVal.Set(rv)
			return nil
		}
		if rv.Kind() == reflect.Slice {
			// Convert typed slice to []interface{} for element-by-element processing
			valueSlice = make([]interface{}, rv.Len())
			for i := range valueSlice {
				valueSlice[i] = rv.Index(i).Interface()
			}
		} else {
			// Single value - wrap in slice
			valueSlice = []interface{}{value}
		}
	}

	elemType := field.GoType.Elem()
	sliceVal := reflect.MakeSlice(field.GoType, 0, len(valueSlice))

	for _, elem := range valueSlice {
		var newElem reflect.Value

		// Handle pointer type aliases (Identity, Keyword) specially
		// Don't treat them like regular pointers that need dereferencing
		if elemType == identityType {
			if id, ok := elem.(datalog.Identity); ok {
				newElem = reflect.ValueOf(id)
			} else {
				return fmt.Errorf("expected Identity, got %T", elem)
			}
		} else if elemType == keywordType {
			if kw, ok := elem.(datalog.Keyword); ok {
				newElem = reflect.ValueOf(kw)
			} else {
				return fmt.Errorf("expected Keyword, got %T", elem)
			}
		} else if elemType.Kind() == reflect.Ptr {
			// Pointer element (but not Identity/Keyword)
			newElem = reflect.New(elemType.Elem())
			if err := sr.setSingleValue(newElem.Elem(), elemType.Elem(), elem, depth, maxDepth); err != nil {
				return err
			}
		} else {
			// Value element
			newElem = reflect.New(elemType).Elem()
			if err := sr.setSingleValue(newElem, elemType, elem, depth, maxDepth); err != nil {
				return err
			}
		}

		sliceVal = reflect.Append(sliceVal, newElem)
	}

	fieldVal.Set(sliceVal)
	return nil
}

// setSingleValue sets a single value (not slice) to a reflect.Value
func (sr *StructReader) setSingleValue(fieldVal reflect.Value, fieldType reflect.Type, value interface{}, depth, maxDepth int) error {
	// Handle pointer types by dereferencing the target type
	// BUT not Identity or Keyword which are pointer type aliases that should stay as-is
	if fieldType.Kind() == reflect.Ptr && fieldType != identityType && fieldType != keywordType {
		fieldType = fieldType.Elem()
	}

	// Handle nested struct (from pull result map or Identity)
	if fieldType.Kind() == reflect.Struct &&
		fieldType != timeType &&
		fieldType != identityType &&
		fieldType != keywordType &&
		fieldType != elementIDType {

		// Could be a map (nested pull result) or Identity (ref without nested pattern)
		switch v := value.(type) {
		case map[string]interface{}:
			nestedInfo, err := GetStructInfo(fieldType)
			if err != nil {
				return err
			}
			// Create temporary reader for nested struct
			nestedReader := &StructReader{info: nestedInfo, schema: sr.schema, ctx: sr.ctx}
			return nestedReader.readIntoStruct(v, fieldVal, depth+1, maxDepth)

		case datalog.Identity:
			// Just an Identity ref - try to set the ID field if the struct has one
			if v != nil {
				return sr.setNestedStructID(fieldVal, fieldType, v)
			}
			return nil

		default:
			return fmt.Errorf("expected map or Identity for nested struct, got %T", value)
		}
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
		if v, ok := value.(datalog.Identity); ok {
			fieldVal.Set(reflect.ValueOf(v))
		} else {
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
		default:
			return fmt.Errorf("expected integer, got %T", value)
		}
		fieldVal.SetInt(i)

	case reflect.Float32, reflect.Float64:
		var f float64
		switch v := value.(type) {
		case float64:
			f = v
		case float32:
			f = float64(v)
		case int64:
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

	case reflect.Interface:
		// For interface{}/any fields, assign the value directly.
		// The caller accepts responsibility for type assertions.
		fieldVal.Set(reflect.ValueOf(value))

	default:
		return fmt.Errorf("unsupported type: %s", fieldType)
	}

	return nil
}

// setNestedStructID sets just the ID field of a nested struct from an Identity
func (sr *StructReader) setNestedStructID(fieldVal reflect.Value, fieldType reflect.Type, id datalog.Identity) error {
	nestedInfo, err := GetStructInfo(fieldType)
	if err != nil {
		return err
	}
	if nestedInfo.IDField != nil {
		idField := fieldVal.Field(nestedInfo.IDField.Index)
		// Identity is always a pointer type now, set it directly
		idField.Set(reflect.ValueOf(id))
	}
	return nil
}

// ReadStruct is a convenience function that creates a reader and populates the struct
func ReadStruct(result map[string]interface{}, v interface{}, s schema.SchemaProvider) error {
	reader, err := NewStructReader(v, s)
	if err != nil {
		return err
	}
	return reader.Read(result, v)
}

// ReadStructWithID reads a struct from a pull result and also sets the ID field from the entityID.
// This is used by PullInto to ensure the struct's ID field is populated.
func ReadStructWithID(result map[string]interface{}, v interface{}, s schema.SchemaProvider, entityID datalog.Identity) error {
	reader, err := NewStructReader(v, s)
	if err != nil {
		return err
	}
	if err := reader.Read(result, v); err != nil {
		return err
	}
	// Set the ID field if the struct has one
	return reader.SetIDField(v, entityID)
}

// SetIDField sets the ID field of a struct to the given entityID
func (sr *StructReader) SetIDField(v interface{}, entityID datalog.Identity) error {
	if sr.info.IDField == nil {
		return nil // No ID field, nothing to set
	}

	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Ptr {
		return fmt.Errorf("SetIDField requires pointer to struct")
	}
	structVal := val.Elem()
	if structVal.Kind() != reflect.Struct {
		return fmt.Errorf("expected pointer to struct, got pointer to %s", structVal.Kind())
	}

	idField := structVal.Field(sr.info.IDField.Index)
	// Identity is always a pointer type now, set it directly
	idField.Set(reflect.ValueOf(entityID))
	return nil
}

// ReadWithID reads a struct and sets the ID field
func (sr *StructReader) ReadWithID(result map[string]interface{}, v interface{}, entityID datalog.Identity) error {
	if err := sr.Read(result, v); err != nil {
		return err
	}
	return sr.SetIDField(v, entityID)
}
