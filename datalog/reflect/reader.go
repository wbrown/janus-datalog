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

	// Handle pointer fields
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

	// Single value
	return sr.setSingleValue(fieldVal, fieldType, value, depth, maxDepth)
}

// setSliceValue handles cardinality-many fields
func (sr *StructReader) setSliceValue(fieldVal reflect.Value, field *FieldInfo, value interface{}, depth, maxDepth int) error {
	// Value should be a slice
	valueSlice, ok := value.([]interface{})
	if !ok {
		// Single value - wrap in slice
		valueSlice = []interface{}{value}
	}

	elemType := field.GoType.Elem()
	sliceVal := reflect.MakeSlice(field.GoType, 0, len(valueSlice))

	for _, elem := range valueSlice {
		var newElem reflect.Value

		if elemType.Kind() == reflect.Ptr {
			// Pointer element
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
	if fieldType.Kind() == reflect.Ptr {
		fieldType = fieldType.Elem()
	}

	// Handle nested struct (from pull result map or Identity)
	if fieldType.Kind() == reflect.Struct &&
		fieldType != timeType &&
		fieldType != identityType &&
		fieldType != keywordType {

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
			return sr.setNestedStructID(fieldVal, fieldType, v)

		case *datalog.Identity:
			// Pointer to Identity ref
			if v != nil {
				return sr.setNestedStructID(fieldVal, fieldType, *v)
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
		if nestedInfo.IDField.GoType.Kind() == reflect.Ptr {
			newID := reflect.New(identityType)
			newID.Elem().Set(reflect.ValueOf(id))
			idField.Set(newID)
		} else {
			idField.Set(reflect.ValueOf(id))
		}
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
	if sr.info.IDField.GoType.Kind() == reflect.Ptr {
		newID := reflect.New(identityType)
		newID.Elem().Set(reflect.ValueOf(entityID))
		idField.Set(newID)
	} else {
		idField.Set(reflect.ValueOf(entityID))
	}
	return nil
}

// ReadWithID reads a struct and sets the ID field
func (sr *StructReader) ReadWithID(result map[string]interface{}, v interface{}, entityID datalog.Identity) error {
	if err := sr.Read(result, v); err != nil {
		return err
	}
	return sr.SetIDField(v, entityID)
}
