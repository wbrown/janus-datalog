package reflect

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// generateUniqueID creates a unique entity ID using timestamp + random bytes
func generateUniqueID() datalog.Identity {
	// 8 random bytes gives us 64 bits of entropy
	var randomBytes [8]byte
	rand.Read(randomBytes[:])
	randomHex := hex.EncodeToString(randomBytes[:])
	return datalog.NewIdentity(fmt.Sprintf("e%d-%s", time.Now().UnixNano(), randomHex))
}

// TransactionAdder is the interface required for adding datoms
// This is implemented by storage.Transaction
type TransactionAdder interface {
	Add(e datalog.Identity, a datalog.Keyword, v interface{}) error
}

// StructWriter handles struct → datom conversion
type StructWriter struct {
	info   *StructInfo
	schema schema.SchemaProvider
}

// NewStructWriter creates a writer for a struct type
func NewStructWriter(v interface{}, s schema.SchemaProvider) (*StructWriter, error) {
	info, err := GetStructInfoFromValue(v)
	if err != nil {
		return nil, err
	}
	return &StructWriter{
		info:   info,
		schema: s,
	}, nil
}

// Write adds all fields from struct to transaction
func (sw *StructWriter) Write(tx TransactionAdder, entity datalog.Identity, v interface{}) error {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return fmt.Errorf("cannot write nil struct")
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return fmt.Errorf("expected struct, got %s", val.Kind())
	}

	for _, field := range sw.info.Fields {
		fieldVal := val.Field(field.Index)

		// Handle pointer fields (optional)
		if field.GoType.Kind() == reflect.Ptr {
			if fieldVal.IsNil() {
				// Skip nil optional fields
				continue
			}
			fieldVal = fieldVal.Elem()
		}

		// Get the keyword for this attribute
		kw := datalog.NewKeyword(field.FullAttr)

		// Handle different field types
		if err := sw.writeField(tx, entity, kw, field, fieldVal); err != nil {
			return fmt.Errorf("field %s: %w", field.FieldName, err)
		}
	}

	return nil
}

// writeField writes a single field value to the transaction
func (sw *StructWriter) writeField(tx TransactionAdder, entity datalog.Identity, kw datalog.Keyword, field *FieldInfo, val reflect.Value) error {
	// Handle slice fields (cardinality-many)
	if IsSliceType(field.GoType) {
		return sw.writeSliceField(tx, entity, kw, field, val)
	}

	// Get the value to write
	writeVal, err := sw.extractValue(field, val)
	if err != nil {
		return err
	}

	// Skip zero values for optional types
	if writeVal == nil {
		return nil
	}

	return tx.Add(entity, kw, writeVal)
}

// writeSliceField handles cardinality-many fields
func (sw *StructWriter) writeSliceField(tx TransactionAdder, entity datalog.Identity, kw datalog.Keyword, field *FieldInfo, val reflect.Value) error {
	if val.Kind() != reflect.Slice {
		return fmt.Errorf("expected slice, got %s", val.Kind())
	}

	for i := 0; i < val.Len(); i++ {
		elem := val.Index(i)

		// Handle pointer elements
		if elem.Kind() == reflect.Ptr {
			if elem.IsNil() {
				continue
			}
			elem = elem.Elem()
		}

		writeVal, err := sw.extractSingleValue(elem)
		if err != nil {
			return fmt.Errorf("element %d: %w", i, err)
		}

		if writeVal == nil {
			continue
		}

		if err := tx.Add(entity, kw, writeVal); err != nil {
			return fmt.Errorf("element %d: %w", i, err)
		}
	}

	return nil
}

// extractValue extracts a value from a reflect.Value for storage
func (sw *StructWriter) extractValue(field *FieldInfo, val reflect.Value) (interface{}, error) {
	return sw.extractSingleValue(val)
}

// extractSingleValue extracts a single value from a reflect.Value
func (sw *StructWriter) extractSingleValue(val reflect.Value) (interface{}, error) {
	// Handle pointer
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, nil
		}
		val = val.Elem()
	}

	// Get interface value
	if !val.CanInterface() {
		return nil, fmt.Errorf("cannot get interface for unexported field")
	}

	v := val.Interface()

	// Handle specific types
	switch typedVal := v.(type) {
	case string:
		return typedVal, nil
	case int64:
		return typedVal, nil
	case int:
		return int64(typedVal), nil
	case int32:
		return int64(typedVal), nil
	case int16:
		return int64(typedVal), nil
	case int8:
		return int64(typedVal), nil
	case float64:
		return typedVal, nil
	case float32:
		return float64(typedVal), nil
	case bool:
		return typedVal, nil
	case time.Time:
		return typedVal, nil
	case []byte:
		return typedVal, nil
	case datalog.Identity:
		return typedVal, nil
	case datalog.Keyword:
		return typedVal, nil
	default:
		// Check if it's a nested struct (reference)
		if val.Kind() == reflect.Struct {
			// Try to get the ID field from the nested struct
			nestedInfo, err := GetStructInfo(val.Type())
			if err != nil {
				return nil, fmt.Errorf("cannot get info for nested struct %s: %w", val.Type().Name(), err)
			}
			if nestedInfo.IDField == nil {
				return nil, fmt.Errorf("nested struct %s has no ID field", val.Type().Name())
			}
			idVal := val.Field(nestedInfo.IDField.Index)
			if idVal.Kind() == reflect.Ptr {
				if idVal.IsNil() {
					return nil, fmt.Errorf("nested struct %s has nil ID", val.Type().Name())
				}
				idVal = idVal.Elem()
			}
			id, ok := idVal.Interface().(datalog.Identity)
			if !ok {
				return nil, fmt.Errorf("nested struct %s ID field is not Identity", val.Type().Name())
			}
			return id, nil
		}
		return nil, fmt.Errorf("unsupported type: %T", v)
	}
}

// WriteAuto generates entity ID if not set, returns the ID
// If the struct has an ID field and it's set, use that
// If the struct has an ID field and it's zero, generate and set it
// If the struct has no ID field, generate an ID
func (sw *StructWriter) WriteAuto(tx TransactionAdder, v interface{}) (datalog.Identity, error) {
	val := reflect.ValueOf(v)

	// Must be pointer to struct for setting ID
	if val.Kind() != reflect.Ptr {
		return datalog.Identity{}, fmt.Errorf("WriteAuto requires pointer to struct")
	}
	if val.IsNil() {
		return datalog.Identity{}, fmt.Errorf("cannot write nil struct")
	}

	structVal := val.Elem()
	if structVal.Kind() != reflect.Struct {
		return datalog.Identity{}, fmt.Errorf("expected pointer to struct, got pointer to %s", structVal.Kind())
	}

	var entity datalog.Identity

	// Check for existing ID
	if sw.info.IDField != nil {
		idField := structVal.Field(sw.info.IDField.Index)

		// Handle pointer ID field
		if sw.info.IDField.GoType.Kind() == reflect.Ptr {
			if !idField.IsNil() {
				entity = idField.Elem().Interface().(datalog.Identity)
			}
		} else {
			entity = idField.Interface().(datalog.Identity)
		}

		// Check if ID is zero (all zeros in the hash)
		var zeroHash [20]byte
		if entity.Hash() == zeroHash {
			// Generate new unique ID
			entity = generateUniqueID()

			// Set the ID field
			if sw.info.IDField.GoType.Kind() == reflect.Ptr {
				newID := reflect.New(identityType)
				newID.Elem().Set(reflect.ValueOf(entity))
				idField.Set(newID)
			} else {
				idField.Set(reflect.ValueOf(entity))
			}
		}
	} else {
		// No ID field, generate a unique ID
		entity = generateUniqueID()
	}

	// Write the struct
	if err := sw.Write(tx, entity, v); err != nil {
		return datalog.Identity{}, err
	}

	return entity, nil
}

// WriteStruct is a convenience function that creates a writer and writes the struct
func WriteStruct(tx TransactionAdder, entity datalog.Identity, v interface{}, s schema.SchemaProvider) error {
	writer, err := NewStructWriter(v, s)
	if err != nil {
		return err
	}
	return writer.Write(tx, entity, v)
}

// WriteStructAuto is a convenience function that creates a writer and writes the struct with auto ID
func WriteStructAuto(tx TransactionAdder, v interface{}, s schema.SchemaProvider) (datalog.Identity, error) {
	writer, err := NewStructWriter(v, s)
	if err != nil {
		return datalog.Identity{}, err
	}
	return writer.WriteAuto(tx, v)
}
