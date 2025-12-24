package reflect

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
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

// TransactionUpdater extends TransactionAdder with retract capability
// This is implemented by storage.Transaction
type TransactionUpdater interface {
	TransactionAdder
	Retract(e datalog.Identity, a datalog.Keyword, v interface{}) error
}

// EntityLookup provides lookup of existing entity attributes
// This is used for upsert operations to find existing values to retract
type EntityLookup interface {
	// LookupAttribute returns the current value of an attribute for an entity
	// Returns (value, true) if found, (nil, false) if not found
	LookupAttribute(entity datalog.Identity, attr datalog.Keyword) (interface{}, bool)

	// LookupAllAttributes returns all values for a cardinality-many attribute
	// Returns empty slice if attribute not found
	LookupAllAttributes(entity datalog.Identity, attr datalog.Keyword) []interface{}
}

// UpdateMode controls how cardinality-many fields are updated
type UpdateMode int

const (
	// UpdateModeAdd uses union semantics: adds new values to existing set
	// Values already present are not duplicated
	UpdateModeAdd UpdateMode = iota

	// UpdateModeReplace uses set assignment semantics: slice IS the new complete state
	// - Retracts values in existing but not in new
	// - Adds values in new but not in existing
	// - Values present in both are left unchanged
	UpdateModeReplace
)

// StructWriter handles struct → datom conversion
type StructWriter struct {
	info   *StructInfo
	schema schema.SchemaProvider
	ctx    ReflectContext
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
		ctx:    &BaseReflectContext{},
	}, nil
}

// NewStructWriterWithHandler creates a writer with annotation support
func NewStructWriterWithHandler(v interface{}, s schema.SchemaProvider, handler annotations.Handler) (*StructWriter, error) {
	info, err := GetStructInfoFromValue(v)
	if err != nil {
		return nil, err
	}
	return &StructWriter{
		info:   info,
		schema: s,
		ctx:    NewReflectContext(handler),
	}, nil
}

// SetHandler configures the annotation handler
func (sw *StructWriter) SetHandler(handler annotations.Handler) {
	sw.ctx = NewReflectContext(handler)
}

// Write adds all fields from struct to transaction
func (sw *StructWriter) Write(tx TransactionAdder, entity datalog.Identity, v interface{}) error {
	sw.ctx.WriteBegin(entity.String(), sw.info.Name, len(sw.info.Fields))

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

	fieldsWritten := 0
	for _, field := range sw.info.Fields {
		fieldVal := val.Field(field.Index)

		// Handle pointer fields (optional)
		// BUT don't dereference Identity or Keyword - they are pointer type aliases
		// that extractSingleValue needs to see as pointers to properly type-assert
		if field.GoType.Kind() == reflect.Ptr {
			if fieldVal.IsNil() {
				// Skip nil optional fields
				continue
			}
			// Check if this is a pointer type alias (Identity, Keyword) that should NOT be dereferenced
			if fieldVal.CanInterface() {
				v := fieldVal.Interface()
				if _, isIdentity := v.(datalog.Identity); isIdentity {
					// Don't dereference - pass through as-is
				} else if _, isKeyword := v.(datalog.Keyword); isKeyword {
					// Don't dereference - pass through as-is
				} else {
					fieldVal = fieldVal.Elem()
				}
			} else {
				fieldVal = fieldVal.Elem()
			}
		}

		// Get the keyword for this attribute
		kw := datalog.NewKeyword(field.FullAttr)

		// Handle different field types
		if err := sw.writeField(tx, entity, kw, field, fieldVal); err != nil {
			return fmt.Errorf("field %s: %w", field.FieldName, err)
		}
		fieldsWritten++
	}

	sw.ctx.WriteComplete(entity.String(), sw.info.Name, fieldsWritten, nil)

	return nil
}

// Update performs an upsert operation: retracts changed values and adds new ones.
// For cardinality-one fields: if the value has changed, retracts the old value and adds the new one.
// For cardinality-many fields: behavior depends on the UpdateMode.
//
// This requires both a TransactionUpdater (for retract capability) and an EntityLookup
// (to find existing values to compare/retract).
func (sw *StructWriter) Update(tx TransactionUpdater, lookup EntityLookup, entity datalog.Identity, v interface{}, mode UpdateMode) error {
	modeStr := "add"
	if mode == UpdateModeReplace {
		modeStr = "replace"
	}
	sw.ctx.UpdateBegin(entity.String(), sw.info.Name, len(sw.info.Fields), modeStr)

	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return fmt.Errorf("cannot update nil struct")
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return fmt.Errorf("expected struct, got %s", val.Kind())
	}

	fieldsProcessed := 0
	for _, field := range sw.info.Fields {
		fieldVal := val.Field(field.Index)

		// Handle pointer fields (optional)
		// BUT don't dereference Identity or Keyword - they are pointer type aliases
		// that extractSingleValue needs to see as pointers to properly type-assert
		if field.GoType.Kind() == reflect.Ptr {
			if fieldVal.IsNil() {
				// For nil optional fields in update mode, we could optionally retract existing
				// For now, skip nil fields (don't change existing value)
				continue
			}
			// Check if this is a pointer type alias (Identity, Keyword) that should NOT be dereferenced
			if fieldVal.CanInterface() {
				v := fieldVal.Interface()
				if _, isIdentity := v.(datalog.Identity); isIdentity {
					// Don't dereference - pass through as-is
				} else if _, isKeyword := v.(datalog.Keyword); isKeyword {
					// Don't dereference - pass through as-is
				} else {
					fieldVal = fieldVal.Elem()
				}
			} else {
				fieldVal = fieldVal.Elem()
			}
		}

		// Get the keyword for this attribute
		kw := datalog.NewKeyword(field.FullAttr)

		// Handle different field types
		if err := sw.updateField(tx, lookup, entity, kw, field, fieldVal, mode); err != nil {
			return fmt.Errorf("field %s: %w", field.FieldName, err)
		}
		fieldsProcessed++
	}

	sw.ctx.UpdateComplete(entity.String(), sw.info.Name, fieldsProcessed, modeStr, nil)

	return nil
}

// updateField updates a single field with upsert semantics
func (sw *StructWriter) updateField(tx TransactionUpdater, lookup EntityLookup, entity datalog.Identity, kw datalog.Keyword, field *FieldInfo, val reflect.Value, mode UpdateMode) error {
	// Handle slice fields (cardinality-many)
	if IsSliceType(field.GoType) {
		return sw.updateSliceField(tx, lookup, entity, kw, field, val, mode)
	}

	// Get the new value to write
	newVal, err := sw.extractValue(field, val)
	if err != nil {
		return err
	}

	// Skip zero values for optional types
	if newVal == nil {
		return nil
	}

	// Look up existing value
	existingVal, found := lookup.LookupAttribute(entity, kw)

	if found {
		// Compare values - if same, no action needed
		if datalog.ValuesEqual(existingVal, newVal) {
			return nil
		}
		// Different value - retract old, add new
		if err := tx.Retract(entity, kw, existingVal); err != nil {
			return fmt.Errorf("retract failed: %w", err)
		}
	}

	return tx.Add(entity, kw, newVal)
}

// containsValue checks if a value exists in a slice using ValuesEqual
func containsValue(vals []interface{}, target interface{}) bool {
	for _, v := range vals {
		if datalog.ValuesEqual(v, target) {
			return true
		}
	}
	return false
}

// updateSliceField handles cardinality-many field updates
// Nil slice means "don't touch existing values" - skip entirely.
// Empty slice (not nil) means "clear all values".
// Non-empty slice means "set to exactly these values" (diff-based).
func (sw *StructWriter) updateSliceField(tx TransactionUpdater, lookup EntityLookup, entity datalog.Identity, kw datalog.Keyword, field *FieldInfo, val reflect.Value, mode UpdateMode) error {
	if val.Kind() != reflect.Slice {
		return fmt.Errorf("expected slice, got %s", val.Kind())
	}

	// Nil slice = "I didn't set this field" → leave existing values alone
	if val.IsNil() {
		return nil
	}

	// Extract new values (empty slice is valid - means "clear all")
	var newVals []interface{}
	for i := 0; i < val.Len(); i++ {
		elem := val.Index(i)
		// Don't dereference pointers here - extractSingleValue handles pointer types
		// including datalog.Identity which is a pointer type alias (*identity)

		writeVal, err := sw.extractSingleValue(elem)
		if err != nil {
			return fmt.Errorf("element %d: %w", i, err)
		}
		if writeVal != nil {
			newVals = append(newVals, writeVal)
		}
	}

	if mode == UpdateModeReplace {
		// Diff-based set assignment: slice IS the new complete state
		existingVals := lookup.LookupAllAttributes(entity, kw)

		// Retract values in existing but not in new
		for _, existing := range existingVals {
			if !containsValue(newVals, existing) {
				if err := tx.Retract(entity, kw, existing); err != nil {
					return fmt.Errorf("retract failed: %w", err)
				}
			}
		}

		// Add values in new but not in existing
		for _, newVal := range newVals {
			if !containsValue(existingVals, newVal) {
				if err := tx.Add(entity, kw, newVal); err != nil {
					return err
				}
			}
		}
	} else {
		// UpdateModeAdd - union semantics: add new values to existing set
		// Only add values that don't already exist
		existingVals := lookup.LookupAllAttributes(entity, kw)
		for _, newVal := range newVals {
			if !containsValue(existingVals, newVal) {
				if err := tx.Add(entity, kw, newVal); err != nil {
					return err
				}
			}
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

		// Don't dereference pointers here - extractSingleValue handles pointer types
		// including datalog.Identity which is a pointer type alias (*identity)

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
	// Check for Identity and Keyword BEFORE dereferencing, since they are pointer type aliases
	// Dereferencing would lose the type information (we'd get identity/keyword instead of *identity/*keyword)
	if val.Kind() == reflect.Ptr && val.CanInterface() {
		if val.IsNil() {
			return nil, nil
		}
		// Try to get the interface value first, to preserve pointer type aliases
		v := val.Interface()
		if id, ok := v.(datalog.Identity); ok {
			return id, nil
		}
		if kw, ok := v.(datalog.Keyword); ok {
			return kw, nil
		}
		// For other pointers, dereference and continue
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
		return nil, fmt.Errorf("WriteAuto requires pointer to struct")
	}
	if val.IsNil() {
		return nil, fmt.Errorf("cannot write nil struct")
	}

	structVal := val.Elem()
	if structVal.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected pointer to struct, got pointer to %s", structVal.Kind())
	}

	var entity datalog.Identity

	// Check for existing ID
	if sw.info.IDField != nil {
		idField := structVal.Field(sw.info.IDField.Index)

		// Identity is always a pointer type now
		entity = idField.Interface().(datalog.Identity)

		// Check if ID is nil or zero (all zeros in the hash)
		var zeroHash [20]byte
		if entity == nil || entity.Hash() == zeroHash {
			// Generate new unique ID
			entity = generateUniqueID()

			// Set the ID field
			idField.Set(reflect.ValueOf(entity))
		}
	} else {
		// No ID field, generate a unique ID
		entity = generateUniqueID()
	}

	// Write the struct
	if err := sw.Write(tx, entity, v); err != nil {
		return nil, err
	}

	return entity, nil
}

// UpdateAuto generates entity ID if not set, then performs upsert with the given mode.
// This combines ID generation with update semantics.
func (sw *StructWriter) UpdateAuto(tx TransactionUpdater, lookup EntityLookup, v interface{}, mode UpdateMode) (datalog.Identity, error) {
	val := reflect.ValueOf(v)

	// Must be pointer to struct for setting ID
	if val.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("UpdateAuto requires pointer to struct")
	}
	if val.IsNil() {
		return nil, fmt.Errorf("cannot update nil struct")
	}

	structVal := val.Elem()
	if structVal.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected pointer to struct, got pointer to %s", structVal.Kind())
	}

	var entity datalog.Identity

	// Check for existing ID
	if sw.info.IDField != nil {
		idField := structVal.Field(sw.info.IDField.Index)

		// Identity is always a pointer type now
		entity = idField.Interface().(datalog.Identity)

		// Check if ID is nil or zero (all zeros in the hash)
		var zeroHash [20]byte
		if entity == nil || entity.Hash() == zeroHash {
			// Generate new unique ID
			entity = generateUniqueID()

			// Set the ID field
			idField.Set(reflect.ValueOf(entity))
		}
	} else {
		// No ID field, generate a unique ID
		entity = generateUniqueID()
	}

	// Update the struct with upsert semantics
	if err := sw.Update(tx, lookup, entity, v, mode); err != nil {
		return nil, err
	}

	return entity, nil
}

// SaveStruct persists a struct with upsert semantics.
// Generates entity ID if not set, returns the ID.
//
// Upsert behavior:
//   - Cardinality-one fields: retracts old value if different, adds new value
//   - Cardinality-many fields (nil slice): leaves existing values unchanged
//   - Cardinality-many fields (empty slice): clears all existing values
//   - Cardinality-many fields (non-empty): diff-based update
func SaveStruct(tx TransactionUpdater, lookup EntityLookup, v interface{}, s schema.SchemaProvider) (datalog.Identity, error) {
	writer, err := NewStructWriter(v, s)
	if err != nil {
		return nil, err
	}
	return writer.UpdateAuto(tx, lookup, v, UpdateModeReplace)
}
