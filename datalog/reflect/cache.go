package reflect

import (
	"reflect"
	"sync"
)

// structInfoCache stores parsed StructInfo by reflect.Type
// Using sync.Map for concurrent read/write safety
var structInfoCache sync.Map

// GetStructInfo returns the StructInfo for a type, using cache
func GetStructInfo(t reflect.Type) (*StructInfo, error) {
	// Normalize to non-pointer type
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Check cache first
	if cached, ok := structInfoCache.Load(t); ok {
		return cached.(*StructInfo), nil
	}

	// Parse and cache
	info, err := ParseStructInfo(t)
	if err != nil {
		return nil, err
	}

	// Store in cache (LoadOrStore handles race conditions)
	actual, _ := structInfoCache.LoadOrStore(t, info)
	return actual.(*StructInfo), nil
}

// GetStructInfoFromValue returns the StructInfo for a value
func GetStructInfoFromValue(v interface{}) (*StructInfo, error) {
	t := reflect.TypeOf(v)
	return GetStructInfo(t)
}

// ClearCache clears the struct info cache (useful for testing)
func ClearCache() {
	structInfoCache = sync.Map{}
}
