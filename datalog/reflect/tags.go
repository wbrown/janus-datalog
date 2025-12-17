// Package reflect provides reflection-based utilities for converting
// Go structs to/from datoms, making the database more ergonomic to use.
package reflect

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"
)

// FieldInfo contains parsed tag information for a struct field
type FieldInfo struct {
	FieldName string       // Go field name
	AttrName  string       // Attribute local name (from tag or field name)
	FullAttr  string       // Full attribute with namespace: ":person/name"
	IsID      bool         // This field holds entity Identity
	Skip      bool         // Skip this field entirely
	Index     int          // Field index in struct
	GoType    reflect.Type // Go type of field
}

// StructInfo contains metadata about a struct type
type StructInfo struct {
	Name      string                // Struct name: "Person"
	Namespace string                // Derived namespace: "person"
	Fields    []*FieldInfo          // All mapped fields (non-ID, non-skipped)
	IDField   *FieldInfo            // The identity field (if any)
	FieldMap  map[string]*FieldInfo // FullAttr → FieldInfo for quick lookup
	Type      reflect.Type          // The underlying reflect.Type
}

// GetField returns the FieldInfo for a given full attribute name
func (si *StructInfo) GetField(fullAttr string) *FieldInfo {
	return si.FieldMap[fullAttr]
}

// GetFieldByName returns the FieldInfo for a given attribute local name
func (si *StructInfo) GetFieldByName(attrName string) *FieldInfo {
	fullAttr := ":" + si.Namespace + "/" + attrName
	return si.FieldMap[fullAttr]
}

// ParseStructInfo parses a struct type and extracts tag information
func ParseStructInfo(t reflect.Type) (*StructInfo, error) {
	// Handle pointer to struct
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct type, got %s", t.Kind())
	}

	info := &StructInfo{
		Name:      t.Name(),
		Namespace: toNamespace(t.Name()),
		Fields:    make([]*FieldInfo, 0),
		FieldMap:  make(map[string]*FieldInfo),
		Type:      t,
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		fieldInfo, err := parseFieldTag(field, i, info.Namespace)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", field.Name, err)
		}

		// Handle ID field
		if fieldInfo.IsID {
			if info.IDField != nil {
				return nil, fmt.Errorf("multiple ID fields: %s and %s", info.IDField.FieldName, fieldInfo.FieldName)
			}
			info.IDField = fieldInfo
			continue
		}

		// Skip explicitly skipped fields
		if fieldInfo.Skip {
			continue
		}

		info.Fields = append(info.Fields, fieldInfo)
		info.FieldMap[fieldInfo.FullAttr] = fieldInfo
	}

	return info, nil
}

// parseFieldTag parses the datalog tag from a struct field
func parseFieldTag(field reflect.StructField, index int, namespace string) (*FieldInfo, error) {
	info := &FieldInfo{
		FieldName: field.Name,
		Index:     index,
		GoType:    field.Type,
	}

	tag := field.Tag.Get("datalog")

	// No tag - use field name converted to kebab-case
	if tag == "" {
		info.AttrName = toKebabCase(field.Name)
		info.FullAttr = ":" + namespace + "/" + info.AttrName
		return info, nil
	}

	// Parse tag parts
	parts := strings.Split(tag, ",")
	name := strings.TrimSpace(parts[0])

	// Check for modifiers
	for _, part := range parts[1:] {
		part = strings.TrimSpace(part)
		switch part {
		case "id":
			info.IsID = true
		}
	}

	// Handle skip marker
	if name == "-" {
		if info.IsID {
			// "-,id" means this is the ID field (skip as regular attribute)
			info.Skip = false
		} else {
			info.Skip = true
		}
		return info, nil
	}

	// Check if full attribute provided (contains /)
	if strings.Contains(name, "/") {
		// Full attribute - ensure it starts with :
		if !strings.HasPrefix(name, ":") {
			name = ":" + name
		}
		info.FullAttr = name
		// Extract local name
		idx := strings.LastIndex(name, "/")
		info.AttrName = name[idx+1:]
	} else {
		// Local name only - add namespace
		info.AttrName = name
		info.FullAttr = ":" + namespace + "/" + name
	}

	return info, nil
}

// toNamespace converts a struct name to a namespace
// PersonInfo → person-info
func toNamespace(name string) string {
	return toKebabCase(name)
}

// toKebabCase converts CamelCase to kebab-case
// PersonInfo → person-info
// HTTPServer → http-server
func toKebabCase(s string) string {
	if s == "" {
		return ""
	}

	var result strings.Builder
	result.Grow(len(s) + 4) // Pre-allocate with some buffer for hyphens

	for i, r := range s {
		if unicode.IsUpper(r) {
			// Add hyphen before uppercase (except at start)
			if i > 0 {
				// Check if previous char was lowercase or if next char is lowercase
				// This handles cases like "HTTPServer" → "http-server"
				prev := rune(s[i-1])
				if unicode.IsLower(prev) {
					result.WriteRune('-')
				} else if i+1 < len(s) && unicode.IsLower(rune(s[i+1])) {
					result.WriteRune('-')
				}
			}
			result.WriteRune(unicode.ToLower(r))
		} else {
			result.WriteRune(r)
		}
	}

	return result.String()
}
