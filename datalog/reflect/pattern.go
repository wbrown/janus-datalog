package reflect

import (
	"reflect"
	"strings"

	"github.com/wbrown/janus-datalog/datalog/schema"
)

// GeneratePullPattern creates a pull pattern string from a struct type
// The pattern includes all fields from the struct, with nested patterns
// for reference fields.
//
// Example:
//
//	type Person struct {
//	    ID      datalog.Identity `datalog:"-,id"`
//	    Name    string           `datalog:"name"`
//	    Age     int64            `datalog:"age"`
//	    Friends []*Person        `datalog:"friends"`
//	}
//
//	pattern := GeneratePullPattern(Person{}, nil)
//	// Returns: "[:person/name :person/age {:person/friends [:person/name :person/age]}]"
//
// Note: To prevent infinite recursion with self-referential structs,
// nested patterns only include one level of the same type.
func GeneratePullPattern(v interface{}, s schema.SchemaProvider) string {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	visited := make(map[reflect.Type]bool)
	return generatePullPatternRecursive(t, s, visited, 0, 3)
}

// GeneratePullPatternWithDepth creates a pull pattern with a maximum nesting depth
func GeneratePullPatternWithDepth(v interface{}, s schema.SchemaProvider, maxDepth int) string {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	visited := make(map[reflect.Type]bool)
	return generatePullPatternRecursive(t, s, visited, 0, maxDepth)
}

// generatePullPatternRecursive builds the pattern string recursively
func generatePullPatternRecursive(t reflect.Type, s schema.SchemaProvider, visited map[reflect.Type]bool, depth, maxDepth int) string {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return ""
	}

	// Check for recursion limit
	if depth >= maxDepth {
		return ""
	}

	// Mark as visited for this path
	if visited[t] {
		return ""
	}
	visited[t] = true
	defer func() { visited[t] = false }()

	info, err := GetStructInfo(t)
	if err != nil {
		return ""
	}

	var specs []string

	for _, field := range info.Fields {
		// Check if this is a reference field with nested struct
		elemType := ElementType(field.GoType)
		if elemType.Kind() == reflect.Ptr {
			elemType = elemType.Elem()
		}

		isNestedStruct := elemType.Kind() == reflect.Struct &&
			elemType != timeType &&
			elemType != identityType &&
			elemType != keywordType &&
			elemType != elementIDType &&
			!isOrderedSetType(elemType) // OrderedSet is a value type, not a nested entity

		if isNestedStruct {
			// Generate nested pattern
			nestedPattern := generatePullPatternRecursive(elemType, s, visited, depth+1, maxDepth)
			if nestedPattern != "" {
				specs = append(specs, "{"+field.FullAttr+" "+nestedPattern+"}")
			} else {
				// No nested pattern (recursion limit), just include the attribute
				specs = append(specs, field.FullAttr)
			}
		} else {
			// Simple attribute
			specs = append(specs, field.FullAttr)
		}
	}

	if len(specs) == 0 {
		return ""
	}

	return "[" + strings.Join(specs, " ") + "]"
}

// GenerateSimplePullPattern creates a flat pull pattern (no nested refs)
// This is useful when you only want the direct attributes of an entity.
func GenerateSimplePullPattern(v interface{}) string {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	info, err := GetStructInfo(t)
	if err != nil {
		return ""
	}

	var specs []string
	for _, field := range info.Fields {
		specs = append(specs, field.FullAttr)
	}

	if len(specs) == 0 {
		return ""
	}

	return "[" + strings.Join(specs, " ") + "]"
}
