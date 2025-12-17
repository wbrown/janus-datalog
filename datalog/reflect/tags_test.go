package reflect

import (
	"reflect"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// Test types for tag parsing
type SimpleStruct struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name string           `datalog:"name"`
	Age  int64            `datalog:"age"`
}

type StructWithDefaults struct {
	ID          datalog.Identity `datalog:"-,id"`
	FirstName   string           // Uses field name converted to kebab-case
	LastName    string           // Uses field name converted to kebab-case
	EmailAddr   string           `datalog:"email"` // Explicit name
	unexported  string           // Should be ignored
	SkipMe      string           `datalog:"-"` // Explicit skip
}

type StructWithFullAttr struct {
	ID     datalog.Identity `datalog:"-,id"`
	Name   string           `datalog:"custom/name"`   // Full attribute name
	Other  string           `datalog:":other/value"`  // Full attribute with colon
}

func TestParseStructInfo_Simple(t *testing.T) {
	info, err := ParseStructInfo(reflect.TypeOf(SimpleStruct{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if info.Name != "SimpleStruct" {
		t.Errorf("expected Name=SimpleStruct, got %s", info.Name)
	}
	if info.Namespace != "simple-struct" {
		t.Errorf("expected Namespace=simple-struct, got %s", info.Namespace)
	}
	if info.IDField == nil {
		t.Fatal("expected IDField to be set")
	}
	if !info.IDField.IsID {
		t.Error("expected IDField.IsID to be true")
	}
	if len(info.Fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(info.Fields))
	}

	// Check Name field
	nameField := info.GetFieldByName("name")
	if nameField == nil {
		t.Fatal("expected to find name field")
	}
	if nameField.FullAttr != ":simple-struct/name" {
		t.Errorf("expected FullAttr=:simple-struct/name, got %s", nameField.FullAttr)
	}

	// Check Age field
	ageField := info.GetFieldByName("age")
	if ageField == nil {
		t.Fatal("expected to find age field")
	}
	if ageField.FullAttr != ":simple-struct/age" {
		t.Errorf("expected FullAttr=:simple-struct/age, got %s", ageField.FullAttr)
	}
}

func TestParseStructInfo_Defaults(t *testing.T) {
	info, err := ParseStructInfo(reflect.TypeOf(StructWithDefaults{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have 3 fields (FirstName, LastName, EmailAddr)
	// unexported and SkipMe should be excluded
	if len(info.Fields) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(info.Fields))
	}

	// Check FirstName field - uses field name converted to kebab-case
	firstNameField := info.GetFieldByName("first-name")
	if firstNameField == nil {
		t.Fatal("expected to find first-name field")
	}
	if firstNameField.FullAttr != ":struct-with-defaults/first-name" {
		t.Errorf("expected FullAttr=:struct-with-defaults/first-name, got %s", firstNameField.FullAttr)
	}

	// Check explicit email field
	emailField := info.GetFieldByName("email")
	if emailField == nil {
		t.Fatal("expected to find email field")
	}
}

func TestParseStructInfo_FullAttr(t *testing.T) {
	info, err := ParseStructInfo(reflect.TypeOf(StructWithFullAttr{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check custom/name
	nameField := info.GetField(":custom/name")
	if nameField == nil {
		t.Fatal("expected to find :custom/name field")
	}

	// Check :other/value
	otherField := info.GetField(":other/value")
	if otherField == nil {
		t.Fatal("expected to find :other/value field")
	}
}

func TestParseStructInfo_Pointer(t *testing.T) {
	// Should handle pointer to struct
	info, err := ParseStructInfo(reflect.TypeOf(&SimpleStruct{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if info.Name != "SimpleStruct" {
		t.Errorf("expected Name=SimpleStruct, got %s", info.Name)
	}
}

func TestToKebabCase(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"Person", "person"},
		{"PersonInfo", "person-info"},
		{"HTTPServer", "http-server"},
		{"XMLParser", "xml-parser"},
		{"ID", "id"},
		{"userID", "user-id"},
		{"firstName", "first-name"},
		{"", ""},
		{"A", "a"},
		{"AB", "ab"},
		{"ABC", "abc"},
	}

	for _, tt := range tests {
		result := toKebabCase(tt.input)
		if result != tt.expected {
			t.Errorf("toKebabCase(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

// Test error cases
type StructWithMultipleIDs struct {
	ID1 datalog.Identity `datalog:"-,id"`
	ID2 datalog.Identity `datalog:"-,id"`
}

func TestParseStructInfo_MultipleIDs(t *testing.T) {
	_, err := ParseStructInfo(reflect.TypeOf(StructWithMultipleIDs{}))
	if err == nil {
		t.Error("expected error for multiple ID fields")
	}
}

func TestParseStructInfo_NonStruct(t *testing.T) {
	_, err := ParseStructInfo(reflect.TypeOf(42))
	if err == nil {
		t.Error("expected error for non-struct type")
	}
}
