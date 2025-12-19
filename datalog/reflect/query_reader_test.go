package reflect

import (
	"reflect"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

// Test structs for various scenarios

type SimpleTagged struct {
	Name string `datalog:"?name"`
	Age  int64  `datalog:"?age"`
}

type SimpleUntagged struct {
	Name string
	Age  int64
}

type WithAggregates struct {
	Dept      string  `datalog:"?dept"`
	TotalPay  float64 `datalog:"(sum ?salary)"`
	HeadCount int64   `datalog:"(count ?emp)"`
}

type MixedTags struct {
	Name string `datalog:"?name"`
	Age  int64  // No tag - mixed!
}

type WithPointers struct {
	Name  string  `datalog:"?name"`
	Email *string `datalog:"?email"`
}

type AllTypes struct {
	Str     string           `datalog:"?str"`
	Int     int64            `datalog:"?int"`
	Float   float64          `datalog:"?float"`
	Bool    bool             `datalog:"?bool"`
	Time    time.Time        `datalog:"?time"`
	ID      datalog.Identity `datalog:"?id"`
	Keyword datalog.Keyword  `datalog:"?kw"`
}

type TypeCoercion struct {
	SmallInt   int     `datalog:"?small"`
	SmallFloat float32 `datalog:"?float"`
}

type WithBytes struct {
	Name string `datalog:"?name"`
	Data []byte `datalog:"?data"`
}

type WithUint64 struct {
	Name  string `datalog:"?name"`
	Count uint64 `datalog:"?count"`
}

type WithIdentityPointer struct {
	Name string            `datalog:"?name"`
	Ref  *datalog.Identity `datalog:"?ref"`
}

type WithAttrTag struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name string           `datalog:"person/name"` // Attribute tag, not query tag
	Age  int64            `datalog:"?age"`
}

func TestNewQueryResultMapper_TaggedFields(t *testing.T) {
	findColumns := []string{"?name", "?age"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(SimpleTagged{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mapper.mappings) != 2 {
		t.Errorf("expected 2 mappings, got %d", len(mapper.mappings))
	}

	// Verify name mapping
	if mapper.mappings[0].Tag != "?name" || mapper.mappings[0].ColIndex != 0 {
		t.Errorf("name mapping incorrect: %+v", mapper.mappings[0])
	}

	// Verify age mapping
	if mapper.mappings[1].Tag != "?age" || mapper.mappings[1].ColIndex != 1 {
		t.Errorf("age mapping incorrect: %+v", mapper.mappings[1])
	}
}

func TestNewQueryResultMapper_PositionalMapping(t *testing.T) {
	findColumns := []string{"?name", "?age"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(SimpleUntagged{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mapper.mappings) != 2 {
		t.Errorf("expected 2 mappings, got %d", len(mapper.mappings))
	}

	// Positional: first field -> column 0, second field -> column 1
	if mapper.mappings[0].ColIndex != 0 {
		t.Errorf("first field should map to column 0, got %d", mapper.mappings[0].ColIndex)
	}
	if mapper.mappings[1].ColIndex != 1 {
		t.Errorf("second field should map to column 1, got %d", mapper.mappings[1].ColIndex)
	}
}

func TestNewQueryResultMapper_Aggregates(t *testing.T) {
	findColumns := []string{"?dept", "(sum ?salary)", "(count ?emp)"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(WithAggregates{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mapper.mappings) != 3 {
		t.Errorf("expected 3 mappings, got %d", len(mapper.mappings))
	}

	// Verify aggregate mappings
	for _, m := range mapper.mappings {
		switch m.Tag {
		case "?dept":
			if m.ColIndex != 0 {
				t.Errorf("?dept should map to column 0, got %d", m.ColIndex)
			}
		case "(sum ?salary)":
			if m.ColIndex != 1 {
				t.Errorf("(sum ?salary) should map to column 1, got %d", m.ColIndex)
			}
		case "(count ?emp)":
			if m.ColIndex != 2 {
				t.Errorf("(count ?emp) should map to column 2, got %d", m.ColIndex)
			}
		}
	}
}

func TestNewQueryResultMapper_MixedTagsError(t *testing.T) {
	findColumns := []string{"?name", "?age"}
	_, err := NewQueryResultMapper(reflect.TypeOf(MixedTags{}), findColumns)
	if err == nil {
		t.Fatal("expected error for mixed tags")
	}
	if err.Error() == "" || !contains(err.Error(), "mixed") {
		t.Errorf("error should mention mixed tags: %v", err)
	}
}

func TestNewQueryResultMapper_SymbolNotFound(t *testing.T) {
	findColumns := []string{"?name"} // Missing ?age
	_, err := NewQueryResultMapper(reflect.TypeOf(SimpleTagged{}), findColumns)
	if err == nil {
		t.Fatal("expected error for missing symbol")
	}
	if err.Error() == "" || !contains(err.Error(), "?age") {
		t.Errorf("error should mention missing symbol ?age: %v", err)
	}
}

func TestNewQueryResultMapper_SkipsAttributeTags(t *testing.T) {
	findColumns := []string{"?age"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(WithAttrTag{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should only have 1 mapping (the ?age field)
	// The "person/name" field should be skipped (attribute tag, not query tag)
	// The "-,id" field should be skipped
	if len(mapper.mappings) != 1 {
		t.Errorf("expected 1 mapping, got %d", len(mapper.mappings))
	}
	if mapper.mappings[0].Tag != "?age" {
		t.Errorf("expected ?age mapping, got %s", mapper.mappings[0].Tag)
	}
}

func TestMapTuple_BasicTypes(t *testing.T) {
	findColumns := []string{"?name", "?age"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(SimpleTagged{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tuple := []interface{}{"Alice", int64(30)}
	var result SimpleTagged
	if err := mapper.MapTuple(tuple, reflect.ValueOf(&result).Elem()); err != nil {
		t.Fatalf("MapTuple failed: %v", err)
	}

	if result.Name != "Alice" {
		t.Errorf("expected Name='Alice', got %q", result.Name)
	}
	if result.Age != 30 {
		t.Errorf("expected Age=30, got %d", result.Age)
	}
}

func TestMapTuple_AllTypes(t *testing.T) {
	findColumns := []string{"?str", "?int", "?float", "?bool", "?time", "?id", "?kw"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(AllTypes{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	now := time.Now()
	id := datalog.NewIdentity("test-entity")
	kw := datalog.NewKeyword(":status/active")

	tuple := []interface{}{"hello", int64(42), float64(3.14), true, now, id, kw}
	var result AllTypes
	if err := mapper.MapTuple(tuple, reflect.ValueOf(&result).Elem()); err != nil {
		t.Fatalf("MapTuple failed: %v", err)
	}

	if result.Str != "hello" {
		t.Errorf("Str: expected 'hello', got %q", result.Str)
	}
	if result.Int != 42 {
		t.Errorf("Int: expected 42, got %d", result.Int)
	}
	if result.Float != 3.14 {
		t.Errorf("Float: expected 3.14, got %f", result.Float)
	}
	if result.Bool != true {
		t.Errorf("Bool: expected true, got %v", result.Bool)
	}
	if !result.Time.Equal(now) {
		t.Errorf("Time: expected %v, got %v", now, result.Time)
	}
	if result.ID != id {
		t.Errorf("ID: expected %v, got %v", id, result.ID)
	}
	if result.Keyword != kw {
		t.Errorf("Keyword: expected %v, got %v", kw, result.Keyword)
	}
}

func TestMapTuple_TypeCoercion(t *testing.T) {
	findColumns := []string{"?small", "?float"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(TypeCoercion{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Query returns int64 and float64, but struct wants int and float32
	tuple := []interface{}{int64(100), float64(1.5)}
	var result TypeCoercion
	if err := mapper.MapTuple(tuple, reflect.ValueOf(&result).Elem()); err != nil {
		t.Fatalf("MapTuple failed: %v", err)
	}

	if result.SmallInt != 100 {
		t.Errorf("SmallInt: expected 100, got %d", result.SmallInt)
	}
	if result.SmallFloat != 1.5 {
		t.Errorf("SmallFloat: expected 1.5, got %f", result.SmallFloat)
	}
}

func TestMapTuple_PointerFields(t *testing.T) {
	findColumns := []string{"?name", "?email"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(WithPointers{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Test with non-nil email
	email := "alice@example.com"
	tuple := []interface{}{"Alice", email}
	var result WithPointers
	if err := mapper.MapTuple(tuple, reflect.ValueOf(&result).Elem()); err != nil {
		t.Fatalf("MapTuple failed: %v", err)
	}

	if result.Name != "Alice" {
		t.Errorf("Name: expected 'Alice', got %q", result.Name)
	}
	if result.Email == nil || *result.Email != email {
		t.Errorf("Email: expected %q, got %v", email, result.Email)
	}

	// Test with nil email
	tuple2 := []interface{}{"Bob", nil}
	var result2 WithPointers
	if err := mapper.MapTuple(tuple2, reflect.ValueOf(&result2).Elem()); err != nil {
		t.Fatalf("MapTuple with nil failed: %v", err)
	}

	if result2.Name != "Bob" {
		t.Errorf("Name: expected 'Bob', got %q", result2.Name)
	}
	if result2.Email != nil {
		t.Errorf("Email: expected nil, got %v", result2.Email)
	}
}

func TestMapTuple_Aggregates(t *testing.T) {
	findColumns := []string{"?dept", "(sum ?salary)", "(count ?emp)"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(WithAggregates{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tuple := []interface{}{"Engineering", float64(500000), int64(50)}
	var result WithAggregates
	if err := mapper.MapTuple(tuple, reflect.ValueOf(&result).Elem()); err != nil {
		t.Fatalf("MapTuple failed: %v", err)
	}

	if result.Dept != "Engineering" {
		t.Errorf("Dept: expected 'Engineering', got %q", result.Dept)
	}
	if result.TotalPay != 500000 {
		t.Errorf("TotalPay: expected 500000, got %f", result.TotalPay)
	}
	if result.HeadCount != 50 {
		t.Errorf("HeadCount: expected 50, got %d", result.HeadCount)
	}
}

func TestMapTuple_ByteSlice(t *testing.T) {
	findColumns := []string{"?name", "?data"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(WithBytes{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data := []byte{0x01, 0x02, 0x03, 0xAB, 0xCD}
	tuple := []interface{}{"test", data}
	var result WithBytes
	if err := mapper.MapTuple(tuple, reflect.ValueOf(&result).Elem()); err != nil {
		t.Fatalf("MapTuple failed: %v", err)
	}

	if result.Name != "test" {
		t.Errorf("Name: expected 'test', got %q", result.Name)
	}
	if len(result.Data) != 5 {
		t.Errorf("Data length: expected 5, got %d", len(result.Data))
	}
	for i, b := range data {
		if result.Data[i] != b {
			t.Errorf("Data[%d]: expected %x, got %x", i, b, result.Data[i])
		}
	}
}

func TestMapTuple_Uint64(t *testing.T) {
	findColumns := []string{"?name", "?count"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(WithUint64{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Test with uint64 value
	tuple := []interface{}{"counter", uint64(18446744073709551615)} // Max uint64
	var result WithUint64
	if err := mapper.MapTuple(tuple, reflect.ValueOf(&result).Elem()); err != nil {
		t.Fatalf("MapTuple failed: %v", err)
	}

	if result.Name != "counter" {
		t.Errorf("Name: expected 'counter', got %q", result.Name)
	}
	if result.Count != 18446744073709551615 {
		t.Errorf("Count: expected max uint64, got %d", result.Count)
	}

	// Test with int64 coercion to uint64
	tuple2 := []interface{}{"small", int64(42)}
	var result2 WithUint64
	if err := mapper.MapTuple(tuple2, reflect.ValueOf(&result2).Elem()); err != nil {
		t.Fatalf("MapTuple with int64 failed: %v", err)
	}
	if result2.Count != 42 {
		t.Errorf("Count: expected 42, got %d", result2.Count)
	}
}

func TestMapTuple_IdentityPointer(t *testing.T) {
	findColumns := []string{"?name", "?ref"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(WithIdentityPointer{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Test with non-nil Identity
	id := datalog.NewIdentity("entity:123")
	tuple := []interface{}{"test", id}
	var result WithIdentityPointer
	if err := mapper.MapTuple(tuple, reflect.ValueOf(&result).Elem()); err != nil {
		t.Fatalf("MapTuple failed: %v", err)
	}

	if result.Name != "test" {
		t.Errorf("Name: expected 'test', got %q", result.Name)
	}
	if result.Ref == nil {
		t.Fatal("Ref: expected non-nil, got nil")
	}
	if *result.Ref != id {
		t.Errorf("Ref: expected %v, got %v", id, *result.Ref)
	}

	// Test with nil ref
	tuple2 := []interface{}{"empty", nil}
	var result2 WithIdentityPointer
	if err := mapper.MapTuple(tuple2, reflect.ValueOf(&result2).Elem()); err != nil {
		t.Fatalf("MapTuple with nil failed: %v", err)
	}
	if result2.Ref != nil {
		t.Errorf("Ref: expected nil, got %v", result2.Ref)
	}

	// Test with *Identity value (pointer passed directly)
	idPtr := &id
	tuple3 := []interface{}{"ptr", idPtr}
	var result3 WithIdentityPointer
	if err := mapper.MapTuple(tuple3, reflect.ValueOf(&result3).Elem()); err != nil {
		t.Fatalf("MapTuple with *Identity failed: %v", err)
	}
	if result3.Ref == nil || *result3.Ref != id {
		t.Errorf("Ref: expected %v, got %v", id, result3.Ref)
	}
}

func TestMapAll(t *testing.T) {
	findColumns := []string{"?name", "?age"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(SimpleTagged{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tuples := [][]interface{}{
		{"Alice", int64(30)},
		{"Bob", int64(25)},
		{"Charlie", int64(35)},
	}

	var results []SimpleTagged
	sliceVal := reflect.ValueOf(&results).Elem()
	if err := mapper.MapAll(tuples, sliceVal); err != nil {
		t.Fatalf("MapAll failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	expected := []SimpleTagged{
		{Name: "Alice", Age: 30},
		{Name: "Bob", Age: 25},
		{Name: "Charlie", Age: 35},
	}

	for i, exp := range expected {
		if results[i].Name != exp.Name || results[i].Age != exp.Age {
			t.Errorf("result[%d]: expected %+v, got %+v", i, exp, results[i])
		}
	}
}

func TestMapAll_EmptyResults(t *testing.T) {
	findColumns := []string{"?name", "?age"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(SimpleTagged{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tuples := [][]interface{}{}
	var results []SimpleTagged
	sliceVal := reflect.ValueOf(&results).Elem()
	if err := mapper.MapAll(tuples, sliceVal); err != nil {
		t.Fatalf("MapAll failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestMapTuple_TypeMismatch(t *testing.T) {
	findColumns := []string{"?name", "?age"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(SimpleTagged{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Pass wrong type: string instead of int64 for age
	tuple := []interface{}{"Alice", "not a number"}
	var result SimpleTagged
	err = mapper.MapTuple(tuple, reflect.ValueOf(&result).Elem())
	if err == nil {
		t.Fatal("expected type mismatch error")
	}
}

func TestIsQueryTag(t *testing.T) {
	tests := []struct {
		tag      string
		expected bool
	}{
		{"?name", true},
		{"?x", true},
		{"(sum ?x)", true},
		{"(count ?emp)", true},
		{"(avg ?salary)", true},
		{"person/name", false},
		{":person/name", false},
		{"", false},
		{"-", false},
		{"name", false},
	}

	for _, tt := range tests {
		t.Run(tt.tag, func(t *testing.T) {
			if got := isQueryTag(tt.tag); got != tt.expected {
				t.Errorf("isQueryTag(%q) = %v, want %v", tt.tag, got, tt.expected)
			}
		})
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
