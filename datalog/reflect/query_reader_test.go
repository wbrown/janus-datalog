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
	Name string           `datalog:"?name"`
	Ref  datalog.Identity `datalog:"?ref"` // Identity is already a pointer type
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

func TestNewQueryResultMapper_MixedMode(t *testing.T) {
	// WithAttrTag has: ID (skipped with "-,id"), Name (attribute tag), Age (query tag)
	// This tests mixed mode: query tags in mappings, attribute tags in pullMappings
	findColumns := []string{"?age"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(WithAttrTag{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have 1 query mapping (the ?age field)
	if len(mapper.mappings) != 1 {
		t.Errorf("expected 1 mapping, got %d", len(mapper.mappings))
	}
	if mapper.mappings[0].Tag != "?age" {
		t.Errorf("expected ?age mapping, got %s", mapper.mappings[0].Tag)
	}

	// Should have 1 pull mapping (the "person/name" field)
	// The "-,id" field is skipped because of the "-" prefix
	if len(mapper.pullMappings) != 1 {
		t.Errorf("expected 1 pullMapping, got %d", len(mapper.pullMappings))
	}
	if len(mapper.pullMappings) > 0 && mapper.pullMappings[0].Tag != "person/name" {
		t.Errorf("expected person/name pullMapping, got %s", mapper.pullMappings[0].Tag)
	}

	// Not pure pull mode since we have query tags
	if mapper.isPullMapping {
		t.Error("expected isPullMapping=false for mixed mode struct")
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
	if result.Ref != id {
		t.Errorf("Ref: expected %v, got %v", id, result.Ref)
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

	// Test with Identity value (passed directly - Identity is now a pointer type)
	tuple3 := []interface{}{"ptr", id}
	var result3 WithIdentityPointer
	if err := mapper.MapTuple(tuple3, reflect.ValueOf(&result3).Elem()); err != nil {
		t.Fatalf("MapTuple with Identity failed: %v", err)
	}
	if result3.Ref == nil || result3.Ref != id {
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

func TestKeywordPtrTypeMatch(t *testing.T) {
	// Verify that datalog.Keyword field type matches keywordPtrType
	keywordPtrType := reflect.TypeOf((datalog.Keyword)(nil))

	fieldType := reflect.TypeOf(AllTypes{}).Field(6).Type // Keyword field
	t.Logf("keywordPtrType: %v", keywordPtrType)
	t.Logf("fieldType: %v", fieldType)
	t.Logf("Equal: %v", fieldType == keywordPtrType)

	// Test interning - same keyword should return same pointer
	kw1 := datalog.NewKeyword(":test/keyword")
	kw2 := datalog.NewKeyword(":test/keyword")
	t.Logf("kw1 pointer: %p", kw1)
	t.Logf("kw2 pointer: %p", kw2)
	t.Logf("Same pointer: %v", kw1 == kw2)

	if kw1 != kw2 {
		t.Errorf("Interned keywords should be same pointer")
	}

	// Test the actual mapping flow
	kw := datalog.NewKeyword(":status/active")
	tuple := []interface{}{kw}
	t.Logf("Input kw pointer: %p", kw)

	findColumns := []string{"?kw"}
	type JustKeyword struct {
		Keyword datalog.Keyword `datalog:"?kw"`
	}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(JustKeyword{}), findColumns)
	if err != nil {
		t.Fatalf("mapper error: %v", err)
	}

	var result JustKeyword
	if err := mapper.MapTuple(tuple, reflect.ValueOf(&result).Elem()); err != nil {
		t.Fatalf("MapTuple failed: %v", err)
	}

	t.Logf("Result kw pointer: %p", result.Keyword)
	t.Logf("Same pointer after mapping: %v", result.Keyword == kw)

	if result.Keyword != kw {
		t.Errorf("Keyword pointers should match: input=%p result=%p", kw, result.Keyword)
	}
}

// =============================================================================
// Pull Mapping Tests
// =============================================================================

// EntityStruct simulates an entity struct with attribute-style tags (for pull result mapping)
// Note: ID uses "db/id" (or ":db/id") like any other attribute - no special handling needed
type EntityStruct struct {
	ID       datalog.Identity `datalog:"db/id"`
	Name     string           `datalog:"person/name"`
	Age      int64            `datalog:"person/age"`
	Email    string           `datalog:"person/email"`
	Active   bool             `datalog:"person/active"`
	Category datalog.Keyword  `datalog:"person/category"`
}

// EntityWithSlices tests cardinality-many attributes
type EntityWithSlices struct {
	ID   datalog.Identity   `datalog:"db/id"`
	Name string             `datalog:"person/name"`
	Tags []string           `datalog:"person/tags"`
	Refs []datalog.Identity `datalog:"person/refs"`
}

func TestNewQueryResultMapper_PullMapping(t *testing.T) {
	// When struct has only attribute-style tags, isPullMapping should be true
	findColumns := []string{"(pull ?e [*])"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(EntityStruct{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !mapper.isPullMapping {
		t.Error("expected isPullMapping=true for attribute-tagged struct")
	}

	// Should have pull mappings for all fields (stored in pullMappings, not mappings)
	if len(mapper.pullMappings) != 6 {
		t.Errorf("expected 6 pullMappings (ID, Name, Age, Email, Active, Category), got %d", len(mapper.pullMappings))
	}

	// mappings should be empty for pure pull mode
	if len(mapper.mappings) != 0 {
		t.Errorf("expected 0 mappings in pure pull mode, got %d", len(mapper.mappings))
	}
}

func TestMapTuple_PullResult(t *testing.T) {
	findColumns := []string{"(pull ?e [*])"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(EntityStruct{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Simulate a pull result map
	// Pull executor uses keys WITHOUT colon prefix (e.g., "person/name" not ":person/name")
	id := datalog.NewIdentity("test:alice")
	category := datalog.NewKeyword(":category/admin")
	pullMap := map[string]interface{}{
		"db/id":           id,
		"person/name":     "Alice",
		"person/age":      int64(30),
		"person/email":    "alice@example.com",
		"person/active":   true,
		"person/category": category,
	}

	tuple := []interface{}{pullMap}
	var result EntityStruct
	if err := mapper.MapTuple(tuple, reflect.ValueOf(&result).Elem()); err != nil {
		t.Fatalf("MapTuple failed: %v", err)
	}

	if result.ID != id {
		t.Errorf("ID mismatch: expected %v, got %v", id, result.ID)
	}
	if result.Name != "Alice" {
		t.Errorf("Name mismatch: expected Alice, got %s", result.Name)
	}
	if result.Age != 30 {
		t.Errorf("Age mismatch: expected 30, got %d", result.Age)
	}
	if result.Email != "alice@example.com" {
		t.Errorf("Email mismatch: expected alice@example.com, got %s", result.Email)
	}
	if result.Active != true {
		t.Errorf("Active mismatch: expected true, got %v", result.Active)
	}
	if result.Category != category {
		t.Errorf("Category mismatch: expected %v, got %v", category, result.Category)
	}
}

func TestMapTuple_PullResult_WithSlices(t *testing.T) {
	findColumns := []string{"(pull ?e [*])"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(EntityWithSlices{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Simulate a pull result with cardinality-many values
	// Pull executor uses keys WITHOUT colon prefix
	id := datalog.NewIdentity("test:bob")
	ref1 := datalog.NewIdentity("test:ref1")
	ref2 := datalog.NewIdentity("test:ref2")
	pullMap := map[string]interface{}{
		"db/id":       id,
		"person/name": "Bob",
		"person/tags": []interface{}{"admin", "developer", "reviewer"},
		"person/refs": []interface{}{ref1, ref2},
	}

	tuple := []interface{}{pullMap}
	var result EntityWithSlices
	if err := mapper.MapTuple(tuple, reflect.ValueOf(&result).Elem()); err != nil {
		t.Fatalf("MapTuple failed: %v", err)
	}

	if result.ID != id {
		t.Errorf("ID mismatch: expected %v, got %v", id, result.ID)
	}
	if result.Name != "Bob" {
		t.Errorf("Name mismatch: expected Bob, got %s", result.Name)
	}
	if len(result.Tags) != 3 {
		t.Errorf("Tags length mismatch: expected 3, got %d", len(result.Tags))
	} else {
		expected := []string{"admin", "developer", "reviewer"}
		for i, tag := range expected {
			if result.Tags[i] != tag {
				t.Errorf("Tag[%d] mismatch: expected %s, got %s", i, tag, result.Tags[i])
			}
		}
	}
	if len(result.Refs) != 2 {
		t.Errorf("Refs length mismatch: expected 2, got %d", len(result.Refs))
	} else {
		if result.Refs[0] != ref1 || result.Refs[1] != ref2 {
			t.Errorf("Refs mismatch: expected [%v, %v], got %v", ref1, ref2, result.Refs)
		}
	}
}

func TestMapAll_PullResults(t *testing.T) {
	findColumns := []string{"(pull ?e [*])"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(EntityStruct{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Pull executor uses keys WITHOUT colon prefix
	id1 := datalog.NewIdentity("test:alice")
	id2 := datalog.NewIdentity("test:bob")
	tuples := [][]interface{}{
		{map[string]interface{}{
			"db/id":       id1,
			"person/name": "Alice",
			"person/age":  int64(30),
		}},
		{map[string]interface{}{
			"db/id":       id2,
			"person/name": "Bob",
			"person/age":  int64(25),
		}},
	}

	var results []EntityStruct
	sliceVal := reflect.ValueOf(&results).Elem()
	if err := mapper.MapAll(tuples, sliceVal); err != nil {
		t.Fatalf("MapAll failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Name != "Alice" || results[0].Age != 30 {
		t.Errorf("first result incorrect: %+v", results[0])
	}
	if results[1].Name != "Bob" || results[1].Age != 25 {
		t.Errorf("second result incorrect: %+v", results[1])
	}
}

func TestMapTuple_PullResult_MissingFields(t *testing.T) {
	// Test that missing fields in pull result are left as zero values
	findColumns := []string{"(pull ?e [*])"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(EntityStruct{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Pull result with only some fields
	// Pull executor uses keys WITHOUT colon prefix
	id := datalog.NewIdentity("test:charlie")
	pullMap := map[string]interface{}{
		"db/id":       id,
		"person/name": "Charlie",
		// age, email, active, category are missing
	}

	tuple := []interface{}{pullMap}
	var result EntityStruct
	if err := mapper.MapTuple(tuple, reflect.ValueOf(&result).Elem()); err != nil {
		t.Fatalf("MapTuple failed: %v", err)
	}

	if result.ID != id {
		t.Errorf("ID mismatch: expected %v, got %v", id, result.ID)
	}
	if result.Name != "Charlie" {
		t.Errorf("Name mismatch: expected Charlie, got %s", result.Name)
	}
	if result.Age != 0 {
		t.Errorf("Age should be zero value, got %d", result.Age)
	}
	if result.Email != "" {
		t.Errorf("Email should be empty, got %s", result.Email)
	}
	if result.Active != false {
		t.Errorf("Active should be false, got %v", result.Active)
	}
}

// =============================================================================
// Mixed Mode Tests - Query variables AND attribute tags in same struct
// =============================================================================

// MixedModeStruct has both query variable tags and attribute tags
type MixedModeStruct struct {
	// Query variable - comes from tuple column
	Name string `datalog:"?name"`
	// Attribute tags - come from pull result map in tuple
	ID  datalog.Identity `datalog:"db/id"`
	Age int64            `datalog:"person/age"`
}

func TestMapTuple_MixedMode(t *testing.T) {
	// Simulates: [:find ?name (pull ?e [:db/id :person/age]) :where [?e :person/name ?name]]
	findColumns := []string{"?name", "(pull ?e [:db/id :person/age])"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(MixedModeStruct{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify mapper state
	if mapper.isPullMapping {
		t.Error("expected isPullMapping=false for mixed mode")
	}
	if len(mapper.mappings) != 1 {
		t.Errorf("expected 1 query mapping, got %d", len(mapper.mappings))
	}
	if len(mapper.pullMappings) != 2 {
		t.Errorf("expected 2 pull mappings, got %d", len(mapper.pullMappings))
	}

	// Create a tuple with query value and pull result map
	id := datalog.NewIdentity("test:alice")
	pullMap := map[string]interface{}{
		"db/id":      id,
		"person/age": int64(30),
	}
	tuple := []interface{}{"Alice", pullMap}

	var result MixedModeStruct
	if err := mapper.MapTuple(tuple, reflect.ValueOf(&result).Elem()); err != nil {
		t.Fatalf("MapTuple failed: %v", err)
	}

	// Verify query variable was mapped
	if result.Name != "Alice" {
		t.Errorf("Name: expected 'Alice', got %q", result.Name)
	}

	// Verify pull attributes were mapped
	if result.ID != id {
		t.Errorf("ID: expected %v, got %v", id, result.ID)
	}
	if result.Age != 30 {
		t.Errorf("Age: expected 30, got %d", result.Age)
	}
}

func TestMapTuple_MixedMode_NoPullMap(t *testing.T) {
	// Test that mixed mode gracefully handles tuple without pull map
	findColumns := []string{"?name"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(MixedModeStruct{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Tuple with only query value, no pull map
	tuple := []interface{}{"Bob"}

	var result MixedModeStruct
	if err := mapper.MapTuple(tuple, reflect.ValueOf(&result).Elem()); err != nil {
		t.Fatalf("MapTuple failed: %v", err)
	}

	// Query variable should be mapped
	if result.Name != "Bob" {
		t.Errorf("Name: expected 'Bob', got %q", result.Name)
	}

	// Pull attributes should remain zero values (no pull map in tuple)
	if result.ID != nil {
		t.Errorf("ID: expected nil, got %v", result.ID)
	}
	if result.Age != 0 {
		t.Errorf("Age: expected 0, got %d", result.Age)
	}
}

func TestMapTuple_MixedMode_PullMapFirst(t *testing.T) {
	// Test with pull map as first column: [:find (pull ?e [*]) ?name ...]
	findColumns := []string{"(pull ?e [*])", "?name"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(MixedModeStruct{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	id := datalog.NewIdentity("test:charlie")
	pullMap := map[string]interface{}{
		"db/id":      id,
		"person/age": int64(25),
	}
	// Pull map first, then query value
	tuple := []interface{}{pullMap, "Charlie"}

	var result MixedModeStruct
	if err := mapper.MapTuple(tuple, reflect.ValueOf(&result).Elem()); err != nil {
		t.Fatalf("MapTuple failed: %v", err)
	}

	if result.Name != "Charlie" {
		t.Errorf("Name: expected 'Charlie', got %q", result.Name)
	}
	if result.ID != id {
		t.Errorf("ID: expected %v, got %v", id, result.ID)
	}
	if result.Age != 25 {
		t.Errorf("Age: expected 25, got %d", result.Age)
	}
}

func TestMapAll_MixedMode(t *testing.T) {
	findColumns := []string{"?name", "(pull ?e [*])"}
	mapper, err := NewQueryResultMapper(reflect.TypeOf(MixedModeStruct{}), findColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	id1 := datalog.NewIdentity("test:alice")
	id2 := datalog.NewIdentity("test:bob")
	tuples := [][]interface{}{
		{"Alice", map[string]interface{}{"db/id": id1, "person/age": int64(30)}},
		{"Bob", map[string]interface{}{"db/id": id2, "person/age": int64(25)}},
	}

	var results []MixedModeStruct
	sliceVal := reflect.ValueOf(&results).Elem()
	if err := mapper.MapAll(tuples, sliceVal); err != nil {
		t.Fatalf("MapAll failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if results[0].Name != "Alice" || results[0].Age != 30 || results[0].ID != id1 {
		t.Errorf("first result incorrect: %+v", results[0])
	}
	if results[1].Name != "Bob" || results[1].Age != 25 || results[1].ID != id2 {
		t.Errorf("second result incorrect: %+v", results[1])
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
