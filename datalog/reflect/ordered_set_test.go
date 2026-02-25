package reflect_test

import (
	"os"
	"reflect"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	dlreflect "github.com/wbrown/janus-datalog/datalog/reflect"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// Test types for OrderedSet integration testing

// TestOrderedSet_ReflectionDebug verifies that OrderedSet fields can be accessed via reflection
func TestOrderedSet_ReflectionDebug(t *testing.T) {
	prefs := datalog.NewOrderedSet[string]()
	prefs.Append("dark-mode")
	prefs.Append("compact-view")
	prefs.Append("notifications")

	t.Logf("Direct Items: %v (len=%d)", prefs.Items, len(prefs.Items))

	type TestStruct struct {
		Prefs datalog.OrderedSet[string] `datalog:"prefs"`
	}

	ts := TestStruct{
		Prefs: *prefs,
	}

	t.Logf("Struct Prefs.Items: %v (len=%d)", ts.Prefs.Items, len(ts.Prefs.Items))

	// Now test reflect
	val := reflect.ValueOf(ts)
	prefsField := val.FieldByName("Prefs")
	t.Logf("Reflect prefsField kind: %v", prefsField.Kind())
	t.Logf("Reflect prefsField type: %v", prefsField.Type())

	itemsField := prefsField.FieldByName("Items")
	t.Logf("Reflect itemsField valid: %v", itemsField.IsValid())
	if !itemsField.IsValid() {
		t.Fatal("Items field not found via reflection")
	}
	t.Logf("Reflect itemsField kind: %v", itemsField.Kind())
	t.Logf("Reflect itemsField Len: %d", itemsField.Len())

	if itemsField.Len() != 3 {
		t.Errorf("expected Items len=3, got %d", itemsField.Len())
	}

	for i := 0; i < itemsField.Len(); i++ {
		elem := itemsField.Index(i)
		t.Logf("  elem[%d]: canInterface=%v", i, elem.CanInterface())
		if elem.CanInterface() {
			v := elem.Interface()
			t.Logf("  elem[%d] Interface: %v (type=%T)", i, v, v)
		}
	}
}

// TestOrderedSet_WriterDirect tests the writer directly without database
func TestOrderedSet_WriterDirect(t *testing.T) {
	prefs := datalog.NewOrderedSet[string]()
	prefs.Append("a")
	prefs.Append("b")
	prefs.Append("c")

	type TestStruct struct {
		ID    datalog.Identity           `datalog:"-,id"`
		Name  string                     `datalog:"name"`
		Prefs datalog.OrderedSet[string] `datalog:"prefs"`
	}

	ts := TestStruct{
		Name:  "test",
		Prefs: *prefs,
	}

	// Create a mock transaction to capture what the writer tries to write
	var setCalls []mockSetCall

	mock := &mockTransaction{
		addFunc: func(e datalog.Identity, a datalog.Keyword, v interface{}) error {
			t.Logf("Add called: e=%s a=%s v=%v (%T)", e.L85()[:10], a.String(), v, v)
			return nil
		},
		setFunc: func(e datalog.Identity, a datalog.Keyword, v interface{}) error {
			t.Logf("Set called: e=%s a=%s v=%v (%T)", e.L85()[:10], a.String(), v, v)
			setCalls = append(setCalls, mockSetCall{a: a.String(), v: v})
			return nil
		},
	}

	sch, err := dlreflect.SchemaFromStruct(TestStruct{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	writer, err := dlreflect.NewStructWriter(&ts, sch)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	entityID := datalog.NewIdentity("test:entity")
	if err := writer.Update(mock, mock, entityID, &ts, dlreflect.UpdateModeReplace); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Check what was passed to Set for the prefs attribute
	for _, call := range setCalls {
		if call.a == ":test-struct/prefs" {
			slice, ok := call.v.([]interface{})
			if !ok {
				t.Errorf("expected []interface{} for prefs, got %T", call.v)
				continue
			}
			t.Logf("Set called with %d values: %v", len(slice), slice)
			if len(slice) != 3 {
				t.Errorf("expected 3 values in Set call, got %d", len(slice))
			}
		}
	}
}

type mockSetCall struct {
	a string
	v interface{}
}

type mockTransaction struct {
	addFunc    func(e datalog.Identity, a datalog.Keyword, v interface{}) error
	setFunc    func(e datalog.Identity, a datalog.Keyword, v interface{}) error
	removeFunc func(e datalog.Identity, a datalog.Keyword, v interface{}) error
}

func (m *mockTransaction) Add(e datalog.Identity, a datalog.Keyword, v interface{}) error {
	if m.addFunc != nil {
		return m.addFunc(e, a, v)
	}
	return nil
}

func (m *mockTransaction) Remove(e datalog.Identity, a datalog.Keyword, v interface{}) error {
	if m.removeFunc != nil {
		return m.removeFunc(e, a, v)
	}
	return nil
}

func (m *mockTransaction) Set(e datalog.Identity, a datalog.Keyword, v interface{}) error {
	if m.setFunc != nil {
		return m.setFunc(e, a, v)
	}
	return nil
}

func (m *mockTransaction) LookupAttribute(entity datalog.Identity, attr datalog.Keyword) (interface{}, bool) {
	return nil, false
}

func (m *mockTransaction) LookupAllAttributes(entity datalog.Identity, attr datalog.Keyword) []interface{} {
	return nil
}

// CharacterWithPreferences tests OrderedSet[string] fields
type CharacterWithPreferences struct {
	ID    datalog.Identity           `datalog:"-,id"`
	Name  string                     `datalog:"name"`
	Prefs datalog.OrderedSet[string] `datalog:"prefs"`
}

// EntityWithOrderedRefs tests OrderedSet[datalog.Identity] fields
type EntityWithOrderedRefs struct {
	ID      datalog.Identity                     `datalog:"-,id"`
	Name    string                               `datalog:"name"`
	Follows datalog.OrderedSet[datalog.Identity] `datalog:"follows"`
}

// TestSchemaFromStruct_OrderedSet verifies that SchemaFromStruct correctly infers
// CardinalityVector with UniqueElements for OrderedSet fields.
func TestSchemaFromStruct_OrderedSet(t *testing.T) {
	sch, err := dlreflect.SchemaFromStruct(CharacterWithPreferences{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	// Check name attribute (regular string)
	nameAttr := sch.GetAttribute(datalog.NewKeyword(":character-with-preferences/name"))
	if nameAttr == nil {
		t.Fatal("expected :character-with-preferences/name attribute")
	}
	if nameAttr.Cardinality != schema.CardinalityOne {
		t.Errorf("expected name cardinality=one, got %s", nameAttr.Cardinality)
	}

	// Check prefs attribute (OrderedSet)
	prefsAttr := sch.GetAttribute(datalog.NewKeyword(":character-with-preferences/prefs"))
	if prefsAttr == nil {
		t.Fatal("expected :character-with-preferences/prefs attribute")
	}

	t.Logf("Prefs attribute: cardinality=%s, uniqueElements=%v, type=%s",
		prefsAttr.Cardinality, prefsAttr.UniqueElements, prefsAttr.ValueType)

	if prefsAttr.Cardinality != schema.CardinalityVector {
		t.Errorf("expected prefs cardinality=vector, got %s", prefsAttr.Cardinality)
	}
	if !prefsAttr.UniqueElements {
		t.Error("expected prefs UniqueElements=true")
	}
	if prefsAttr.ValueType != schema.TypeString {
		t.Errorf("expected prefs type=string, got %s", prefsAttr.ValueType)
	}
}

// TestSchemaFromStruct_OrderedSetRef verifies ref types in OrderedSet
func TestSchemaFromStruct_OrderedSetRef(t *testing.T) {
	sch, err := dlreflect.SchemaFromStruct(EntityWithOrderedRefs{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	followsAttr := sch.GetAttribute(datalog.NewKeyword(":entity-with-ordered-refs/follows"))
	if followsAttr == nil {
		t.Fatal("expected :entity-with-ordered-refs/follows attribute")
	}

	t.Logf("Follows attribute: cardinality=%s, uniqueElements=%v, type=%s",
		followsAttr.Cardinality, followsAttr.UniqueElements, followsAttr.ValueType)

	if followsAttr.Cardinality != schema.CardinalityVector {
		t.Errorf("expected follows cardinality=vector, got %s", followsAttr.Cardinality)
	}
	if !followsAttr.UniqueElements {
		t.Error("expected follows UniqueElements=true")
	}
	if followsAttr.ValueType != schema.TypeRef {
		t.Errorf("expected follows type=ref, got %s", followsAttr.ValueType)
	}
}

// TestSaveStruct_OrderedSet_SchemaVerify verifies schema is correctly applied
func TestSaveStruct_OrderedSet_SchemaVerify(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "reflect-orderedset-schema-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create schema from struct
	sch, err := dlreflect.SchemaFromStruct(CharacterWithPreferences{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	// Verify attribute exists in schema
	prefsKw := datalog.NewKeyword(":character-with-preferences/prefs")
	prefsAttr := sch.GetAttribute(prefsKw)
	if prefsAttr == nil {
		t.Fatalf("prefs attribute not found in schema")
	}
	t.Logf("Schema attribute: %s cardinality=%s unique=%v",
		prefsKw.String(), prefsAttr.Cardinality, prefsAttr.UniqueElements)

	db, err := storage.NewDatabaseWithSchema(tmpDir, sch)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Verify database schema has the attribute
	dbSchema := db.Schema()
	if dbSchema == nil {
		t.Fatal("database has no schema")
	}
	dbAttr := dbSchema.GetAttribute(prefsKw)
	if dbAttr == nil {
		t.Fatal("prefs attribute not found in database schema")
	}
	t.Logf("DB schema attribute: %s cardinality=%s unique=%v",
		prefsKw.String(), dbAttr.Cardinality, dbAttr.UniqueElements)

	// Now test direct Set() on the transaction
	tx := db.NewTransaction()
	entityID := datalog.NewIdentity("test:entity")

	// Write 3 values via Set
	vals := []interface{}{"a", "b", "c"}
	if err := tx.Set(entityID, prefsKw, vals); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Query to verify values stored
	// Note: Vectors return the entire ordered list as a single value (array)
	// This is different from sets where each element is a separate tuple
	tuples, err := executor.CollectTuples(db.Query(
		`[:find ?v :in $ ?e :where [?e :character-with-preferences/prefs ?v]]`,
		entityID,
	))
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	t.Logf("Query returned %d tuples: %v", len(tuples), tuples)
	if len(tuples) != 1 {
		t.Fatalf("expected 1 tuple (vector as single value), got %d", len(tuples))
	}

	// The value should be a typed []string containing all 3 elements
	vectorVal, ok := tuples[0][0].([]string)
	if !ok {
		t.Fatalf("expected []string, got %T: %v", tuples[0][0], tuples[0][0])
	}
	t.Logf("Vector value contains %d elements: %v", len(vectorVal), vectorVal)
	if len(vectorVal) != 3 {
		t.Errorf("expected 3 elements in vector, got %d", len(vectorVal))
	}
}

// TestSaveStruct_OrderedSet verifies that SaveStruct correctly writes OrderedSet fields
func TestSaveStruct_OrderedSet(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "reflect-orderedset-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create schema from struct
	sch, err := dlreflect.SchemaFromStruct(CharacterWithPreferences{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	db, err := storage.NewDatabaseWithSchema(tmpDir, sch)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create character with OrderedSet of preferences
	prefs := datalog.NewOrderedSet[string]()
	prefs.Append("dark-mode")
	prefs.Append("compact-view")
	prefs.Append("notifications")

	character := CharacterWithPreferences{
		Name:  "Alice",
		Prefs: *prefs,
	}

	tx := db.NewTransaction()
	charID, err := tx.SaveStruct(&character)
	if err != nil {
		t.Fatalf("SaveStruct failed: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	t.Logf("Saved character with ID: %s", charID.L85())

	// Query to verify values are stored
	// Note: Vectors return entire ordered list as single array value
	tuples, err := executor.CollectTuples(db.Query(
		`[:find ?v :in $ ?e :where [?e :character-with-preferences/prefs ?v]]`,
		charID,
	))
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	t.Logf("Found %d tuples: %v", len(tuples), tuples)
	if len(tuples) != 1 {
		t.Fatalf("expected 1 tuple (vector as single value), got %d", len(tuples))
	}

	// Verify the vector contains 3 elements
	vectorVal, ok := tuples[0][0].([]string)
	if !ok {
		t.Fatalf("expected []string, got %T: %v", tuples[0][0], tuples[0][0])
	}
	if len(vectorVal) != 3 {
		t.Errorf("expected 3 elements in vector, got %d: %v", len(vectorVal), vectorVal)
	}
}

// TestSaveStruct_OrderedSetDuplicates verifies that duplicates are rejected at storage level
func TestSaveStruct_OrderedSetDuplicates(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "reflect-orderedset-dup-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sch, err := dlreflect.SchemaFromStruct(CharacterWithPreferences{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	db, err := storage.NewDatabaseWithSchema(tmpDir, sch)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create character and save initial prefs
	prefs := datalog.NewOrderedSet[string]()
	prefs.Append("a")
	prefs.Append("b")

	character := CharacterWithPreferences{
		Name:  "Bob",
		Prefs: *prefs,
	}

	tx := db.NewTransaction()
	charID, err := tx.SaveStruct(&character)
	if err != nil {
		t.Fatalf("SaveStruct failed: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Try to add a duplicate via direct transaction
	tx2 := db.NewTransaction()
	kw := datalog.NewKeyword(":character-with-preferences/prefs")
	// Adding "a" again should be a no-op due to UniqueElements enforcement
	if err := tx2.Add(charID, kw, "a"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if _, err := tx2.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Query to verify - vectors return as single array value
	tuples, err := executor.CollectTuples(db.Query(
		`[:find ?v :in $ ?e :where [?e :character-with-preferences/prefs ?v]]`,
		charID,
	))
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	t.Logf("Found %d tuples: %v", len(tuples), tuples)
	if len(tuples) != 1 {
		t.Fatalf("expected 1 tuple (vector as single value), got %d", len(tuples))
	}

	// Verify the vector still has only 2 unique values
	vectorVal, ok := tuples[0][0].([]string)
	if !ok {
		t.Fatalf("expected []string, got %T: %v", tuples[0][0], tuples[0][0])
	}
	t.Logf("Vector contains %d elements: %v (expecting 2, no duplicates)", len(vectorVal), vectorVal)
	if len(vectorVal) != 2 {
		t.Errorf("expected 2 unique values (duplicate rejected), got %d", len(vectorVal))
	}
}

// TestPullInto_OrderedSet verifies that PullInto correctly populates OrderedSet fields
func TestPullInto_OrderedSet(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "reflect-orderedset-pull-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sch, err := dlreflect.SchemaFromStruct(CharacterWithPreferences{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	db, err := storage.NewDatabaseWithSchema(tmpDir, sch)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create and save character
	prefs := datalog.NewOrderedSet[string]()
	prefs.Append("dark-mode")
	prefs.Append("compact-view")
	prefs.Append("notifications")

	character := CharacterWithPreferences{
		Name:  "Alice",
		Prefs: *prefs,
	}

	tx := db.NewTransaction()
	charID, err := tx.SaveStruct(&character)
	if err != nil {
		t.Fatalf("SaveStruct failed: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Pull back into new struct
	var loaded CharacterWithPreferences
	if err := db.PullInto(charID, &loaded); err != nil {
		t.Fatalf("PullInto failed: %v", err)
	}

	t.Logf("Loaded name: %s", loaded.Name)
	t.Logf("Loaded prefs count: %d", loaded.Prefs.Len())

	if loaded.Name != "Alice" {
		t.Errorf("expected Name='Alice', got %q", loaded.Name)
	}

	if loaded.Prefs.Len() != 3 {
		t.Errorf("expected 3 prefs, got %d", loaded.Prefs.Len())
	}

	// Verify the OrderedSet contains all expected values
	if !loaded.Prefs.Contains("dark-mode") {
		t.Error("expected prefs to contain 'dark-mode'")
	}
	if !loaded.Prefs.Contains("compact-view") {
		t.Error("expected prefs to contain 'compact-view'")
	}
	if !loaded.Prefs.Contains("notifications") {
		t.Error("expected prefs to contain 'notifications'")
	}
}

// TestSaveStruct_OrderedSetUpdate verifies that updating an OrderedSet works correctly
func TestSaveStruct_OrderedSetUpdate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "reflect-orderedset-update-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sch, err := dlreflect.SchemaFromStruct(CharacterWithPreferences{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	db, err := storage.NewDatabaseWithSchema(tmpDir, sch)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create and save character with initial prefs
	prefs := datalog.NewOrderedSet[string]()
	prefs.Append("a")
	prefs.Append("b")
	prefs.Append("c")

	character := CharacterWithPreferences{
		Name:  "Carol",
		Prefs: *prefs,
	}

	tx := db.NewTransaction()
	charID, err := tx.SaveStruct(&character)
	if err != nil {
		t.Fatalf("SaveStruct failed: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Update with new prefs (remove b, add d)
	newPrefs := datalog.NewOrderedSet[string]()
	newPrefs.Append("a")
	newPrefs.Append("c")
	newPrefs.Append("d")

	character.Prefs = *newPrefs

	tx2 := db.NewTransaction()
	if _, err := tx2.SaveStruct(&character); err != nil {
		t.Fatalf("SaveStruct update failed: %v", err)
	}
	if _, err := tx2.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Pull back and verify
	var loaded CharacterWithPreferences
	if err := db.PullInto(charID, &loaded); err != nil {
		t.Fatalf("PullInto failed: %v", err)
	}

	t.Logf("Updated prefs: %v", loaded.Prefs.Slice())

	if loaded.Prefs.Len() != 3 {
		t.Errorf("expected 3 prefs after update, got %d", loaded.Prefs.Len())
	}

	if !loaded.Prefs.Contains("a") {
		t.Error("expected prefs to contain 'a'")
	}
	if !loaded.Prefs.Contains("c") {
		t.Error("expected prefs to contain 'c'")
	}
	if !loaded.Prefs.Contains("d") {
		t.Error("expected prefs to contain 'd'")
	}
	if loaded.Prefs.Contains("b") {
		t.Error("expected prefs NOT to contain 'b' after update")
	}
}

// TestSchemaBuilder_OrderedSet verifies the schema builder convenience method
func TestSchemaBuilder_OrderedSet(t *testing.T) {
	builder := schema.NewBuilder()
	builder.Attribute(":test/items").Type(schema.TypeString).OrderedSet().Add()
	sch, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build schema: %v", err)
	}

	itemsAttr := sch.GetAttribute(datalog.NewKeyword(":test/items"))
	if itemsAttr == nil {
		t.Fatal("expected :test/items attribute")
	}

	if itemsAttr.Cardinality != schema.CardinalityVector {
		t.Errorf("expected cardinality=vector, got %s", itemsAttr.Cardinality)
	}
	if !itemsAttr.UniqueElements {
		t.Error("expected UniqueElements=true for OrderedSet")
	}
}

// TestAttributeDefinition_IsOrderedSet verifies the derived getter
func TestAttributeDefinition_IsOrderedSet(t *testing.T) {
	builder := schema.NewBuilder()

	// Regular vector (not ordered set)
	builder.Attribute(":test/vector").Type(schema.TypeString).Vector().Add()

	// Ordered set (vector + unique)
	builder.Attribute(":test/orderedset").Type(schema.TypeString).OrderedSet().Add()

	// Many (not vector at all)
	builder.Attribute(":test/many").Type(schema.TypeString).Many().Add()

	sch, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build schema: %v", err)
	}

	vectorAttr := sch.GetAttribute(datalog.NewKeyword(":test/vector"))
	orderedSetAttr := sch.GetAttribute(datalog.NewKeyword(":test/orderedset"))
	manyAttr := sch.GetAttribute(datalog.NewKeyword(":test/many"))

	if vectorAttr.IsOrderedSet() {
		t.Error("expected regular vector NOT to be IsOrderedSet")
	}
	if !orderedSetAttr.IsOrderedSet() {
		t.Error("expected ordered set to be IsOrderedSet")
	}
	if manyAttr.IsOrderedSet() {
		t.Error("expected many NOT to be IsOrderedSet")
	}
}

// TestSaveStruct_OrderedSetEmpty verifies that empty OrderedSet works correctly
func TestSaveStruct_OrderedSetEmpty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "reflect-orderedset-empty-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sch, err := dlreflect.SchemaFromStruct(CharacterWithPreferences{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	db, err := storage.NewDatabaseWithSchema(tmpDir, sch)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create character with empty OrderedSet
	character := CharacterWithPreferences{
		Name:  "Eve",
		Prefs: datalog.OrderedSet[string]{}, // zero value
	}

	tx := db.NewTransaction()
	charID, err := tx.SaveStruct(&character)
	if err != nil {
		t.Fatalf("SaveStruct failed: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Pull back
	var loaded CharacterWithPreferences
	if err := db.PullInto(charID, &loaded); err != nil {
		t.Fatalf("PullInto failed: %v", err)
	}

	if loaded.Name != "Eve" {
		t.Errorf("expected Name='Eve', got %q", loaded.Name)
	}

	// Empty OrderedSet should remain empty
	if loaded.Prefs.Len() != 0 {
		t.Errorf("expected 0 prefs for empty set, got %d", loaded.Prefs.Len())
	}
}

// TestSaveStruct_OrderedSetRefs verifies OrderedSet with Identity references
func TestSaveStruct_OrderedSetRefs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "reflect-orderedset-refs-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sch, err := dlreflect.SchemaFromStruct(EntityWithOrderedRefs{})
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	db, err := storage.NewDatabaseWithSchema(tmpDir, sch)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create some entities to reference
	id1 := datalog.NewIdentity("person:alice")
	id2 := datalog.NewIdentity("person:bob")
	id3 := datalog.NewIdentity("person:carol")

	// Create entity with ordered refs
	follows := datalog.NewOrderedSet[datalog.Identity]()
	follows.Append(id1)
	follows.Append(id2)
	follows.Append(id3)

	entity := EntityWithOrderedRefs{
		Name:    "Dave",
		Follows: *follows,
	}

	tx := db.NewTransaction()
	entityID, err := tx.SaveStruct(&entity)
	if err != nil {
		t.Fatalf("SaveStruct failed: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Query to verify refs are stored
	// Note: Vectors return entire ordered list as single array value
	tuples, err := executor.CollectTuples(db.Query(
		`[:find ?f :in $ ?e :where [?e :entity-with-ordered-refs/follows ?f]]`,
		entityID,
	))
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	t.Logf("Found %d tuples: %v", len(tuples), tuples)
	if len(tuples) != 1 {
		t.Fatalf("expected 1 tuple (vector as single value), got %d", len(tuples))
	}

	// Verify the vector contains 3 elements
	vectorVal, ok := tuples[0][0].([]any)
	if !ok {
		t.Fatalf("expected []any, got %T: %v", tuples[0][0], tuples[0][0])
	}
	if len(vectorVal) != 3 {
		t.Errorf("expected 3 refs in vector, got %d: %v", len(vectorVal), vectorVal)
	}

	// Pull back and verify
	var loaded EntityWithOrderedRefs
	if err := db.PullInto(entityID, &loaded); err != nil {
		t.Fatalf("PullInto failed: %v", err)
	}

	t.Logf("Loaded follows count: %d", loaded.Follows.Len())
	if loaded.Follows.Len() != 3 {
		t.Errorf("expected 3 follows, got %d", loaded.Follows.Len())
	}

	if !loaded.Follows.Contains(id1) {
		t.Error("expected follows to contain id1")
	}
	if !loaded.Follows.Contains(id2) {
		t.Error("expected follows to contain id2")
	}
	if !loaded.Follows.Contains(id3) {
		t.Error("expected follows to contain id3")
	}
}
