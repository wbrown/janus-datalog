package reflect

import (
	"fmt"
	"reflect"

	"github.com/wbrown/janus-datalog/datalog/schema"
)

// SchemaFromStruct generates a Schema from a struct type
// The struct's fields are used to define attributes, with the struct name
// providing the namespace.
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
//	schema, err := reflect.SchemaFromStruct(Person{})
//	// Produces:
//	// :person/name   → TypeString, CardinalityOne
//	// :person/age    → TypeLong, CardinalityOne
//	// :person/friends → TypeRef, CardinalityMany
func SchemaFromStruct(v interface{}) (*schema.Schema, error) {
	return SchemaFromStructs(v)
}

// SchemaFromStructs generates a Schema from multiple struct types
// This is useful when your data model spans multiple struct types.
//
// Example:
//
//	schema, err := reflect.SchemaFromStructs(Person{}, Company{}, Address{})
func SchemaFromStructs(vs ...interface{}) (*schema.Schema, error) {
	builder := schema.NewBuilder()

	for _, v := range vs {
		if err := addStructToBuilder(builder, v); err != nil {
			return nil, err
		}
	}

	return builder.Build()
}

// addStructToBuilder adds all fields from a struct to a schema builder
func addStructToBuilder(builder *schema.Builder, v interface{}) error {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	info, err := GetStructInfo(t)
	if err != nil {
		return fmt.Errorf("parsing struct %s: %w", t.Name(), err)
	}

	for _, field := range info.Fields {
		valueType, err := GoTypeToSchemaType(field.GoType)
		if err != nil {
			return fmt.Errorf("field %s.%s: %w", t.Name(), field.FieldName, err)
		}

		cardinality := InferCardinality(field.GoType)
		uniqueElements := InferUniqueElements(field.GoType)

		ab := builder.Attribute(field.FullAttr).Type(valueType)

		switch cardinality {
		case schema.CardinalityMany:
			ab.Many()
		case schema.CardinalityVector:
			ab.Vector()
			if uniqueElements {
				ab.UniqueElements(true)
			}
		}

		ab.Add()
	}

	return nil
}

// MustSchemaFromStruct is like SchemaFromStruct but panics on error
// Useful for static schema definitions
func MustSchemaFromStruct(v interface{}) *schema.Schema {
	s, err := SchemaFromStruct(v)
	if err != nil {
		panic(err)
	}
	return s
}

// MustSchemaFromStructs is like SchemaFromStructs but panics on error
func MustSchemaFromStructs(vs ...interface{}) *schema.Schema {
	s, err := SchemaFromStructs(vs...)
	if err != nil {
		panic(err)
	}
	return s
}
