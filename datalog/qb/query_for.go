package qb

import (
	"fmt"
	"reflect"
)

// TypedQueryBuilder provides type-safe variable references from a result struct.
// The struct's datalog tags define variable names.
// Use QueryFor[T]() to create one.
type TypedQueryBuilder[T any] struct {
	F          T                // Zero-valued struct for field references
	vars       map[uintptr]*Var // Cache: field offset -> *Var
	findVars   []*Var           // Accumulated Find elements (ordered)
	findSet    map[uintptr]bool // Track which fields added to Find
	structInfo *structVarInfo   // Cached struct metadata
}

// structVarInfo caches struct field metadata for variable extraction
type structVarInfo struct {
	fields []fieldVarInfo
}

type fieldVarInfo struct {
	offset uintptr
	tag    string // The datalog tag value (e.g., "?name")
}

// QueryFor creates a new type-safe query builder for result type T.
// The type parameter T should be a struct with datalog tags on fields.
//
// Example:
//
//	type Result struct {
//	    Name string `datalog:"?name"`
//	}
//	q := QueryFor[Result]()
func QueryFor[T any]() *TypedQueryBuilder[T] {
	q := &TypedQueryBuilder[T]{
		vars:    make(map[uintptr]*Var),
		findSet: make(map[uintptr]bool),
	}
	q.structInfo = parseStructVarInfo[T]()
	return q
}

// V returns the *Var for a field without adding it to Find.
// Use for referencing variables that shouldn't appear in results.
//
// Example:
//
//	// Use entity in pattern but don't include in results
//	Pat(q.V(&f.Person), PersonName, q.Find(&f.Name))
func (q *TypedQueryBuilder[T]) V(fieldPtr any) *Var {
	offset := q.fieldOffset(fieldPtr)

	if v, ok := q.vars[offset]; ok {
		return v
	}

	tag := q.findTag(offset)
	v := NewVar(tag)
	q.vars[offset] = v
	return v
}

// Find returns the *Var for a field AND adds it to the Find clause.
// First call for a field determines its position in Find.
// Subsequent calls return the same *Var without duplicating in Find.
//
// Example:
//
//	// Add ?name to Find clause and use in pattern
//	Pat(e, PersonName, q.Find(&f.Name))
func (q *TypedQueryBuilder[T]) Find(fieldPtr any) *Var {
	offset := q.fieldOffset(fieldPtr)
	v := q.V(fieldPtr) // Get or create the Var

	if !q.findSet[offset] {
		q.findVars = append(q.findVars, v)
		q.findSet[offset] = true
	}
	return v
}

// Where creates a QueryBuilder with the accumulated Find clause.
// The Find clause contains variables added via Find() calls, in call order.
func (q *TypedQueryBuilder[T]) Where(clauses ...interface{}) *QueryBuilder {
	findElements := make([]interface{}, len(q.findVars))
	for i, v := range q.findVars {
		findElements[i] = v
	}
	return Query().Find(findElements...).Where(clauses...)
}

// fieldOffset computes the offset of fieldPtr from the struct base.
func (q *TypedQueryBuilder[T]) fieldOffset(fieldPtr any) uintptr {
	ptr := reflect.ValueOf(fieldPtr).Pointer()
	base := reflect.ValueOf(&q.F).Pointer()
	return ptr - base
}

// findTag looks up the datalog tag for a field by its offset.
func (q *TypedQueryBuilder[T]) findTag(offset uintptr) string {
	for _, f := range q.structInfo.fields {
		if f.offset == offset {
			return f.tag
		}
	}
	panic(fmt.Sprintf("QueryFor: field at offset %d not found in struct (missing datalog tag?)", offset))
}

// parseStructVarInfo extracts field metadata from type T.
func parseStructVarInfo[T any]() *structVarInfo {
	var zero T
	t := reflect.TypeOf(zero)

	info := &structVarInfo{}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("datalog")
		if tag == "" || tag == "-" {
			continue
		}
		// Must be a variable tag (starts with ?)
		if !isVarTag(tag) {
			continue
		}
		info.fields = append(info.fields, fieldVarInfo{
			offset: field.Offset,
			tag:    tag,
		})
	}
	return info
}

// isVarTag checks if tag is a query variable (starts with ?).
func isVarTag(tag string) bool {
	return len(tag) > 0 && tag[0] == '?'
}
