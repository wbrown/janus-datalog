package storage

import (
	"fmt"
	"reflect"

	"github.com/wbrown/janus-datalog/datalog"
)

// AttributeSchema maps keywords to accessor functions for NewSliceSource.
// Each entry defines how to extract a value from a slice item for a given attribute.
type AttributeSchema[T any] map[datalog.Keyword]func(T) any

// NewSliceSource opens a memory database holding a Go slice's facts under an
// attribute schema. The item at index i is the entity "slice:i", holding at
// each attribute the value its accessor answers, or every element of the
// slice or array it answers. The facts open through OpenFacts, so an
// attribute at which one item holds two elements is cardinality-many.
//
// Example:
//
//	type Rule struct {
//	    Key       string
//	    DependsOn []string
//	}
//
//	source, err := NewSliceSource(rules, AttributeSchema[Rule]{
//	    datalog.NewKeyword(":rule/key"):        func(r Rule) any { return r.Key },
//	    datalog.NewKeyword(":rule/depends-on"): func(r Rule) any { return r.DependsOn },
//	})
//	if err != nil {
//	    return err
//	}
//	defer source.Close()
func NewSliceSource[T any](items []T, schema AttributeSchema[T]) (*Database, error) {
	var facts []datalog.Datom
	for i, item := range items {
		entity := datalog.NewIdentity(fmt.Sprintf("slice:%d", i))
		for attr, accessor := range schema {
			value := accessor(item)
			rv := reflect.ValueOf(value)
			if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
				for j := 0; j < rv.Len(); j++ {
					facts = append(facts, datalog.Datom{E: entity, A: attr, V: rv.Index(j).Interface()})
				}
				continue
			}
			facts = append(facts, datalog.Datom{E: entity, A: attr, V: value})
		}
	}
	return OpenFacts(facts, DatabaseOptions{})
}
