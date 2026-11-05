package storage

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// OpenFacts opens a database on the memory-trees backend, under opts, holding
// facts: each datom's value asserted at its entity and attribute. Every fact
// holds. So an attribute opts.Schema leaves undefined is cardinality-many
// where one entity holds two values of it, and cardinality-vector where its
// value is a vector, each entity's vector written whole. Two different vectors
// at one entity cannot both hold, nor can a vector and a scalar at one
// attribute, and OpenFacts refuses each. A value is held as a transaction's
// Add holds it, and refused with Add's error where Add refuses it
// (writtenValue); a vector's elements each so.
//
// OpenFacts writes the facts as an import writes a dump: each datom under the
// op its attribute's cardinality writes, asserted into the store, and then the
// schema installed from what the store states. The store infers the
// cardinality of each attribute no definition covers from those ops, so
// OpenFacts declares nothing beyond a given schema.
//
// A datom's Tx is the database's clock's to assign and is not read. A datom
// carrying an op is a log's record, not a fact, and is refused: a log opens
// from its dump.
func OpenFacts(facts []datalog.Datom, opts DatabaseOptions) (*Database, error) {
	type entityAttribute struct {
		e datalog.Identity
		a datalog.Keyword
	}
	values := make([]interface{}, len(facts))
	first := make(map[entityAttribute]interface{}, len(facts))
	firstAt := make(map[datalog.Keyword]int)
	implied := schema.NewSchema()
	for i, f := range facts {
		if f.Op != datalog.OpNone {
			return nil, fmt.Errorf("storage.OpenFacts: %v %v %v carries the op %v, a log's record; a log opens from its dump", f.E, f.A, f.V, f.Op)
		}
		undefined := opts.Schema == nil || opts.Schema.GetAttribute(f.A) == nil
		value := f.V
		var err error
		if undefined && datalog.IsVector(f.V) {
			vector := reflect.ValueOf(f.V)
			elements := make([]interface{}, vector.Len())
			for j := range elements {
				if elements[j], err = writtenValue(false, opts.Schema, f.A, vector.Index(j).Interface()); err != nil {
					return nil, fmt.Errorf("storage.OpenFacts: %w", err)
				}
			}
			value = elements
		} else if value, err = writtenValue(false, opts.Schema, f.A, f.V); err != nil {
			return nil, fmt.Errorf("storage.OpenFacts: %w", err)
		}
		values[i] = value

		if earlier, held := firstAt[f.A]; !held {
			firstAt[f.A] = i
			if undefined && datalog.IsVector(value) {
				implied.Add(&schema.AttributeDefinition{Ident: f.A, Cardinality: schema.CardinalityVector})
			}
		} else if datalog.IsVector(values[earlier]) != datalog.IsVector(value) {
			return nil, fmt.Errorf("storage.OpenFacts: %v holds %v and %v holds %v at %v: an attribute holds vectors or scalars, not both", facts[earlier].E, values[earlier], f.E, value, f.A)
		}
		key := entityAttribute{e: f.E, a: f.A}
		held, ok := first[key]
		if !ok {
			first[key] = value
			continue
		}
		if datalog.ValuesEqual(held, value) {
			continue
		}
		if datalog.IsVector(value) {
			return nil, fmt.Errorf("storage.OpenFacts: %v holds the vectors %v and %v at %v: an entity holds one vector at an attribute", f.E, held, value, f.A)
		}
		if !undefined || implied.GetAttribute(f.A) != nil {
			continue
		}
		implied.Add(&schema.AttributeDefinition{Ident: f.A, Cardinality: schema.CardinalityMany})
	}
	opts.BackendName = "memory-trees"
	d, err := NewDatabaseWithOptions(opts)
	if err != nil {
		return nil, fmt.Errorf("storage.OpenFacts: %w", err)
	}

	// The log holds each fact as the datom its attribute's cardinality writes.
	// tails holds the last element written of each entity's vector, HEAD where
	// none is, and elements the elements written of each entity's vector.
	running := d.Schema()
	log := make([]datalog.Datom, 0, len(facts))
	tails := make(map[entityAttribute]datalog.ElementID)
	elements := make(map[entityAttribute][]interface{})
	for i, f := range facts {
		definition := implied.GetAttribute(f.A)
		if definition == nil {
			definition = running.GetAttribute(f.A)
		}
		value := values[i]
		key := entityAttribute{e: f.E, a: f.A}
		switch {
		case definition != nil && definition.Cardinality == schema.CardinalityVector:
			if vector, whole := value.([]interface{}); whole {
				if _, written := tails[key]; written {
					continue
				}
				log, tails[key] = appendVectorInserts(log, f.E, f.A, vector, datalog.ElementID{}, d.clock)
				continue
			}
			if definition.UniqueElements && slices.ContainsFunc(elements[key], func(element interface{}) bool { return datalog.ValuesEqual(element, value) }) {
				continue
			}
			elements[key] = append(elements[key], value)
			log, tails[key] = appendVectorInserts(log, f.E, f.A, []interface{}{value}, tails[key], d.clock)
		case definition != nil && definition.Cardinality == schema.CardinalityMany:
			log = append(log, datalog.Datom{E: f.E, A: f.A, V: value, Tx: d.clock.Next(), Op: datalog.OpCRDTAdd})
		default:
			log = append(log, datalog.Datom{E: f.E, A: f.A, V: value, Tx: d.clock.Next()})
		}
	}
	writeErr := d.store.Assert(log)
	if writeErr == nil {
		writeErr = d.installSchema(true)
	}
	if writeErr != nil {
		if closeErr := d.Close(); closeErr != nil {
			return nil, fmt.Errorf("storage.OpenFacts: %w; closing the database: %w", writeErr, closeErr)
		}
		return nil, fmt.Errorf("storage.OpenFacts: %w", writeErr)
	}
	return d, nil
}
