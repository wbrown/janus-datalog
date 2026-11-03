package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// OpenFacts opens a database on the memory-trees backend, under opts, holding
// facts: each datom's value asserted at its entity and attribute, in one
// transaction. Every fact holds. So an attribute opts.Schema leaves undefined
// is declared cardinality-many where one entity holds two values of it, and
// cardinality-vector where its value is a vector, each entity's vector
// written whole. Two different vectors at one entity cannot both hold, nor
// can a vector and a scalar at one attribute, and OpenFacts refuses each. A
// datom's Tx is the transaction's to assign and is not read. A datom carrying
// an op is a log's record, not a fact, and is refused: a log opens from its
// dump.
func OpenFacts(facts []datalog.Datom, opts DatabaseOptions) (*Database, error) {
	type entityAttribute struct {
		e datalog.Identity
		a datalog.Keyword
	}
	first := make(map[entityAttribute]interface{}, len(facts))
	firstAt := make(map[datalog.Keyword]datalog.Datom)
	sets := schema.NewSchema()
	declared := false
	for _, f := range facts {
		if f.Op != datalog.OpNone {
			return nil, fmt.Errorf("storage.OpenFacts: %v %v %v carries the op %v, a log's record; a log opens from its dump", f.E, f.A, f.V, f.Op)
		}
		undefined := opts.Schema == nil || opts.Schema.GetAttribute(f.A) == nil
		if earlier, held := firstAt[f.A]; !held {
			firstAt[f.A] = f
			if undefined && datalog.IsVector(f.V) {
				sets.Add(&schema.AttributeDefinition{Ident: f.A, Cardinality: schema.CardinalityVector})
				declared = true
			}
		} else if datalog.IsVector(earlier.V) != datalog.IsVector(f.V) {
			return nil, fmt.Errorf("storage.OpenFacts: %v holds %v and %v holds %v at %v: an attribute holds vectors or scalars, not both", earlier.E, earlier.V, f.E, f.V, f.A)
		}
		key := entityAttribute{e: f.E, a: f.A}
		value, held := first[key]
		if !held {
			first[key] = f.V
			continue
		}
		if datalog.ValuesEqual(value, f.V) {
			continue
		}
		if datalog.IsVector(f.V) {
			return nil, fmt.Errorf("storage.OpenFacts: %v holds the vectors %v and %v at %v: an entity holds one vector at an attribute", f.E, value, f.V, f.A)
		}
		if !undefined || sets.GetAttribute(f.A) != nil {
			continue
		}
		sets.Add(&schema.AttributeDefinition{Ident: f.A, Cardinality: schema.CardinalityMany})
		declared = true
	}
	if declared {
		if opts.Schema != nil {
			opts.Schema = schema.Overlay(sets, opts.Schema)
		} else {
			opts.Schema = sets
		}
	}
	opts.BackendName = "memory-trees"
	d, err := NewDatabaseWithOptions(opts)
	if err != nil {
		return nil, fmt.Errorf("storage.OpenFacts: %w", err)
	}
	tx := d.NewTransaction()
	var writeErr error
	// Set writes an entity's vector whole, diffing it against the committed
	// vector and not against this transaction's writes, so each entity's
	// vector is written once.
	written := make(map[entityAttribute]bool)
	for _, f := range facts {
		if def := sets.GetAttribute(f.A); def != nil && def.Cardinality == schema.CardinalityVector {
			key := entityAttribute{e: f.E, a: f.A}
			if written[key] {
				continue
			}
			written[key] = true
			writeErr = tx.Set(f.E, f.A, f.V)
		} else {
			writeErr = tx.Add(f.E, f.A, f.V)
		}
		if writeErr != nil {
			break
		}
	}
	if writeErr == nil {
		_, writeErr = tx.Commit()
	}
	if writeErr != nil {
		if closeErr := d.Close(); closeErr != nil {
			return nil, fmt.Errorf("storage.OpenFacts: %w; closing the database: %w", writeErr, closeErr)
		}
		return nil, fmt.Errorf("storage.OpenFacts: %w", writeErr)
	}
	return d, nil
}
