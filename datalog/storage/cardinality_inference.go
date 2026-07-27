package storage

import (
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Schema is optional and is NOT persisted in the store, so a database can be
// reopened without one (e.g. the `datalog` CLI's storage.NewDatabase). The
// datoms are self-describing, though: each records its write-time CRDT op
// atomically with its value — one→OpNone, many→OpCRDTAdd/Remove,
// vector→OpRGAInsert/Tombstone — so cardinality is recoverable from the data
// alone. inferSchemaFromStore reconstructs a schema from those ops, which (once
// installed) makes every existing read path resolve vector/many attributes
// correctly instead of collapsing them to a single LWW value, with no
// query-path special-casing.

// cardFromOp maps a CRDT op to the cardinality whose resolver handles it, and
// reports whether the op is DECISIVE. OpCRDTAdd→many, OpRGAInsert/Tombstone→
// vector, OpNone→one are decisive. OpCRDTRemove is NOT: it is written for both a
// cardinality-one tombstone (database.go Remove, CardinalityOne) and a
// cardinality-many member removal (CardinalityMany), so it cannot classify an
// attribute by itself — callers skip Remove entries and wait for a decisive op.
func cardFromOp(op datalog.CRDTOp) (datalog.Keyword, bool) {
	switch op {
	case datalog.OpCRDTAdd:
		return schema.CardinalityMany, true
	case datalog.OpRGAInsert, datalog.OpRGATombstone:
		return schema.CardinalityVector, true
	case datalog.OpNone:
		return schema.CardinalityOne, true
	default: // OpCRDTRemove — ambiguous between one and many
		return schema.CardinalityOne, false
	}
}

// valueTypeFromValue maps a decoded datom value to its schema ValueType. Used to
// populate an inferred attribute's ValueType from a representative stored value
// (affects typed-vector formatting, not resolution correctness); falls back to
// TypeString for anything unrecognized.
func valueTypeFromValue(v interface{}) datalog.Keyword {
	switch v.(type) {
	case string:
		return schema.TypeString
	case int64:
		return schema.TypeLong
	case float64:
		return schema.TypeDouble
	case bool:
		return schema.TypeBoolean
	case time.Time:
		return schema.TypeInstant
	case datalog.Identity:
		return schema.TypeRef
	case datalog.Keyword:
		return schema.TypeKeyword
	case datalog.Symbol:
		return schema.TypeSymbol
	case []byte:
		return schema.TypeBytes
	default:
		return schema.TypeString
	}
}

// inferSchemaFromStore reconstructs a cardinality/value-type schema by reading
// the CRDT ops already stored on disk (see the package note above for why this
// is sound). It performs one keys-only pass over the ATEV index
// ([A][Tx↓][E][V]), which groups every datom by attribute. Within each
// attribute it classifies from the first DECISIVE op (see cardFromOp): leading
// OpCRDTRemove entries are skipped because they are ambiguous, and an attribute
// whose entries are ALL removes resolves empty under either cardinality, so it
// defaults to one. Returns a non-nil (possibly empty) schema.
func inferSchemaFromStore(store Store) (*schema.Schema, error) {
	s := schema.NewSchema()
	iter, err := store.ScanKeysOnly(ScanBound{Index: ATEV})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var curA datalog.Keyword
	var curStr string
	haveA := false
	decided := false
	card := schema.CardinalityOne
	var vt datalog.Keyword

	flush := func() {
		if haveA {
			s.Add(&schema.AttributeDefinition{
				Ident:       curA,
				Cardinality: card,
				ValueType:   vt,
			})
		}
	}

	for iter.Next() {
		d, derr := iter.Datom()
		if derr != nil {
			return nil, derr
		}
		// Compare by string form rather than relying on Keyword equality
		// semantics; ATEV groups all entries for an attribute contiguously.
		if aStr := d.A.String(); !haveA || aStr != curStr {
			flush()
			curA = d.A
			curStr = aStr
			haveA = true
			decided = false
			card = schema.CardinalityOne // default if only removes appear
			vt = valueTypeFromValue(d.V) // sensible default; refined below
		}
		if decided {
			continue // attribute already classified — skip to the next attribute
		}
		if c, ok := cardFromOp(d.Op); ok {
			card = c
			vt = valueTypeFromValue(d.V)
			decided = true
		}
		// else OpCRDTRemove — keep scanning this attribute for a decisive op
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	flush()
	return s, nil
}
