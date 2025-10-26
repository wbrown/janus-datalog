package storage

import (
	"encoding/binary"

	"github.com/wbrown/janus-datalog/datalog"
)

// QueryBuilder helps construct common query patterns
type QueryBuilder struct {
	store   Store
	encoder KeyEncoder
}

// NewQueryBuilder creates a new query builder
func NewQueryBuilder(store Store, encoder KeyEncoder) *QueryBuilder {
	// Default to Binary encoding for performance
	if encoder == nil {
		encoder = NewKeyEncoder(BinaryStrategy)
	}
	return &QueryBuilder{
		store:   store,
		encoder: encoder,
	}
}

// GetEntity returns all datoms for a given entity
func (q *QueryBuilder) GetEntity(entity datalog.Identity) ([]*datalog.Datom, error) {
	eBytes := entity.Bytes()
	start, end := q.encoder.EncodePrefixRange(EAVT, eBytes[:])
	return q.scan(EAVT, start, end)
}

// GetAttribute returns all datoms with a given attribute
func (q *QueryBuilder) GetAttribute(attr datalog.Keyword) ([]*datalog.Datom, error) {
	aBytes := attr.Bytes()
	start, end := q.encoder.EncodePrefixRange(AEVT, aBytes[:])
	return q.scan(AEVT, start, end)
}

// GetEntityAttribute returns all datoms for entity+attribute
func (q *QueryBuilder) GetEntityAttribute(entity datalog.Identity, attr datalog.Keyword) ([]*datalog.Datom, error) {
	eBytes := entity.Bytes()
	aBytes := attr.Bytes()
	start, end := q.encoder.EncodePrefixRange(EAVT, eBytes[:], aBytes[:])
	return q.scan(EAVT, start, end)
}

// GetAttributeValue returns all entities with attribute=value
func (q *QueryBuilder) GetAttributeValue(attr datalog.Keyword, value interface{}) ([]*datalog.Datom, error) {
	aBytes := attr.Bytes()
	// TODO: Need to encode value properly
	start, end := q.encoder.EncodePrefixRange(AVET, aBytes[:])
	return q.scan(AVET, start, end)
}

// GetTimeRange returns all datoms within a time range
func (q *QueryBuilder) GetTimeRange(startTx, endTx uint64) ([]*datalog.Datom, error) {
	// Convert uint64 to bytes
	var startBytes, endBytes [8]byte
	binary.BigEndian.PutUint64(startBytes[:], startTx)
	binary.BigEndian.PutUint64(endBytes[:], endTx)

	start := q.encoder.EncodePrefix(TAEV, startBytes[:])
	end := q.encoder.EncodePrefix(TAEV, endBytes[:])
	return q.scan(TAEV, start, end)
}

// GetEntityTimeRange returns datoms for an entity within a time range
func (q *QueryBuilder) GetEntityTimeRange(entity datalog.Identity, startTx, endTx uint64) ([]*datalog.Datom, error) {
	// Need to scan EAVT and filter by time
	allDatoms, err := q.GetEntity(entity)
	if err != nil {
		return nil, err
	}

	// Filter by time range
	var result []*datalog.Datom
	for _, d := range allDatoms {
		if d.Tx >= startTx && d.Tx < endTx {
			result = append(result, d)
		}
	}

	return result, nil
}

// GetReferences returns all datoms that reference the given entity
func (q *QueryBuilder) GetReferences(entity datalog.Identity) ([]*datalog.Datom, error) {
	// For entity references, we need to look for the entity as a value
	// This would require encoding the entity reference as a value
	// For now, do a full scan on VAET
	// TODO: Implement proper reference value encoding
	return q.scan(VAET, nil, nil)
}

// scan performs a range scan and collects results
func (q *QueryBuilder) scan(index IndexType, start, end []byte) ([]*datalog.Datom, error) {
	it, err := q.store.Scan(index, start, end)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var result []*datalog.Datom
	for it.Next() {
		d, err := it.Datom()
		if err != nil {
			return nil, err
		}
		result = append(result, d)
	}

	return result, nil
}

// CompareTx compares two transaction IDs
func CompareTx(a, b uint64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
