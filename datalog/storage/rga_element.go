package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// RGAElement represents an element in a Replicated Growable Array (RGA).
// RGA is a CRDT for ordered sequences that supports concurrent inserts
// while maintaining a deterministic final order across all replicas.
//
// Each element has:
//   - ID: Unique identifier for this element (from Lamport clock)
//   - Value: The actual data stored at this position
//   - AfterRef: The element ID this was inserted after (HEAD for first elements)
//   - Tombstone: If non-nil, this element has been deleted (the value is when it was deleted)
//
// RGA maintains order through the AfterRef chain:
//   - Elements form a tree rooted at HEAD
//   - Siblings (same AfterRef) are sorted by their ID
//   - DFS traversal produces the final ordered list
type RGAElement struct {
	ID        ElementID  // This element's unique identifier
	Value     any        // The actual value
	AfterRef  ElementID  // What this element was inserted after (HEAD = zero value)
	Tombstone *ElementID // Non-nil if deleted, value is the delete timestamp
}

// RGA encoding format (stored as datom V):
//
//	[AfterRef:16][TombstoneFlag:1][Tombstone:16 if flag=1][ValueType:1][ValueData:var]
//
// AfterRef uses natural byte order (not bitwise NOT like key Tx)
// because we need to compare these values, not just sort them in keys.
//
// Note: The element's ID is NOT encoded here - it comes from the datom's Tx field.

// EncodeRGAElement serializes an RGAElement for storage.
// The ID is NOT included - it's stored as the datom's Tx field.
func EncodeRGAElement(elem RGAElement) []byte {
	// Start with AfterRef (16 bytes, natural order)
	buf := elem.AfterRef.Bytes()

	// Tombstone flag + optional tombstone value
	if elem.Tombstone != nil {
		buf = append(buf, 1) // Present flag
		buf = append(buf, elem.Tombstone.Bytes()...)
	} else {
		buf = append(buf, 0) // Not present
	}

	// Value: type byte + data
	vType := byte(datalog.Type(elem.Value))
	vData := datalog.ValueBytes(elem.Value)
	buf = append(buf, vType)
	buf = append(buf, vData...)

	return buf
}

// DecodeRGAElement deserializes an RGAElement from storage.
// The ID is passed in from the datom's Tx field.
func DecodeRGAElement(id ElementID, data []byte) (RGAElement, error) {
	if len(data) < 18 { // 16 (AfterRef) + 1 (tombstone flag) + 1 (min value type)
		return RGAElement{}, fmt.Errorf("rga element too short: need at least 18 bytes, got %d", len(data))
	}

	// Decode AfterRef (natural byte order)
	afterRef := datalog.ElementIDFromBytes(data[0:16])

	// Decode tombstone
	var tombstone *ElementID
	valueTypeOffset := 17 // After AfterRef (16) + tombstone flag (1)

	if data[16] == 1 {
		// Tombstone present
		if len(data) < 34 { // 16 + 1 + 16 + 1 (at least)
			return RGAElement{}, fmt.Errorf("rga element tombstone incomplete: need 34 bytes, got %d", len(data))
		}
		ts := datalog.ElementIDFromBytes(data[17:33])
		tombstone = &ts
		valueTypeOffset = 33 // After AfterRef (16) + flag (1) + tombstone (16)
	}

	// Decode value
	if valueTypeOffset >= len(data) {
		return RGAElement{}, fmt.Errorf("rga element missing value: offset %d, len %d", valueTypeOffset, len(data))
	}

	vType := datalog.ValueType(data[valueTypeOffset])
	vData := data[valueTypeOffset+1:]

	value, err := datalog.ValueFromBytes(vType, vData)
	if err != nil {
		return RGAElement{}, fmt.Errorf("decoding rga element value: %w", err)
	}

	return RGAElement{
		ID:        id,
		Value:     value,
		AfterRef:  afterRef,
		Tombstone: tombstone,
	}, nil
}

// IsDeleted returns true if this element has been tombstoned.
func (e RGAElement) IsDeleted() bool {
	return e.Tombstone != nil
}

// String returns a human-readable representation of the element.
func (e RGAElement) String() string {
	deleted := ""
	if e.Tombstone != nil {
		deleted = fmt.Sprintf(" (deleted at %s)", e.Tombstone.String())
	}
	return fmt.Sprintf("RGA[%s after %s: %v%s]", e.ID.String(), e.AfterRef.String(), e.Value, deleted)
}
