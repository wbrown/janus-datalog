package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// Op represents the operation type for set entries.
// Constants are ordered so Add < Remove.
// With key order [Value][Op][Tx↓], this means:
//   - For same Value: all Adds sort before all Removes
//   - At same (Value, Tx): Add sorts first (add-wins semantics)
const (
	OpAdd    uint8 = 0 // Sorts first - wins ties
	OpRemove uint8 = 1
)

// SetEntry represents a value in a cardinality-many set with its operation.
// Used for CRDT add-wins set semantics.
type SetEntry struct {
	Value interface{}
	Op    uint8
}

// EncodeSetEntry encodes a set entry for storage.
// Format: [type:1][Value:var][Op:1]
//
// CRITICAL: Value comes BEFORE Op in encoding.
// This ensures that entries for the same logical value sort together in EAVT index,
// enabling efficient add-wins resolution by grouping all ops for the same value.
//
// Example EAVT scan for [E][:tags]:
//
//	[E][:tags][str]["veteran"][Op:Add][Tx_high]    ← Same value grouped
//	[E][:tags][str]["veteran"][Op:Remove][Tx_low]  ← Same value grouped
//	[E][:tags][str]["warrior"][Op:Add][Tx_high]    ← Different value
func EncodeSetEntry(entry SetEntry) []byte {
	// Encode the value: [type:1][value:var]
	vType := byte(datalog.Type(entry.Value))
	vBytes := datalog.ValueBytes(entry.Value)

	// Result: [type:1][value:var][op:1]
	result := make([]byte, 1+len(vBytes)+1)
	result[0] = vType
	copy(result[1:], vBytes)
	result[len(result)-1] = entry.Op

	return result
}

// DecodeSetEntry decodes a set entry from storage format.
// Returns the entry and any error encountered.
func DecodeSetEntry(data []byte) (SetEntry, error) {
	if len(data) < 3 { // At minimum: 1 byte type + 1 byte value + 1 byte op
		return SetEntry{}, fmt.Errorf("set entry too short: %d bytes", len(data))
	}

	// Op is the last byte
	op := data[len(data)-1]
	if op != OpAdd && op != OpRemove {
		return SetEntry{}, fmt.Errorf("invalid set entry op: %d", op)
	}

	// Type is the first byte
	vType := datalog.ValueType(data[0])

	// Value bytes are between type and op
	vBytes := data[1 : len(data)-1]

	value, err := datalog.ValueFromBytes(vType, vBytes)
	if err != nil {
		return SetEntry{}, fmt.Errorf("failed to decode set entry value: %w", err)
	}

	return SetEntry{Value: value, Op: op}, nil
}

// IsAdd returns true if this is an add operation
func (e SetEntry) IsAdd() bool {
	return e.Op == OpAdd
}

// IsRemove returns true if this is a remove operation
func (e SetEntry) IsRemove() bool {
	return e.Op == OpRemove
}

// String returns a human-readable representation
func (e SetEntry) String() string {
	opStr := "Add"
	if e.Op == OpRemove {
		opStr = "Remove"
	}
	return fmt.Sprintf("SetEntry{%s: %v}", opStr, e.Value)
}
