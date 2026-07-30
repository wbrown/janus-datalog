package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// extractElementIDFromKey extracts the ElementID from a key based on index type.
// The Tx is encoded with bitwise NOT, so we reverse it here.
func extractElementIDFromKey(index IndexType, key []byte) datalog.ElementID {
	if len(key) < prefixSize+txSize {
		return datalog.ElementID{}
	}

	var txBytes []byte

	switch index {
	case TAEV:
		// TAEV: [prefix:1][Tx:16][A:32][E:20][V:var]
		// Tx is right after prefix
		txBytes = key[prefixSize : prefixSize+txSize]

	case EATV:
		// EATV: [prefix:1][E:20][A:32][Tx:16][V:var][Op:1]
		// Tx is after E+A
		offset := prefixSize + entitySize + attrSize
		if len(key) < offset+txSize {
			return datalog.ElementID{}
		}
		txBytes = key[offset : offset+txSize]

	case AETV:
		// AETV: [prefix:1][A:32][E:20][Tx:16][V:var][Op:1]
		// Tx is after A+E
		offset := prefixSize + attrSize + entitySize
		if len(key) < offset+txSize {
			return datalog.ElementID{}
		}
		txBytes = key[offset : offset+txSize]

	case ATEV:
		// ATEV: [prefix:1][A:32][Tx:16][E:20][V:var][AfterRef?:16][Op:1]
		// Tx is immediately after A
		txBytes = key[prefixSize+attrSize : prefixSize+attrSize+txSize]

	case EAVT, AEVT, AVET, VAET:
		// These indices have Tx near the tail, but the tail also
		// carries Op (1 byte, always last) and optionally AfterRef
		// (16 bytes, immediately before Op when Op.HasAfterRef()).
		// Layout: [...][Tx:16][AfterRef:16?][Op:1]
		if len(key) < 1 {
			return datalog.ElementID{}
		}
		tailAfterTx := keyTailSize(key)
		if len(key) < tailAfterTx+txSize {
			return datalog.ElementID{}
		}
		txBytes = key[len(key)-tailAfterTx-txSize : len(key)-tailAfterTx]

	default:
		// Programmer error: a new IndexType was added without teaching this
		// switch where Tx lives in its layout. Matches the
		// encoder switch in key_encoder_binary.go.
		panic(fmt.Sprintf("extractElementIDFromKey: unknown index type %v", index))
	}

	// Reverse bitwise NOT to get original ElementID
	return DecodeElementID(txBytes)
}
