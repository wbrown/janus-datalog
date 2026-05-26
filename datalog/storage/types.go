package storage

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

// Entity represents a unique identifier for an entity (20-byte SHA1 hash)
type Entity [20]byte

// Attribute represents an attribute name, stored directly as the keyword's
// UTF-8 bytes. Names longer than the array are rejected at write and schema
// time (see datalog.MaxAttributeBytes); they are never truncated.
type Attribute [32]byte

// Compile-time guarantee that the Attribute storage form matches the advertised
// maximum attribute length. If Attribute is resized, datalog.MaxAttributeBytes
// must change to match (or this assignment stops compiling).
var _ [datalog.MaxAttributeBytes]byte = Attribute{}

// Tx represents a transaction/CRDT identifier (16 bytes = ElementID)
// Layout: Lamport (8 bytes big-endian) + ReplicaID (8 bytes big-endian)
// This replaces the old 20-byte SHA1-based transaction ID.
type Tx [16]byte

// StorageDatom is the internal storage representation
// It uses fixed-size byte arrays for efficient storage and indexing
type StorageDatom struct {
	E        Entity         // Entity identifier (20 bytes)
	A        Attribute      // Attribute identifier (32 bytes)
	V        datalog.Value  // The actual value (unbounded, stored last)
	Tx       Tx             // Transaction/time identifier (16 bytes = ElementID)
	Op       datalog.CRDTOp // CRDT operation (0=none, 1=add, 2=remove, 3=rga-insert, 4=rga-tombstone)
	AfterRef Tx             // RGA position reference (16 bytes, only used when Op.HasAfterRef() is true)
}

// NewEntity creates an entity ID from a string identifier
func NewEntity(id string) Entity {
	return sha1.Sum([]byte(id))
}

// NewTxFromElementID creates a Tx from an ElementID
func NewTxFromElementID(eid datalog.ElementID) Tx {
	var tx Tx
	binary.BigEndian.PutUint64(tx[0:8], eid.Lamport)
	binary.BigEndian.PutUint64(tx[8:16], eid.ReplicaID)
	return tx
}

// NewTxFromTime creates a transaction ID from a timestamp
// Encodes the time as nanoseconds in Lamport position (first 8 bytes)
// ReplicaID is set to 0
func NewTxFromTime(t time.Time) Tx {
	var tx Tx
	binary.BigEndian.PutUint64(tx[0:8], uint64(t.UnixNano()))
	// ReplicaID (bytes 8-16) remains zero
	return tx
}

// String returns the entity as hex string (first 8 bytes)
func (e Entity) String() string {
	return fmt.Sprintf("e:%x", e[:8])
}

// String returns the attribute as a string (if stored directly)
func (a Attribute) String() string {
	// Check if it's likely a direct string (has null bytes or printable ASCII)
	s := string(bytes.TrimRight(a[:], "\x00"))
	if s != "" && isPrintableASCII(s) {
		return s
	}
	// Otherwise it's a hash, show first 8 bytes
	return fmt.Sprintf("attr:%x", a[:8])
}

// isPrintableASCII checks if all bytes are printable ASCII
func isPrintableASCII(s string) bool {
	for _, r := range s {
		if r < 32 || r > 126 {
			return false
		}
	}
	return true
}

// String returns a string representation of Tx (ElementID format)
func (tx Tx) String() string {
	lamport := binary.BigEndian.Uint64(tx[0:8])
	replicaID := binary.BigEndian.Uint64(tx[8:16])

	// If ReplicaID is 0 and Lamport looks like a timestamp, format as time
	if replicaID == 0 && lamport > 0 {
		t := time.Unix(0, int64(lamport))
		if t.Year() > 1970 && t.Year() < 3000 { // Sanity check
			return t.Format(time.RFC3339)
		}
	}

	// Otherwise show as ElementID format: L<lamport>@R<replica>
	return fmt.Sprintf("L%d@R%d", lamport, replicaID)
}

// String returns a string representation of the StorageDatom
func (d StorageDatom) String() string {
	return fmt.Sprintf("[%x %s %v %s]",
		d.E[:8], d.A.String(), d.V, d.Tx.String())
}

// Bytes returns the serialized form of the storage datom
// Format: E(20) + A(32) + Tx(16) + VSize(2) + VType(1) + V(variable)
func (d StorageDatom) Bytes() []byte {
	vBytes := datalog.ValueBytes(d.V)
	size := 68 + 3 + len(vBytes) // E+A+Tx + size+type + value

	buf := make([]byte, size)
	copy(buf[0:20], d.E[:])
	copy(buf[20:52], d.A[:])
	copy(buf[52:68], d.Tx[:])

	// Value size (2 bytes)
	binary.BigEndian.PutUint16(buf[68:70], uint16(len(vBytes)))

	// Value type (1 byte)
	buf[70] = byte(datalog.Type(d.V))

	// Value data
	copy(buf[71:], vBytes)

	return buf
}

// ToStorageDatom converts a user-facing datom to storage representation
func ToStorageDatom(d datalog.Datom) StorageDatom {
	var e Entity
	if d.E != nil {
		copy(e[:], d.E.Bytes())
	}

	var a Attribute
	copy(a[:], d.A.String())

	return StorageDatom{
		E:        e,
		A:        a,
		V:        d.V,
		Tx:       NewTxFromElementID(d.Tx),
		Op:       d.Op,
		AfterRef: NewTxFromElementID(d.AfterRef),
	}
}

// ToDatom converts a storage datom to user-facing representation
// This requires a resolver to map hashes back to meaningful names
func (d StorageDatom) ToDatom(resolver Resolver) datalog.Datom {
	return datalog.Datom{
		E:        resolver.ResolveEntity(d.E),
		A:        resolver.ResolveAttribute(d.A),
		V:        d.V, // Values are already user-facing
		Tx:       d.Tx.ToElementID(),
		Op:       d.Op,
		AfterRef: d.AfterRef.ToElementID(),
	}
}

// StorageDatomFromBytes deserializes a datom from bytes
func StorageDatomFromBytes(data []byte) (*StorageDatom, error) {
	if len(data) < 71 { // E(20) + A(32) + Tx(16) + size(2) + type(1)
		return nil, fmt.Errorf("datom data too short: %d bytes", len(data))
	}

	var d StorageDatom
	copy(d.E[:], data[0:20])
	copy(d.A[:], data[20:52])
	copy(d.Tx[:], data[52:68])

	// Read value size
	vSize := binary.BigEndian.Uint16(data[68:70])

	// Read value type
	vType := data[70]

	// Check we have enough data for the value
	if len(data) < 71+int(vSize) {
		return nil, fmt.Errorf("datom data truncated: expected %d bytes, got %d", 71+vSize, len(data))
	}

	// Read value data
	vData := data[71 : 71+vSize]

	// Decode value based on type
	var err error
	d.V, err = datalog.ValueFromBytes(datalog.ValueType(vType), vData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode value: %w", err)
	}

	return &d, nil
}

// Uint64 returns the Lamport component of the transaction ID as uint64
func (tx Tx) Uint64() uint64 {
	return binary.BigEndian.Uint64(tx[0:8])
}

// ToElementID converts Tx to ElementID
func (tx Tx) ToElementID() datalog.ElementID {
	return datalog.ElementID{
		Lamport:   binary.BigEndian.Uint64(tx[0:8]),
		ReplicaID: binary.BigEndian.Uint64(tx[8:16]),
	}
}

// Resolver provides mappings from storage to user representations
type Resolver interface {
	ResolveEntity(Entity) datalog.Identity
	ResolveAttribute(Attribute) datalog.Keyword
	ResolveTx(Tx) datalog.ElementID
}

// toStorageValue converts user values to storage values
func toStorageValue(v interface{}) datalog.Value {
	// Most values pass through directly
	switch val := v.(type) {
	case datalog.Value:
		return val
	case string:
		return datalog.String(val)
	case int64:
		return datalog.Int(val)
	case int:
		return datalog.Int(int64(val))
	case float64:
		return datalog.Float(val)
	case bool:
		return datalog.Bool(val)
	case datalog.Symbol:
		return datalog.SymbolValue(val)
	default:
		// Fall back to string representation
		return datalog.String(fmt.Sprintf("%v", v))
	}
}

