package storage

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

// Entity represents a unique identifier for an entity (20-byte SHA1 hash)
type Entity [20]byte

// Attribute represents an attribute name (stored directly if ≤32 bytes, SHA256 if longer)
type Attribute [32]byte

// Tx represents a transaction/time identifier (20 bytes)
// Applications can encode timestamps, transaction IDs, or other ordering information
type Tx [20]byte

// StorageDatom is the internal storage representation
// It uses fixed-size byte arrays for efficient storage and indexing
type StorageDatom struct {
	E  Entity        // Entity identifier (20 bytes)
	A  Attribute     // Attribute identifier (32 bytes)
	V  datalog.Value // The actual value (unbounded, stored last)
	Tx Tx            // Transaction/time identifier (20 bytes)
}

// NewEntity creates an entity ID from a string identifier
func NewEntity(id string) Entity {
	return sha1.Sum([]byte(id))
}

// NewAttribute creates an attribute from a keyword string (e.g., ":user/name")
func NewAttribute(keyword string) Attribute {
	var a Attribute
	if len(keyword) <= 32 {
		// Store directly, null-padded
		copy(a[:], keyword)
	} else {
		// Fall back to SHA256 for long attributes
		hash := sha256.Sum256([]byte(keyword))
		copy(a[:], hash[:])
	}
	return a
}

// NewTx creates a transaction ID from a string
func NewTx(id string) Tx {
	return Tx(sha1.Sum([]byte(id)))
}

// NewTxFromTime creates a transaction ID from a timestamp
// Encodes the time as nanoseconds in the first 8 bytes, rest zeros
func NewTxFromTime(t time.Time) Tx {
	var tx Tx
	binary.BigEndian.PutUint64(tx[:8], uint64(t.UnixNano()))
	return tx
}

// NewTxFromUint creates a transaction ID from a uint64
// Useful for sequential transaction numbers
func NewTxFromUint(n uint64) Tx {
	var tx Tx
	binary.BigEndian.PutUint64(tx[:8], n)
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

// String returns a string representation of Tx
func (tx Tx) String() string {
	// Check if it looks like a timestamp (first 8 bytes non-zero, rest zero)
	isTimestamp := true
	for i := 8; i < 20; i++ {
		if tx[i] != 0 {
			isTimestamp = false
			break
		}
	}

	if isTimestamp && tx[0] != 0 {
		// Try to decode as timestamp
		nano := binary.BigEndian.Uint64(tx[:8])
		t := time.Unix(0, int64(nano))
		if t.Year() > 1970 && t.Year() < 3000 { // Sanity check
			return t.Format(time.RFC3339)
		}
	}

	// Otherwise show as hex
	return fmt.Sprintf("tx:%x", tx[:8])
}

// String returns a string representation of the StorageDatom
func (d StorageDatom) String() string {
	return fmt.Sprintf("[%x %s %v %s]",
		d.E[:8], d.A.String(), d.V, d.Tx.String())
}

// Bytes returns the serialized form of the storage datom
// Format: E(20) + A(32) + Tx(20) + VSize(2) + VType(1) + V(variable)
func (d StorageDatom) Bytes() []byte {
	vBytes := datalog.ValueBytes(d.V)
	size := 72 + 3 + len(vBytes) // E+A+Tx + size+type + value

	buf := make([]byte, size)
	copy(buf[0:20], d.E[:])
	copy(buf[20:52], d.A[:])
	copy(buf[52:72], d.Tx[:])

	// Value size (2 bytes)
	binary.BigEndian.PutUint16(buf[72:74], uint16(len(vBytes)))

	// Value type (1 byte)
	buf[74] = byte(datalog.Type(d.V))

	// Value data
	copy(buf[75:], vBytes)

	return buf
}

// ToStorageDatom converts a user-facing datom to storage representation
func ToStorageDatom(d datalog.Datom) StorageDatom {
	var e Entity
	copy(e[:], d.E.Bytes())

	return StorageDatom{
		E:  e,
		A:  NewAttribute(d.A.String()),
		V:  d.V,
		Tx: NewTxFromUint(d.Tx),
	}
}

// ToDatom converts a storage datom to user-facing representation
// This requires a resolver to map hashes back to meaningful names
func (d StorageDatom) ToDatom(resolver Resolver) datalog.Datom {
	return datalog.Datom{
		E:  resolver.ResolveEntity(d.E),
		A:  resolver.ResolveAttribute(d.A),
		V:  d.V, // Values are already user-facing
		Tx: resolver.ResolveTx(d.Tx),
	}
}

// StorageDatomFromBytes deserializes a datom from bytes
func StorageDatomFromBytes(data []byte) (*StorageDatom, error) {
	if len(data) < 75 { // E(20) + A(32) + Tx(20) + size(2) + type(1)
		return nil, fmt.Errorf("datom data too short: %d bytes", len(data))
	}

	var d StorageDatom
	copy(d.E[:], data[0:20])
	copy(d.A[:], data[20:52])
	copy(d.Tx[:], data[52:72])

	// Read value size
	vSize := binary.BigEndian.Uint16(data[72:74])

	// Read value type
	vType := data[74]

	// Check we have enough data for the value
	if len(data) < 75+int(vSize) {
		return nil, fmt.Errorf("datom data truncated: expected %d bytes, got %d", 75+vSize, len(data))
	}

	// Read value data
	vData := data[75 : 75+vSize]

	// Decode value based on type
	var err error
	d.V, err = datalog.ValueFromBytes(datalog.ValueType(vType), vData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode value: %w", err)
	}

	return &d, nil
}

// Uint64 returns the transaction ID as a uint64
func (tx Tx) Uint64() uint64 {
	return binary.BigEndian.Uint64(tx[:8])
}

// Resolver provides mappings from storage to user representations
type Resolver interface {
	ResolveEntity(Entity) datalog.Identity
	ResolveAttribute(Attribute) datalog.Keyword
	ResolveTx(Tx) uint64
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
	default:
		// Fall back to string representation
		return datalog.String(fmt.Sprintf("%v", v))
	}
}

// toStorageTx converts user transaction ID to storage format
func toStorageTx(tx uint64) Tx {
	return NewTxFromUint(tx)
}
