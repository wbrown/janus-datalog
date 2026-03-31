package datalog

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/wbrown/janus-datalog/datalog/codec"
)

// ValueType represents the type of a value
type ValueType byte

const (
	TypeString ValueType = iota
	TypeInt
	TypeFloat
	TypeBool
	TypeTime
	TypeBytes
	TypeReference
	TypeKeyword
	TypeSymbol
	TypeElementID        // 0x09 - CRDT ElementID for transaction ordering
	TypeCompressedString // 0x0A - compressed string (LZ77+FSE, data in key)
	TypeCompressedBytes  // 0x0B - compressed []byte (LZ77+FSE, data in key)
	TypeHashedString     // 0x0C - content hash of compressed string (data in blob store)
	TypeHashedBytes      // 0x0D - content hash of compressed []byte (data in blob store)
)

// BlobData holds the compressed bytes for a Tier 3 value (content hash in key,
// compressed data stored separately in the blob store).
type BlobData struct {
	Hash            [20]byte // SHA1 of CompressedBytes
	CompressedBytes []byte   // compressed payload including version header
}

// maxKeyValueSize is the maximum compressed value size that fits in a key.
// Above this, the value is stored in the blob store (Tier 3).
// Badger has a 64KB key limit; subtract overhead for E(20)+A(32)+Tx(16)+Op(1)=69 bytes.
const maxKeyValueSize = 60000

// Type returns the type of a value
func Type(v Value) ValueType {
	// Handle pointers by checking what they point to
	switch val := v.(type) {
	case *Identity:
		return TypeReference
	case *Keyword:
		return TypeKeyword
	case *Symbol:
		return TypeSymbol
	case *uint64:
		return TypeInt
	case string:
		return TypeString
	case int64:
		return TypeInt
	case float64:
		return TypeFloat
	case bool:
		return TypeBool
	case time.Time:
		return TypeTime
	case []byte:
		return TypeBytes
	case Identity:
		return TypeReference
	case Keyword:
		return TypeKeyword
	case Symbol:
		return TypeSymbol
	case ElementID:
		return TypeElementID
	case *ElementID:
		return TypeElementID
	default:
		panic(fmt.Sprintf("unknown value type: %T", val))
	}
}

// Bytes serializes a value to bytes
func ValueBytes(v Value) []byte {
	// Handle pointer types first
	switch ptr := v.(type) {
	case Identity:
		return ptr.Bytes()
	case Keyword:
		return []byte(ptr.String())
	case Symbol:
		return []byte(ptr.String())
	case *uint64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, *ptr)
		return buf
	}

	// Handle values
	switch val := v.(type) {
	case string:
		return []byte(val)
	case int64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(val))
		return buf
	case float64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(val))
		return buf
	case bool:
		if val {
			return []byte{1}
		}
		return []byte{0}
	case time.Time:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(val.UnixNano()))
		return buf
	case []byte:
		return val
	case Identity:
		return val.Bytes()
	case Keyword:
		return []byte(val.String())
	case Symbol:
		return []byte(val.String())
	case ElementID:
		// Uses natural order encoding (not bitwise NOT like key encoding)
		return val.Bytes()
	case *ElementID:
		if val != nil {
			return val.Bytes()
		}
		return make([]byte, ElementIDSize)
	default:
		panic(fmt.Sprintf("cannot encode value type: %T", v))
	}
}

// ValueFromBytes deserializes a value from bytes
func ValueFromBytes(vType ValueType, data []byte) (Value, error) {
	switch vType {
	case TypeString:
		return string(data), nil
	case TypeInt:
		if len(data) != 8 {
			return nil, fmt.Errorf("int value must be 8 bytes, got %d", len(data))
		}
		return int64(binary.BigEndian.Uint64(data)), nil
	case TypeFloat:
		if len(data) != 8 {
			return nil, fmt.Errorf("float value must be 8 bytes, got %d", len(data))
		}
		return math.Float64frombits(binary.BigEndian.Uint64(data)), nil
	case TypeBool:
		if len(data) != 1 {
			return nil, fmt.Errorf("bool value must be 1 byte, got %d", len(data))
		}
		return data[0] != 0, nil
	case TypeTime:
		if len(data) != 8 {
			return nil, fmt.Errorf("time value must be 8 bytes, got %d", len(data))
		}
		nanos := int64(binary.BigEndian.Uint64(data))
		return time.Unix(0, nanos), nil
	case TypeBytes:
		return data, nil
	case TypeReference:
		if len(data) != 20 {
			return nil, fmt.Errorf("reference value must be 20 bytes, got %d", len(data))
		}
		// Convert bytes back to Identity
		var hash [20]byte
		copy(hash[:], data)
		return NewIdentityFromHash(hash), nil
	case TypeKeyword:
		return NewKeyword(string(data)), nil
	case TypeSymbol:
		return NewSymbol(string(data)), nil
	case TypeElementID:
		if len(data) != ElementIDSize {
			return nil, fmt.Errorf("ElementID value must be %d bytes, got %d", ElementIDSize, len(data))
		}
		return ElementIDFromBytes(data), nil
	case TypeCompressedString:
		decompressed, err := codec.Decompress(data)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress string: %w", err)
		}
		return string(decompressed), nil
	case TypeCompressedBytes:
		decompressed, err := codec.Decompress(data)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress bytes: %w", err)
		}
		return decompressed, nil
	case TypeHashedString, TypeHashedBytes:
		return nil, fmt.Errorf("hashed value type %d requires blob store lookup", vType)
	default:
		return nil, fmt.Errorf("unknown value type: %v", vType)
	}
}

// EncodeValue encodes a value with compression awareness.
// For string/[]byte values above the threshold, compresses and routes to the
// appropriate tier:
//   - Tier 1 (raw): below threshold → (TypeString/TypeBytes, rawBytes, nil)
//   - Tier 2 (compressed in key): compressed fits in key → (TypeCompressedString/Bytes, compressed, nil)
//   - Tier 3 (blob store): compressed too large → (TypeHashedString/Bytes, hash, &BlobData)
//
// For all other types, returns the standard encoding with nil BlobData.
// threshold <= 0 disables compression (all values stored raw).
func EncodeValue(v Value, threshold int) (ValueType, []byte, *BlobData) {
	if threshold <= 0 {
		return Type(v), ValueBytes(v), nil
	}

	switch val := v.(type) {
	case string:
		raw := []byte(val)
		if len(raw) < threshold {
			return TypeString, raw, nil
		}
		return compressAndRoute(raw, TypeCompressedString, TypeHashedString)

	case []byte:
		if len(val) < threshold {
			return TypeBytes, val, nil
		}
		return compressAndRoute(val, TypeCompressedBytes, TypeHashedBytes)

	default:
		return Type(v), ValueBytes(v), nil
	}
}

// compressAndRoute compresses data and routes to Tier 2 (key) or Tier 3 (blob).
func compressAndRoute(raw []byte, compressedType, hashedType ValueType) (ValueType, []byte, *BlobData) {
	compressed := codec.Compress(raw)
	if compressed == nil {
		// Safety net: compression didn't help, store raw
		if compressedType == TypeCompressedString {
			return TypeString, raw, nil
		}
		return TypeBytes, raw, nil
	}

	if len(compressed) <= maxKeyValueSize {
		// Tier 2: compressed fits in key
		return compressedType, compressed, nil
	}

	// Tier 3: too large for key, store hash in key and compressed in blob
	hash := sha1.Sum(compressed)
	return hashedType, hash[:], &BlobData{
		Hash:            hash,
		CompressedBytes: compressed,
	}
}
