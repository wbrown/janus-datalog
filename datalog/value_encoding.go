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
// NormalizeValue coerces a value's dynamic integer type to the engine's
// canonical int64 representation: int, int8, int16, int32 become int64; every
// other type (including int64) is returned unchanged. Storage decode, the EDN
// parser, and comparison all standardize on int64, so normalizing user-supplied
// values where they enter the engine (writes, query inputs, query-builder
// constants) keeps a Go int from diverging from stored int64 data. int64 and
// non-integers pass through untouched, adding no allocation for the common case.
func NormalizeValue(v Value) Value {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	}
	return v
}

func Type(v Value) ValueType {
	v = NormalizeValue(v) // int/int8/int16/int32 -> canonical int64
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

// Scalar values are encoded so that the lexicographic order of their bytes
// matches their numeric order. This is required because int64/float64/time.Time
// values appear in the AVET and VAET index keys, and those keys live in a single
// byte-sorted keyspace — value-range scans, index min/max, and ordered iteration
// are only correct when bytes.Compare(enc(a), enc(b)) == cmp(a, b). Plain
// big-endian two's-complement / IEEE-754 bits fail this for negatives (and
// pre-1970 times). Each transform below is a bijection, so the matching decode in
// ValueFromBytes inverts it exactly — there is no separate non-ordered encoding.

// orderedInt64 encodes v as 8 big-endian bytes that sort in numeric order.
// Two's-complement already orders correctly within a sign group; flipping the
// sign bit moves all negatives ahead of all non-negatives. Self-inverse.
func orderedInt64(v int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v)^(1<<63))
	return buf
}

func decodeOrderedInt64(data []byte) int64 {
	return int64(binary.BigEndian.Uint64(data) ^ (1 << 63))
}

// orderedFloat64 encodes v as 8 big-endian bytes that sort in numeric order: the
// standard IEEE-754 total-order transform. Negative floats have their bits
// reversed (raw magnitude order runs backward for negatives) and non-negatives
// get the sign bit set so they sort after negatives.
func orderedFloat64(v float64) []byte {
	bits := math.Float64bits(v)
	if bits&(1<<63) != 0 {
		bits = ^bits // negative: flip all bits
	} else {
		bits |= 1 << 63 // non-negative: flip only the sign bit
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, bits)
	return buf
}

func decodeOrderedFloat64(data []byte) float64 {
	bits := binary.BigEndian.Uint64(data)
	if bits&(1<<63) != 0 {
		bits &^= 1 << 63 // encoded high bit set => was non-negative
	} else {
		bits = ^bits // encoded high bit clear => was negative
	}
	return math.Float64frombits(bits)
}

// Bytes serializes a value to bytes
func ValueBytes(v Value) []byte {
	v = NormalizeValue(v) // int/int8/int16/int32 -> canonical int64
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
		return orderedInt64(val)
	case float64:
		return orderedFloat64(val)
	case bool:
		if val {
			return []byte{1}
		}
		return []byte{0}
	case time.Time:
		return orderedInt64(val.UnixNano())
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
		return decodeOrderedInt64(data), nil
	case TypeFloat:
		if len(data) != 8 {
			return nil, fmt.Errorf("float value must be 8 bytes, got %d", len(data))
		}
		return decodeOrderedFloat64(data), nil
	case TypeBool:
		if len(data) != 1 {
			return nil, fmt.Errorf("bool value must be 1 byte, got %d", len(data))
		}
		return data[0] != 0, nil
	case TypeTime:
		if len(data) != 8 {
			return nil, fmt.Errorf("time value must be 8 bytes, got %d", len(data))
		}
		nanos := decodeOrderedInt64(data)
		return time.Unix(0, nanos).UTC(), nil
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
