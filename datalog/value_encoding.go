package datalog

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
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
)

// Type returns the type of a value
func Type(v Value) ValueType {
	// Handle pointers by checking what they point to
	switch val := v.(type) {
	case *Identity:
		return TypeReference
	case *Keyword:
		return TypeKeyword
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
	default:
		panic(fmt.Sprintf("unknown value type: %T", val))
	}
}

// Bytes serializes a value to bytes
func ValueBytes(v Value) []byte {
	// Handle pointers by dereferencing first
	switch ptr := v.(type) {
	case *Identity:
		return ptr.Bytes()
	case *Keyword:
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
	default:
		return nil, fmt.Errorf("unknown value type: %v", vType)
	}
}
