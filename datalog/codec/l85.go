package codec

import (
	"errors"
	"fmt"
)

// L85 implements the exact L85 encoding from the C implementation
// This is a lexicographically-sortable Base85 variant

// L85Alphabet is the exact alphabet from the C implementation
const L85Alphabet = "!$%&()+,-./" +
	"0123456789:;<=>@" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ[]_`" +
	"abcdefghijklmnopqrstuvwxyz{}"

var (
	// l85Decode is the decode lookup table
	l85Decode [256]byte

	// ErrInvalidCharacter indicates an invalid character in input
	ErrInvalidCharacter = errors.New("invalid L85 character")
)

func init() {
	// Initialize decode table
	for i := range l85Decode {
		l85Decode[i] = 0 // Invalid marker (0 means invalid in C code)
	}
	// C code stores i+1 in the decode table
	for i, c := range L85Alphabet {
		l85Decode[byte(c)] = byte(i + 1)
	}
}

// EncodeL85 encodes bytes to L85 format
func EncodeL85(src []byte) string {
	if len(src) == 0 {
		return ""
	}

	result := make([]byte, 0, len(src)*5/4+5)

	// Process full 4-byte groups
	for i := 0; i+4 <= len(src); i += 4 {
		// Get 4 bytes as uint32 (big endian)
		v := uint32(src[i])<<24 | uint32(src[i+1])<<16 |
			uint32(src[i+2])<<8 | uint32(src[i+3])

		// Convert to 5 base85 digits
		chars := [5]byte{}
		for j := 4; j >= 0; j-- {
			chars[j] = L85Alphabet[v%85]
			v /= 85
		}
		result = append(result, chars[:]...)
	}

	// Handle remainder bytes
	remainder := len(src) % 4
	if remainder > 0 {
		// Treat remainder bytes as a small integer (big-endian)
		// 1 byte  -> value 0-255      -> 2 chars (85^2 = 7225 > 256)
		// 2 bytes -> value 0-65535    -> 3 chars (85^3 = 614125 > 65536)
		// 3 bytes -> value 0-16777215 -> 4 chars (85^4 = 52200625 > 16777216)
		offset := len(src) - remainder
		var v uint32
		switch remainder {
		case 1:
			v = uint32(src[offset])
		case 2:
			v = uint32(src[offset])<<8 | uint32(src[offset+1])
		case 3:
			v = uint32(src[offset])<<16 | uint32(src[offset+1])<<8 | uint32(src[offset+2])
		}

		// Convert to base85 (remainder+1 chars needed)
		switch remainder {
		case 1:
			result = append(result, L85Alphabet[v/85], L85Alphabet[v%85])
		case 2:
			result = append(result, L85Alphabet[v/7225], L85Alphabet[(v/85)%85], L85Alphabet[v%85])
		case 3:
			result = append(result, L85Alphabet[v/614125], L85Alphabet[(v/7225)%85], L85Alphabet[(v/85)%85], L85Alphabet[v%85])
		}
	}

	return string(result)
}

// DecodeL85 decodes L85 format back to bytes
func DecodeL85(src string) ([]byte, error) {
	if len(src) == 0 {
		return []byte{}, nil
	}

	// Validate all characters
	for i, c := range src {
		if c >= 256 || l85Decode[byte(c)] == 0 {
			return nil, fmt.Errorf("%w at position %d: %c", ErrInvalidCharacter, i, c)
		}
	}

	result := make([]byte, 0, len(src)*4/5+4)

	// Process full 5-char groups
	for i := 0; i+5 <= len(src); i += 5 {
		// Convert 5 base85 chars to uint32
		// C code stores i+1, so we need to subtract 1
		v := uint32(0)
		for j := 0; j < 5; j++ {
			v = v*85 + uint32(l85Decode[src[i+j]]-1)
		}

		// Convert to 4 bytes (big endian)
		bytes := [4]byte{
			byte(v >> 24),
			byte(v >> 16),
			byte(v >> 8),
			byte(v),
		}
		result = append(result, bytes[:]...)
	}

	// Handle remainder
	remainder := len(src) % 5
	if remainder > 0 {
		// The number of bytes encoded is remainder-1
		// (2 chars = 1 byte, 3 chars = 2 bytes, 4 chars = 3 bytes)
		if remainder == 1 {
			return nil, errors.New("invalid L85 encoding: incomplete group")
		}

		// Convert remainder chars directly to integer (no padding)
		offset := len(src) - remainder
		var v uint32
		switch remainder {
		case 2:
			v = uint32(l85Decode[src[offset]]-1)*85 + uint32(l85Decode[src[offset+1]]-1)
			result = append(result, byte(v))
		case 3:
			v = uint32(l85Decode[src[offset]]-1)*7225 + uint32(l85Decode[src[offset+1]]-1)*85 + uint32(l85Decode[src[offset+2]]-1)
			result = append(result, byte(v>>8), byte(v))
		case 4:
			v = uint32(l85Decode[src[offset]]-1)*614125 + uint32(l85Decode[src[offset+1]]-1)*7225 + uint32(l85Decode[src[offset+2]]-1)*85 + uint32(l85Decode[src[offset+3]]-1)
			result = append(result, byte(v>>16), byte(v>>8), byte(v))
		}
	}

	return result, nil
}

// EncodeFixed20 encodes a 20-byte array to exactly 25 characters
func EncodeFixed20(src [20]byte) string {
	return EncodeL85(src[:])
}

// DecodeFixed20 decodes exactly 25 characters to a 20-byte array
func DecodeFixed20(src string) ([20]byte, error) {
	var result [20]byte

	if len(src) != 25 {
		return result, fmt.Errorf("expected 25 characters, got %d", len(src))
	}

	decoded, err := DecodeL85(src)
	if err != nil {
		return result, err
	}

	if len(decoded) != 20 {
		return result, fmt.Errorf("decoded to %d bytes, expected 20", len(decoded))
	}

	copy(result[:], decoded)
	return result, nil
}

// EncodeFixed16 encodes a 16-byte array to exactly 20 characters
// Used for ElementID (Lamport + ReplicaID) encoding in transaction IDs
func EncodeFixed16(src [16]byte) string {
	return EncodeL85(src[:])
}

// DecodeFixed16 decodes exactly 20 characters to a 16-byte array
// Used for ElementID (Lamport + ReplicaID) decoding in transaction IDs
func DecodeFixed16(src string) ([16]byte, error) {
	var result [16]byte

	if len(src) != 20 {
		return result, fmt.Errorf("expected 20 characters, got %d", len(src))
	}

	decoded, err := DecodeL85(src)
	if err != nil {
		return result, err
	}

	if len(decoded) != 16 {
		return result, fmt.Errorf("decoded to %d bytes, expected 16", len(decoded))
	}

	copy(result[:], decoded)
	return result, nil
}

// EncodeFixed32 encodes a 32-byte array to exactly 40 characters
func EncodeFixed32(src [32]byte) string {
	return EncodeL85(src[:])
}

// DecodeFixed32 decodes exactly 40 characters to a 32-byte array
func DecodeFixed32(src string) ([32]byte, error) {
	var result [32]byte

	if len(src) != 40 {
		return result, fmt.Errorf("expected 40 characters, got %d", len(src))
	}

	decoded, err := DecodeL85(src)
	if err != nil {
		return result, err
	}

	if len(decoded) != 32 {
		return result, fmt.Errorf("decoded to %d bytes, expected 32", len(decoded))
	}

	copy(result[:], decoded)
	return result, nil
}
