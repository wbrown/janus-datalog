package storage

// concatBytes efficiently concatenates byte slices
func concatBytes(parts ...[]byte) []byte {
	size := 0
	for _, p := range parts {
		size += len(p)
	}

	result := make([]byte, size)
	offset := 0
	for _, p := range parts {
		copy(result[offset:], p)
		offset += len(p)
	}

	return result
}
