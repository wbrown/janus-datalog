package storage

// incrementLastByte creates an end key by incrementing the last byte of start.
// Used for prefix range scans.
func incrementLastByte(start []byte) []byte {
	end := make([]byte, len(start))
	copy(end, start)

	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			break
		}
		if i == 0 {
			end = append(end, 0x00)
		}
	}
	return end
}

// concatBytes joins the parts into a single slice, sizing the result
// up front to avoid the growth-copy cascade of repeated appends. Used
// throughout key encoding, where keys are assembled from several
// fixed- and variable-length fields.
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
