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
