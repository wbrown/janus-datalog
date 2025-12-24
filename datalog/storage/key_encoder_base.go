package storage

// historyIndexToBase maps history index types to their base current-state equivalents
func historyIndexToBase(index IndexType) IndexType {
	switch index {
	case EAVT_HISTORY:
		return EAVT
	case AEVT_HISTORY:
		return AEVT
	case AVET_HISTORY:
		return AVET
	case VAET_HISTORY:
		return VAET
	case TAEV_HISTORY:
		return TAEV
	default:
		return index
	}
}

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
