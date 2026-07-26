package storage

import "fmt"

// incrementLastByte returns the exclusive end of a prefix range: the least key
// strictly greater than every key beginning with start.
//
// The carry is the whole of it. A byte at 0xFF has no successor in place, so it
// becomes 0x00 and the carry moves left. Leaving the 0xFF and incrementing to
// its left instead yields a key *above* the true successor, and the range then
// swallows the whole next sibling subtree below that byte — not a corner case:
// orderedInt64 encodes every negative long with a trailing 0xFF (-1 is
// 0x7FFFFFFFFFFFFFFF), and one 20-byte entity hash in 256 ends in one.
//
// An all-0xFF prefix has no exclusive end and cannot occur: every index key
// opens with its index byte, and there are eight indices, so the leading byte
// always admits the carry. It panics rather than returning a sentinel — an
// unreachable branch that two backends would read differently is fiction, and
// the neighbouring encodeKeyWithParts panics on its own impossible arm.
func incrementLastByte(start []byte) []byte {
	end := make([]byte, len(start))
	copy(end, start)

	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end
		}
		end[i] = 0x00
	}
	panic(fmt.Sprintf("prefix has no exclusive end: every byte is 0xFF (%x)", start))
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
