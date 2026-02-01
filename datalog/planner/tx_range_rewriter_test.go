package planner

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestElementIDEncoder(t *testing.T) {
	enc := ElementIDEncoder{}

	t.Run("encode decode roundtrip", func(t *testing.T) {
		values := []uint64{0, 1, 100, 1000, 5000, 1<<32 - 1, 1 << 63}
		for _, v := range values {
			encoded := enc.EncodeForKey(v)
			decoded := enc.DecodeFromKey(encoded)
			assert.Equal(t, v, decoded, "roundtrip failed for %d", v)
		}
	})

	t.Run("higher lamport encodes to smaller bytes", func(t *testing.T) {
		low := enc.EncodeForKey(1000)
		high := enc.EncodeForKey(2000)

		// Higher Lamport should encode to SMALLER bytes (due to bitwise NOT)
		assert.True(t, bytes.Compare(high, low) < 0,
			"encoded(2000) should be < encoded(1000), got %x vs %x", high, low)
	})

	t.Run("zero lamport encodes to max bytes", func(t *testing.T) {
		encoded := enc.EncodeForKey(0)
		// ^0 = 0xFFFFFFFFFFFFFFFF, so first 8 bytes should be all 0xFF
		require.Len(t, encoded, 16)
		for i := 0; i < 8; i++ {
			assert.Equal(t, byte(0xFF), encoded[i], "byte %d should be 0xFF", i)
		}
	})

	t.Run("max lamport encodes to min bytes", func(t *testing.T) {
		encoded := enc.EncodeForKey(^uint64(0)) // max uint64
		// ^(max) = 0, so first 8 bytes should be all 0x00
		require.Len(t, encoded, 16)
		for i := 0; i < 8; i++ {
			assert.Equal(t, byte(0x00), encoded[i], "byte %d should be 0x00", i)
		}
	})

	t.Run("decode short buffer returns 0", func(t *testing.T) {
		result := enc.DecodeFromKey([]byte{1, 2, 3})
		assert.Equal(t, uint64(0), result)
	})
}

func TestRewriteTxRange(t *testing.T) {
	t.Run("bounds are inverted", func(t *testing.T) {
		start, end := RewriteTxRange(1000, 2000)

		enc := ElementIDEncoder{}
		// Start should be encoded(high) = encoded(2000)
		expectedStart := enc.EncodeForKey(2000)
		// End should be encoded(low) = encoded(1000)
		expectedEnd := enc.EncodeForKey(1000)

		assert.Equal(t, expectedStart, start, "start key should be encoded(2000)")
		assert.Equal(t, expectedEnd, end, "end key should be encoded(1000)")
	})

	t.Run("start sorts before end", func(t *testing.T) {
		start, end := RewriteTxRange(1000, 2000)
		// In byte order, start should be < end (for forward iteration)
		assert.True(t, bytes.Compare(start, end) < 0,
			"start should sort before end for forward iteration")
	})

	t.Run("same low and high", func(t *testing.T) {
		start, end := RewriteTxRange(5000, 5000)
		// Start and end should be the same for a single-point range
		assert.Equal(t, start, end)
	})

	t.Run("full range", func(t *testing.T) {
		start, end := RewriteTxRange(0, ^uint64(0))
		// This should give the full range of possible keys
		// start = encoded(max) = all zeros (minus ReplicaID part)
		// end = encoded(0) = all ones (minus ReplicaID part)
		assert.True(t, bytes.Compare(start, end) < 0,
			"start should sort before end")
	})
}

func TestTxRangeBounds(t *testing.T) {
	t.Run("creates correct bounds", func(t *testing.T) {
		bounds := NewTxRangeBounds(1000, 2000)

		assert.Equal(t, uint64(1000), bounds.Low)
		assert.Equal(t, uint64(2000), bounds.High)
		assert.NotNil(t, bounds.StartKey)
		assert.NotNil(t, bounds.EndKey)
	})

	t.Run("InRange checks correctly", func(t *testing.T) {
		bounds := NewTxRangeBounds(1000, 2000)

		// In range
		assert.True(t, bounds.InRange(1000), "1000 should be in range")
		assert.True(t, bounds.InRange(1500), "1500 should be in range")
		assert.True(t, bounds.InRange(2000), "2000 should be in range")

		// Out of range
		assert.False(t, bounds.InRange(999), "999 should be out of range")
		assert.False(t, bounds.InRange(2001), "2001 should be out of range")
		assert.False(t, bounds.InRange(0), "0 should be out of range")
	})
}

func TestIsLamportInRange(t *testing.T) {
	t.Run("in range values", func(t *testing.T) {
		assert.True(t, IsLamportInRange(1000, 1000, 2000))
		assert.True(t, IsLamportInRange(1500, 1000, 2000))
		assert.True(t, IsLamportInRange(2000, 1000, 2000))
	})

	t.Run("out of range values", func(t *testing.T) {
		assert.False(t, IsLamportInRange(999, 1000, 2000))
		assert.False(t, IsLamportInRange(2001, 1000, 2000))
	})

	t.Run("single point range", func(t *testing.T) {
		assert.True(t, IsLamportInRange(5000, 5000, 5000))
		assert.False(t, IsLamportInRange(4999, 5000, 5000))
		assert.False(t, IsLamportInRange(5001, 5000, 5000))
	})
}

func TestElementIDFromLamport(t *testing.T) {
	eid := ElementIDFromLamport(12345)
	assert.Equal(t, uint64(12345), eid.Lamport)
	assert.Equal(t, uint64(0), eid.ReplicaID)
}

// Benchmark encoding performance
func BenchmarkElementIDEncode(b *testing.B) {
	enc := ElementIDEncoder{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = enc.EncodeForKey(uint64(i))
	}
}

func BenchmarkElementIDDecode(b *testing.B) {
	enc := ElementIDEncoder{}
	encoded := enc.EncodeForKey(12345)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = enc.DecodeFromKey(encoded)
	}
}
