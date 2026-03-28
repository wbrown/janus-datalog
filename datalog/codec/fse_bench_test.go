package codec

import (
	"math/rand"
	"strings"
	"testing"
)

func BenchmarkFSECompress_EnglishText(b *testing.B) {
	input := []byte(strings.Repeat("The quick brown fox jumps over the lazy dog. ", 100))
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FSECompress(input)
	}
}

func BenchmarkFSEDecompress_EnglishText(b *testing.B) {
	input := []byte(strings.Repeat("The quick brown fox jumps over the lazy dog. ", 100))
	compressed, _ := FSECompress(input)
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FSEDecompress(compressed, len(input))
	}
}

func BenchmarkFSECompress_Skewed1KB(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	input := make([]byte, 1024)
	for i := range input {
		if rng.Intn(20) == 0 {
			input[i] = 'b'
		} else {
			input[i] = 'a'
		}
	}
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FSECompress(input)
	}
}

func BenchmarkFSEDecompress_Skewed1KB(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	input := make([]byte, 1024)
	for i := range input {
		if rng.Intn(20) == 0 {
			input[i] = 'b'
		} else {
			input[i] = 'a'
		}
	}
	compressed, _ := FSECompress(input)
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FSEDecompress(compressed, len(input))
	}
}

func BenchmarkFSECompress_Sizes(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"256B", 256}, {"1KB", 1024}, {"4KB", 4096}, {"16KB", 16384},
	}
	for _, s := range sizes {
		base := []byte(strings.Repeat("The quick brown fox jumps. ", s.size/27+2))
		input := base[:s.size]
		b.Run(s.name, func(b *testing.B) {
			b.SetBytes(int64(s.size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				FSECompress(input)
			}
		})
	}
}

func BenchmarkFSEDecompress_Sizes(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"256B", 256}, {"1KB", 1024}, {"4KB", 4096}, {"16KB", 16384},
	}
	for _, s := range sizes {
		base := []byte(strings.Repeat("The quick brown fox jumps. ", s.size/27+2))
		input := base[:s.size]
		compressed, _ := FSECompress(input)
		if compressed == nil {
			continue
		}
		b.Run(s.name, func(b *testing.B) {
			b.SetBytes(int64(s.size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				FSEDecompress(compressed, s.size)
			}
		})
	}
}
