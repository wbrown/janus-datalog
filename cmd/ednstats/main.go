// ednstats analyzes value sizes and distribution in janus-datalog EDN exports.
//
// Usage:
//
//	go run ./cmd/ednstats -input data.edn
//
// Produces statistics including:
//   - Value type distribution (counts, total bytes)
//   - String/bytes size percentiles (P50, P95, P99, max)
//   - Per-attribute breakdown for large value types
//   - []string vector analysis (same entity+attribute RGA groups)
//   - Compression tier projections at various thresholds
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

func main() {
	inputPath := flag.String("input", "", "path to EDN export file")
	topN := flag.Int("top", 20, "number of top attributes to show")
	flag.Parse()

	if *inputPath == "" {
		fmt.Fprintln(os.Stderr, "usage: ednstats -input <file.edn>")
		os.Exit(1)
	}

	f, err := os.Open(*inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	stats := newStats()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB buffer

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, ";") {
			continue
		}

		datom, err := storage.ParseDatomEDN(line)
		if err != nil {
			if lineNum <= 10 {
				fmt.Fprintf(os.Stderr, "line %d: parse error: %v\n", lineNum, err)
			}
			stats.parseErrors++
			continue
		}

		stats.record(datom)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scanner error: %v\n", err)
		os.Exit(1)
	}

	stats.report(*topN)
}

// entityAttrKey identifies a unique (entity, attribute) pair for vector grouping.
type entityAttrKey struct {
	entityL85 string
	attr      string
}

type attrStats struct {
	count      int
	totalBytes int
	sizes      []int
}

type stats struct {
	totalDatoms int
	parseErrors int

	// Per value type
	typeCounts map[string]int
	typeBytes  map[string]int

	// All string sizes and bytes sizes for percentile calculation
	stringSizes []int
	bytesSizes  []int

	// Per attribute stats for string and bytes values
	attrString map[string]*attrStats
	attrBytes  map[string]*attrStats

	// RGA vector groups: (entity, attribute) → list of value sizes
	// Only tracks string values with rga-insert ops
	rgaGroups map[entityAttrKey]*rgaGroup
}

type rgaGroup struct {
	attr       string
	sizes      []int
	totalBytes int
	values     []string // actual string values for entropy calculation
}

func newStats() *stats {
	return &stats{
		typeCounts: make(map[string]int),
		typeBytes:  make(map[string]int),
		attrString: make(map[string]*attrStats),
		attrBytes:  make(map[string]*attrStats),
		rgaGroups:  make(map[entityAttrKey]*rgaGroup),
	}
}

func (s *stats) record(d *datalog.Datom) {
	s.totalDatoms++

	typeName, size := valueTypeAndSize(d.V)
	s.typeCounts[typeName]++
	s.typeBytes[typeName] += size

	switch typeName {
	case "string":
		s.stringSizes = append(s.stringSizes, size)
		attr := d.A.String()
		as, ok := s.attrString[attr]
		if !ok {
			as = &attrStats{}
			s.attrString[attr] = as
		}
		as.count++
		as.totalBytes += size
		as.sizes = append(as.sizes, size)

		// Track RGA groups for vector analysis
		if d.Op == datalog.OpRGAInsert {
			key := entityAttrKey{
				entityL85: d.E.L85(),
				attr:      attr,
			}
			rg, ok := s.rgaGroups[key]
			if !ok {
				rg = &rgaGroup{attr: attr}
				s.rgaGroups[key] = rg
			}
			rg.sizes = append(rg.sizes, size)
			rg.totalBytes += size
			if str, ok := d.V.(string); ok {
				rg.values = append(rg.values, str)
			}
		}

	case "bytes":
		s.bytesSizes = append(s.bytesSizes, size)
		attr := d.A.String()
		as, ok := s.attrBytes[attr]
		if !ok {
			as = &attrStats{}
			s.attrBytes[attr] = as
		}
		as.count++
		as.totalBytes += size
		as.sizes = append(as.sizes, size)
	}
}

func valueTypeAndSize(v interface{}) (string, int) {
	if v == nil {
		return "nil", 0
	}
	switch val := v.(type) {
	case string:
		return "string", len(val)
	case int64:
		return "int64", 8
	case int:
		return "int", 8
	case float64:
		return "float64", 8
	case bool:
		return "bool", 1
	case []byte:
		return "bytes", len(val)
	case datalog.Identity:
		return "identity", 20
	case datalog.Keyword:
		return "keyword", len(val.String())
	case datalog.ElementID:
		return "elementid", 16
	default:
		return fmt.Sprintf("unknown(%T)", v), 0
	}
}

func (s *stats) report(topN int) {
	fmt.Println("# EDN Value Statistics")
	fmt.Println()

	// Overall counts
	fmt.Printf("Total datoms: %d\n", s.totalDatoms)
	if s.parseErrors > 0 {
		fmt.Printf("Parse errors: %d\n", s.parseErrors)
	}
	fmt.Println()

	// Type distribution
	fmt.Println("## Value Type Distribution")
	fmt.Println()
	fmt.Println("| Type | Count | Total Bytes | Avg Size |")
	fmt.Println("|------|-------|-------------|----------|")

	typeNames := sortedKeys(s.typeCounts)
	for _, tn := range typeNames {
		count := s.typeCounts[tn]
		total := s.typeBytes[tn]
		avg := 0.0
		if count > 0 {
			avg = float64(total) / float64(count)
		}
		fmt.Printf("| %s | %d | %s | %.1f |\n", tn, count, humanBytes(total), avg)
	}
	fmt.Println()

	// String size distribution (overall)
	if len(s.stringSizes) > 0 {
		fmt.Println("## String Value Size Distribution (All Attributes)")
		fmt.Println()
		printPercentiles(s.stringSizes)
		fmt.Println()
		printHistogram(s.stringSizes)
		fmt.Println()
	}

	// Bytes size distribution (overall)
	if len(s.bytesSizes) > 0 {
		fmt.Println("## Bytes Value Size Distribution (All Attributes)")
		fmt.Println()
		printPercentiles(s.bytesSizes)
		fmt.Println()
	}

	// Build per-attribute RGA summaries for cross-referencing
	type attrRGASummary struct {
		groupCount  int
		totalElems  int
		totalBytes  int
		groupSizes  []int // elements per group
		concatSizes []int // concatenated bytes per group
	}
	attrRGA := make(map[string]*attrRGASummary)
	for _, rg := range s.rgaGroups {
		ar, ok := attrRGA[rg.attr]
		if !ok {
			ar = &attrRGASummary{}
			attrRGA[rg.attr] = ar
		}
		ar.groupCount++
		ar.totalElems += len(rg.sizes)
		ar.totalBytes += rg.totalBytes
		ar.groupSizes = append(ar.groupSizes, len(rg.sizes))
		ar.concatSizes = append(ar.concatSizes, rg.totalBytes)
	}

	// Per-attribute detailed report for ALL string attributes
	fmt.Println("## Per-Attribute String Analysis")
	fmt.Println()

	allStringAttrs := topAttrsByBytes(s.attrString, len(s.attrString))
	for _, attr := range allStringAttrs {
		as := s.attrString[attr]
		sort.Ints(as.sizes)

		fmt.Printf("### %s\n\n", attr)

		// Check if this attribute has RGA groups
		rga := attrRGA[attr]

		// Count non-RGA datoms for this attribute
		nonRGACount := as.count
		nonRGABytes := as.totalBytes
		if rga != nil {
			nonRGACount = as.count - rga.totalElems
			nonRGABytes = as.totalBytes - rga.totalBytes
		}

		fmt.Printf("- Total datoms: %d, total bytes: %s\n", as.count, humanBytes(as.totalBytes))
		if rga != nil {
			fmt.Printf("- RGA vector elements: %d (%s), non-RGA: %d (%s)\n",
				rga.totalElems, humanBytes(rga.totalBytes),
				nonRGACount, humanBytes(nonRGABytes))
			fmt.Printf("- RGA groups (unique entities): %d\n", rga.groupCount)
		}
		fmt.Println()

		// Individual element sizes
		fmt.Println("**Individual value sizes:**")
		fmt.Println()
		fmt.Printf("| Min | P50 | P75 | P90 | P95 | P99 | Max |\n")
		fmt.Printf("|-----|-----|-----|-----|-----|-----|-----|\n")
		fmt.Printf("| %s | %s | %s | %s | %s | %s | %s |\n",
			humanBytes(as.sizes[0]),
			humanBytes(percentile(as.sizes, 50)),
			humanBytes(percentile(as.sizes, 75)),
			humanBytes(percentile(as.sizes, 90)),
			humanBytes(percentile(as.sizes, 95)),
			humanBytes(percentile(as.sizes, 99)),
			humanBytes(as.sizes[len(as.sizes)-1]))
		fmt.Println()

		// Size histogram for this attribute
		printHistogram(as.sizes)
		fmt.Println()

		// RGA group details if present
		if rga != nil && rga.groupCount > 0 {
			sort.Ints(rga.groupSizes)
			sort.Ints(rga.concatSizes)

			fmt.Println("**RGA vector group analysis:**")
			fmt.Println()
			fmt.Printf("| Metric | P50 | P95 | P99 | Max |\n")
			fmt.Printf("|--------|-----|-----|-----|-----|\n")
			fmt.Printf("| Elements/group | %d | %d | %d | %d |\n",
				percentile(rga.groupSizes, 50),
				percentile(rga.groupSizes, 95),
				percentile(rga.groupSizes, 99),
				rga.groupSizes[len(rga.groupSizes)-1])
			fmt.Printf("| Concat size/group | %s | %s | %s | %s |\n",
				humanBytes(percentile(rga.concatSizes, 50)),
				humanBytes(percentile(rga.concatSizes, 95)),
				humanBytes(percentile(rga.concatSizes, 99)),
				humanBytes(rga.concatSizes[len(rga.concatSizes)-1]))
			fmt.Println()

			// Entropy analysis on joined groups
			var groupEntropies []float64
			var origDatoms, projDatoms int
			var origBytes int
			var projBytesSum float64

			for _, rg := range s.rgaGroups {
				if rg.attr != attr {
					continue
				}
				origDatoms += len(rg.sizes)
				projDatoms++
				origBytes += rg.totalBytes

				// Join all values in this group
				joined := strings.Join(rg.values, "\n\n")
				if len(joined) > 0 {
					ent := shannonEntropy([]byte(joined))
					groupEntropies = append(groupEntropies, ent)
					// Theoretical compressed size: (entropy/8) * len
					projBytesSum += (ent / 8.0) * float64(len(joined))
				}
			}

			if len(groupEntropies) > 0 {
				sort.Float64s(groupEntropies)
				fmt.Println("**Entropy of joined groups (bits/byte, lower = more compressible):**")
				fmt.Println()
				fmt.Printf("| P50 | P95 | P99 | Max | Theoretical ratio |\n")
				fmt.Printf("|-----|-----|-----|-----|-------------------|\n")
				p50 := percentileFloat(groupEntropies, 50)
				fmt.Printf("| %.2f | %.2f | %.2f | %.2f | %.1fx |\n",
					p50,
					percentileFloat(groupEntropies, 95),
					percentileFloat(groupEntropies, 99),
					groupEntropies[len(groupEntropies)-1],
					8.0/p50)
				fmt.Println()

				fmt.Println("**If consolidated into single compressed values (using measured entropy):**")
				fmt.Println()
				projBytes := int(projBytesSum)
				if origBytes > 0 {
					savings := 100.0 * float64(origBytes-projBytes) / float64(origBytes)
					fmt.Printf("- %d datoms → %d datoms (%.1fx reduction)\n",
						origDatoms, projDatoms, float64(origDatoms)/float64(projDatoms))
					fmt.Printf("- %s → %s (%.0f%% byte savings)\n",
						humanBytes(origBytes), humanBytes(projBytes), savings)
				}
			}
			fmt.Println()
		}
	}

	// Per-attribute bytes analysis (if any significant)
	if len(s.bytesSizes) > 0 {
		fmt.Println("## Per-Attribute Bytes Analysis")
		fmt.Println()
		allBytesAttrs := topAttrsByBytes(s.attrBytes, len(s.attrBytes))
		for _, attr := range allBytesAttrs {
			as := s.attrBytes[attr]
			sort.Ints(as.sizes)
			fmt.Printf("### %s\n\n", attr)
			fmt.Printf("- Datoms: %d, total bytes: %s\n", as.count, humanBytes(as.totalBytes))
			fmt.Printf("- P50: %s, P95: %s, Max: %s\n\n",
				humanBytes(percentile(as.sizes, 50)),
				humanBytes(percentile(as.sizes, 95)),
				humanBytes(as.sizes[len(as.sizes)-1]))
		}
	}

	// Overall compression projections
	fmt.Println("## Overall Compression Projections")
	fmt.Println()
	fmt.Println("Assuming 4x compression ratio on text.")
	fmt.Println()

	thresholds := []int{256, 512, 1024, 2048}
	for _, threshold := range thresholds {
		fmt.Printf("### Threshold: %d bytes\n\n", threshold)

		var tier1Count, tier2Count int
		var tier1Bytes, tier2Bytes, tier2Compressed int

		for _, size := range s.stringSizes {
			if size < threshold {
				tier1Count++
				tier1Bytes += size
			} else {
				tier2Count++
				tier2Bytes += size
				compressed := size / 4
				if compressed < 1 {
					compressed = 1
				}
				tier2Compressed += compressed
			}
		}

		fmt.Printf("| Tier | Count | Original | Stored | Savings |\n")
		fmt.Printf("|------|-------|----------|--------|----------|\n")
		fmt.Printf("| Tier 1 (raw) | %d | %s | %s | 0%% |\n",
			tier1Count, humanBytes(tier1Bytes), humanBytes(tier1Bytes))
		if tier2Count > 0 {
			savings := 100.0 * float64(tier2Bytes-tier2Compressed) / float64(tier2Bytes)
			fmt.Printf("| Tier 2 (compressed) | %d | %s | %s | %.0f%% |\n",
				tier2Count, humanBytes(tier2Bytes), humanBytes(tier2Compressed), savings)
		}
		totalOrig := tier1Bytes + tier2Bytes
		totalStored := tier1Bytes + tier2Compressed
		if totalOrig > 0 {
			totalSavings := 100.0 * float64(totalOrig-totalStored) / float64(totalOrig)
			fmt.Printf("| **Total** | %d | %s | %s | **%.0f%%** |\n",
				tier1Count+tier2Count, humanBytes(totalOrig), humanBytes(totalStored), totalSavings)
		}
		fmt.Println()
	}
}

func printPercentiles(sizes []int) {
	sort.Ints(sizes)
	fmt.Printf("| Count | Min | P50 | P75 | P90 | P95 | P99 | Max | Total |\n")
	fmt.Printf("|-------|-----|-----|-----|-----|-----|-----|-----|-------|\n")
	fmt.Printf("| %d | %s | %s | %s | %s | %s | %s | %s | %s |\n",
		len(sizes),
		humanBytes(sizes[0]),
		humanBytes(percentile(sizes, 50)),
		humanBytes(percentile(sizes, 75)),
		humanBytes(percentile(sizes, 90)),
		humanBytes(percentile(sizes, 95)),
		humanBytes(percentile(sizes, 99)),
		humanBytes(sizes[len(sizes)-1]),
		humanBytes(sum(sizes)))
}

func printHistogram(sizes []int) {
	buckets := []struct {
		label string
		min   int
		max   int
	}{
		{"0-64B", 0, 64},
		{"64-256B", 64, 256},
		{"256-512B", 256, 512},
		{"512B-1KB", 512, 1024},
		{"1-4KB", 1024, 4096},
		{"4-16KB", 4096, 16384},
		{"16-64KB", 16384, 65536},
		{"64KB+", 65536, math.MaxInt},
	}

	fmt.Println("| Range | Count | Total Bytes | % of Total Bytes |")
	fmt.Println("|-------|-------|-------------|------------------|")

	totalBytes := sum(sizes)
	for _, b := range buckets {
		count := 0
		bytes := 0
		for _, s := range sizes {
			if s >= b.min && s < b.max {
				count++
				bytes += s
			}
		}
		if count > 0 {
			pct := 100.0 * float64(bytes) / float64(totalBytes)
			fmt.Printf("| %s | %d | %s | %.1f%% |\n", b.label, count, humanBytes(bytes), pct)
		}
	}
}

func shannonEntropy(data []byte) float64 {
	if len(data) == 0 {
		return 0
	}
	var counts [256]int
	for _, b := range data {
		counts[b]++
	}
	n := float64(len(data))
	entropy := 0.0
	for _, c := range counts {
		if c == 0 {
			continue
		}
		p := float64(c) / n
		entropy -= p * math.Log2(p)
	}
	return entropy
}

func percentileFloat(sorted []float64, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(p)/100.0*float64(len(sorted)-1) + 0.5)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func percentile(sorted []int, p int) int {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(p)/100.0*float64(len(sorted)-1) + 0.5)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func sum(sizes []int) int {
	total := 0
	for _, s := range sizes {
		total += s
	}
	return total
}

func humanBytes(b int) string {
	switch {
	case b >= 1024*1024*1024:
		return fmt.Sprintf("%.1fGB", float64(b)/(1024*1024*1024))
	case b >= 1024*1024:
		return fmt.Sprintf("%.1fMB", float64(b)/(1024*1024))
	case b >= 1024:
		return fmt.Sprintf("%.1fKB", float64(b)/1024)
	default:
		return fmt.Sprintf("%dB", b)
	}
}

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func topAttrsByBytes(m map[string]*attrStats, n int) []string {
	type kv struct {
		key   string
		bytes int
	}
	var sorted []kv
	for k, v := range m {
		sorted = append(sorted, kv{k, v.totalBytes})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].bytes > sorted[j].bytes
	})
	if len(sorted) > n {
		sorted = sorted[:n]
	}
	result := make([]string, len(sorted))
	for i, s := range sorted {
		result[i] = s.key
	}
	return result
}
