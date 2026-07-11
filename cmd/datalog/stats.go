package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// runStats scans the full database (EAVT index, all history) and reports
// per-attribute cardinality, value size distribution, and value duplication.
// The source may be a BadgerDB directory or an .edn dump.
func runStats(dbPath string) {
	db, cleanup, err := openDatabaseOrEDN(dbPath)
	if err != nil {
		log.Fatalf("%v", err)
	}
	defer cleanup()

	report, err := collectStats(db)
	if err != nil {
		log.Fatalf("Failed to collect statistics: %v", err)
	}
	report.print(os.Stdout, dbPath)
}

// valueInfo tracks one distinct value: how often it occurs, its encoded size,
// and a short preview for the duplication report.
type valueInfo struct {
	count   int
	size    int
	typ     string
	preview string
}

// attrAccum accumulates statistics for a single attribute.
type attrAccum struct {
	name       string
	count      int
	totalBytes int
	entities   map[[20]byte]struct{}
	values     map[[32]byte]*valueInfo
	sizes      []int
	types      map[string]int
}

func newAttrAccum(name string) *attrAccum {
	return &attrAccum{
		name:     name,
		entities: make(map[[20]byte]struct{}),
		values:   make(map[[32]byte]*valueInfo),
		types:    make(map[string]int),
	}
}

// dbStats holds the full scan result.
type dbStats struct {
	totalDatoms  int
	attrs        map[string]*attrAccum
	entities     map[[20]byte]struct{}
	globalValues map[[32]byte]*valueInfo
	typeCounts   map[string]int
	typeBytes    map[string]int
	opCounts     map[datalog.CRDTOp]int
}

func newDBStats() *dbStats {
	return &dbStats{
		attrs:        make(map[string]*attrAccum),
		entities:     make(map[[20]byte]struct{}),
		globalValues: make(map[[32]byte]*valueInfo),
		typeCounts:   make(map[string]int),
		typeBytes:    make(map[string]int),
		opCounts:     make(map[datalog.CRDTOp]int),
	}
}

// collectStats scans the EAVT index and accumulates statistics over every
// datom in the database, including full CRDT history (removes, RGA ops).
func collectStats(db *storage.Database) (*dbStats, error) {
	// Scan the entire EAVT index, same bounds as Database.Export.
	start := []byte{byte(storage.EAVT)}
	end := []byte{byte(storage.EATV)}
	iter, err := db.Store().Scan(storage.EAVT, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to scan database: %w", err)
	}
	defer iter.Close()

	s := newDBStats()
	for iter.Next() {
		d, err := iter.Datom()
		if err != nil {
			return nil, fmt.Errorf("failed to read datom: %w", err)
		}
		s.record(d)
	}
	return s, nil
}

func (s *dbStats) record(d *datalog.Datom) {
	s.totalDatoms++
	s.opCounts[d.Op]++

	if d.E != nil {
		s.entities[d.E.Hash()] = struct{}{}
	}

	attrName := d.A.String()
	aa, ok := s.attrs[attrName]
	if !ok {
		aa = newAttrAccum(attrName)
		s.attrs[attrName] = aa
	}
	aa.count++
	if d.E != nil {
		aa.entities[d.E.Hash()] = struct{}{}
	}

	typ, encoded := encodeValueForStats(d.V)
	size := len(encoded)

	s.typeCounts[typ]++
	s.typeBytes[typ] += size
	aa.types[typ]++
	aa.totalBytes += size
	aa.sizes = append(aa.sizes, size)

	key := hashValue(typ, encoded)
	recordValue(aa.values, key, typ, size, d.V)
	recordValue(s.globalValues, key, typ, size, d.V)
}

// encodeValueForStats returns the value's type name and encoded bytes as they
// would appear in index keys (uncompressed). Nil values (which should not
// occur in storage) are reported under their own type rather than panicking.
func encodeValueForStats(v interface{}) (string, []byte) {
	if v == nil {
		return "nil", nil
	}
	return valueTypeName(v), datalog.ValueBytes(v)
}

func valueTypeName(v interface{}) string {
	switch v.(type) {
	case string:
		return "string"
	case int64, int:
		return "int64"
	case float64:
		return "float64"
	case bool:
		return "bool"
	case []byte:
		return "bytes"
	case datalog.Identity:
		return "ref"
	case datalog.Keyword:
		return "keyword"
	case datalog.Symbol:
		return "symbol"
	case datalog.ElementID:
		return "elementid"
	case time.Time:
		return "time"
	default:
		return fmt.Sprintf("%T", v)
	}
}

func hashValue(typ string, encoded []byte) [32]byte {
	h := sha256.New()
	h.Write([]byte(typ))
	h.Write([]byte{0})
	h.Write(encoded)
	var key [32]byte
	copy(key[:], h.Sum(nil))
	return key
}

func recordValue(m map[[32]byte]*valueInfo, key [32]byte, typ string, size int, v interface{}) {
	vi, ok := m[key]
	if !ok {
		vi = &valueInfo{size: size, typ: typ, preview: valuePreview(v)}
		m[key] = vi
	}
	vi.count++
}

// valuePreview renders a short, quoted preview of a value for the duplication
// report. Long values are truncated; binary values show only their size.
func valuePreview(v interface{}) string {
	const maxLen = 40
	switch val := v.(type) {
	case string:
		if len(val) > maxLen {
			return strconv.Quote(val[:maxLen]) + "…"
		}
		return strconv.Quote(val)
	case []byte:
		return fmt.Sprintf("<%d bytes>", len(val))
	case datalog.Identity:
		return "#identity " + val.L85()
	case nil:
		return "nil"
	default:
		s := fmt.Sprintf("%v", val)
		if len(s) > maxLen {
			s = s[:maxLen] + "…"
		}
		return s
	}
}

// uniqueBytes sums the encoded size of each distinct value once.
func uniqueBytes(m map[[32]byte]*valueInfo) int {
	total := 0
	for _, vi := range m {
		total += vi.size
	}
	return total
}

func percentile(sorted []int, p float64) int {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p*float64(len(sorted))+0.5) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func humanBytes(n int) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2f KiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

func (s *dbStats) print(w io.Writer, dbPath string) {
	totalValueBytes := 0
	for _, b := range s.typeBytes {
		totalValueBytes += b
	}
	globalUnique := uniqueBytes(s.globalValues)

	fmt.Fprintf(w, "# Database Statistics: %s\n\n", dbPath)
	fmt.Fprintf(w, "Total datoms (all CRDT history): %d\n", s.totalDatoms)
	fmt.Fprintf(w, "Distinct entities:               %d\n", len(s.entities))
	fmt.Fprintf(w, "Distinct attributes:             %d\n", len(s.attrs))
	fmt.Fprintf(w, "Distinct values (global):        %d\n", len(s.globalValues))
	fmt.Fprintf(w, "Encoded value bytes (total):     %s\n", humanBytes(totalValueBytes))
	fmt.Fprintf(w, "Encoded value bytes (deduped):   %s", humanBytes(globalUnique))
	if totalValueBytes > 0 {
		fmt.Fprintf(w, "  (%.1f%% duplicated)", 100*float64(totalValueBytes-globalUnique)/float64(totalValueBytes))
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "\nNote: sizes are uncompressed encoded value bytes; each value is embedded\nin all 8 index keys, so on-disk value bytes are ~8x the totals above.\n")

	s.printTypeDistribution(w)
	s.printAttrTable(w)
	s.printSizeDistribution(w)
	s.printTopDuplicated(w)
	s.printOpDistribution(w)
}

func (s *dbStats) printTypeDistribution(w io.Writer) {
	fmt.Fprintf(w, "\n## Value Type Distribution\n\n")
	fmt.Fprintln(w, "| Type | Count | Total Bytes | Avg Size |")
	fmt.Fprintln(w, "|------|------:|------------:|---------:|")

	names := make([]string, 0, len(s.typeCounts))
	for t := range s.typeCounts {
		names = append(names, t)
	}
	sort.Slice(names, func(i, j int) bool { return s.typeBytes[names[i]] > s.typeBytes[names[j]] })
	for _, t := range names {
		count := s.typeCounts[t]
		avg := 0.0
		if count > 0 {
			avg = float64(s.typeBytes[t]) / float64(count)
		}
		fmt.Fprintf(w, "| %s | %d | %s | %.1f |\n", t, count, humanBytes(s.typeBytes[t]), avg)
	}
}

// sortedAttrs returns attribute accumulators ordered by total value bytes descending.
func (s *dbStats) sortedAttrs() []*attrAccum {
	attrs := make([]*attrAccum, 0, len(s.attrs))
	for _, aa := range s.attrs {
		attrs = append(attrs, aa)
	}
	sort.Slice(attrs, func(i, j int) bool {
		if attrs[i].totalBytes != attrs[j].totalBytes {
			return attrs[i].totalBytes > attrs[j].totalBytes
		}
		return attrs[i].name < attrs[j].name
	})
	return attrs
}

func (aa *attrAccum) typeList() string {
	names := make([]string, 0, len(aa.types))
	for t := range aa.types {
		names = append(names, t)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

func (s *dbStats) printAttrTable(w io.Writer) {
	fmt.Fprintf(w, "\n## Per-Attribute Cardinality and Duplication\n\n")
	fmt.Fprintln(w, "| Attribute | Datoms | Entities | Distinct V | Dup x | Datoms/E | Types | V Bytes | Unique V Bytes | Saved |")
	fmt.Fprintln(w, "|-----------|-------:|---------:|-----------:|------:|---------:|-------|--------:|---------------:|------:|")

	for _, aa := range s.sortedAttrs() {
		distinct := len(aa.values)
		dup := 0.0
		if distinct > 0 {
			dup = float64(aa.count) / float64(distinct)
		}
		perEntity := 0.0
		if len(aa.entities) > 0 {
			perEntity = float64(aa.count) / float64(len(aa.entities))
		}
		unique := uniqueBytes(aa.values)
		saved := 0.0
		if aa.totalBytes > 0 {
			saved = 100 * float64(aa.totalBytes-unique) / float64(aa.totalBytes)
		}
		fmt.Fprintf(w, "| %s | %d | %d | %d | %.1f | %.1f | %s | %s | %s | %.1f%% |\n",
			aa.name, aa.count, len(aa.entities), distinct, dup, perEntity,
			aa.typeList(), humanBytes(aa.totalBytes), humanBytes(unique), saved)
	}
}

func (s *dbStats) printSizeDistribution(w io.Writer) {
	// Percentiles only make sense for variable-size value types.
	variable := make([]*attrAccum, 0)
	for _, aa := range s.sortedAttrs() {
		if aa.types["string"] > 0 || aa.types["bytes"] > 0 || aa.types["keyword"] > 0 || aa.types["symbol"] > 0 {
			variable = append(variable, aa)
		}
	}
	if len(variable) == 0 {
		return
	}

	fmt.Fprintf(w, "\n## Value Size Distribution (variable-size attributes)\n\n")
	fmt.Fprintln(w, "| Attribute | Count | Avg | P50 | P90 | P99 | Max | Total |")
	fmt.Fprintln(w, "|-----------|------:|----:|----:|----:|----:|----:|------:|")

	for _, aa := range variable {
		sorted := make([]int, len(aa.sizes))
		copy(sorted, aa.sizes)
		sort.Ints(sorted)
		avg := 0.0
		if aa.count > 0 {
			avg = float64(aa.totalBytes) / float64(aa.count)
		}
		fmt.Fprintf(w, "| %s | %d | %.1f | %d | %d | %d | %d | %s |\n",
			aa.name, aa.count, avg,
			percentile(sorted, 0.50), percentile(sorted, 0.90), percentile(sorted, 0.99),
			sorted[len(sorted)-1], humanBytes(aa.totalBytes))
	}
}

func (s *dbStats) printTopDuplicated(w io.Writer) {
	type dupEntry struct {
		vi     *valueInfo
		wasted int
	}
	dups := make([]dupEntry, 0)
	for _, vi := range s.globalValues {
		if vi.count > 1 {
			dups = append(dups, dupEntry{vi: vi, wasted: (vi.count - 1) * vi.size})
		}
	}
	if len(dups) == 0 {
		return
	}
	sort.Slice(dups, func(i, j int) bool { return dups[i].wasted > dups[j].wasted })
	if len(dups) > 10 {
		dups = dups[:10]
	}

	fmt.Fprintf(w, "\n## Top Duplicated Values (by wasted bytes)\n\n")
	fmt.Fprintln(w, "| Type | Size | Count | Wasted | Preview |")
	fmt.Fprintln(w, "|------|-----:|------:|-------:|---------|")
	for _, d := range dups {
		fmt.Fprintf(w, "| %s | %s | %d | %s | %s |\n",
			d.vi.typ, humanBytes(d.vi.size), d.vi.count, humanBytes(d.wasted), d.vi.preview)
	}
}

func (s *dbStats) printOpDistribution(w io.Writer) {
	fmt.Fprintf(w, "\n## CRDT Op Distribution\n\n")
	fmt.Fprintln(w, "| Op | Count |")
	fmt.Fprintln(w, "|----|------:|")

	ops := make([]datalog.CRDTOp, 0, len(s.opCounts))
	for op := range s.opCounts {
		ops = append(ops, op)
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i] < ops[j] })
	for _, op := range ops {
		fmt.Fprintf(w, "| %s | %d |\n", storage.FormatCRDTOpEDN(op), s.opCounts[op])
	}
}
