package main

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

func TestPercentile(t *testing.T) {
	tests := []struct {
		name   string
		sorted []int
		p      float64
		want   int
	}{
		{"empty", nil, 0.50, 0},
		{"single p50", []int{7}, 0.50, 7},
		{"single p99", []int{7}, 0.99, 7},
		{"ten values p50", []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 0.50, 5},
		{"ten values p90", []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 0.90, 9},
		{"ten values p99", []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 0.99, 10},
		{"two values p50", []int{3, 9}, 0.50, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := percentile(tt.sorted, tt.p); got != tt.want {
				t.Errorf("percentile(%v, %v) = %d, want %d", tt.sorted, tt.p, got, tt.want)
			}
		})
	}
}

func TestHumanBytes(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.00 KiB"},
		{1536, "1.50 KiB"},
		{1 << 20, "1.00 MiB"},
		{1 << 30, "1.00 GiB"},
	}
	for _, tt := range tests {
		if got := humanBytes(tt.n); got != tt.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

func TestValueTypeName(t *testing.T) {
	tests := []struct {
		v    interface{}
		want string
	}{
		{"hello", "string"},
		{int64(42), "int64"},
		{3.14, "float64"},
		{true, "bool"},
		{[]byte{1, 2}, "bytes"},
		{datalog.NewIdentity("x"), "ref"},
		{datalog.NewKeyword(":status/active"), "keyword"},
	}
	for _, tt := range tests {
		if got := valueTypeName(tt.v); got != tt.want {
			t.Errorf("valueTypeName(%T) = %q, want %q", tt.v, got, tt.want)
		}
	}
}

func TestHashValueDistinguishesTypeAndContent(t *testing.T) {
	// Same bytes, different type must hash differently: the int64 5 and an
	// 8-byte string with identical encoded bytes are distinct values.
	enc := datalog.ValueBytes(int64(5))
	if hashValue("int64", enc) == hashValue("string", enc) {
		t.Error("hashValue must distinguish values of different types with identical encodings")
	}
	if hashValue("int64", enc) != hashValue("int64", enc) {
		t.Error("hashValue must be deterministic")
	}
	if hashValue("int64", datalog.ValueBytes(int64(5))) == hashValue("int64", datalog.ValueBytes(int64(6))) {
		t.Error("hashValue must distinguish different values of the same type")
	}
}

func TestValuePreviewTruncation(t *testing.T) {
	long := strings.Repeat("a", 100)
	got := valuePreview(long)
	if len(got) > 50 {
		t.Errorf("valuePreview did not truncate long string: %q (len %d)", got, len(got))
	}
	if !strings.HasSuffix(got, "…") {
		t.Errorf("truncated preview should end with ellipsis: %q", got)
	}
	if valuePreview("hi") != `"hi"` {
		t.Errorf("short string preview = %q, want %q", valuePreview("hi"), `"hi"`)
	}
	if valuePreview([]byte{1, 2, 3}) != "<3 bytes>" {
		t.Errorf("bytes preview = %q, want %q", valuePreview([]byte{1, 2, 3}), "<3 bytes>")
	}
}

// createStatsTestDatabase builds a database with known cardinality and
// duplication structure:
//   - 3 person entities
//   - :person/name  → 3 datoms, 3 distinct values (no duplication)
//   - :person/city  → 3 datoms, 1 distinct value  ("Springfield" x3)
//   - :person/age   → 2 datoms, 2 distinct values
//   - :person/bio   → 1 datom, one 100-byte string
//
// The single commit also writes transaction metadata: one :db/txInstant
// datom on a transaction entity. Storage therefore holds 10 datoms,
// 4 entities, 5 attributes, and 8 distinct values.
func createStatsTestDatabase(t *testing.T) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "stats-test.db")

	db, err := storage.NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	name := datalog.NewKeyword(":person/name")
	city := datalog.NewKeyword(":person/city")
	age := datalog.NewKeyword(":person/age")
	bio := datalog.NewKeyword(":person/bio")

	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	carol := datalog.NewIdentity("carol")

	tx.Add(alice, name, "Alice")
	tx.Add(bob, name, "Bob")
	tx.Add(carol, name, "Carol")

	tx.Add(alice, city, "Springfield")
	tx.Add(bob, city, "Springfield")
	tx.Add(carol, city, "Springfield")

	tx.Add(alice, age, int64(30))
	tx.Add(bob, age, int64(25))

	tx.Add(alice, bio, strings.Repeat("x", 100))

	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	return dbPath
}

func TestCollectStats(t *testing.T) {
	dbPath := createStatsTestDatabase(t)

	db, err := storage.NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	s, err := collectStats(db)
	if err != nil {
		t.Fatalf("collectStats failed: %v", err)
	}

	// 9 person datoms + 1 :db/txInstant transaction-metadata datom.
	if s.totalDatoms != 10 {
		t.Errorf("totalDatoms = %d, want 10", s.totalDatoms)
	}
	// 3 person entities + 1 transaction entity.
	if len(s.entities) != 4 {
		t.Errorf("distinct entities = %d, want 4", len(s.entities))
	}
	// 4 person attributes + :db/txInstant.
	if len(s.attrs) != 5 {
		t.Errorf("distinct attributes = %d, want 5", len(s.attrs))
	}

	city, ok := s.attrs[":person/city"]
	if !ok {
		t.Fatal("missing :person/city stats")
	}
	if city.count != 3 {
		t.Errorf(":person/city count = %d, want 3", city.count)
	}
	if len(city.values) != 1 {
		t.Errorf(":person/city distinct values = %d, want 1 (all Springfield)", len(city.values))
	}
	if len(city.entities) != 3 {
		t.Errorf(":person/city entities = %d, want 3", len(city.entities))
	}
	// Total bytes = 3 x len("Springfield"); unique bytes = 1 x len("Springfield").
	wantTotal := 3 * len("Springfield")
	if city.totalBytes != wantTotal {
		t.Errorf(":person/city totalBytes = %d, want %d", city.totalBytes, wantTotal)
	}
	if got := uniqueBytes(city.values); got != len("Springfield") {
		t.Errorf(":person/city unique bytes = %d, want %d", got, len("Springfield"))
	}

	name, ok := s.attrs[":person/name"]
	if !ok {
		t.Fatal("missing :person/name stats")
	}
	if len(name.values) != 3 {
		t.Errorf(":person/name distinct values = %d, want 3", len(name.values))
	}

	age, ok := s.attrs[":person/age"]
	if !ok {
		t.Fatal("missing :person/age stats")
	}
	if age.count != 2 || len(age.values) != 2 {
		t.Errorf(":person/age count/distinct = %d/%d, want 2/2", age.count, len(age.values))
	}
	if age.types["int64"] != 2 {
		t.Errorf(":person/age int64 type count = %d, want 2", age.types["int64"])
	}

	bio, ok := s.attrs[":person/bio"]
	if !ok {
		t.Fatal("missing :person/bio stats")
	}
	if bio.totalBytes != 100 {
		t.Errorf(":person/bio totalBytes = %d, want 100", bio.totalBytes)
	}

	// Global distinct values: 3 names + 1 city + 2 ages + 1 bio + 1 txInstant = 8.
	if len(s.globalValues) != 8 {
		t.Errorf("global distinct values = %d, want 8", len(s.globalValues))
	}

	// The duplicated Springfield value should be tracked with count 3.
	found := false
	for _, vi := range s.globalValues {
		if vi.count == 3 && vi.typ == "string" {
			found = true
		}
	}
	if !found {
		t.Error("expected a string value with count 3 (Springfield) in global values")
	}
}

func TestStatsReportOutput(t *testing.T) {
	dbPath := createStatsTestDatabase(t)

	db, err := storage.NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	s, err := collectStats(db)
	if err != nil {
		t.Fatalf("collectStats failed: %v", err)
	}

	var buf strings.Builder
	s.print(&buf, dbPath)
	out := buf.String()

	for _, want := range []string{
		"Total datoms (all CRDT history): 10",
		"Distinct entities:               4",
		":person/city",
		":person/name",
		"## Per-Attribute Cardinality and Duplication",
		"## Value Size Distribution",
		"## Top Duplicated Values",
		"Springfield",
		"## CRDT Op Distribution",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report output missing %q\n---\n%s", want, out)
		}
	}
}

func TestStatsFlagCLI(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := createStatsTestDatabase(t)

	cmd := exec.Command(binPath, "-db", dbPath, "-stats")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("CLI -stats failed: %v\n%s", err, out)
	}
	output := string(out)
	if !strings.Contains(output, "Total datoms") {
		t.Errorf("-stats output missing summary header:\n%s", output)
	}
	if !strings.Contains(output, ":person/city") {
		t.Errorf("-stats output missing attribute row:\n%s", output)
	}
}
