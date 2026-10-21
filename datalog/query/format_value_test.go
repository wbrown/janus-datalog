package query

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/codec"
)

// FormatValueEDN renders every value kind the parser reads in the form it
// reads: keywords and symbols bare, strings escaped, numbers and booleans
// plain, an identity, an instant and bytes as their tagged literals, and a
// vector element by element.
func TestFormatValueEDN(t *testing.T) {
	alice := datalog.NewIdentity("user:alice")
	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"keyword", datalog.NewKeyword(":user/name"), ":user/name"},
		{"string", "hello", `"hello"`},
		{"string with quotes", `hello "world"`, `"hello \"world\""`},
		{"string with newline", "line1\nline2", `"line1\nline2"`},
		{"string with tab", "col1\tcol2", `"col1\tcol2"`},
		{"string with backslash", `path\to\file`, `"path\\to\\file"`},
		{"int64", int64(42), "42"},
		{"int", 42, "42"},
		{"float64", 3.14, "3.14"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"identity", alice, `#identity "` + alice.String() + `"`},
		{"instant", time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC), `#inst "2024-06-15T10:30:00Z"`},
		{"bytes", []byte{1, 2}, `#bytes "` + codec.EncodeL85([]byte{1, 2}) + `"`},
		{"vector", []interface{}{int64(1), "a", datalog.NewKeyword(":k")}, `[1 "a" :k]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormatValueEDN(tt.value); got != tt.expected {
				t.Errorf("FormatValueEDN(%v) = %q, want %q", tt.value, got, tt.expected)
			}
		})
	}
}
