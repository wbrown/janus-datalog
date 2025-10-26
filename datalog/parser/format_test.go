package parser

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestFormatQueryEDN(t *testing.T) {
	tests := []struct {
		name     string
		query    *query.Query
		expected string
	}{
		{
			name: "simple query with variables",
			query: &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: "?name"},
					query.FindVariable{Symbol: "?age"},
				},
				Where: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?e"},
							query.Constant{Value: datalog.NewKeyword(":user/name")},
							query.Variable{Name: "?name"},
						},
					},
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?e"},
							query.Constant{Value: datalog.NewKeyword(":user/age")},
							query.Variable{Name: "?age"},
						},
					},
				},
			},
			expected: `[:find ?name ?age
 :where [?e :user/name ?name]
        [?e :user/age ?age]]`,
		},
		{
			name: "query with constants",
			query: &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: "?name"},
				},
				Where: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?e"},
							query.Constant{Value: datalog.NewKeyword(":user/name")},
							query.Constant{Value: "Alice"},
						},
					},
				},
			},
			expected: `[:find ?name
 :where [?e :user/name "Alice"]]`,
		},
		{
			name: "query with function pattern",
			query: &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: "?name"},
					query.FindVariable{Symbol: "?age"},
				},
				Where: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?e"},
							query.Constant{Value: datalog.NewKeyword(":user/name")},
							query.Variable{Name: "?name"},
						},
					},
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?e"},
							query.Constant{Value: datalog.NewKeyword(":user/age")},
							query.Variable{Name: "?age"},
						},
					},
					&query.Comparison{
						Op:    query.OpLT,
						Left:  query.VariableTerm{Symbol: "?age"},
						Right: query.ConstantTerm{Value: int64(30)},
					},
				},
			},
			expected: `[:find ?name ?age
 :where [?e :user/name ?name]
        [?e :user/age ?age]
        [(< ?age 30)]]`,
		},
		{
			name: "query with blanks",
			query: &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: "?name"},
				},
				Where: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?e"},
							query.Constant{Value: datalog.NewKeyword(":user/name")},
							query.Variable{Name: "?name"},
							query.Blank{},
						},
					},
				},
			},
			expected: `[:find ?name
 :where [?e :user/name ?name _]]`,
		},
		{
			name: "query with entity reference",
			query: &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: "?name"},
				},
				Where: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?e"},
							query.Constant{Value: datalog.NewKeyword(":user/friend")},
							query.Constant{Value: datalog.NewIdentity("user:alice")},
						},
					},
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?e"},
							query.Constant{Value: datalog.NewKeyword(":user/name")},
							query.Variable{Name: "?name"},
						},
					},
				},
			},
			expected: `[:find ?name
 :where [?e :user/friend #db/id "user:alice"]
        [?e :user/name ?name]]`,
		},
		{
			name: "query with escaped strings",
			query: &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: "?msg"},
				},
				Where: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?e"},
							query.Constant{Value: datalog.NewKeyword(":message")},
							query.Constant{Value: "Hello \"world\"\nNew line\t\tTab"},
						},
					},
				},
			},
			expected: `[:find ?msg
 :where [?e :message "Hello \"world\"\nNew line\t\tTab"]]`,
		},
		{
			name: "query with numeric types",
			query: &query.Query{
				Find: []query.FindElement{
					query.FindVariable{Symbol: "?val"},
				},
				Where: []query.Clause{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?e"},
							query.Constant{Value: datalog.NewKeyword(":int-val")},
							query.Constant{Value: int64(42)},
						},
					},
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?e"},
							query.Constant{Value: datalog.NewKeyword(":float-val")},
							query.Constant{Value: 3.14159},
						},
					},
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?e"},
							query.Constant{Value: datalog.NewKeyword(":bool-val")},
							query.Constant{Value: true},
						},
					},
				},
			},
			expected: `[:find ?val
 :where [?e :int-val 42]
        [?e :float-val 3.14159]
        [?e :bool-val true]]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatQuery(tt.query)
			if got != tt.expected {
				t.Errorf("FormatQuery() mismatch:\ngot:\n%s\n\nexpected:\n%s", got, tt.expected)
			}
		})
	}
}

func TestFormatQueryRoundTrip(t *testing.T) {
	// Test that parsing and formatting produces equivalent results
	queries := []string{
		`[:find ?name :where [?e :user/name ?name]]`,
		`[:find ?x ?y :where [?x :follows ?y] [(< ?x ?y)]]`,
		`[:find ?name ?age
		  :where [?e :user/name ?name]
		         [?e :user/age ?age]
		         [(> ?age 21)]
		         [(< ?age 65)]]`,
	}

	for _, original := range queries {
		t.Run("roundtrip", func(t *testing.T) {
			// Parse the query
			parsed, err := ParseQuery(original)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			// Format it back
			formatted := FormatQuery(parsed)

			// Parse the formatted version
			reparsed, err := ParseQuery(formatted)
			if err != nil {
				t.Fatalf("Failed to parse formatted query: %v\nFormatted:\n%s", err, formatted)
			}

			// Compare the queries structurally
			if len(parsed.Find) != len(reparsed.Find) {
				t.Errorf("Find clauses differ in length: %d vs %d", len(parsed.Find), len(reparsed.Find))
			}
			for i := range parsed.Find {
				if parsed.Find[i] != reparsed.Find[i] {
					t.Errorf("Find symbol %d differs: %v vs %v", i, parsed.Find[i], reparsed.Find[i])
				}
			}

			if len(parsed.Where) != len(reparsed.Where) {
				t.Errorf("Where clauses differ in length: %d vs %d", len(parsed.Where), len(reparsed.Where))
			}

			// Note: Deep comparison of patterns would be more complex
			// For now, we'll just check the string representation
			for i := range parsed.Where {
				if parsed.Where[i].String() != reparsed.Where[i].String() {
					t.Errorf("Where pattern %d differs:\n%v\nvs\n%v",
						i, parsed.Where[i].String(), reparsed.Where[i].String())
				}
			}
		})
	}
}

func TestFormatValue(t *testing.T) {
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sb strings.Builder
			formatValue(&sb, tt.value)
			got := sb.String()
			if got != tt.expected {
				t.Errorf("formatValue(%v) = %q, want %q", tt.value, got, tt.expected)
			}
		})
	}
}
