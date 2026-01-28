package parser

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestParseSourceQualifiedPattern(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		wantSource query.Symbol
		wantElems  int
	}{
		{
			name:       "unqualified 3-tuple",
			query:      `[:find ?e :where [?e :attr ?v]]`,
			wantSource: "",
			wantElems:  3,
		},
		{
			name:       "source-qualified 3-tuple",
			query:      `[:find ?e :in $users :where [$users ?e :user/name ?n]]`,
			wantSource: "$users",
			wantElems:  3,
		},
		{
			name:       "source-qualified 4-tuple",
			query:      `[:find ?e :in $db :where [$db ?e :attr ?v ?tx]]`,
			wantSource: "$db",
			wantElems:  4,
		},
		{
			name:       "default source qualified",
			query:      `[:find ?e :in $ :where [$ ?e :attr ?v]]`,
			wantSource: "$",
			wantElems:  3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(q.Where) < 1 {
				t.Fatalf("expected at least 1 where clause, got %d", len(q.Where))
			}
			pat, ok := q.Where[0].(*query.DataPattern)
			if !ok {
				t.Fatalf("expected DataPattern, got %T", q.Where[0])
			}
			if pat.Source != tt.wantSource {
				t.Errorf("source: expected %q, got %q", tt.wantSource, pat.Source)
			}
			if len(pat.Elements) != tt.wantElems {
				t.Errorf("elements: expected %d, got %d", tt.wantElems, len(pat.Elements))
			}
		})
	}
}

func TestParseSourceQualifiedPatternRoundTrip(t *testing.T) {
	// Verify that String() output for source-qualified patterns is correct
	input := `[:find ?name :in $users :where [$users ?e :user/name ?name]]`

	q, err := ParseQuery(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pat := q.Where[0].(*query.DataPattern)
	got := pat.String()
	want := "[$users ?e :user/name ?name]"
	if got != want {
		t.Errorf("String(): expected %q, got %q", want, got)
	}
}
