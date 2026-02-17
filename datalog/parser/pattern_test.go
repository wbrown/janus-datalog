package parser

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
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
			wantSource: nil,
			wantElems:  3,
		},
		{
			name:       "source-qualified 3-tuple",
			query:      `[:find ?e :in $users :where [$users ?e :user/name ?n]]`,
			wantSource: datalog.NewSymbol("$users"),
			wantElems:  3,
		},
		{
			name:       "source-qualified 4-tuple",
			query:      `[:find ?e :in $db :where [$db ?e :attr ?v ?tx]]`,
			wantSource: datalog.NewSymbol("$db"),
			wantElems:  4,
		},
		{
			name:       "default source qualified",
			query:      `[:find ?e :in $ :where [$ ?e :attr ?v]]`,
			wantSource: datalog.NewSymbol("$"),
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

func TestParseTaggedLiteralsInPatterns(t *testing.T) {
	// #identity in entity position - look up a specific entity by hash
	t.Run("identity in entity position", func(t *testing.T) {
		alice := datalog.NewIdentity("alice")
		q, err := ParseQuery(`[:find ?a ?v :where [#identity "` + alice.L85() + `" ?a ?v]]`)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		pat := q.Where[0].(*query.DataPattern)
		c, ok := pat.Elements[0].(query.Constant)
		if !ok {
			t.Fatalf("expected Constant, got %T", pat.Elements[0])
		}
		got, ok := c.Value.(datalog.Identity)
		if !ok {
			t.Fatalf("expected Identity value, got %T", c.Value)
		}
		if !got.Equal(alice) {
			t.Errorf("expected identity %s, got %s", alice.L85(), got.L85())
		}
	})

	// #identity in value position - match a ref value
	t.Run("identity in value position", func(t *testing.T) {
		bob := datalog.NewIdentity("bob")
		q, err := ParseQuery(`[:find ?e :where [?e :person/friend #identity "` + bob.L85() + `"]]`)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		pat := q.Where[0].(*query.DataPattern)
		c, ok := pat.Elements[2].(query.Constant)
		if !ok {
			t.Fatalf("expected Constant, got %T", pat.Elements[2])
		}
		got, ok := c.Value.(datalog.Identity)
		if !ok {
			t.Fatalf("expected Identity value, got %T", c.Value)
		}
		if !got.Equal(bob) {
			t.Errorf("expected identity %s, got %s", bob.L85(), got.L85())
		}
	})

	// #inst in value position - match a timestamp
	t.Run("inst in value position", func(t *testing.T) {
		q, err := ParseQuery(`[:find ?e :where [?e :event/date #inst "2024-06-15T10:30:00Z"]]`)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		pat := q.Where[0].(*query.DataPattern)
		c, ok := pat.Elements[2].(query.Constant)
		if !ok {
			t.Fatalf("expected Constant, got %T", pat.Elements[2])
		}
		got, ok := c.Value.(time.Time)
		if !ok {
			t.Fatalf("expected time.Time value, got %T", c.Value)
		}
		want := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	// #inst in predicate position
	t.Run("inst in predicate", func(t *testing.T) {
		q, err := ParseQuery(`[:find ?e :where [?e :event/date ?d] [(> ?d #inst "2024-01-01T00:00:00Z")]]`)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		if len(q.Where) != 2 {
			t.Fatalf("expected 2 where clauses, got %d", len(q.Where))
		}
	})
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
