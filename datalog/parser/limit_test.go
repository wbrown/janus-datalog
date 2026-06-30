package parser

import (
	"testing"
)

// TestLimitParsing covers the :limit clause grammar: it reads a single
// non-negative integer, rejects negatives/non-integers loudly, and is
// independent of :order-by (a bare :limit with no ordering is valid).
func TestLimitParsing(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
		want    int // expected *Limit when no error and Limit set
		wantNil bool
	}{
		{
			name: "limit with order-by (latest record)",
			query: `[:find ?e ?tx
			         :where [?e :event/crawl ?tx]
			         :order-by [[?tx :desc]]
			         :limit 1]`,
			want: 1,
		},
		{
			name: "limit without order-by is valid",
			query: `[:find ?e
			         :where [?e :entity/type :entity.type/telemetry]
			         :limit 1]`,
			want: 1,
		},
		{
			name: "limit larger than one",
			query: `[:find ?e
			         :where [?e :person/name ?n]
			         :limit 10]`,
			want: 10,
		},
		{
			name: "limit zero is valid",
			query: `[:find ?e
			         :where [?e :person/name ?n]
			         :limit 0]`,
			want: 0,
		},
		{
			name: "no limit leaves Limit nil",
			query: `[:find ?e
			         :where [?e :person/name ?n]]`,
			wantNil: true,
		},
		{
			name: "negative limit is an error",
			query: `[:find ?e
			         :where [?e :person/name ?n]
			         :limit -1]`,
			wantErr: true,
		},
		{
			name: "non-integer (float) limit is an error",
			query: `[:find ?e
			         :where [?e :person/name ?n]
			         :limit 3.5]`,
			wantErr: true,
		},
		{
			name: "string limit is an error",
			query: `[:find ?e
			         :where [?e :person/name ?n]
			         :limit "x"]`,
			wantErr: true,
		},
		{
			name: "missing limit value is an error",
			query: `[:find ?e
			         :where [?e :person/name ?n]
			         :limit]`,
			wantErr: true,
		},
		{
			name: "two limit values is an error",
			query: `[:find ?e
			         :where [?e :person/name ?n]
			         :limit 5 7]`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if tt.wantNil {
				if q.Limit != nil {
					t.Fatalf("expected Limit nil, got %d", *q.Limit)
				}
				return
			}
			if q.Limit == nil {
				t.Fatalf("expected Limit %d, got nil", tt.want)
			}
			if *q.Limit != tt.want {
				t.Errorf("expected Limit %d, got %d", tt.want, *q.Limit)
			}
		})
	}
}

// TestLimitScalarInteraction covers the janus convention for :limit combined
// with the scalar find-spec (:find ... .): limit 0 and 1 are coherent, limit
// N>1 contradicts "return a single value" and is a parse error.
func TestLimitScalarInteraction(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name: "scalar with limit 1 is valid",
			query: `[:find ?name .
			         :where [?p :person/name ?name]
			         :limit 1]`,
		},
		{
			name: "scalar with limit 0 is valid",
			query: `[:find ?name .
			         :where [?p :person/name ?name]
			         :limit 0]`,
		},
		{
			name: "scalar with limit 2 is an error",
			query: `[:find ?name .
			         :where [?p :person/name ?name]
			         :limit 2]`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestLimitRejectedInSubquery: :limit inside a subquery is rejected at parse
// time (the subquery execution path does not apply it, so allowing it would
// silently ignore the cap). A subquery without :limit must still parse, and a
// top-level :limit on a query that contains a subquery is fine.
func TestLimitRejectedInSubquery(t *testing.T) {
	t.Run("subquery with :limit is rejected", func(t *testing.T) {
		_, err := ParseQuery(`[:find ?c ?v
		                       :where [?cat :cat/name ?c]
		                              [(q [:find ?val
		                                   :in $ ?c
		                                   :where [?i :item/cat ?c] [?i :item/val ?val]
		                                   :limit 2]
		                                  $ ?c) [[?v] ...]]]`)
		if err == nil {
			t.Error("expected parse error for :limit inside a subquery, got nil")
		}
	})

	t.Run("subquery without :limit still parses", func(t *testing.T) {
		_, err := ParseQuery(`[:find ?c ?v
		                       :where [?cat :cat/name ?c]
		                              [(q [:find ?val
		                                   :in $ ?c
		                                   :where [?i :item/cat ?c] [?i :item/val ?val]]
		                                  $ ?c) [[?v] ...]]]`)
		if err != nil {
			t.Errorf("subquery without :limit should parse, got %v", err)
		}
	})

	t.Run("top-level :limit with a subquery is fine", func(t *testing.T) {
		q, err := ParseQuery(`[:find ?c ?v
		                       :where [?cat :cat/name ?c]
		                              [(q [:find ?val
		                                   :in $ ?c
		                                   :where [?i :item/cat ?c] [?i :item/val ?val]]
		                                  $ ?c) [[?v] ...]]
		                       :limit 5]`)
		if err != nil {
			t.Fatalf("top-level :limit with subquery should parse, got %v", err)
		}
		if q.Limit == nil || *q.Limit != 5 {
			t.Errorf("expected top-level Limit 5, got %v", q.Limit)
		}
	})
}

// TestLimitRoundTrip verifies that a query carrying :limit round-trips through
// both formatters (parser.FormatQuery and query.Query.String()).
func TestLimitRoundTrip(t *testing.T) {
	queries := []string{
		`[:find ?e ?tx
		  :where [?e :event/crawl ?tx]
		  :order-by [[?tx :desc]]
		  :limit 1]`,
		`[:find ?e
		  :where [?e :person/name ?n]
		  :limit 25]`,
		`[:find ?e
		  :where [?e :person/name ?n]
		  :limit 0]`,
	}

	for _, qs := range queries {
		q, err := ParseQuery(qs)
		if err != nil {
			t.Fatalf("ParseQuery() error = %v", err)
		}

		for _, formatted := range []string{FormatQuery(q), q.String()} {
			q2, err := ParseQuery(formatted)
			if err != nil {
				t.Fatalf("re-parse of %q error = %v", formatted, err)
			}
			if q2.Limit == nil {
				t.Fatalf("re-parse dropped :limit from %q", formatted)
			}
			if *q2.Limit != *q.Limit {
				t.Errorf("limit mismatch after round-trip: got %d want %d (formatted: %q)", *q2.Limit, *q.Limit, formatted)
			}
		}
	}
}
