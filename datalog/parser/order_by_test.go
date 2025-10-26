package parser

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestOrderByParsing(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
		check   func(t *testing.T, q *query.Query)
	}{
		{
			name: "simple ascending order",
			query: `[:find ?name ?age
			         :where [?p :person/name ?name]
			                [?p :person/age ?age]
			         :order-by [?age]]`,
			check: func(t *testing.T, q *query.Query) {
				if len(q.OrderBy) != 1 {
					t.Errorf("expected 1 order-by clause, got %d", len(q.OrderBy))
				}
				if q.OrderBy[0].Variable != "?age" {
					t.Errorf("expected ?age, got %s", q.OrderBy[0].Variable)
				}
				if q.OrderBy[0].Direction != query.OrderAsc {
					t.Errorf("expected asc, got %s", q.OrderBy[0].Direction)
				}
			},
		},
		{
			name: "explicit descending order",
			query: `[:find ?name ?score
			         :where [?p :person/name ?name]
			                [?p :test/score ?score]
			         :order-by [[?score :desc]]]`,
			check: func(t *testing.T, q *query.Query) {
				if len(q.OrderBy) != 1 {
					t.Errorf("expected 1 order-by clause, got %d", len(q.OrderBy))
				}
				if q.OrderBy[0].Variable != "?score" {
					t.Errorf("expected ?score, got %s", q.OrderBy[0].Variable)
				}
				if q.OrderBy[0].Direction != query.OrderDesc {
					t.Errorf("expected desc, got %s", q.OrderBy[0].Direction)
				}
			},
		},
		{
			name: "multiple sort keys",
			query: `[:find ?name ?dept ?salary
			         :where [?p :person/name ?name]
			                [?p :person/dept ?dept]
			                [?p :person/salary ?salary]
			         :order-by [[?dept :asc] [?salary :desc]]]`,
			check: func(t *testing.T, q *query.Query) {
				if len(q.OrderBy) != 2 {
					t.Errorf("expected 2 order-by clauses, got %d", len(q.OrderBy))
				}
				if q.OrderBy[0].Variable != "?dept" || q.OrderBy[0].Direction != query.OrderAsc {
					t.Errorf("first clause should be ?dept asc")
				}
				if q.OrderBy[1].Variable != "?salary" || q.OrderBy[1].Direction != query.OrderDesc {
					t.Errorf("second clause should be ?salary desc")
				}
			},
		},
		{
			name: "mixed implicit and explicit",
			query: `[:find ?x ?y ?z
			         :where [?e :a ?x]
			                [?e :b ?y]
			                [?e :c ?z]
			         :order-by [?x [?y :desc] ?z]]`,
			check: func(t *testing.T, q *query.Query) {
				if len(q.OrderBy) != 3 {
					t.Errorf("expected 3 order-by clauses, got %d", len(q.OrderBy))
				}
				if q.OrderBy[0].Direction != query.OrderAsc {
					t.Errorf("?x should be ascending")
				}
				if q.OrderBy[1].Direction != query.OrderDesc {
					t.Errorf("?y should be descending")
				}
				if q.OrderBy[2].Direction != query.OrderAsc {
					t.Errorf("?z should be ascending")
				}
			},
		},
		{
			name: "invalid - non-variable",
			query: `[:find ?name
			         :where [?p :person/name ?name]
			         :order-by ["name"]]`,
			wantErr: true,
		},
		{
			name: "invalid - bad direction",
			query: `[:find ?name
			         :where [?p :person/name ?name]
			         :order-by [[?name :ascending]]]`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && tt.check != nil {
				tt.check(t, q)
			}
		})
	}
}

func TestOrderByFormatting(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name: "simple order-by",
			query: `[:find ?x
			         :where [?e :attr ?x]
			         :order-by [?x]]`,
		},
		{
			name: "descending order",
			query: `[:find ?x
			         :where [?e :attr ?x]
			         :order-by [[?x :desc]]]`,
		},
		{
			name: "multiple keys",
			query: `[:find ?x ?y
			         :where [?e :a ?x]
			                [?e :b ?y]
			         :order-by [?x [?y :desc]]]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("ParseQuery() error = %v", err)
			}

			formatted := FormatQuery(q)

			// Parse the formatted query back
			q2, err := ParseQuery(formatted)
			if err != nil {
				t.Fatalf("ParseQuery(formatted) error = %v", err)
			}

			// Compare order-by clauses
			if len(q.OrderBy) != len(q2.OrderBy) {
				t.Errorf("order-by clause count mismatch: %d vs %d", len(q.OrderBy), len(q2.OrderBy))
			}

			for i := range q.OrderBy {
				if q.OrderBy[i].Variable != q2.OrderBy[i].Variable {
					t.Errorf("variable mismatch at %d: %s vs %s", i, q.OrderBy[i].Variable, q2.OrderBy[i].Variable)
				}
				if q.OrderBy[i].Direction != q2.OrderBy[i].Direction {
					t.Errorf("direction mismatch at %d: %s vs %s", i, q.OrderBy[i].Direction, q2.OrderBy[i].Direction)
				}
			}
		})
	}
}
