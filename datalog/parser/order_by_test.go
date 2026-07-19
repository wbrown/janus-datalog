package parser

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
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
				if q.OrderBy[0].Variable != datalog.NewSymbol("?age") {
					t.Errorf("expected ?age, got %s", q.OrderBy[0].Variable)
				}
				if q.OrderBy[0].Descending {
					t.Errorf("expected ascending order")
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
				if q.OrderBy[0].Variable != datalog.NewSymbol("?score") {
					t.Errorf("expected ?score, got %s", q.OrderBy[0].Variable)
				}
				if !q.OrderBy[0].Descending {
					t.Errorf("expected descending order")
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
				if q.OrderBy[0].Variable != datalog.NewSymbol("?dept") || q.OrderBy[0].Descending {
					t.Errorf("first clause should be ?dept asc")
				}
				if q.OrderBy[1].Variable != datalog.NewSymbol("?salary") || !q.OrderBy[1].Descending {
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
				if q.OrderBy[0].Descending {
					t.Errorf("?x should be ascending")
				}
				if !q.OrderBy[1].Descending {
					t.Errorf("?y should be descending")
				}
				if q.OrderBy[2].Descending {
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
				if q.OrderBy[i].Descending != q2.OrderBy[i].Descending {
					t.Errorf("descending mismatch at %d: %t vs %t", i, q.OrderBy[i].Descending, q2.OrderBy[i].Descending)
				}
			}
		})
	}
}

// TestOrderByValidation pins the sort-key binding rules from
// docs/bugs/resolved/BUG_ORDER_BY_NON_PROJECTED_VARIABLE_SILENTLY_IGNORED.md
// (Design Decision section): a sort key must be bound by :where or be a
// scalar/tuple :in constant; unbound variables, relation/collection input
// symbols not bound by :where, and non-group-key variables in aggregate
// queries are parse errors.
func TestOrderByValidation(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name: "where-bound non-projected variable is valid",
			query: `[:find ?name
			         :where [?e :user/name ?name]
			                [?e :user/age ?age]
			         :order-by [[?age :asc]]]`,
			wantErr: false,
		},
		{
			name: "scalar input constant is valid (no-op sort key)",
			query: `[:find ?name
			         :in $ ?status
			         :where [?e :user/status ?status]
			                [?e :user/name ?name]
			         :order-by [[?status :asc]]]`,
			wantErr: false,
		},
		{
			name: "variable bound nowhere is an error (safety violation)",
			query: `[:find ?name
			         :where [?e :user/name ?name]
			         :order-by [[?bogus :asc]]]`,
			wantErr: true,
		},
		{
			name: "relation input symbol not bound by :where is an error",
			query: `[:find ?v
			         :in $ [[?k ?tag] ...]
			         :where [?e :item/key ?k]
			                [?e :item/val ?v]
			         :order-by [[?tag :asc]]]`,
			wantErr: true,
		},
		{
			name: "collection input not bound by :where is an error",
			query: `[:find ?name
			         :in $ [?c ...]
			         :where [?e :user/name ?name]
			         :order-by [[?c :asc]]]`,
			wantErr: true,
		},
		{
			name: "aggregate query ordering by non-group-key variable is an error",
			query: `[:find ?city (count ?p)
			         :where [?p :person/city ?city]
			                [?p :person/age ?age]
			         :order-by [[?age :desc]]]`,
			wantErr: true,
		},
		{
			name: "aggregate query ordering by group key is valid",
			query: `[:find ?city (count ?p)
			         :where [?p :person/city ?city]
			         :order-by [[?city :asc]]]`,
			wantErr: false,
		},
		{
			name: "aggregate query ordering by scalar input constant is valid",
			query: `[:find ?city (count ?p)
			         :in $ ?country
			         :where [?p :person/country ?country]
			                [?p :person/city ?city]
			         :order-by [[?country :asc]]]`,
			wantErr: false,
		},
		{
			name: "expression-bound variable is valid",
			query: `[:find ?name
			         :where [?e :user/name ?name]
			                [?e :user/age ?age]
			                [(+ ?age 10) ?agePlus]
			         :order-by [[?agePlus :asc]]]`,
			wantErr: false,
		},
		{
			name: "variable bound by all or-branches is valid",
			query: `[:find ?name
			         :where [?e :user/name ?name]
			                (or [?e :user/rank ?rank]
			                    [(ground 0) ?rank])
			         :order-by [[?rank :desc]]]`,
			wantErr: false,
		},
		{
			name: "subquery-bound variable inside or is valid",
			query: `[:find ?scenario ?last
			         :where [?scenario :entity/type :entity.type/scenario]
			                (or [(q [:find (max ?ca)
			                         :in $ ?s
			                         :where [?t :task/root ?s]
			                                [?t :task/completed-at ?ca]]
			                        $ ?scenario) [[?last]]]
			                    [(ground :none) ?last])
			         :order-by [[?last :desc]]]`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseQuery(tt.query)
			if tt.wantErr && err == nil {
				t.Errorf("expected parse error, got success")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected parse error: %v", err)
			}
		})
	}
}
