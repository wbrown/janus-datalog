package parser

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestParseNotClause(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		verify func(t *testing.T, q *query.Query)
	}{
		{
			name: "simple not",
			input: `[:find ?name
                     :where [?e :person/name ?name]
                            (not [?e :archived true])]`,
			verify: func(t *testing.T, q *query.Query) {
				if len(q.Where) != 2 {
					t.Fatalf("expected 2 where clauses, got %d", len(q.Where))
				}
				notClause, ok := q.Where[1].(*query.NotClause)
				if !ok {
					t.Fatalf("expected NotClause, got %T", q.Where[1])
				}
				if len(notClause.Clauses) != 1 {
					t.Fatalf("expected 1 inner clause, got %d", len(notClause.Clauses))
				}
			},
		},
		{
			name: "not with multiple clauses",
			input: `[:find ?name
                     :where [?e :person/name ?name]
                            (not [?e :archived true]
                                 [?e :deleted true])]`,
			verify: func(t *testing.T, q *query.Query) {
				if len(q.Where) != 2 {
					t.Fatalf("expected 2 where clauses, got %d", len(q.Where))
				}
				notClause, ok := q.Where[1].(*query.NotClause)
				if !ok {
					t.Fatalf("expected NotClause, got %T", q.Where[1])
				}
				if len(notClause.Clauses) != 2 {
					t.Fatalf("expected 2 inner clauses, got %d", len(notClause.Clauses))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			tt.verify(t, q)
		})
	}
}

func TestParseNotJoinClause(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		verify func(t *testing.T, q *query.Query)
	}{
		{
			name: "simple not-join",
			input: `[:find ?name
                     :where [?e :person/name ?name]
                            (not-join [?e]
                                      [?e :archived true])]`,
			verify: func(t *testing.T, q *query.Query) {
				if len(q.Where) != 2 {
					t.Fatalf("expected 2 where clauses, got %d", len(q.Where))
				}
				notJoinClause, ok := q.Where[1].(*query.NotJoinClause)
				if !ok {
					t.Fatalf("expected NotJoinClause, got %T", q.Where[1])
				}
				if len(notJoinClause.JoinVars) != 1 {
					t.Fatalf("expected 1 join var, got %d", len(notJoinClause.JoinVars))
				}
				if notJoinClause.JoinVars[0] != datalog.NewSymbol("?e") {
					t.Fatalf("expected join var ?e, got %s", notJoinClause.JoinVars[0])
				}
				if len(notJoinClause.Clauses) != 1 {
					t.Fatalf("expected 1 inner clause, got %d", len(notJoinClause.Clauses))
				}
			},
		},
		{
			name: "not-join with multiple join vars",
			input: `[:find ?name
                     :where [?e :person/name ?name]
                            [?e :person/city ?c]
                            (not-join [?e ?c]
                                      [?e :blacklisted-in ?c])]`,
			verify: func(t *testing.T, q *query.Query) {
				if len(q.Where) != 3 {
					t.Fatalf("expected 3 where clauses, got %d", len(q.Where))
				}
				notJoinClause, ok := q.Where[2].(*query.NotJoinClause)
				if !ok {
					t.Fatalf("expected NotJoinClause, got %T", q.Where[2])
				}
				if len(notJoinClause.JoinVars) != 2 {
					t.Fatalf("expected 2 join vars, got %d", len(notJoinClause.JoinVars))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			tt.verify(t, q)
		})
	}
}

func TestParseOrClause(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		verify func(t *testing.T, q *query.Query)
	}{
		{
			name: "simple or with single-clause branches",
			input: `[:find ?name
                     :where [?e :person/name ?name]
                            (or [?e :status "active"]
                                [?e :status "pending"])]`,
			verify: func(t *testing.T, q *query.Query) {
				if len(q.Where) != 2 {
					t.Fatalf("expected 2 where clauses, got %d", len(q.Where))
				}
				orClause, ok := q.Where[1].(*query.OrClause)
				if !ok {
					t.Fatalf("expected OrClause, got %T", q.Where[1])
				}
				if len(orClause.Branches) != 2 {
					t.Fatalf("expected 2 branches, got %d", len(orClause.Branches))
				}
				// Each branch should have 1 clause
				if len(orClause.Branches[0]) != 1 {
					t.Fatalf("expected 1 clause in branch 0, got %d", len(orClause.Branches[0]))
				}
				if len(orClause.Branches[1]) != 1 {
					t.Fatalf("expected 1 clause in branch 1, got %d", len(orClause.Branches[1]))
				}
			},
		},
		{
			name: "or with and branch",
			input: `[:find ?name
                     :where [?e :person/name ?name]
                            (or [?e :status "active"]
                                (and [?e :status "pending"]
                                     [?e :priority "high"]))]`,
			verify: func(t *testing.T, q *query.Query) {
				if len(q.Where) != 2 {
					t.Fatalf("expected 2 where clauses, got %d", len(q.Where))
				}
				orClause, ok := q.Where[1].(*query.OrClause)
				if !ok {
					t.Fatalf("expected OrClause, got %T", q.Where[1])
				}
				if len(orClause.Branches) != 2 {
					t.Fatalf("expected 2 branches, got %d", len(orClause.Branches))
				}
				// Second branch should have 2 clauses (from the and)
				if len(orClause.Branches[1]) != 2 {
					t.Fatalf("expected 2 clauses in branch 1 (and), got %d", len(orClause.Branches[1]))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			tt.verify(t, q)
		})
	}
}

func TestParseOrJoinClause(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		verify func(t *testing.T, q *query.Query)
	}{
		{
			name: "simple or-join",
			input: `[:find ?name
                     :where [?e :person/name ?name]
                            (or-join [?e]
                                     [?e :user/status "active"]
                                     [?e :admin/status "enabled"])]`,
			verify: func(t *testing.T, q *query.Query) {
				if len(q.Where) != 2 {
					t.Fatalf("expected 2 where clauses, got %d", len(q.Where))
				}
				orJoinClause, ok := q.Where[1].(*query.OrJoinClause)
				if !ok {
					t.Fatalf("expected OrJoinClause, got %T", q.Where[1])
				}
				if len(orJoinClause.JoinVars) != 1 {
					t.Fatalf("expected 1 join var, got %d", len(orJoinClause.JoinVars))
				}
				if orJoinClause.JoinVars[0] != datalog.NewSymbol("?e") {
					t.Fatalf("expected join var ?e, got %s", orJoinClause.JoinVars[0])
				}
				if len(orJoinClause.Branches) != 2 {
					t.Fatalf("expected 2 branches, got %d", len(orJoinClause.Branches))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			tt.verify(t, q)
		})
	}
}

func TestNotOrParseErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
		error string
	}{
		{
			name:  "not without clauses",
			input: `[:find ?e :where [?e :foo ?v] (not)]`,
			error: "list clause must have at least a keyword and one element",
		},
		{
			name:  "not-join without inner clauses",
			input: `[:find ?e :where [?e :foo ?v] (not-join [?e])]`,
			error: "not-join clause must have join vars and at least one clause",
		},
		{
			name:  "or without branches",
			input: `[:find ?e :where [?e :foo ?v] (or)]`,
			error: "list clause must have at least a keyword and one element",
		},
		{
			name:  "or-join without enough elements",
			input: `[:find ?e :where [?e :foo ?v] (or-join [?e])]`,
			error: "or-join clause must have join vars and at least two branches",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseQuery(tt.input)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.error)
			}
			if !contains(err.Error(), tt.error) {
				t.Errorf("expected error containing %q, got %q", tt.error, err.Error())
			}
		})
	}
}
