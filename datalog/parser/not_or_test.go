package parser

import (
	"strings"
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
				if len(notJoinClause.RequiredVars) != 1 {
					t.Fatalf("expected 1 join var, got %d", len(notJoinClause.RequiredVars))
				}
				if notJoinClause.RequiredVars[0] != datalog.NewSymbol("?e") {
					t.Fatalf("expected join var ?e, got %s", notJoinClause.RequiredVars[0])
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
				if len(notJoinClause.RequiredVars) != 2 {
					t.Fatalf("expected 2 join vars, got %d", len(notJoinClause.RequiredVars))
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

// TestParseNotJoinEmptyHeaderRejected pins the disjoint-NOT ruling at the
// syntax boundary: an empty not-join header would declare "unifies with the
// outer on nothing", a form the language rejects rather than giving global
// semantics. parseRequiredRun is the enforcement point.
func TestParseNotJoinEmptyHeaderRejected(t *testing.T) {
	_, err := ParseQuery(`[:find ?name
	                       :where [?e :person/name ?name]
	                              (not-join []
	                                        [?x :archived true])]`)
	if err == nil {
		t.Fatal("expected empty not-join header to be rejected at parse")
	}
	if !strings.Contains(err.Error(), "required run cannot be empty") {
		t.Fatalf("rejection must state the header rule, got: %v", err)
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
				if len(orJoinClause.OutputVars) != 1 {
					t.Fatalf("expected 1 join var, got %d", len(orJoinClause.OutputVars))
				}
				if orJoinClause.OutputVars[0] != datalog.NewSymbol("?e") {
					t.Fatalf("expected join var ?e, got %s", orJoinClause.OutputVars[0])
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
			error: "not clause must have at least one inner clause",
		},
		{
			name:  "not-join without inner clauses",
			input: `[:find ?e :where [?e :foo ?v] (not-join [?e])]`,
			error: "not-join clause must have a header and at least one clause",
		},
		{
			name:  "or without branches",
			input: `[:find ?e :where [?e :foo ?v] (or)]`,
			error: "or clause must have at least one branch",
		},
		{
			name:  "empty list",
			input: `[:find ?e :where [?e :foo ?v] ()]`,
			error: "list clause must not be empty",
		},
		{
			name:  "or-join without enough elements",
			input: `[:find ?e :where [?e :foo ?v] (or-join [?e])]`,
			error: "or-join clause must have a header vector and at least one branch",
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

func TestParseOrDefaultClause(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		verify func(t *testing.T, q *query.Query)
	}{
		{
			name: "simple or-default with ground fallback",
			input: `[:find ?x
                     :where [?e :user/name ?x]
                            (or-default [?e :user/score ?x]
                                        [(ground 0) ?x])]`,
			verify: func(t *testing.T, q *query.Query) {
				if len(q.Where) != 2 {
					t.Fatalf("expected 2 where clauses, got %d", len(q.Where))
				}
				od, ok := q.Where[1].(*query.OrDefaultClause)
				if !ok {
					t.Fatalf("expected OrDefaultClause, got %T", q.Where[1])
				}
				if len(od.Branches) != 2 {
					t.Fatalf("expected 2 branches, got %d", len(od.Branches))
				}
				if len(od.Branches[0]) != 1 {
					t.Fatalf("expected 1 clause in branch 0, got %d", len(od.Branches[0]))
				}
				if len(od.Branches[1]) != 1 {
					t.Fatalf("expected 1 clause in branch 1, got %d", len(od.Branches[1]))
				}
				// Branch 0 should be a DataPattern
				if _, ok := od.Branches[0][0].(*query.DataPattern); !ok {
					t.Fatalf("expected DataPattern in branch 0, got %T", od.Branches[0][0])
				}
				// Branch 1 should be an Expression
				if _, ok := od.Branches[1][0].(*query.Expression); !ok {
					t.Fatalf("expected Expression in branch 1, got %T", od.Branches[1][0])
				}
			},
		},
		{
			name: "or-default with and branch",
			input: `[:find ?x
                     :where (or-default (and [?e :a1 ?x] [?e :a2 ?y])
                                        [(ground 0) ?x])]`,
			verify: func(t *testing.T, q *query.Query) {
				od, ok := q.Where[0].(*query.OrDefaultClause)
				if !ok {
					t.Fatalf("expected OrDefaultClause, got %T", q.Where[0])
				}
				if len(od.Branches[0]) != 2 {
					t.Fatalf("expected 2 clauses in branch 0 (and), got %d", len(od.Branches[0]))
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

func TestParseOrDefaultJoinClause(t *testing.T) {
	t.Run("flat header declares outputs (global fallback)", func(t *testing.T) {
		// ?e in the branch deliberately shadows the outer ?e: a branch
		// variable outside the header is a local, and the name collision
		// is meaningless (alpha-equivalence). It parses, validates, and
		// does not correlate.
		input := `[:find ?x
	               :where [?e :user/name ?name]
	                      (or-default-join [?x]
	                        [?e :user/score ?x]
	                        [(ground 0) ?x])]`
		q, err := ParseQuery(input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(q.Where) != 2 {
			t.Fatalf("expected 2 where clauses, got %d", len(q.Where))
		}
		odj, ok := q.Where[1].(*query.OrDefaultJoinClause)
		if !ok {
			t.Fatalf("expected OrDefaultJoinClause, got %T", q.Where[1])
		}
		if len(odj.RequiredVars) != 0 {
			t.Fatalf("expected no required vars, got %v", odj.RequiredVars)
		}
		if len(odj.OutputVars) != 1 || odj.OutputVars[0] != datalog.NewSymbol("?x") {
			t.Fatalf("expected output var ?x, got %v", odj.OutputVars)
		}
		if len(odj.Branches) != 2 {
			t.Fatalf("expected 2 branches, got %d", len(odj.Branches))
		}
	})

	t.Run("nested first element declares required vars (per-group fallback)", func(t *testing.T) {
		input := `[:find ?x
	               :where [?e :user/name ?name]
	                      (or-default-join [[?e] ?x]
	                        [?e :user/score ?x]
	                        [(ground 0) ?x])]`
		q, err := ParseQuery(input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		odj, ok := q.Where[1].(*query.OrDefaultJoinClause)
		if !ok {
			t.Fatalf("expected OrDefaultJoinClause, got %T", q.Where[1])
		}
		if len(odj.RequiredVars) != 1 || odj.RequiredVars[0] != datalog.NewSymbol("?e") {
			t.Fatalf("expected required var ?e, got %v", odj.RequiredVars)
		}
		if len(odj.OutputVars) != 1 || odj.OutputVars[0] != datalog.NewSymbol("?x") {
			t.Fatalf("expected output var ?x, got %v", odj.OutputVars)
		}
	})

	t.Run("old correlation-key flat header is refused at the parser's boundary", func(t *testing.T) {
		// A flat header names ?e as an output the ground branch cannot bind:
		// the parse refuses it naming the form, the branch and the variable,
		// with the repair of moving ?e into the required run, never a silent
		// meaning change.
		input := `[:find ?v
	               :where [?e :user/name ?name]
	                      (or-default-join [?e ?v]
	                        [?e :user/score ?v]
	                        [(ground 0) ?v])]`
		_, err := ParseQuery(input)
		if err == nil {
			t.Fatal("expected error: branch 2 does not bind ?e")
		}
		for _, part := range []string{"(or-default-join [?e ?v]", "branch 2 does not bind ?e", "move ?e into the required run"} {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("the refusal %q does not carry %q", err, part)
			}
		}
	})

	t.Run("required vars only in first position", func(t *testing.T) {
		input := `[:find ?x
	               :where (or-default-join [?x [?e]]
	                        [?e :user/score ?x]
	                        [(ground 0) ?x])]`
		_, err := ParseQuery(input)
		if err == nil {
			t.Fatal("expected error: nested vector not in first position")
		}
	})
}

// A union of one branch is that branch over the header
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §5).
func TestParseQueryAcceptsAUnionOfOneBranch(t *testing.T) {
	q, err := ParseQuery(`[:find ?e :where [?e :t/n _] (or-join [?e] [?e :t/p _]) (or [?e :t/q _])]`)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	orJoin, ok := q.Where[1].(*query.OrJoinClause)
	if !ok || len(orJoin.Branches) != 1 {
		t.Errorf("the or-join of one branch parses to %v, want one branch", q.Where[1])
	}
	or, ok := q.Where[2].(*query.OrClause)
	if !ok || len(or.Branches) != 1 {
		t.Errorf("the or of one branch parses to %v, want one branch", q.Where[2])
	}
}

// The parser refuses a written header its branches do not satisfy with the
// message Prepare gives the same shape
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §6).
func TestParseQueryRefusesABranchOutsideItsHeaderAsPrepareDoes(t *testing.T) {
	cases := []struct {
		name  string
		input string
		parts []string
	}{
		{
			"an or-join output a branch does not bind",
			`[:find ?e :where [?e :t/n _] [(ground :t/tag) ?a] (or-join [?e ?a] [?e ?a ?v] [?e :t/tag :t/none])]`,
			[]string{"(or-join [?e ?a]", "branch 2 does not bind ?a", "move ?a into the required run"},
		},
		{
			"a not-join body reading outside its header",
			`[:find ?e :where [?e :t/p ?w] (not-join [?e] [?e :t/q ?x] [(> ?x ?w)])]`,
			[]string{"(not-join [?e]", "branch 1 reads ?w", "bind ?w in the branch or name it in the header"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseQuery(c.input)
			if err == nil {
				t.Fatalf("ParseQuery accepts the query; it is refused naming %q", c.parts)
			}
			for _, part := range c.parts {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("the refusal %q does not carry %q", err, part)
				}
			}
		})
	}
}

// A parse error inside a form names the branch, or the clause of a not or a
// not-join, by its position counted from one
// (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §2.4).
func TestParseErrorNamesTheBranchCountedFromOne(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"or", `[:find ?e :where (or (not) [?e :t/a _])]`, "error parsing or branch 1:"},
		{"or-join", `[:find ?e :where (or-join [?e] (not) [?e :t/a _])]`, "error parsing or-join branch 1:"},
		{"or-default", `[:find ?e :where (or-default (not) [?e :t/a _])]`, "error parsing or-default branch 1:"},
		{"or-default-join", `[:find ?e :where (or-default-join [?e] (not) [?e :t/a _])]`, "error parsing or-default-join branch 1:"},
		{"not", `[:find ?e :where [?e :t/a _] (not (not))]`, "error parsing not clause element 1:"},
		{"not-join", `[:find ?e :where [?e :t/a _] (not-join [?e] (not))]`, "error parsing not-join clause element 1:"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseQuery(c.input)
			if err == nil {
				t.Fatalf("ParseQuery accepts the query; its first branch holds an empty not")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("the refusal %q does not carry %q", err, c.want)
			}
		})
	}
}

func TestParseOrDefaultErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
		error string
	}{
		{
			name:  "or-default with single branch",
			input: `[:find ?e :where (or-default [?e :foo ?v])]`,
			error: "or-default clause must have at least two branches",
		},
		{
			name:  "or-default-join without enough elements",
			input: `[:find ?e :where (or-default-join [?e])]`,
			error: "or-default-join clause must have a header vector and at least two branches",
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
