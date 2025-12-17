package parser

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestParseGetElse(t *testing.T) {
	tests := []struct {
		name        string
		queryStr    string
		wantErr     bool
		errContains string
		validate    func(t *testing.T, q *query.Query)
	}{
		{
			name:     "basic get-else with string default",
			queryStr: `[:find ?e ?name :where [?e :entity/id _] [(get-else $ ?e :entity/name "Unknown") ?name]]`,
			wantErr:  false,
			validate: func(t *testing.T, q *query.Query) {
				if len(q.Where) != 2 {
					t.Fatalf("expected 2 where clauses, got %d", len(q.Where))
				}
				expr, ok := q.Where[1].(*query.Expression)
				if !ok {
					t.Fatalf("expected Expression, got %T", q.Where[1])
				}
				getElse, ok := expr.Function.(*query.GetElseFunction)
				if !ok {
					t.Fatalf("expected GetElseFunction, got %T", expr.Function)
				}
				if getElse.Attr.String() != ":entity/name" {
					t.Errorf("expected attr :entity/name, got %s", getElse.Attr.String())
				}
				if getElse.Default != "Unknown" {
					t.Errorf("expected default 'Unknown', got %v", getElse.Default)
				}
				if expr.Binding != "?name" {
					t.Errorf("expected binding ?name, got %s", expr.Binding)
				}
			},
		},
		{
			name:     "get-else with integer default",
			queryStr: `[:find ?e ?count :where [?e :entity/id _] [(get-else $ ?e :entity/count 0) ?count]]`,
			wantErr:  false,
			validate: func(t *testing.T, q *query.Query) {
				expr := q.Where[1].(*query.Expression)
				getElse := expr.Function.(*query.GetElseFunction)
				// Note: EDN parser may return int64 for integers
				switch v := getElse.Default.(type) {
				case int64:
					if v != 0 {
						t.Errorf("expected default 0, got %v", v)
					}
				case int:
					if v != 0 {
						t.Errorf("expected default 0, got %v", v)
					}
				default:
					t.Errorf("expected numeric default, got %T", getElse.Default)
				}
			},
		},
		{
			name:     "get-else with empty string default",
			queryStr: `[:find ?e ?desc :where [?e :entity/id _] [(get-else $ ?e :entity/description "") ?desc]]`,
			wantErr:  false,
			validate: func(t *testing.T, q *query.Query) {
				expr := q.Where[1].(*query.Expression)
				getElse := expr.Function.(*query.GetElseFunction)
				if getElse.Default != "" {
					t.Errorf("expected empty string default, got %v", getElse.Default)
				}
			},
		},
		{
			name:        "get-else missing database ref",
			queryStr:    `[:find ?e ?name :where [?e :entity/id _] [(get-else ?e :entity/name "Unknown") ?name]]`,
			wantErr:     true,
			errContains: "get-else requires exactly 4 arguments",
		},
		{
			name:        "get-else wrong number of args",
			queryStr:    `[:find ?e ?name :where [?e :entity/id _] [(get-else $ ?e :entity/name) ?name]]`,
			wantErr:     true,
			errContains: "get-else requires exactly 4 arguments",
		},
		{
			name:        "get-else with variable as attribute (invalid)",
			queryStr:    `[:find ?e ?name :where [?e :entity/id _] [(get-else $ ?e ?attr "Unknown") ?name]]`,
			wantErr:     true,
			errContains: "attribute must be a keyword",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.queryStr)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.validate != nil {
				tt.validate(t, q)
			}
		})
	}
}

func TestParseMissingAttr(t *testing.T) {
	tests := []struct {
		name        string
		queryStr    string
		wantErr     bool
		errContains string
		validate    func(t *testing.T, q *query.Query)
	}{
		{
			name:     "missing? as predicate filter (no binding)",
			queryStr: `[:find ?e :where [?e :entity/id _] [(missing? $ ?e :entity/name)]]`,
			wantErr:  false,
			validate: func(t *testing.T, q *query.Query) {
				if len(q.Where) != 2 {
					t.Fatalf("expected 2 where clauses, got %d", len(q.Where))
				}
				// missing? without binding is parsed as DatabaseFunctionPredicate
				pred, ok := q.Where[1].(*query.DatabaseFunctionPredicate)
				if !ok {
					t.Fatalf("expected DatabaseFunctionPredicate, got %T", q.Where[1])
				}
				missing, ok := pred.Function.(*query.MissingFunction)
				if !ok {
					t.Fatalf("expected MissingFunction, got %T", pred.Function)
				}
				if missing.Attr.String() != ":entity/name" {
					t.Errorf("expected attr :entity/name, got %s", missing.Attr.String())
				}
			},
		},
		{
			name:     "missing? as expression (with binding)",
			queryStr: `[:find ?e ?is_missing :where [?e :entity/id _] [(missing? $ ?e :entity/name) ?is_missing]]`,
			wantErr:  false,
			validate: func(t *testing.T, q *query.Query) {
				if len(q.Where) != 2 {
					t.Fatalf("expected 2 where clauses, got %d", len(q.Where))
				}
				// missing? with binding is parsed as Expression
				expr, ok := q.Where[1].(*query.Expression)
				if !ok {
					t.Fatalf("expected Expression, got %T", q.Where[1])
				}
				missing, ok := expr.Function.(*query.MissingFunction)
				if !ok {
					t.Fatalf("expected MissingFunction, got %T", expr.Function)
				}
				if missing.Attr.String() != ":entity/name" {
					t.Errorf("expected attr :entity/name, got %s", missing.Attr.String())
				}
				if expr.Binding != "?is_missing" {
					t.Errorf("expected binding ?is_missing, got %s", expr.Binding)
				}
			},
		},
		{
			name:        "missing? with wrong arg count",
			queryStr:    `[:find ?e :where [?e :entity/id _] [(missing? $ ?e)]]`,
			wantErr:     true,
			errContains: "missing? requires exactly 3 arguments",
		},
		{
			name:        "missing? without database ref",
			queryStr:    `[:find ?e :where [?e :entity/id _] [(missing? ?e :entity/name)]]`,
			wantErr:     true,
			errContains: "missing? requires exactly 3 arguments",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.queryStr)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.validate != nil {
				tt.validate(t, q)
			}
		})
	}
}

func TestParseGetSome(t *testing.T) {
	tests := []struct {
		name        string
		queryStr    string
		wantErr     bool
		errContains string
		validate    func(t *testing.T, q *query.Query)
	}{
		{
			name:     "get-some with two attributes",
			queryStr: `[:find ?e ?val :where [?e :entity/id _] [(get-some $ ?e :user/nickname :user/name) ?val]]`,
			wantErr:  false,
			validate: func(t *testing.T, q *query.Query) {
				if len(q.Where) != 2 {
					t.Fatalf("expected 2 where clauses, got %d", len(q.Where))
				}
				expr := q.Where[1].(*query.Expression)
				getSome, ok := expr.Function.(*query.GetSomeFunction)
				if !ok {
					t.Fatalf("expected GetSomeFunction, got %T", expr.Function)
				}
				if len(getSome.Attrs) != 2 {
					t.Errorf("expected 2 attrs, got %d", len(getSome.Attrs))
				}
				if getSome.Attrs[0].String() != ":user/nickname" {
					t.Errorf("expected first attr :user/nickname, got %s", getSome.Attrs[0].String())
				}
				if getSome.Attrs[1].String() != ":user/name" {
					t.Errorf("expected second attr :user/name, got %s", getSome.Attrs[1].String())
				}
			},
		},
		{
			name:     "get-some with three attributes",
			queryStr: `[:find ?e ?val :where [?e :entity/id _] [(get-some $ ?e :user/nickname :user/name :user/email) ?val]]`,
			wantErr:  false,
			validate: func(t *testing.T, q *query.Query) {
				expr := q.Where[1].(*query.Expression)
				getSome := expr.Function.(*query.GetSomeFunction)
				if len(getSome.Attrs) != 3 {
					t.Errorf("expected 3 attrs, got %d", len(getSome.Attrs))
				}
			},
		},
		{
			name:        "get-some with only one attribute (invalid - need at least 2 for fallback)",
			queryStr:    `[:find ?e ?val :where [?e :entity/id _] [(get-some $ ?e :user/name) ?val]]`,
			wantErr:     false, // Actually valid - at least 1 attr is fine
			validate: func(t *testing.T, q *query.Query) {
				expr := q.Where[1].(*query.Expression)
				getSome := expr.Function.(*query.GetSomeFunction)
				if len(getSome.Attrs) != 1 {
					t.Errorf("expected 1 attr, got %d", len(getSome.Attrs))
				}
			},
		},
		{
			name:        "get-some without attributes",
			queryStr:    `[:find ?e ?val :where [?e :entity/id _] [(get-some $ ?e) ?val]]`,
			wantErr:     true,
			errContains: "get-some requires at least 3 arguments",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.queryStr)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.validate != nil {
				tt.validate(t, q)
			}
		})
	}
}

func TestDatabaseFunctionRequiredSymbols(t *testing.T) {
	// Test that RequiredSymbols correctly identifies the entity variable
	getElse := &query.GetElseFunction{
		Entity:  query.VariableTerm{Symbol: "?e"},
		Attr:    datalog.NewKeyword(":entity/name"),
		Default: "",
	}
	syms := getElse.RequiredSymbols()
	if len(syms) != 1 || syms[0] != "?e" {
		t.Errorf("expected [?e], got %v", syms)
	}

	missing := &query.MissingFunction{
		Entity: query.VariableTerm{Symbol: "?person"},
		Attr:   datalog.NewKeyword(":person/email"),
	}
	syms = missing.RequiredSymbols()
	if len(syms) != 1 || syms[0] != "?person" {
		t.Errorf("expected [?person], got %v", syms)
	}

	getSome := &query.GetSomeFunction{
		Entity: query.VariableTerm{Symbol: "?user"},
		Attrs: []datalog.Keyword{
			datalog.NewKeyword(":user/nickname"),
			datalog.NewKeyword(":user/name"),
		},
	}
	syms = getSome.RequiredSymbols()
	if len(syms) != 1 || syms[0] != "?user" {
		t.Errorf("expected [?user], got %v", syms)
	}
}

func TestDatabaseFunctionString(t *testing.T) {
	// Test String() methods for debugging output
	getElse := &query.GetElseFunction{
		Entity:  query.VariableTerm{Symbol: "?e"},
		Attr:    datalog.NewKeyword(":entity/name"),
		Default: "Unknown",
	}
	str := getElse.String()
	if !strings.Contains(str, "get-else") || !strings.Contains(str, ":entity/name") {
		t.Errorf("unexpected String() output: %s", str)
	}

	missing := &query.MissingFunction{
		Entity: query.VariableTerm{Symbol: "?e"},
		Attr:   datalog.NewKeyword(":entity/email"),
	}
	str = missing.String()
	if !strings.Contains(str, "missing?") || !strings.Contains(str, ":entity/email") {
		t.Errorf("unexpected String() output: %s", str)
	}

	getSome := &query.GetSomeFunction{
		Entity: query.VariableTerm{Symbol: "?e"},
		Attrs: []datalog.Keyword{
			datalog.NewKeyword(":a"),
			datalog.NewKeyword(":b"),
		},
	}
	str = getSome.String()
	if !strings.Contains(str, "get-some") || !strings.Contains(str, ":a") || !strings.Contains(str, ":b") {
		t.Errorf("unexpected String() output: %s", str)
	}
}

