package parser

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// An or-join header declares the clause's whole interface: an optional
// nested vector of required variables first, bound by the enclosing query
// before the clause runs, then the output variables every branch binds. A
// flat header declares outputs only.

func TestParseQuery_OrJoinRequiredHeader(t *testing.T) {
	q, err := ParseQuery(`[:find ?e :in $ ?a :where [?e :t/n _] (or-join [[?a] ?e] [?e ?a ?v] [?e :t/tag :t/none])]`)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	oj, ok := q.Where[1].(*query.OrJoinClause)
	if !ok {
		t.Fatalf("clause 1 = %T, want an or-join", q.Where[1])
	}
	if got, want := symbolNames(oj.RequiredVars), "?a"; got != want {
		t.Errorf("RequiredVars = %s, want %s", got, want)
	}
	if got, want := symbolNames(oj.OutputVars), "?e"; got != want {
		t.Errorf("JoinVars = %s, want %s", got, want)
	}

	_, err = ParseQuery(`[:find ?e :in $ ?a :where [?e :t/n _] (or-join [?e [?a]] [?e ?a ?v] [?e :t/tag :t/none])]`)
	if err == nil {
		t.Errorf("a header with the required vector after an output parses; the required vector is the header's first element only")
	}

	q, err = ParseQuery(`[:find ?e :where [?e :t/n _] (or-join [?e] [?e :t/tag _] [?e :t/other _])]`)
	if err != nil {
		t.Fatalf("ParseQuery of a flat header: %v", err)
	}
	oj = q.Where[1].(*query.OrJoinClause)
	if len(oj.RequiredVars) != 0 || symbolNames(oj.OutputVars) != "?e" {
		t.Errorf("a flat header [?e] parses as required %v, outputs %v; it declares outputs only", oj.RequiredVars, oj.OutputVars)
	}

	for _, refused := range []struct{ name, text string }{
		{"an empty header", `[:find ?e :where [?e :t/n _] (or-join [] [?e :t/tag _] [?e :t/other _])]`},
		{"a non-symbol output", `[:find ?e :where [?e :t/n _] (or-join [?e :t/tag] [?e :t/tag _] [?e :t/other _])]`},
		{"an output not starting with ?", `[:find ?e :where [?e :t/n _] (or-join [?e e] [?e :t/tag _] [?e :t/other _])]`},
	} {
		if _, err := ParseQuery(refused.text); err == nil {
			t.Errorf("%s parses; the header is refused", refused.name)
		}
	}
}

func TestQueryString_RoundTripsOrJoinRequiredHeader(t *testing.T) {
	text := `[:find ?e :in $ ?a :where [?e :t/n _] (or-join [[?a] ?e] [?e ?a ?v] [?e :t/tag :t/none])]`
	q, err := ParseQuery(text)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	reparsed, err := ParseQuery(q.String())
	if err != nil {
		t.Fatalf("ParseQuery of the formatted query: %v\n%s", err, q.String())
	}
	if got, want := reparsed.Where[1].String(), q.Where[1].String(); got != want {
		t.Errorf("the formatted or-join reparses as %s, want %s", got, want)
	}
}

// symbolNames joins the symbols' names with spaces.
func symbolNames(symbols []query.Symbol) string {
	out := ""
	for i, s := range symbols {
		if i > 0 {
			out += " "
		}
		out += s.String()
	}
	return out
}
