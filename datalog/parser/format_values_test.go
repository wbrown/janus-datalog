package parser

import (
	"reflect"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/codec"
)

// A query renders to text the parser reads back as the same query, a byte
// constant, an instant constant and a pattern's source among its parts, so
// every phase the planner emits parses back to itself
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §6).
func TestQueryStringRendersBytesInstantsAndTheSource(t *testing.T) {
	text := `[:find ?e
 :where [$src ?e :t/blob #bytes "` + codec.EncodeL85([]byte{1, 2}) + `"]
        [?e :t/at #inst "2024-06-15T10:30:00Z"]]`
	q, err := ParseQuery(text)
	if err != nil {
		t.Fatalf("ParseQuery(%q): %v", text, err)
	}
	rendered := q.String()
	again, err := ParseQuery(rendered)
	if err != nil {
		t.Fatalf("the rendering does not parse: %v\n%s", err, rendered)
	}
	if !reflect.DeepEqual(again.Where, q.Where) {
		t.Errorf("the rendering parses to %v, want the query it rendered, %v\n%s", again.Where, q.Where, rendered)
	}
	if second := again.String(); second != rendered {
		t.Errorf("the text changed on a second render:\n%s\n--\n%s", rendered, second)
	}
}

// A query renders to text the parser reads back as the same query whatever
// value type its constants hold: a whole-valued float keeps its point, a
// string is quoted at any depth of a vector, and a keyword, a boolean, an
// instant, a byte vector and an identity each render in the form the parser
// reads (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §6).
func TestQueryStringRoundTripsEveryValueType(t *testing.T) {
	blob := codec.EncodeL85([]byte{0xde, 0xad, 0xbe, 0xef, 0x01})
	cases := []struct{ name, text string }{
		{"a whole-valued float", `[:find ?e :where [?e :t/a 5.0]]`},
		{"a negative whole-valued float", `[:find ?e :where [?e :t/a -3.0]]`},
		{"a fractional float", `[:find ?e :where [?e :t/a 1.5] [?e :t/b -2.25]]`},
		{"a float with an exponent", `[:find ?e :where [?e :t/a 3.125e10]]`},
		{"zero as a float", `[:find ?e :where [?e :t/a 0.0]]`},
		{"an integer", `[:find ?e :where [?e :t/a 5]]`},
		{"a string vector ground", `[:find ?x :where [?e :t/a _] [(ground ["1" "2"]) [?x ...]]]`},
		{"a nested vector ground", `[:find ?x ?y :where [?e :t/a _] [(ground [[1 "a"] [2 "b"]]) [[?x ?y]]]]`},
		{"a float in a ground", `[:find ?x :where [?e :t/a _] [(ground 5.0) ?x]]`},
		{"a float in a predicate", `[:find ?e :where [?e :t/a ?v] [(> ?v 2.0)]]`},
		{"a keyword, a boolean and a string", `[:find ?e :where [?e :t/k :t/p] [?e :t/b true] [?e :t/s "quoted \"inside\""]]`},
		{"an identity", `[:find ?e :where [?e :t/ref #identity "` + datalogIdentityText("e1") + `"]]`},
		{"a byte vector in a ground", `[:find ?x :where [(ground #bytes "` + blob + `") ?w] [?x :t/v ?w]]`},
		{"a byte vector in a predicate", `[:find ?x :where [?x :t/v ?w] [(= ?w #bytes "` + blob + `")]]`},
		{"a byte vector as a get-else default", `[:find ?x ?g :where [?x :t/n _] [(get-else $ ?x :t/missing #bytes "` + blob + `") ?g]]`},
		{"byte vectors in a collection ground", `[:find ?x :where [(ground [#bytes "` + blob + `" #bytes "` + codec.EncodeL85([]byte{3}) + `"]) [?w ...]] [?x :t/v ?w]]`},
		{"a byte vector in a tuple ground beside a string", `[:find ?x :where [(ground [#bytes "` + blob + `" "one"]) [?w ?n]] [?x :t/v ?w] [?x :t/n ?n]]`},
		{"a byte vector as an invocation argument", `[:find ?x :in $ % :where [?x :t/n _] (r ?x #bytes "` + blob + `")]`},
		{"a byte vector under a not", `[:find ?x :where [?x :t/n _] (not [?x :t/v #bytes "` + blob + `"])]`},
		{"an empty byte vector", `[:find ?x :where [?x :t/v #bytes ""]]`},
		{"an infinite float", `[:find ?e :where [?e :t/a ##Inf] [?e :t/b ##-Inf]]`},
		{"an infinite float in a predicate", `[:find ?e :where [?e :t/a ?v] [(< ?v ##Inf)]]`},
		{"a transaction value at the transaction position", `[:find ?e :where [?e :t/a ?v #eid [2 1]]]`},
		{"a transaction value in a ground", `[:find ?x :where [?e :t/a _] [(ground #eid [5 1]) ?x]]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, err := ParseQuery(tc.text)
			if err != nil {
				t.Fatalf("ParseQuery(%q): %v", tc.text, err)
			}
			rendered := q.String()
			again, err := ParseQuery(rendered)
			if err != nil {
				t.Fatalf("the rendering does not parse: %v\n%s", err, rendered)
			}
			if !reflect.DeepEqual(again.Where, q.Where) {
				t.Errorf("the rendering parses to %v, want the query it rendered, %v\n%s", again.Where, q.Where, rendered)
			}
			if second := again.String(); second != rendered {
				t.Errorf("the text changed on a second render:\n%s\n--\n%s", rendered, second)
			}
		})
	}
}

// A pull default renders in the form the parser reads a value by, so a query
// pulling with a default parses back as the same query whatever the default
// holds: a string, one with a space or of digits, nil, a whole-valued float,
// an integer, a keyword, a boolean, and the tagged literals a pattern
// constant takes.
func TestQueryStringRoundTripsPullDefaults(t *testing.T) {
	cases := []struct{ name, text string }{
		{"a string", `[:find (pull ?e [(default :t/b "unknown")]) :where [?e :t/a _]]`},
		{"a string with a space", `[:find (pull ?e [(default :t/b "a b")]) :where [?e :t/a _]]`},
		{"a string of digits", `[:find (pull ?e [(default :t/b "1")]) :where [?e :t/a _]]`},
		{"nil", `[:find (pull ?e [(default :t/b nil)]) :where [?e :t/a _]]`},
		{"a whole-valued float", `[:find (pull ?e [(default :t/b 1.0)]) :where [?e :t/a _]]`},
		{"an integer", `[:find (pull ?e [(default :t/b 1)]) :where [?e :t/a _]]`},
		{"a keyword", `[:find (pull ?e [(default :t/b :k/w)]) :where [?e :t/a _]]`},
		{"a boolean", `[:find (pull ?e [(default :t/b true)]) :where [?e :t/a _]]`},
		{"an instant", `[:find (pull ?e [(default :t/b #inst "2024-06-15T10:30:00Z")]) :where [?e :t/a _]]`},
		{"a transaction value", `[:find (pull ?e [(default :t/b #eid [5 1])]) :where [?e :t/a _]]`},
		{"an identity", `[:find (pull ?e [(default :t/b #identity "` + datalogIdentityText("e1") + `")]) :where [?e :t/a _]]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, err := ParseQuery(tc.text)
			if err != nil {
				t.Fatalf("ParseQuery(%q): %v", tc.text, err)
			}
			rendered := q.String()
			again, err := ParseQuery(rendered)
			if err != nil {
				t.Fatalf("the rendering does not parse: %v\n%s", err, rendered)
			}
			if !reflect.DeepEqual(again.Find, q.Find) {
				t.Errorf("the rendering parses to %v, want the query it rendered, %v\n%s", again.Find, q.Find, rendered)
			}
			if second := again.String(); second != rendered {
				t.Errorf("the text changed on a second render:\n%s\n--\n%s", rendered, second)
			}
		})
	}
}

// datalogIdentityText is the L85 text of the identity minted from seed, the
// form #identity reads.
func datalogIdentityText(seed string) string {
	return datalog.NewIdentity(seed).String()
}

// A database function predicate, [(missing? $ ?e :t/c)], and a transaction
// range predicate, [(tx-between ?tx 1 5)], render as the bracketed clauses
// the parser reads a predicate by, so their text parses back as the same
// predicate and never as a rule invocation.
func TestQueryStringRendersDatabaseAndHistoryPredicatesAsClauses(t *testing.T) {
	cases := []struct{ name, text string }{
		{"missing?", `[:find ?e :where [?e :t/a _] [(missing? $ ?e :t/c)]]`},
		{"tx-between", `[:find ?e :where [?e :t/a _ ?tx] [(tx-between ?tx 1 5)]]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, err := ParseQuery(tc.text)
			if err != nil {
				t.Fatalf("ParseQuery(%q): %v", tc.text, err)
			}
			rendered := q.String()
			again, err := ParseQuery(rendered)
			if err != nil {
				t.Fatalf("the rendering does not parse: %v\n%s", err, rendered)
			}
			if !reflect.DeepEqual(again.Where, q.Where) {
				t.Errorf("the rendering parses to %v, want the query it rendered, %v\n%s", again.Where, q.Where, rendered)
			}
		})
	}
}

// A scalar find, [:find ?n . …], renders with its marker, so its text parses
// back as a scalar query and renders the same again.
func TestQueryStringRendersTheScalarFindMarker(t *testing.T) {
	q, err := ParseQuery(`[:find ?n . :where [?e :t/name ?n]]`)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	if !q.ScalarReturn {
		t.Fatalf("the parser read no scalar marker from the text")
	}
	rendered := q.String()
	again, err := ParseQuery(rendered)
	if err != nil {
		t.Fatalf("the rendering does not parse: %v\n%s", err, rendered)
	}
	if !again.ScalarReturn {
		t.Errorf("the rendering parses as a set query, want the scalar marker kept:\n%s", rendered)
	}
	if second := again.String(); second != rendered {
		t.Errorf("the text changed on a second render:\n%s\n--\n%s", rendered, second)
	}
}
