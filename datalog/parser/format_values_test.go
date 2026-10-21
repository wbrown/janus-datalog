package parser

import (
	"reflect"
	"testing"

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
