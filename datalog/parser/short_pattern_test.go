package parser

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// A data pattern of one or two elements parses to a pattern of that count,
// with or without a source named, and its rendering reads back as the same
// query.
func TestParseQueryReadsAShortDataPattern(t *testing.T) {
	cases := []struct {
		text     string
		elements int
	}{
		{`[:find ?e :where [?e :t/n]]`, 2},
		{`[:find ?e :where [?e]]`, 1},
		{`[:find ?e :where [$ ?e :t/n]]`, 2},
	}
	for _, c := range cases {
		t.Run(c.text, func(t *testing.T) {
			q, err := ParseQuery(c.text)
			if err != nil {
				t.Fatalf("ParseQuery refuses the short pattern: %v", err)
			}
			pattern, ok := q.Where[0].(*query.DataPattern)
			if !ok {
				t.Fatalf("the clause parses to %T, want a data pattern", q.Where[0])
			}
			if len(pattern.Elements) != c.elements {
				t.Errorf("the pattern holds %d elements, want %d: %s", len(pattern.Elements), c.elements, pattern)
			}
			if failure := roundTrip(q); failure != "" {
				t.Error(failure)
			}
		})
	}
}
