package qb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A data pattern of one or two elements builds, and renders as written: the
// positions it omits are unbound.
func TestBuild_AcceptsAShortDataPattern(t *testing.T) {
	e := NewVar("e")
	attr := Kw(":t/n")
	cases := []struct {
		name     string
		pattern  *Pattern
		rendered string
	}{
		{"the value omitted", Pat(e, attr), "[?e :t/n]"},
		{"the attribute and the value omitted", Pat(e), "[?e]"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q, err := Query().Find(e).Where(c.pattern).Build()
			require.NoError(t, err)
			pattern, ok := q.Where[0].(*query.DataPattern)
			require.True(t, ok, "the clause builds to %T, want a data pattern", q.Where[0])
			require.Equal(t, c.rendered, pattern.String())
		})
	}
}
