package qb

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// rendersAsTheParserReads asserts that q's text parses back as q: the same
// text again, and the same query part for part, constants compared as
// values.
func rendersAsTheParserReads(t *testing.T, q *query.Query) {
	t.Helper()
	rendered := q.String()
	again, err := parser.ParseQuery(rendered)
	require.NoError(t, err, "the rendering parses:\n%s", rendered)
	require.Equal(t, rendered, again.String(), "the text is the same on a second render")
	require.True(t, q.Equal(again), "the parsed query is the built one:\n%s", rendered)
}

// Every query the builder accepts renders to text the parser reads back as
// the same query, whatever value a ground, a pattern constant or a pull
// default holds: an infinity, an unsigned integer, a transaction value by
// value or by pointer, a whole-valued float and a string of digits or with a
// space. A value outside the domain is refused at Build naming its type, or
// the value for an unsigned integer above int64's range.
func TestBuild_AdmitsTheDomainsValuesAndRendersThemAsTheParserReads(t *testing.T) {
	e, x := NewVar("e"), NewVar("x")
	attr := Kw(":t/a")
	values := []struct {
		name  string
		value interface{}
	}{
		{"positive infinity", math.Inf(1)},
		{"negative infinity", math.Inf(-1)},
		{"an unsigned integer", uint64(7)},
		{"a transaction value", datalog.ElementID{Lamport: 5, ReplicaID: 1}},
		{"a transaction value by pointer", &datalog.ElementID{Lamport: 2, ReplicaID: 1}},
		{"a whole-valued float", 1.0},
		{"a string of digits", "1"},
		{"a string with a space", "a b"},
		{"a keyword", datalog.NewKeyword(":k/w")},
	}
	for _, c := range values {
		t.Run(c.name+" as a ground", func(t *testing.T) {
			q, err := Query().Find(x).Where(Pat(e, attr, Blank()), Ground(c.value).As(x)).Build()
			require.NoError(t, err)
			rendersAsTheParserReads(t, q)
		})
		t.Run(c.name+" as a pattern constant", func(t *testing.T) {
			q, err := Query().Find(e).Where(Pat(e, attr, V(c.value))).Build()
			require.NoError(t, err)
			rendersAsTheParserReads(t, q)
		})
		t.Run(c.name+" as a pull default", func(t *testing.T) {
			q, err := Query().Find(Pull(e, Default(attr, c.value))).Where(Pat(e, attr, Blank())).Build()
			require.NoError(t, err)
			rendersAsTheParserReads(t, q)
		})
	}

	t.Run("a transaction value at the transaction position", func(t *testing.T) {
		q, err := Query().Find(e).Where(Pat(e, attr, x, V(&datalog.ElementID{Lamport: 2, ReplicaID: 1}))).Build()
		require.NoError(t, err)
		rendersAsTheParserReads(t, q)
	})

	refusals := []struct {
		name  string
		value interface{}
		names string
	}{
		{"a float32", float32(2), "float32"},
		{"a map", map[string]interface{}{"a": int64(1)}, "map[string]interface {}"},
		{"an unsigned integer above int64's range", uint64(math.MaxInt64) + 1, "9223372036854775808"},
	}
	for _, c := range refusals {
		t.Run(c.name+" is refused as a ground", func(t *testing.T) {
			_, err := Query().Find(x).Where(Pat(e, attr, Blank()), Ground(c.value).As(x)).Build()
			require.ErrorContains(t, err, c.names)
		})
		t.Run(c.name+" is refused as a pattern constant", func(t *testing.T) {
			_, err := Query().Find(e).Where(Pat(e, attr, V(c.value))).Build()
			require.ErrorContains(t, err, c.names)
		})
		t.Run(c.name+" is refused as a pull default", func(t *testing.T) {
			_, err := Query().Find(Pull(e, Default(attr, c.value))).Where(Pat(e, attr, Blank())).Build()
			require.ErrorContains(t, err, c.names)
		})
	}
}

// A keyword with a space or with no name is refused where the keyword is
// made, so no query ever holds one.
func TestKw_RefusesAKeywordWithASpaceOrNoName(t *testing.T) {
	require.Panics(t, func() { Kw(":a b") })
	require.Panics(t, func() { Kw(":") })
}
