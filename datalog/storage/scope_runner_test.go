package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// A nested form is a scope run once over its required run, and its result
// joins the enclosing scope on the required run and the outputs. The worked
// examples of the design's §2.3 answer the same rows in every written order
// of the query's clauses, on both planner paths, and a relation group a form
// does not read stands as it is until the find boundary, where an empty one
// empties the result.

// fact is one attribute and value of a named entity; a ref value names
// another entity.
type fact struct {
	attr  string
	value interface{}
}

// ref names another entity among an entity's facts.
type ref string

// namedEntities commits each entity's facts under its name, a ref value as
// that entity's identity, and returns the names by identity for rendering.
func namedEntities(t testing.TB, d *Database, entities map[string][]fact) map[datalog.Identity]string {
	t.Helper()
	names := map[datalog.Identity]string{}
	tx := d.NewTransaction()
	for name, facts := range entities {
		id := datalog.NewIdentity(name)
		names[id] = name
		for _, f := range facts {
			value := f.value
			if r, ok := value.(ref); ok {
				value = datalog.NewIdentity(string(r))
			}
			require.NoError(t, tx.Add(id, kw(f.attr), value))
		}
	}
	_, err := tx.Commit()
	require.NoError(t, err)
	return names
}

// everyOrder runs the query finding find over every ordering of clauses on d
// and asserts that each ordering answers want.
func everyOrder(t *testing.T, d *Database, names map[datalog.Identity]string, find string, clauses []string, want []string) {
	t.Helper()
	for _, order := range orderings(len(clauses)) {
		ordered := make([]string, len(order))
		for i, at := range order {
			ordered[i] = clauses[at]
		}
		text := "[:find " + find + " :where " + strings.Join(ordered, " ") + "]"
		rel, err := d.Query(text)
		require.NoError(t, err, text)
		require.Equal(t, want, renderedRows(t, rel, names), text)
	}
}

// The third example of §2.3: an output another clause binds is joined, never
// superseded. ?e is an output and ?y is required, so for each ?y the form
// answers the ?e values the first branch with a result produces, with ?e
// free, and the enclosing scope keeps a tuple when its ?e is among them. The
// first branch, every entity carrying :p, has a result for both ?y, so n2,
// which carries :p, is kept and n3, which does not, is dropped, though y2
// reaches n2 through the second branch's attribute.
func TestOrDefaultOutputBoundByAnEarlierClauseIsJoined(t *testing.T) {
	entities := map[string][]fact{
		"n1": {{":p", ref("n2")}},
		"n2": {{":p", ref("n3")}},
		"n3": {{":t/n", int64(3)}},
		"y1": {{":q", ref("n2")}},
		"y2": {{":q", ref("n3")}, {":r", ref("n2")}},
	}
	clauses := []string{`[?w :p ?e]`, `[?y :q ?e]`, `(or-default [?e :p ?m] (and [?m :p ?e] [?y ?a ?m]))`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e", clauses, []string{"n2"})
		})
	}
}

// Two or-defaults over one required variable, each binding the same output,
// are each decided per ?e with ?c free and joined with the enclosing scope on
// ?c: an entity is kept when both forms answer the same ?c, its :t/a and
// :t/b alike, or neither attribute and the default twice.
func TestTwoFormsSharingAnOutputAreJoined(t *testing.T) {
	entities := map[string][]fact{
		"e0": {{":t/n", int64(0)}, {":t/a", int64(1)}, {":t/b", int64(1)}},
		"e1": {{":t/n", int64(1)}, {":t/a", int64(2)}},
		"e2": {{":t/n", int64(2)}},
		"e3": {{":t/n", int64(3)}, {":t/a", int64(3)}, {":t/b", int64(4)}},
	}
	clauses := []string{
		`[?e :t/n _]`,
		`(or-default [?e :t/a ?c] [(ground 0) ?c])`,
		`(or-default [?e :t/b ?c] [(ground 0) ?c])`,
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?c", clauses, []string{"e0 1", "e2 0"})
		})
	}
}

// A relation group a form does not read stands as it is until the find
// boundary: a pattern sharing no variable with the rest and matching nothing
// empties the result, written before an or-default, a not or an or, and one
// matching something leaves the find group's rows as they are.
func TestScopeKeepsTheGroupsItDoesNotRead(t *testing.T) {
	entities := map[string][]fact{
		"e0": {{":t/n", int64(0)}, {":t/a", int64(1)}},
		"e1": {{":t/n", int64(1)}},
	}
	forms := []struct {
		name, form string
		rows       []string
	}{
		{"or-default", `(or-default [?e :t/a ?c] [(ground 0) ?c])`, []string{"e0", "e1"}},
		{"not", `(not [?e :t/a _])`, []string{"e1"}},
		{"or", `(or [?e :t/a _] [?e :t/n 1])`, []string{"e0", "e1"}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			for _, f := range forms {
				empty := "[:find ?e :where [?e :t/n _] [?z :t/absent _] " + f.form + "]"
				rel, err := d.Query(empty)
				require.NoError(t, err, empty)
				require.Empty(t, renderedRows(t, rel, names), "%s: the unread group matches nothing", empty)
				some := "[:find ?e :where [?e :t/n _] [?z :t/n _] " + f.form + "]"
				rel, err = d.Query(some)
				require.NoError(t, err, some)
				require.Equal(t, f.rows, renderedRows(t, rel, names), "%s: the unread group matches something", some)
			}
		})
	}
}

// The first example of §2.3, the count with a zero default: both branches
// bind ?count, so it is an output, and the subquery reads ?c, so ?c is
// required, and each category gets its count or zero.
func TestOrDefaultZeroCountGivesEachGroupItsCountOrZero(t *testing.T) {
	entities := map[string][]fact{
		"c1": {{":category/name", "books"}},
		"c2": {{":category/name", "tools"}},
		"p1": {{":product/category", ref("c1")}},
		"p2": {{":product/category", ref("c1")}},
	}
	clauses := []string{
		`[?c :category/name ?category]`,
		`(or-default [(q [:find (count ?p) :in $ ?c :where [?p :product/category ?c]] $ ?c) [[?count]]] [(ground 0) ?count])`,
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?category ?count", clauses, []string{"books 2", "tools 0"})
		})
	}
}

// The fourth example of §2.3: a not provides nothing, so ?e is required, and
// ?x, which occurs only inside the form, is the body's own, so the form holds
// for an entity carrying no :q at all.
func TestNotWithABranchLocalKeepsAnEntityWithoutTheAttribute(t *testing.T) {
	entities := map[string][]fact{
		"e0": {{":p", int64(1)}},
		"e1": {{":p", int64(1)}, {":q", int64(2)}},
	}
	clauses := []string{`[?e :p _]`, `(not [?e :q ?x])`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e", clauses, []string{"e0"})
		})
	}
}
