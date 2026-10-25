package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// A nested form is a scope run once over its required run, and its result
// joins the enclosing scope on the required run and the outputs. The worked
// examples of the design's §2.3 answer the same tuples in every written order
// of the query's clauses, on both planner paths, and a relation group a form
// does not read stands as it is until the find boundary, where an empty one
// empties the result.

// namedEntity is an entity a fixture names: its identity, minted from the
// name once, which every fact about the entity and every ref to it carries,
// and the name a rendered tuple shows the identity by.
type namedEntity struct {
	id   datalog.Identity
	name string
}

// named mints the entity called name.
func named(name string) namedEntity {
	return namedEntity{id: datalog.NewIdentity(name), name: name}
}

// fact is one datom a fixture commits: the entity it is about, its
// attribute, and its value. A namedEntity value is a ref to that entity and
// is committed as its identity.
type fact struct {
	of    namedEntity
	attr  string
	value interface{}
}

// commitFacts commits the facts in the order written and returns the name of
// every entity they mention, by identity, for rendering.
func commitFacts(t testing.TB, d *Database, facts []fact) map[datalog.Identity]string {
	t.Helper()
	names := map[datalog.Identity]string{}
	tx := d.NewTransaction()
	for _, f := range facts {
		names[f.of.id] = f.of.name
		value := f.value
		if r, ok := value.(namedEntity); ok {
			names[r.id] = r.name
			value = r.id
		}
		require.NoError(t, tx.Add(f.of.id, kw(f.attr), value))
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
		require.Equal(t, want, renderedTuples(t, rel, names), text)
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
	n1, n2, n3, y1, y2 := named("n1"), named("n2"), named("n3"), named("y1"), named("y2")
	facts := []fact{
		{n1, ":p", n2},
		{n2, ":p", n3},
		{n3, ":t/n", int64(3)},
		{y1, ":q", n2},
		{y2, ":q", n3},
		{y2, ":r", n2},
	}
	clauses := []string{`[?w :p ?e]`, `[?y :q ?e]`, `(or-default [?e :p ?m] (and [?m :p ?e] [?y ?a ?m]))`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, facts)
			everyOrder(t, d, names, "?e", clauses, []string{"n2"})
		})
	}
}

// Two or-defaults over one required variable, each binding the same output,
// are each decided per ?e with ?c free and joined with the enclosing scope on
// ?c: an entity is kept when both forms answer the same ?c, its :t/a and
// :t/b alike, or neither attribute and the default twice.
func TestTwoFormsSharingAnOutputAreJoined(t *testing.T) {
	e0, e1, e2, e3 := named("e0"), named("e1"), named("e2"), named("e3")
	facts := []fact{
		{e0, ":t/n", int64(0)}, {e0, ":t/a", int64(1)}, {e0, ":t/b", int64(1)},
		{e1, ":t/n", int64(1)}, {e1, ":t/a", int64(2)},
		{e2, ":t/n", int64(2)},
		{e3, ":t/n", int64(3)}, {e3, ":t/a", int64(3)}, {e3, ":t/b", int64(4)},
	}
	clauses := []string{
		`[?e :t/n _]`,
		`(or-default [?e :t/a ?c] [(ground 0) ?c])`,
		`(or-default [?e :t/b ?c] [(ground 0) ?c])`,
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, facts)
			everyOrder(t, d, names, "?e ?c", clauses, []string{"e0 1", "e2 0"})
		})
	}
}

// A relation group a form does not read stands as it is until the find
// boundary: a pattern sharing no variable with the rest and matching nothing
// empties the result, written before an or-default, a not or an or, and one
// matching something leaves the find group's tuples as they are.
func TestScopeKeepsTheGroupsItDoesNotRead(t *testing.T) {
	e0, e1 := named("e0"), named("e1")
	facts := []fact{
		{e0, ":t/n", int64(0)}, {e0, ":t/a", int64(1)},
		{e1, ":t/n", int64(1)},
	}
	forms := []struct {
		name, form string
		tuples     []string
	}{
		{"or-default", `(or-default [?e :t/a ?c] [(ground 0) ?c])`, []string{"e0", "e1"}},
		{"not", `(not [?e :t/a _])`, []string{"e1"}},
		{"or", `(or [?e :t/a _] [?e :t/n 1])`, []string{"e0", "e1"}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, facts)
			for _, f := range forms {
				empty := "[:find ?e :where [?e :t/n _] [?z :t/absent _] " + f.form + "]"
				rel, err := d.Query(empty)
				require.NoError(t, err, empty)
				require.Empty(t, renderedTuples(t, rel, names), "%s: the unread group matches nothing", empty)
				some := "[:find ?e :where [?e :t/n _] [?z :t/n _] " + f.form + "]"
				rel, err = d.Query(some)
				require.NoError(t, err, some)
				require.Equal(t, f.tuples, renderedTuples(t, rel, names), "%s: the unread group matches something", some)
			}
		})
	}
}

// The first example of §2.3, the count with a zero default: both branches
// bind ?count, so it is an output, and the subquery reads ?c, so ?c is
// required, and each category gets its count or zero.
func TestOrDefaultZeroCountGivesEachGroupItsCountOrZero(t *testing.T) {
	c1, c2, p1, p2 := named("c1"), named("c2"), named("p1"), named("p2")
	facts := []fact{
		{c1, ":category/name", "books"},
		{c2, ":category/name", "tools"},
		{p1, ":product/category", c1},
		{p2, ":product/category", c1},
	}
	clauses := []string{
		`[?c :category/name ?category]`,
		`(or-default [(q [:find (count ?p) :in $ ?c :where [?p :product/category ?c]] $ ?c) [[?count]]] [(ground 0) ?count])`,
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, facts)
			everyOrder(t, d, names, "?category ?count", clauses, []string{"books 2", "tools 0"})
		})
	}
}

// The fourth example of §2.3: a not provides nothing, so ?e is required, and
// ?x, which occurs only inside the form, is the body's own, so the form holds
// for an entity carrying no :q at all.
func TestNotWithABranchLocalKeepsAnEntityWithoutTheAttribute(t *testing.T) {
	e0, e1 := named("e0"), named("e1")
	facts := []fact{
		{e0, ":p", int64(1)},
		{e1, ":p", int64(1)}, {e1, ":q", int64(2)},
	}
	clauses := []string{`[?e :p _]`, `(not [?e :q ?x])`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, facts)
			everyOrder(t, d, names, "?e", clauses, []string{"e0"})
		})
	}
}
