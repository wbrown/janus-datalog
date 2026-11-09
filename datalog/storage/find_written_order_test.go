package storage

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A query's result is the relation over its :find's elements in written
// order, each aggregate under its display symbol, and a subquery's binding
// names its inner :find's elements by position, on both optimizer paths
// (NESTED_SCOPES_AS_QUERIES.md §2.5).

// writtenOrderFacts is six entities, each holding an :o/k, an :o/v and an
// :o/w. The :o/k values repeat, so a group of one :o/k holds several
// entities, and within a group the :o/v values repeat too.
func writtenOrderFacts() []fact {
	o1, o2, o3, o4, o5, o6 := named("o1"), named("o2"), named("o3"), named("o4"), named("o5"), named("o6")
	return []fact{
		{o1, ":o/k", int64(1)}, {o1, ":o/v", int64(1)}, {o1, ":o/w", int64(10)},
		{o2, ":o/k", int64(1)}, {o2, ":o/v", int64(2)}, {o2, ":o/w", int64(20)},
		{o3, ":o/k", int64(2)}, {o3, ":o/v", int64(2)}, {o3, ":o/w", int64(30)},
		{o4, ":o/k", int64(2)}, {o4, ":o/v", int64(2)}, {o4, ":o/w", int64(40)},
		{o5, ":o/k", int64(3)}, {o5, ":o/v", int64(3)}, {o5, ":o/w", int64(50)},
		{o6, ":o/k", int64(1)}, {o6, ":o/v", int64(1)}, {o6, ":o/w", int64(60)},
	}
}

// writtenOrderEntity is one fixture entity: its name and its three values.
type writtenOrderEntity struct {
	name    string
	k, v, w int64
}

// writtenOrderEntities reads the fixture into one record per entity, in the
// order the fixture first names each.
func writtenOrderEntities() []writtenOrderEntity {
	index := map[string]int{}
	var entities []writtenOrderEntity
	for _, f := range writtenOrderFacts() {
		at, seen := index[f.of.name]
		if !seen {
			at = len(entities)
			index[f.of.name] = at
			entities = append(entities, writtenOrderEntity{name: f.of.name})
		}
		value := f.value.(int64)
		switch f.attr {
		case ":o/k":
			entities[at].k = value
		case ":o/v":
			entities[at].v = value
		case ":o/w":
			entities[at].w = value
		}
	}
	return entities
}

// Under every ordering of a :find holding a group variable and two
// aggregates, the result carries the elements' symbols in the order written
// and each tuple's values in that order, and over no matching datom it is the
// empty relation over the same symbols.
func TestAnAggregatingFindAnswersItsElementsInWrittenOrder(t *testing.T) {
	type group struct{ k, count, maxV int64 }
	byKey := map[int64]*group{}
	for _, e := range writtenOrderEntities() {
		g, seen := byKey[e.k]
		if !seen {
			g = &group{k: e.k, maxV: e.v}
			byKey[e.k] = g
		}
		g.count++
		if e.v > g.maxV {
			g.maxV = e.v
		}
	}
	elements := []struct {
		text  string
		value func(group) int64
	}{
		{"?k", func(g group) int64 { return g.k }},
		{"(count ?e)", func(g group) int64 { return g.count }},
		{"(max ?v)", func(g group) int64 { return g.maxV }},
	}
	for _, mode := range legsFor(optimizerModes, fusionOption, aggregationOption) {
		d := createOptimizerModeDB(t, mode, DatabaseOptions{})
		commitFacts(t, d, writtenOrderFacts())
		for _, order := range orderings(len(elements)) {
			texts := make([]string, len(order))
			symbols := make([]query.Symbol, len(order))
			for i, at := range order {
				texts[i] = elements[at].text
				symbols[i] = datalog.NewSymbol(elements[at].text)
			}
			var want []string
			for _, g := range byKey {
				parts := make([]string, len(order))
				for i, at := range order {
					parts[i] = fmt.Sprint(elements[at].value(*g))
				}
				want = append(want, strings.Join(parts, " "))
			}
			sort.Strings(want)
			find := strings.Join(texts, " ")
			t.Run(mode.name+"/"+find, func(t *testing.T) {
				text := "[:find " + find + " :where [?e :o/k ?k] [?e :o/v ?v]]"
				rel, err := d.Query(text)
				require.NoError(t, err, text)
				require.Equal(t, symbols, rel.Symbols(), "the result carries the find's elements in written order")
				require.Equal(t, want, renderedTuples(t, rel, nil), "each tuple holds its values in the find's written order")

				empty := "[:find " + find + " :where [?e :o/none ?k] [?e :o/v ?v]]"
				rel, err = d.Query(empty)
				require.NoError(t, err, empty)
				require.Equal(t, symbols, rel.Symbols(), "over no matching datom the result is the empty relation over the written symbols")
				require.Empty(t, renderedTuples(t, rel, nil))
			})
		}
	}
}

// QueryInto fills each tagged field with the element its tag names, and each
// untagged field with the element written at its place, for a :find writing
// an aggregate before a group variable.
func TestQueryIntoMapsAnAggregateFirstFindByTagAndByPosition(t *testing.T) {
	counts := map[int64]int64{}
	for _, e := range writtenOrderEntities() {
		counts[e.k]++
	}
	type tagged struct {
		N int64 `datalog:"(count ?e)"`
		K int64 `datalog:"?k"`
	}
	type positional struct {
		N int64
		K int64
	}
	text := `[:find (count ?e) ?k :where [?e :o/k ?k]]`
	for _, mode := range legsFor(optimizerModes, aggregationOption) {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			commitFacts(t, d, writtenOrderFacts())

			var byTag []tagged
			require.NoError(t, d.QueryInto(&byTag, text))
			gotByTag := map[int64]int64{}
			for _, r := range byTag {
				gotByTag[r.K] = r.N
			}
			require.Equal(t, counts, gotByTag, "each tagged field holds the element its tag names")

			var byPosition []positional
			require.NoError(t, d.QueryInto(&byPosition, text))
			gotByPosition := map[int64]int64{}
			for _, r := range byPosition {
				gotByPosition[r.K] = r.N
			}
			require.Equal(t, counts, gotByPosition, "each untagged field holds the element written at its place")
		})
	}
}

// A subquery answers alike under every ordering of its inner :find written
// with its binding in the same order, and the answer is the reference
// computed from the facts: one parameter, two parameters, a subquery nested
// in a subquery, and a subquery under an or-default-join. The outer :find
// names the binding's symbols in one fixed order, so a binding that names an
// element other than the one written at its place answers other tuples.
func TestASubqueryAnswersAlikeUnderAPermutationOfItsFindAndBinding(t *testing.T) {
	entities := writtenOrderEntities()

	// Per outer entity q, the entities sharing q's :o/k grouped by :o/v: the
	// group's :o/v, its entity count and the sum of its :o/w.
	var byKeyGroupedByV []string
	for _, q := range entities {
		type group struct{ count, sum int64 }
		groups := map[int64]*group{}
		var order []int64
		for _, z := range entities {
			if z.k != q.k {
				continue
			}
			g, seen := groups[z.v]
			if !seen {
				g = &group{}
				groups[z.v] = g
				order = append(order, z.v)
			}
			g.count++
			g.sum += z.w
		}
		for _, v := range order {
			byKeyGroupedByV = append(byKeyGroupedByV, fmt.Sprintf("%s %d %d %d", q.name, v, groups[v].count, groups[v].sum))
		}
	}
	sort.Strings(byKeyGroupedByV)

	// Per outer entity q, the entities sharing q's :o/k and :o/v grouped by
	// :o/w: the group's :o/w and its entity count.
	var byKeyAndVGroupedByW []string
	for _, q := range entities {
		groups := map[int64]int64{}
		var order []int64
		for _, z := range entities {
			if z.k != q.k || z.v != q.v {
				continue
			}
			if _, seen := groups[z.w]; !seen {
				order = append(order, z.w)
			}
			groups[z.w]++
		}
		for _, w := range order {
			byKeyAndVGroupedByW = append(byKeyAndVGroupedByW, fmt.Sprintf("%s %d %d", q.name, w, groups[w]))
		}
	}
	sort.Strings(byKeyAndVGroupedByW)

	type element struct{ find, name string }
	threeElements := []element{{"?v", "?x"}, {"(count ?z)", "?y"}, {"(sum ?w)", "?u"}}
	cases := []struct {
		name     string
		elements []element
		// query is the text with the inner :find and the binding left as %s.
		query string
		want  []string
	}{
		{"one parameter", threeElements,
			`[:find ?q ?x ?y ?u :where [?q :o/k ?k] [(q [:find %s :in $ ?b :where [?z :o/k ?b] [?z :o/v ?v] [?z :o/w ?w]] $ ?k) [[%s]]]]`,
			byKeyGroupedByV},
		{"two parameters", []element{{"?w", "?x"}, {"(count ?z)", "?y"}},
			`[:find ?q ?x ?y :where [?q :o/k ?k] [?q :o/v ?c] [(q [:find %s :in $ ?b ?d :where [?z :o/k ?b] [?z :o/v ?d] [?z :o/w ?w]] $ ?k ?c) [[%s]]]]`,
			byKeyAndVGroupedByW},
		{"nested", threeElements,
			`[:find ?q ?x ?y ?u :where [?q :o/k ?k] [(q [:find ?x ?y ?u :in $ ?a :where [(q [:find %s :in $ ?b :where [?z :o/k ?b] [?z :o/v ?v] [?z :o/w ?w]] $ ?a) [[%s]]]] $ ?k) [[?x ?y ?u]]]]`,
			byKeyGroupedByV},
		{"under an or-default-join", threeElements,
			`[:find ?q ?x ?y ?u :where [?q :o/k ?k] (or-default-join [[?k] ?x ?y ?u] [(q [:find %s :in $ ?b :where [?z :o/k ?b] [?z :o/v ?v] [?z :o/w ?w]] $ ?k) [[%s]]] (and [(ground -1) ?x] [(ground -1) ?y] [(ground -1) ?u]))]`,
			byKeyGroupedByV},
	}
	for _, c := range cases {
		for _, mode := range legsFor(optimizerModes, algebraOption, sharingOption, fusionOption, aggregationOption) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, writtenOrderFacts())
			for _, order := range orderings(len(c.elements)) {
				finds := make([]string, len(order))
				bindings := make([]string, len(order))
				for i, at := range order {
					finds[i] = c.elements[at].find
					bindings[i] = c.elements[at].name
				}
				text := fmt.Sprintf(c.query, strings.Join(finds, " "), strings.Join(bindings, " "))
				t.Run(c.name+"/"+mode.name+"/"+strings.Join(finds, " "), func(t *testing.T) {
					rel, err := d.Query(text)
					require.NoError(t, err, text)
					require.Equal(t, c.want, renderedTuples(t, rel, names), text)
				})
			}
		}
	}
}

// A pull beside an aggregate in a :find groups by the pull's entity, and each
// tuple answers the pull's map and the aggregate at their written places,
// in either written order, on both optimizer paths.
func TestAPullBesideAnAggregateGroupsByItsEntity(t *testing.T) {
	g1, g2 := named("g1"), named("g2")
	m1, m2, m3 := named("m1"), named("m2"), named("m3")
	facts := []fact{
		{g1, ":o/name", "first"}, {g2, ":o/name", "second"},
		{m1, ":o/group", g1}, {m2, ":o/group", g1}, {m3, ":o/group", g2},
	}
	cases := []struct {
		text string
		want []string
	}{
		{`[:find (pull ?g [:o/name]) (count ?m) :where [?m :o/group ?g]]`,
			[]string{"map[o/name:first] 2", "map[o/name:second] 1"}},
		{`[:find (count ?m) (pull ?g [:o/name]) :where [?m :o/group ?g]]`,
			[]string{"1 map[o/name:second]", "2 map[o/name:first]"}},
	}
	for _, mode := range legsFor(optimizerModes, aggregationOption) {
		d := createOptimizerModeDB(t, mode, DatabaseOptions{})
		names := commitFacts(t, d, facts)
		for _, c := range cases {
			t.Run(mode.name+"/"+c.text, func(t *testing.T) {
				rel, err := d.Query(c.text)
				require.NoError(t, err, c.text)
				require.Equal(t, c.want, renderedTuples(t, rel, names), c.text)
			})
		}
	}
}
