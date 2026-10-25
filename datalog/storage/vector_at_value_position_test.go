package storage

// A vector is the value domain's one composite, and a datom's value is a
// scalar. A vector at a data pattern's value position matches a datom only
// through the vector arm, under a declared vector attribute. Under every
// other attribute the pattern answers no tuple, with no error and no panic,
// however the vector arrives: a binding tuple by any route, or the
// pattern's own literal.

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// valueRouteFacts: x :a 1 :b 10 :r 1; y :a 2 :b 20 :r 2; z :a 2 :b 30 :r 2;
// w :r 3. No value is a vector, so a vector at :r's value position matches
// nothing.
func valueRouteFacts() []fact {
	x, y, z, w := named("x"), named("y"), named("z"), named("w")
	return []fact{
		{x, ":a", int64(1)}, {x, ":b", int64(10)}, {x, ":r", int64(1)},
		{y, ":a", int64(2)}, {y, ":b", int64(20)}, {y, ":r", int64(2)},
		{z, ":a", int64(2)}, {z, ":b", int64(30)}, {z, ":r", int64(2)},
		{w, ":r", int64(3)},
	}
}

// vectorOneTwo is the vector [1 2] as the engine holds a parsed vector.
var vectorOneTwo = []interface{}{int64(1), int64(2)}

// valueRouteCase is one query carrying a vector to a value position, its
// inputs, and the tuples it answers, rendered and sorted.
type valueRouteCase struct {
	name   string
	query  string
	inputs []interface{}
	want   []string
}

// runValueRouteCases runs each case's query over a database holding the
// facts, in both optimizer modes, and asserts the case's answer with no panic
// and no error.
func runValueRouteCases(t *testing.T, facts []fact, cases []valueRouteCase) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, facts)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					assertCase(t, d, names, c)
				})
			}
		})
	}
}

// assertCase runs one case against the handle and asserts its answer.
func assertCase(t *testing.T, d *Database, names map[datalog.Identity]string, c valueRouteCase) {
	t.Helper()
	var got []string
	var err error
	require.NotPanics(t, func() {
		var rel executor.Relation
		rel, err = d.Query(c.query, c.inputs...)
		if err == nil {
			got = renderedTuples(t, rel, names)
		}
	}, c.query)
	require.NoError(t, err, c.query)
	if len(c.want) == 0 {
		require.Empty(t, got, c.query)
		return
	}
	require.Equal(t, c.want, got, c.query)
}

// Every route by which a binding tuple carries a vector into a pattern's
// value position, under an undeclared attribute.
func TestVectorAtValuePosition_ABoundVectorAnswersNoTupleUnderAScalarAttribute(t *testing.T) {
	cases := []valueRouteCase{
		{"ground scalar binding a nested vector", `[:find ?e :where [(ground [[1] [2]]) ?v] [?e :r ?v]]`, nil, nil},
		{"ground scalar binding a flat vector", `[:find ?e :where [(ground [1 2]) ?v] [?e :r ?v]]`, nil, nil},
		{":in scalar given a vector", `[:find ?e :in $ ?v :where [?e :r ?v]]`, []interface{}{vectorOneTwo}, nil},
		{":in collection whose member is a vector", `[:find ?e :in $ [?v ...] :where [?e :r ?v]]`, []interface{}{[]interface{}{vectorOneTwo}}, nil},
		{":in tuple whose element is a vector", `[:find ?e :in $ [?v ?w] :where [?e :r ?v]]`, []interface{}{[]interface{}{vectorOneTwo, int64(3)}}, nil},
		{":in relation whose element is a vector", `[:find ?e :in $ [[?v]] :where [?e :r ?v]]`, []interface{}{[]interface{}{[]interface{}{vectorOneTwo}}}, nil},
		{"expression collection binding a nested vector", `[:find ?e :where [(ground [[1 2]]) [?v ...]] [?e :r ?v]]`, nil, nil},
		{"subquery scalar argument holding a vector", `[:find ?c :where [(ground [1 2]) ?ns] [(q [:find ?e :in $ ?n :where [?e :r ?n]] $ ?ns) [[?c]]]]`, nil, nil},
		{"ground binding the empty vector", `[:find ?e :where [(ground []) ?v] [?e :r ?v]]`, nil, nil},
		{"the pattern written before its binder", `[:find ?e :where [?e :r ?v] [(ground [1 2]) ?v]]`, nil, nil},
		{"the vector bound inside a not-join body", `[:find ?e :where [?e :a _] (not-join [?e] [(ground [1 2]) ?v] [?e :r ?v])]`, nil, []string{"x", "y", "z"}},
		{"the entity bound beside the vector", `[:find ?e :where [?e :a 2] [(ground [1 2]) ?v] [?e :r ?v]]`, nil, nil},
	}
	runValueRouteCases(t, valueRouteFacts(), cases)
}

// A bound vector under every handle: the history handle reads raw datoms
// without CRDT resolution and answers no binding-driven match from the cache,
// and the live and as-of handles answer from it. By a scalar input with the
// entity unbound, by a scalar input beside an entity a pattern binds, and by
// a collection input of vectors beside one, under an undeclared attribute, a
// declared cardinality one and a declared cardinality many, the pattern
// answers no tuple, with no error and no panic.
func TestVectorAtValuePosition_UnderEveryHandle(t *testing.T) {
	x := named("x")
	facts := []fact{
		{x, ":a", int64(1)}, {x, ":r", int64(1)}, {x, ":one", int64(1)},
		{x, ":m", int64(1)}, {x, ":m", int64(2)},
	}
	attributes := []struct{ name, attr string }{
		{"undeclared", ":r"},
		{"declared one", ":one"},
		{"declared many", ":m"},
	}
	routes := []struct {
		name, query string
		inputs      []interface{}
	}{
		{"scalar input, entity unbound", `[:find ?e :in $ ?v :where [?e %s ?v]]`, []interface{}{vectorOneTwo}},
		{"scalar input beside an entity a pattern binds", `[:find ?e :in $ ?v :where [?e :a 1] [?e %s ?v]]`, []interface{}{vectorOneTwo}},
		{"collection input of vectors beside an entity a pattern binds", `[:find ?e :in $ [?v ...] :where [?e :a 1] [?e %s ?v]]`, []interface{}{[]interface{}{vectorOneTwo, []interface{}{int64(3)}}}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			s, err := schema.NewBuilder().
				Attribute(":one").Type(schema.TypeLong).One().Add().
				Attribute(":m").Type(schema.TypeLong).Many().Add().
				Build()
			require.NoError(t, err)
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
			names := map[datalog.Identity]string{x.id: x.name}
			tx := d.NewTransaction()
			for _, f := range facts {
				require.NoError(t, tx.Add(f.of.id, kw(f.attr), f.value))
			}
			at, err := tx.Commit()
			require.NoError(t, err)
			handles := []struct {
				name   string
				handle *Database
			}{
				{"history", d.History()},
				{"live", d},
				{"as-of", d.AsOf(at)},
			}
			for _, h := range handles {
				for _, a := range attributes {
					for _, r := range routes {
						t.Run(h.name+"/"+a.name+"/"+r.name, func(t *testing.T) {
							assertCase(t, h.handle, names, valueRouteCase{r.name, fmt.Sprintf(r.query, a.attr), r.inputs, nil})
						})
					}
				}
			}
		})
	}
}

// A vector read from a declared vector attribute and carried to an
// undeclared attribute's value position, a rule body given a vector argument,
// and the contrast: the same bound vector under the vector attribute itself
// answers the entity whose vector it is.
func TestVectorAtValuePosition_ACarriedVectorAndARuleArgumentAnswerNoTuple(t *testing.T) {
	x, y, tagged := named("x"), named("y"), named("t")
	facts := []fact{
		{x, ":r", int64(1)},
		{y, ":r", int64(2)},
		{tagged, ":tags", int64(1)}, {tagged, ":tags", int64(2)},
	}
	cases := []valueRouteCase{
		{"declared vector attribute's value carried to an undeclared attribute", `[:find ?e :where [?t :tags ?v] [?e :r ?v]]`, nil, nil},
		{"rule body reached with a vector argument", `[:find ?e :in $ % ?v :where (hasr ?e ?v)]`, []interface{}{`[[(hasr ?e ?v) [?e :r ?v]]]`, vectorOneTwo}, nil},
		{"the bound vector under the vector attribute answers its entity", `[:find ?t :in $ ?v :where [?t :tags ?v]]`, []interface{}{vectorOneTwo}, []string{"t"}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			s, err := schema.NewBuilder().Attribute(":tags").Type(schema.TypeLong).Vector().Add().Build()
			require.NoError(t, err)
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
			names := commitFacts(t, d, facts)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					assertCase(t, d, names, c)
				})
			}
		})
	}
}

// The vector at a declared attribute's value position: cardinality one, the
// cardinality many the archive reports green, and cardinality one declared
// unique, whose bound value reaches the AVET walk.
func TestVectorAtValuePosition_ADeclaredAttributeAnswersNoTuple(t *testing.T) {
	declarations := []struct {
		name    string
		declare func(*schema.AttributeBuilder) *schema.AttributeBuilder
	}{
		{"cardinality one", func(b *schema.AttributeBuilder) *schema.AttributeBuilder { return b.One() }},
		{"cardinality many", func(b *schema.AttributeBuilder) *schema.AttributeBuilder { return b.Many() }},
		{"cardinality one and unique", func(b *schema.AttributeBuilder) *schema.AttributeBuilder { return b.One().Unique(schema.UniqueValue) }},
	}
	cases := []valueRouteCase{
		{"ground scalar binding a vector", `[:find ?e :where [(ground [1 2]) ?v] [?e :r ?v]]`, nil, nil},
		{":in scalar given a vector", `[:find ?e :in $ ?v :where [?e :r ?v]]`, []interface{}{vectorOneTwo}, nil},
	}
	for _, declaration := range declarations {
		for _, mode := range optimizerModes {
			t.Run(declaration.name+"/"+mode.name, func(t *testing.T) {
				s, err := declaration.declare(schema.NewBuilder().Attribute(":r").Type(schema.TypeLong)).Add().Build()
				require.NoError(t, err)
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
				names := commitFacts(t, d, []fact{{named("x"), ":r", int64(1)}})
				for _, c := range cases {
					t.Run(c.name, func(t *testing.T) {
						assertCase(t, d, names, c)
					})
				}
			})
		}
	}
}

// The parser's vector literal at a pattern's value position: under an
// undeclared attribute, under a declared many with the entity unbound and
// with it written, beside a bound entity, and the contrast under the vector
// attribute, which answers the entity whose vector it is.
func TestVectorAtValuePosition_AConstantVectorAnswersNoTupleUnderAScalarAttribute(t *testing.T) {
	x, tagged := named("x"), named("t")
	facts := []fact{
		{x, ":a", int64(1)}, {x, ":r", int64(1)}, {x, ":m", int64(1)},
		{tagged, ":tags", int64(1)}, {tagged, ":tags", int64(2)},
	}
	cases := []valueRouteCase{
		{"undeclared attribute, entity unbound", `[:find ?e :where [?e :r [1 2]]]`, nil, nil},
		{"declared many, entity unbound", `[:find ?e :where [?e :m [1 2]]]`, nil, nil},
		{"declared many, entity written", `[:find ?t :where [#id "x" :m [1 2] ?t]]`, nil, nil},
		{"undeclared attribute, entity bound beside the literal", `[:find ?e :where [?e :a 1] [?e :r [1 2]]]`, nil, nil},
		{"the literal under the vector attribute answers its entity", `[:find ?e :where [?e :tags [1 2]]]`, nil, []string{"t"}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			s, err := schema.NewBuilder().
				Attribute(":tags").Type(schema.TypeLong).Vector().Add().
				Attribute(":m").Type(schema.TypeLong).Many().Add().
				Build()
			require.NoError(t, err)
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
			names := commitFacts(t, d, facts)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					assertCase(t, d, names, c)
				})
			}
		})
	}
}

// QueryInto and QueryOneInto over a scalar :in entry given a vector, read at
// an undeclared attribute's value position.
func TestVectorAtValuePosition_ThroughQueryIntoAndQueryOneInto(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(datalog.NewIdentity("x"), kw(":r"), int64(1)))
			_, err := tx.Commit()
			require.NoError(t, err)
			require.NotPanics(t, func() {
				var es []datalog.Identity
				require.NoError(t, d.QueryInto(&es, `[:find ?e :in $ ?v :where [?e :r ?v]]`, vectorOneTwo))
				require.Empty(t, es)
			}, "QueryInto")
			require.NotPanics(t, func() {
				var e datalog.Identity
				found, err := d.QueryOneInto(&e, `[:find ?e :in $ ?v :where [?e :r ?v]]`, vectorOneTwo)
				require.NoError(t, err)
				require.False(t, found)
			}, "QueryOneInto")
		})
	}
}
