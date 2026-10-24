package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// An attribute whose cardinality the schema does not state is read as
// cardinality one and post-validated, so a value a later assertion
// superseded answers no pattern: through a seek on the entity, a seek on the
// value with one key or several in either order, a collection input, and a
// branch; on a database declaring the attribute, one declaring others, and
// one declaring nothing; with the cache on and off
// (docs/reference/INDEX_SELECTION_PROOF.md, Theorem 3b).

// supersededRef writes d1 and d2, each carrying :t/ident :x and its own
// :t/doc, and moves a's :t/ref from d1 to d2 in a second transaction.
func supersededRef(t *testing.T, d *Database) (a, d1, d2 datalog.Identity) {
	t.Helper()
	a, d1, d2 = datalog.NewIdentity("a"), datalog.NewIdentity("d1"), datalog.NewIdentity("d2")
	tx := d.NewTransaction()
	require.NoError(t, tx.Set(d1, kw(":t/ident"), kw(":x")))
	require.NoError(t, tx.Set(d1, kw(":t/doc"), "first"))
	require.NoError(t, tx.Set(a, kw(":t/ref"), d1))
	_, err := tx.Commit()
	require.NoError(t, err)
	tx = d.NewTransaction()
	require.NoError(t, tx.Set(d2, kw(":t/ident"), kw(":x")))
	require.NoError(t, tx.Set(d2, kw(":t/doc"), "second"))
	require.NoError(t, tx.Set(a, kw(":t/ref"), d2))
	_, err = tx.Commit()
	require.NoError(t, err)
	return a, d1, d2
}

func TestUndeclaredAttribute_TheCurrentValueAnswersUnderEveryKeyOrder(t *testing.T) {
	declared, err := schema.NewBuilder().
		Attribute(":t/ref").Type(schema.TypeRef).Add().
		Attribute(":t/ident").Type(schema.TypeKeyword).Add().
		Attribute(":t/doc").Type(schema.TypeString).Add().Build()
	require.NoError(t, err)
	undeclared, err := schema.NewBuilder().
		Attribute(":t/other").Type(schema.TypeRef).Add().
		Attribute(":t/ident").Type(schema.TypeKeyword).Add().
		Attribute(":t/doc").Type(schema.TypeString).Add().Build()
	require.NoError(t, err)
	schemas := []struct {
		name string
		s    *schema.Schema
	}{{"declared", declared}, {"undeclared under a given schema", undeclared}, {"no schema", nil}}
	for _, sc := range schemas {
		for _, mode := range optimizerModes {
			for _, disableCache := range []bool{false, true} {
				name := sc.name + "/" + mode.name
				if disableCache {
					name += "/cache off"
				}
				t.Run(name, func(t *testing.T) {
					d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: sc.s, DisableCache: disableCache})
					a, d1, d2 := supersededRef(t, d)
					q1, err := executor.CollectTuples(d.Query(`[:find ?doc :where [?a :t/ref ?d] [?d :t/ident :x] [?d :t/doc ?doc]]`))
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{"second"}}, q1, "the ref seeks on the identity the ident pattern bound, and the superseded referent answers no doc")
					q2, err := executor.CollectTuples(d.Query(`[:find ?a :in $ [?d ...] :where [?a :t/ref ?d]]`, []interface{}{d1, d2}))
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{a}}, q2, "a binding of both referents answers a once, through its current referent")
					q3, err := executor.CollectTuples(d.Query(`[:find ?doc :where [?d :t/ident :x] [?d :t/doc ?doc] [?a :t/ref ?d]]`))
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{"second"}}, q3, "the same under the written order that binds the referent first")
					q4, err := executor.CollectTuples(d.Query(`[:find ?doc :where [?d :t/doc ?doc] (or-join [?d] [?a :t/ref ?d] [?d :t/doc "never"])]`))
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{"second"}}, q4, "a branch seeking on the referent answers the current one alone")
					one, err := executor.CollectTuples(d.Query(`[:find ?a :where [?d :t/doc "first"] [?a :t/ref ?d]]`))
					require.NoError(t, err)
					require.Empty(t, one, "a seek on the superseded referent alone answers nothing")
				})
			}
		}
	}
}

// The matcher's binding-driven scan of a cardinality-one attribute answers
// the current value under every binding: one key or several in either
// order, a binding over the entity and the value, and more keys than the
// per-key seek takes, under a declared attribute and an undeclared one,
// with the cache on and off. A key that is a superseded referent matches
// nothing, and a key that is the current one matches its entity.
func TestUndeclaredAttribute_TheMatcherAnswersTheCurrentValueUnderEveryBinding(t *testing.T) {
	declared, err := schema.NewBuilder().Attribute(":t/ref").Type(schema.TypeRef).Add().Build()
	require.NoError(t, err)
	for _, sc := range []struct {
		name string
		s    *schema.Schema
	}{{"declared", declared}, {"no schema", nil}} {
		for _, mode := range optimizerModes {
			for _, disableCache := range []bool{false, true} {
				name := sc.name + "/" + mode.name
				if disableCache {
					name += "/cache off"
				}
				t.Run(name, func(t *testing.T) {
					d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: sc.s, DisableCache: disableCache})
					a, d1, d2 := supersededRef(t, d)
					b, d3 := datalog.NewIdentity("b"), datalog.NewIdentity("d3")
					tx := d.NewTransaction()
					require.NoError(t, tx.Set(b, kw(":t/ref"), d3))
					wide := []executor.Tuple{{d1}, {d2}}
					wideWant := [][]interface{}{{a, d2}}
					for i := 0; len(wide) <= executor.SmallBindingBoundary; i++ {
						holder := datalog.NewIdentity(fmt.Sprintf("holder/%d", i))
						referent := datalog.NewIdentity(fmt.Sprintf("referent/%d", i))
						require.NoError(t, tx.Set(holder, kw(":t/ref"), referent))
						wide = append(wide, executor.Tuple{referent})
						wideWant = append(wideWant, []interface{}{holder, referent})
					}
					_, err := tx.Commit()
					require.NoError(t, err)
					q, err := parser.ParseQuery(`[:find ?a :in $ ?d :where [?a :t/ref ?d]]`)
					require.NoError(t, err)
					pattern := q.Where[0].(*query.DataPattern)
					value := []query.Symbol{datalog.NewSymbol("?d")}
					entityAndValue := []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?d")}
					cases := []struct {
						name    string
						symbols []query.Symbol
						keys    []executor.Tuple
						want    [][]interface{}
					}{
						{"the superseded referent", value, []executor.Tuple{{d1}}, nil},
						{"the current referent", value, []executor.Tuple{{d2}}, [][]interface{}{{a, d2}}},
						{"superseded then current", value, []executor.Tuple{{d1}, {d2}}, [][]interface{}{{a, d2}}},
						{"superseded and another entity's", value, []executor.Tuple{{d1}, {d3}}, [][]interface{}{{b, d3}}},
						{"current then superseded", value, []executor.Tuple{{d2}, {d1}}, [][]interface{}{{a, d2}}},
						{"the entity beside each of its referents", entityAndValue, []executor.Tuple{{a, d1}, {a, d2}}, [][]interface{}{{a, d2}}},
						{"more referents than the seek takes", value, wide, wideWant},
					}
					for _, c := range cases {
						t.Run(c.name, func(t *testing.T) {
							rel, err := d.Matcher().Match(query.PatternQuery(pattern), executor.Relations{executor.NewMaterializedRelation(c.symbols, c.keys)})
							require.NoError(t, err)
							tuples, err := executor.CollectTuples(rel, nil)
							require.NoError(t, err)
							require.ElementsMatch(t, c.want, tuples)
						})
					}
				})
			}
		}
	}
}

// A cardinality-one ref moved between two targets carrying the same name
// answers through its current target alone, whether the ref is declared or
// the database declares nothing: the ref alone, the ref then the target's
// number, and the name's constant before or after the ref.
func TestUndeclaredAttribute_AMovedRefAnswersItsCurrentTargetAlone(t *testing.T) {
	declared, err := schema.NewBuilder().
		Attribute(":t/ref").Type(schema.TypeRef).One().Add().
		Attribute(":t/name").Type(schema.TypeKeyword).One().Add().
		Attribute(":t/n").Type(schema.TypeLong).One().Add().Build()
	require.NoError(t, err)
	for _, sc := range []struct {
		name string
		s    *schema.Schema
	}{{"declared", declared}, {"no schema", nil}} {
		for _, mode := range optimizerModes {
			t.Run(sc.name+"/"+mode.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: sc.s})
				e, r1, r2 := datalog.NewIdentity("e"), datalog.NewIdentity("r1"), datalog.NewIdentity("r2")
				tx := d.NewTransaction()
				require.NoError(t, tx.Add(r1, kw(":t/name"), kw(":t/x")))
				require.NoError(t, tx.Add(r1, kw(":t/n"), int64(1)))
				require.NoError(t, tx.Add(e, kw(":t/ref"), r1))
				_, err := tx.Commit()
				require.NoError(t, err)
				tx = d.NewTransaction()
				require.NoError(t, tx.Add(r2, kw(":t/name"), kw(":t/x")))
				require.NoError(t, tx.Add(r2, kw(":t/n"), int64(2)))
				require.NoError(t, tx.Add(e, kw(":t/ref"), r2))
				_, err = tx.Commit()
				require.NoError(t, err)
				cases := []struct {
					name, text string
					want       [][]interface{}
				}{
					{"the ref alone", `[:find ?r :where [?e :t/ref ?r]]`, [][]interface{}{{r2}}},
					{"the ref then the number", `[:find ?n :where [?e :t/ref ?r] [?r :t/n ?n]]`, [][]interface{}{{int64(2)}}},
					{"the ref, the name's constant, the number", `[:find ?n :where [?e :t/ref ?r] [?r :t/name :t/x] [?r :t/n ?n]]`, [][]interface{}{{int64(2)}}},
					{"the name's constant, the ref, the number", `[:find ?n :where [?r :t/name :t/x] [?e :t/ref ?r] [?r :t/n ?n]]`, [][]interface{}{{int64(2)}}},
				}
				for _, c := range cases {
					t.Run(c.name, func(t *testing.T) {
						tuples, err := executor.CollectTuples(d.Query(c.text))
						require.NoError(t, err)
						require.Equal(t, c.want, tuples)
					})
				}
			})
		}
	}
}
