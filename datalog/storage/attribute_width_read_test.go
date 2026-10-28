package storage

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/qb"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// widthAttr is an attribute keyword's text in the namespace ns, exactly as
// wide as storage holds, or one byte wider.
func widthAttr(ns string, wide bool) string {
	text := ":" + ns + "/" + strings.Repeat("a", datalog.MaxAttributeBytes-len(":"+ns+"/"))
	if wide {
		text += "z"
	}
	return text
}

type widthReadRoute struct {
	name string
	run  func(d *Database, e datalog.Identity, tx datalog.ElementID, wide bool) (interface{}, error)
}

// widthReadRoutes is every read entry that takes an attribute keyword: a
// query's constant attribute, the database functions' attributes, a keyword
// input at the attribute position, the builder's constant, a find pull,
// every direct pull, the typed accessors, the attribute resolver, and the
// same routes through an AsOf and a History handle and a rule body.
func widthReadRoutes() []widthReadRoute {
	s := func(wide bool) string { return widthAttr("s", wide) }
	kw := func(ns string, wide bool) datalog.Keyword { return datalog.NewKeyword(widthAttr(ns, wide)) }
	return []widthReadRoute{
		{"a query's constant attribute", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			return executor.CollectTuples(d.Query(fmt.Sprintf(`[:find ?v :where [?e %s ?v]]`, s(wide))))
		}},
		{"a get-else's attribute", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			return executor.CollectTuples(d.Query(fmt.Sprintf(`[:find ?v :in $ ?e :where [(get-else $ ?e %s "none") ?v]]`, s(wide)), e))
		}},
		{"a get-some's attribute", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			return executor.CollectTuples(d.Query(fmt.Sprintf(`[:find ?v :in $ ?e :where [(get-some $ ?e %s) ?v]]`, s(wide)), e))
		}},
		{"a missing? predicate's attribute", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			return executor.CollectTuples(d.Query(fmt.Sprintf(`[:find ?e :in $ ?e :where [(missing? $ ?e %s)]]`, s(wide)), e))
		}},
		{"a keyword input at the attribute position", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			return executor.CollectTuples(d.Query(`[:find ?v :in $ ?a :where [?e ?a ?v]]`, kw("s", wide)))
		}},
		{"the builder's constant attribute", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			ev, v := qb.NewVar("e"), qb.NewVar("v")
			q, err := qb.Query().Find(v).Where(qb.Pat(ev, qb.Kw(s(wide)), v)).Build()
			if err != nil {
				return nil, err
			}
			return executor.CollectTuples(d.Query(q))
		}},
		{"a find pull's attribute", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			return executor.CollectTuples(d.Query(fmt.Sprintf(`[:find (pull ?e [%s]) :where [?e %s _]]`, s(wide), s(false))))
		}},
		{"Pull", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			return d.Pull(e, fmt.Sprintf(`[%s]`, s(wide)))
		}},
		{"PullMany", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			return d.PullMany([]datalog.Identity{e}, fmt.Sprintf(`[%s]`, s(wide)))
		}},
		{"a nested pull under a ref", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			return d.Pull(e, fmt.Sprintf(`[{%s [%s]}]`, widthAttr("r", false), s(wide)))
		}},
		{"an AsOf pull", func(d *Database, e datalog.Identity, tx datalog.ElementID, wide bool) (interface{}, error) {
			return d.AsOf(tx).Pull(e, fmt.Sprintf(`[%s]`, s(wide)))
		}},
		{"PullInto's struct tag", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			if wide {
				var into struct {
					V string `datalog:"s/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaz"`
				}
				err := d.PullInto(e, &into)
				return into.V, err
			}
			var into struct {
				V string `datalog:"s/aaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`
			}
			err := d.PullInto(e, &into)
			return into.V, err
		}},
		{"PullIntoMany's struct tag", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			if wide {
				var into []struct {
					V string `datalog:"s/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaz"`
				}
				err := d.PullIntoMany([]datalog.Identity{e}, &into)
				return into, err
			}
			var into []struct {
				V string `datalog:"s/aaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`
			}
			err := d.PullIntoMany([]datalog.Identity{e}, &into)
			return into, err
		}},
		{"GetString", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			v, found, err := d.GetString(e, kw("s", wide))
			return fmt.Sprintf("%q found=%v", v, found), err
		}},
		{"GetInt", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			v, found, err := d.GetInt(e, kw("i", wide))
			return fmt.Sprintf("%v found=%v", v, found), err
		}},
		{"GetRef", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			v, found, err := d.GetRef(e, kw("r", wide))
			return fmt.Sprintf("%v found=%v", v, found), err
		}},
		{"GetStrings", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			return d.GetStrings(e, kw("m", wide))
		}},
		{"GetVectorNth", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			return d.GetVectorNth(e, kw("v", wide), 0)
		}},
		{"GetVectorLength", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			return d.GetVectorLength(e, kw("v", wide))
		}},
		{"ResolveEntityAttributes", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			return d.ResolveEntityAttributes(e, []datalog.Keyword{kw("s", wide)})
		}},
		{"an AsOf query's get-else attribute", func(d *Database, e datalog.Identity, tx datalog.ElementID, wide bool) (interface{}, error) {
			return executor.CollectTuples(d.AsOf(tx).Query(fmt.Sprintf(`[:find ?v :in $ ?e :where [(get-else $ ?e %s "none") ?v]]`, s(wide)), e))
		}},
		{"a History query's constant attribute", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			return executor.CollectTuples(d.History().Query(fmt.Sprintf(`[:find ?v :where [?e %s ?v]]`, s(wide))))
		}},
		{"a rule body's constant attribute", func(d *Database, e datalog.Identity, _ datalog.ElementID, wide bool) (interface{}, error) {
			return executor.CollectTuples(d.Query(`[:find ?v :in $ % :where (r ?e ?v)]`, fmt.Sprintf(`[[(r ?e ?v) [?e %s ?v]]]`, s(wide))))
		}},
	}
}

// An attribute keyword one byte wider than storage holds is refused at every
// read entry with the width rule's one message, in both optimizer modes with
// the cache on and off, and the keyword exactly as wide as storage holds is
// answered at each. The store carries the 32-byte attributes: a string, an
// integer, a ref to an entity carrying the string, a declared set and a
// declared vector.
func TestAnAttributeWiderThanStorageHoldsIsRefusedAtEveryReadEntry(t *testing.T) {
	require.Equal(t, datalog.MaxAttributeBytes, len(widthAttr("s", false)))
	require.Equal(t, datalog.MaxAttributeBytes+1, len(widthAttr("s", true)))
	for _, mode := range optimizerModes {
		for _, disableCache := range []bool{false, true} {
			cache := "cache on"
			if disableCache {
				cache = "cache off"
			}
			t.Run(mode.name+"/"+cache, func(t *testing.T) {
				sch := schema.NewBuilder().
					Attribute(widthAttr("m", false)).Type(schema.TypeString).Many().Add().
					Attribute(widthAttr("v", false)).Type(schema.TypeString).Vector().Add().
					MustBuild()
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: sch, DisableCache: disableCache})
				e, f := datalog.NewIdentity("e"), datalog.NewIdentity("f")
				tx := d.NewTransaction()
				require.NoError(t, tx.Set(e, datalog.NewKeyword(widthAttr("s", false)), "short"))
				require.NoError(t, tx.Set(e, datalog.NewKeyword(widthAttr("i", false)), int64(7)))
				require.NoError(t, tx.Set(e, datalog.NewKeyword(widthAttr("r", false)), f))
				require.NoError(t, tx.Set(f, datalog.NewKeyword(widthAttr("s", false)), "fshort"))
				require.NoError(t, tx.Add(e, datalog.NewKeyword(widthAttr("m", false)), "x"))
				require.NoError(t, tx.Add(e, datalog.NewKeyword(widthAttr("v", false)), "one"))
				txid, err := tx.Commit()
				require.NoError(t, err)

				tuples, err := executor.CollectTuples(d.Query(fmt.Sprintf(`[:find ?v :in $ ?e :where [?e %s ?v]]`, widthAttr("s", false)), e))
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"short"}}, [][]interface{}(tuples), "the store carries the 32-byte attribute")

				for _, route := range widthReadRoutes() {
					t.Run(route.name, func(t *testing.T) {
						answer, err := route.run(d, e, txid, true)
						require.Error(t, err, "the wide keyword is refused; answered %v", answer)
						require.Contains(t, err.Error(), "exceeds the 32-byte storage limit", "answered %v", answer)
						_, err = route.run(d, e, txid, false)
						require.NoError(t, err, "the keyword at the limit is answered")
					})
				}
			})
		}
	}
}
