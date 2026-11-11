package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A unique attribute's value at an entity is what the unique rule's walk
// settles: the entity's newest assertion no other entity's later claim
// supersedes. Holder asserts "y", then "x"; claimant asserts "x" after it. The
// walk settles holder's value at "y" and claimant's at "x". A pattern with the
// attribute free and the value constant, bound by an input, or beside a
// constant entity answers the value's holder under the attribute exactly where
// the bound-attribute pattern, the attribute scan and pull do: in the latest
// view, as of holder's second commit, where holder's value is "x" and nothing
// holds "y", and as of the claimant's commit, with the EA cache on and off.
func TestReviewAFreeAttributeValueMatchAnswersAUniqueValueAsTheWalkSettlesIt(t *testing.T) {
	code := datalog.NewKeyword(":item/code")
	name := datalog.NewKeyword(":item/name")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: code, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne, Unique: schema.UniqueIdentity})
	s.Add(&schema.AttributeDefinition{Ident: name, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})

	entities := []struct {
		id   datalog.Identity
		seed string
	}{
		{datalog.NewIdentity("item:holder"), "item:holder"},
		{datalog.NewIdentity("item:claimant"), "item:claimant"},
	}
	holder, claimant := entities[0].id, entities[1].id
	seeds := map[datalog.Identity]string{holder: entities[0].seed, claimant: entities[1].seed}
	values := []string{"y", "x"}

	// forms match one value with the attribute free.
	forms := []struct {
		name  string
		query func(value string) (string, []interface{})
	}{
		{"constant value", func(value string) (string, []interface{}) {
			return fmt.Sprintf(`[:find ?e ?a :where [?e ?a %q]]`, value), nil
		}},
		{"value from an input", func(value string) (string, []interface{}) {
			return `[:find ?e ?a :in $ ?v :where [?e ?a ?v]]`, []interface{}{value}
		}},
	}

	for _, mode := range legsFor(optimizerModes) {
		for _, cacheMode := range cacheTestModes {
			t.Run(mode.name+"/"+cacheMode.name, func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: cacheMode.disableCache})
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(holder, name, "holder"))
				require.NoError(t, tx.Add(claimant, name, "claimant"))
				require.NoError(t, tx.Add(holder, code, "y"))
				_, err := tx.Commit()
				require.NoError(t, err)
				tx2 := db.NewTransaction()
				require.NoError(t, tx2.Add(holder, code, "x"))
				second, err := tx2.Commit()
				require.NoError(t, err)
				tx3 := db.NewTransaction()
				require.NoError(t, tx3.Add(claimant, code, "x"))
				third, err := tx3.Commit()
				require.NoError(t, err)

				answers := func(view *Database, text string, args ...interface{}) [][]interface{} {
					t.Helper()
					got, err := executor.CollectTuples(view.Query(text, args...))
					require.NoError(t, err, text)
					return got
				}
				render := func(tuples [][]interface{}) []string {
					rendered := make([]string, 0, len(tuples))
					for _, tuple := range tuples {
						rendered = append(rendered, fmt.Sprintf("%s %v", seeds[tuple[0].(datalog.Identity)], tuple[1]))
					}
					return rendered
				}

				// holds is, per view, the entity whose walk settles on each value.
				views := []struct {
					name  string
					db    *Database
					holds map[string]datalog.Identity
				}{
					{"latest", db, map[string]datalog.Identity{"y": holder, "x": claimant}},
					{"as of holder's second commit", db.AsOf(second), map[string]datalog.Identity{"x": holder}},
					{"as of the claimant's commit", db.AsOf(third), map[string]datalog.Identity{"y": holder, "x": claimant}},
				}
				for _, view := range views {
					for _, e := range entities {
						var held interface{}
						for _, value := range values {
							if view.holds[value] == e.id {
								held = value
							}
						}
						pulled, err := view.db.Pull(e.id, "[:item/code]")
						require.NoError(t, err)
						require.Equal(t, held, pulled["item/code"], "%s: pull answers the value the walk settles at %s", view.name, e.seed)
					}
					for _, value := range values {
						var want [][]interface{}
						if owner, held := view.holds[value]; held {
							want = [][]interface{}{{owner, code}}
						}
						bound := answers(view.db, fmt.Sprintf(`[:find ?e ?a :where [?e :item/code %q] [(ground :item/code) ?a]]`, value))
						require.ElementsMatch(t, want, bound, "%s: the bound attribute answers %q's holder", view.name, value)
						scanned := answers(view.db, fmt.Sprintf(`[:find ?e ?a :where [?e :item/code ?v] [(= ?v %q)] [(ground :item/code) ?a]]`, value))
						require.ElementsMatch(t, want, scanned, "%s: the attribute scan answers %q's holder", view.name, value)
						for _, form := range forms {
							text, args := form.query(value)
							got := answers(view.db, text, args...)
							assert.ElementsMatch(t, want, got, "%s, %s, %q: answered %v", view.name, form.name, value, render(got))
						}
						for _, e := range entities {
							var wantNamed [][]interface{}
							if view.holds[value] == e.id {
								wantNamed = [][]interface{}{{code}}
							}
							got := answers(view.db, fmt.Sprintf(`[:find ?a :where [#id %q ?a %q]]`, e.seed, value))
							assert.ElementsMatch(t, wantNamed, got, "%s, the constant entity %s, %q: answered %v", view.name, e.seed, value, got)
						}
					}
				}
			})
		}
	}
}

// A unique value an entity withdrew and asserted again is the entity's value
// in the latest view and as of its first assertion, and no entity's as of the
// withdrawal. A pattern with the attribute free, the value constant or bound
// by an input, answers in each view what the bound-attribute pattern and pull
// answer, with the EA cache on and off.
func TestAFreeAttributeValueMatchAnswersAUniqueValueWithdrawnAndReasserted(t *testing.T) {
	code := datalog.NewKeyword(":item/code")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: code, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne, Unique: schema.UniqueIdentity})
	doc := datalog.NewIdentity("item:doc")

	for _, mode := range legsFor(optimizerModes) {
		for _, cacheMode := range cacheTestModes {
			t.Run(mode.name+"/"+cacheMode.name, func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: cacheMode.disableCache})
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(doc, code, "w"))
				first, err := tx.Commit()
				require.NoError(t, err)
				tx2 := db.NewTransaction()
				require.NoError(t, tx2.Remove(doc, code, "w"))
				withdrawn, err := tx2.Commit()
				require.NoError(t, err)
				tx3 := db.NewTransaction()
				require.NoError(t, tx3.Add(doc, code, "w"))
				_, err = tx3.Commit()
				require.NoError(t, err)

				answers := func(view *Database, text string, args ...interface{}) [][]interface{} {
					t.Helper()
					got, err := executor.CollectTuples(view.Query(text, args...))
					require.NoError(t, err, text)
					return got
				}

				for _, view := range []struct {
					name string
					db   *Database
					held bool
				}{
					{"latest", db, true},
					{"as of the first assertion", db.AsOf(first), true},
					{"as of the withdrawal", db.AsOf(withdrawn), false},
				} {
					pulled, err := view.db.Pull(doc, "[:item/code]")
					require.NoError(t, err)
					var want [][]interface{}
					if view.held {
						require.Equal(t, "w", pulled["item/code"], "%s: pull answers the value", view.name)
						want = [][]interface{}{{doc, code}}
					} else {
						require.Nil(t, pulled["item/code"], "%s: pull answers no value", view.name)
					}
					bound := answers(view.db, `[:find ?e ?a :where [?e :item/code "w"] [(ground :item/code) ?a]]`)
					require.ElementsMatch(t, want, bound, "%s: the bound attribute", view.name)
					constant := answers(view.db, `[:find ?e ?a :where [?e ?a "w"]]`)
					assert.ElementsMatch(t, want, constant, "%s: the constant value with the attribute free", view.name)
					input := answers(view.db, `[:find ?e ?a :in $ ?v :where [?e ?a ?v]]`, "w")
					assert.ElementsMatch(t, want, input, "%s: the value from an input with the attribute free", view.name)
				}
			})
		}
	}
}
