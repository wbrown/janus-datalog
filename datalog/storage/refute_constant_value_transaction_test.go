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

// In a view that resolves, a data pattern's transaction position selects the
// pairs whose resolved datom the transaction wrote, so [?e ?a V T] answers
// what [?e ?a ?v ?t] [(= ?v V)] [(= ?t T)] answers
// (docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md, T1). Three histories
// of :item/one, cardinality one, each opening with first's assertion of "x",
// whose ElementID is T. Second asserts "x" later, and T selects first alone.
// First asserts "z" later, and T selects nothing in the latest view and first
// as of T. First asserts "x" again later, and T selects nothing in the latest
// view, where the later ElementID selects first. Every spelling with the value
// constant or from an input, the attribute free or bound, the transaction a
// constant, an integer Lamport or from an input, on $, a named source and a
// rule body, answers the reference in each view, with the EA cache on and
// off. In History the same patterns answer every datom T wrote carrying "x".
func TestRefuteAConstantValueMatchHonorsItsTransactionPosition(t *testing.T) {
	one := datalog.NewKeyword(":item/one")
	name := datalog.NewKeyword(":item/name")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: one, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
	s.Add(&schema.AttributeDefinition{Ident: name, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})

	first := datalog.NewIdentity("item:first")
	second := datalog.NewIdentity("item:second")
	seeds := map[datalog.Identity]string{first: "first", second: "second"}

	// selection is what T selects in each view of a history.
	type selection struct {
		latest, asOfLater, asOfT, history []datalog.Identity
	}
	histories := []struct {
		name  string
		later func(db *Database) (datalog.ElementID, error)
		want  selection
	}{
		{"second asserts the value later", func(db *Database) (datalog.ElementID, error) {
			tx := db.NewTransaction()
			if err := tx.Add(second, one, "x"); err != nil {
				return datalog.ElementID{}, err
			}
			return tx.Commit()
		}, selection{[]datalog.Identity{first}, []datalog.Identity{first}, []datalog.Identity{first}, []datalog.Identity{first}}},
		{"first asserts another value later", func(db *Database) (datalog.ElementID, error) {
			tx := db.NewTransaction()
			if err := tx.Add(first, one, "z"); err != nil {
				return datalog.ElementID{}, err
			}
			return tx.Commit()
		}, selection{nil, nil, []datalog.Identity{first}, []datalog.Identity{first}}},
		{"first asserts the value again later", func(db *Database) (datalog.ElementID, error) {
			tx := db.NewTransaction()
			if err := tx.Add(first, one, "x"); err != nil {
				return datalog.ElementID{}, err
			}
			return tx.Commit()
		}, selection{nil, nil, []datalog.Identity{first}, []datalog.Identity{first}}},
	}

	for _, h := range histories {
		t.Run(h.name, func(t *testing.T) {
			for _, mode := range legsFor(optimizerModes) {
				for _, cacheMode := range cacheTestModes {
					t.Run(mode.name+"/"+cacheMode.name, func(t *testing.T) {
						db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: cacheMode.disableCache})
						tx := db.NewTransaction()
						require.NoError(t, tx.Add(first, name, "first"))
						require.NoError(t, tx.Add(second, name, "second"))
						require.NoError(t, tx.Add(first, one, "x"))
						_, err := tx.Commit()
						require.NoError(t, err)

						asserted, err := executor.CollectTuples(db.Query(`[:find ?t :where [#id "item:first" :item/one "x" ?t]]`))
						require.NoError(t, err)
						require.Len(t, asserted, 1)
						at := *asserted[0][0].(*datalog.ElementID)
						eid := fmt.Sprintf("#eid [%d %d]", at.Lamport, at.ReplicaID)

						later, err := h.later(db)
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
								rendered = append(rendered, seeds[tuple[0].(datalog.Identity)])
							}
							return rendered
						}

						for _, view := range []struct {
							name string
							db   *Database
							want []datalog.Identity
						}{
							{"latest", db, h.want.latest},
							{"as of the later commit", db.AsOf(later), h.want.asOfLater},
							{"as of T", db.AsOf(at), h.want.asOfT},
							{"History", db.History(), h.want.history},
						} {
							var want [][]interface{}
							for _, e := range view.want {
								want = append(want, []interface{}{e})
							}
							reference := answers(view.db, fmt.Sprintf(`[:find ?e :where [?e ?a ?v ?t] [(= ?v "x")] [(= ?t %s)]]`, eid))
							require.ElementsMatch(t, want, reference, "%s: the reference selects %v, answered %v", view.name, render(want), render(reference))

							for _, c := range []struct {
								name  string
								query string
								args  []interface{}
							}{
								{"attribute free, value and transaction constant", fmt.Sprintf(`[:find ?e :where [?e ?a "x" %s]]`, eid), nil},
								{"attribute bound, value and transaction constant", fmt.Sprintf(`[:find ?e :where [?e :item/one "x" %s]]`, eid), nil},
								{"attribute free, value constant, transaction an integer Lamport", fmt.Sprintf(`[:find ?e :where [?e ?a "x" %d]]`, at.Lamport), nil},
								{"attribute free, value constant, transaction from an input", `[:find ?e :in $ ?t :where [?e ?a "x" ?t]]`, []interface{}{at}},
								{"attribute free, value from an input, transaction constant", fmt.Sprintf(`[:find ?e :in $ ?v :where [?e ?a ?v %s]]`, eid), []interface{}{"x"}},
								{"attribute free, value and transaction from a relation input", `[:find ?e :in $ [[?v ?t]] :where [?e ?a ?v ?t]]`, []interface{}{[]interface{}{[]interface{}{"x", at}}}},
								{"attribute free, value filtered, transaction constant", fmt.Sprintf(`[:find ?e :where [?e ?a ?v %s] [(= ?v "x")]]`, eid), nil},
								{"attribute bound, value filtered, transaction constant", fmt.Sprintf(`[:find ?e :where [?e :item/one ?v %s] [(= ?v "x")]]`, eid), nil},
							} {
								got := answers(view.db, c.query, c.args...)
								assert.ElementsMatch(t, want, got, "%s, %s: answered %v, want %v", view.name, c.name, render(got), render(want))
							}
							named := answers(db, fmt.Sprintf(`[:find ?e :in $ $past :where [$past ?e ?a "x" %s]]`, eid), sourceNamed("$past", view.db))
							assert.ElementsMatch(t, want, named, "%s, named source, attribute free: answered %v, want %v", view.name, render(named), render(want))
							ruled := answers(view.db, `[:find ?e :in $ % :where (asserted ?e)]`, mustRuleSet(t, fmt.Sprintf(`[[(asserted ?e) [?e ?a "x" %s]]]`, eid)))
							assert.ElementsMatch(t, want, ruled, "%s, rule body, attribute free: answered %v, want %v", view.name, render(ruled), render(want))
						}
					})
				}
			}
		})
	}
}

// A pattern with its entity free and its transaction bound answers, on a many
// attribute, the members whose newest assertion the transaction wrote, and on
// a vector attribute the collection when the transaction wrote the group's
// highest operation (docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md, T6,
// T11). The member "a" is asserted in the first commit and again in the third,
// so its first assertion's Tx selects nothing in the latest view and "a" as of
// the first commit, and its second assertion's Tx selects "a". The member "b"
// is asserted in the second commit alone, so "a"'s first Tx selects no entity
// holding "b". The vector holds "p" from the first commit and "q" from the
// second, so the second commit's operation selects ["p" "q"] in the latest view
// and the first commit's selects nothing there and ["p"] as of the first
// commit. With the EA cache on and off.
func TestATransactionBoundPatternAnswersAManyMemberAndAVectorByTheirResolution(t *testing.T) {
	tags := datalog.NewKeyword(":item/tags")
	list := datalog.NewKeyword(":item/list")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: tags, ValueType: schema.TypeString, Cardinality: schema.CardinalityMany})
	s.Add(&schema.AttributeDefinition{Ident: list, ValueType: schema.TypeString, Cardinality: schema.CardinalityVector})
	e := datalog.NewIdentity("item:e")

	for _, mode := range legsFor(optimizerModes) {
		for _, cacheMode := range cacheTestModes {
			t.Run(mode.name+"/"+cacheMode.name, func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: cacheMode.disableCache})

				answers := func(view *Database, text string) [][]interface{} {
					t.Helper()
					got, err := executor.CollectTuples(view.Query(text))
					require.NoError(t, err, text)
					return got
				}
				// txOf reads the one Tx the latest view binds for a pattern.
				txOf := func(text string) datalog.ElementID {
					t.Helper()
					got := answers(db, text)
					require.Len(t, got, 1, "%s binds one Tx", text)
					id, ok := datalog.DerefElementID(got[0][0])
					require.True(t, ok, "the transaction position binds an ElementID, got %T", got[0][0])
					return id
				}

				tx := db.NewTransaction()
				require.NoError(t, tx.Add(e, tags, "a"))
				require.NoError(t, tx.Add(e, list, "p"))
				first, err := tx.Commit()
				require.NoError(t, err)
				aFirst := txOf(`[:find ?t :where [#id "item:e" :item/tags ?v ?t] [(= ?v "a")]]`)
				listFirst := txOf(`[:find ?t :where [#id "item:e" :item/list ?v ?t]]`)

				tx2 := db.NewTransaction()
				require.NoError(t, tx2.Add(e, tags, "b"))
				require.NoError(t, tx2.Add(e, list, "q"))
				_, err = tx2.Commit()
				require.NoError(t, err)
				listSecond := txOf(`[:find ?t :where [#id "item:e" :item/list ?v ?t]]`)

				tx3 := db.NewTransaction()
				require.NoError(t, tx3.Add(e, tags, "a"))
				_, err = tx3.Commit()
				require.NoError(t, err)
				aAgain := txOf(`[:find ?t :where [#id "item:e" :item/tags ?v ?t] [(= ?v "a")]]`)
				require.NotEqual(t, aFirst, aAgain, "the member's second assertion is its own datom")

				eid := func(id datalog.ElementID) string { return fmt.Sprintf("#eid [%d %d]", id.Lamport, id.ReplicaID) }

				// The latest view: the member "a" resolves to its second
				// assertion, and the vector to both elements under the second
				// commit's operation.
				assert.Empty(t, answers(db, fmt.Sprintf(`[:find ?v :where [?e :item/tags ?v %s]]`, eid(aFirst))), "latest: the members \"a\"'s first assertion wrote")
				assert.ElementsMatch(t, [][]interface{}{{"a"}}, answers(db, fmt.Sprintf(`[:find ?v :where [?e :item/tags ?v %s]]`, eid(aAgain))), "latest: the members \"a\"'s second assertion wrote")
				assert.Empty(t, answers(db, fmt.Sprintf(`[:find ?e :where [?e :item/tags "b" %s]]`, eid(aFirst))), "latest: the entities holding \"b\" under \"a\"'s first Tx")
				assert.ElementsMatch(t, [][]interface{}{{tags, "a"}}, answers(db, fmt.Sprintf(`[:find ?a ?v :where [?e ?a ?v %s]]`, eid(aAgain))), "latest: the attribute free under \"a\"'s second Tx")
				assert.ElementsMatch(t, [][]interface{}{{[]string{"p", "q"}}}, answers(db, fmt.Sprintf(`[:find ?v :where [?e :item/list ?v %s]]`, eid(listSecond))), "latest: the vector under its second operation")
				assert.Empty(t, answers(db, fmt.Sprintf(`[:find ?v :where [?e :item/list ?v %s]]`, eid(listFirst))), "latest: the vector under its first operation")

				// As of the first commit: the member's first assertion and the
				// one-element vector are the resolved datoms.
				asOf := db.AsOf(first)
				assert.ElementsMatch(t, [][]interface{}{{"a"}}, answers(asOf, fmt.Sprintf(`[:find ?v :where [?e :item/tags ?v %s]]`, eid(aFirst))), "as of the first commit: the members \"a\"'s first assertion wrote")
				assert.ElementsMatch(t, [][]interface{}{{[]string{"p"}}}, answers(asOf, fmt.Sprintf(`[:find ?v :where [?e :item/list ?v %s]]`, eid(listFirst))), "as of the first commit: the vector under its first operation")
			})
		}
	}
}
