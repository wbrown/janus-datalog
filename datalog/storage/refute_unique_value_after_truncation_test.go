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

// Holder asserts the unique value "y" and the database snapshots; claimant
// asserts "y" after it, which the unique walk reads as superseding holder's
// claim. Every form is asked once, so the EA cache holds the walk's answer for
// holder. TruncateTo the snapshot erases claimant's assertion, and holder holds
// "y" again, under its own assertion's Tx: the attribute scan, the bound
// attribute with the value constant, and the database with the EA cache off
// answer it. Every form answers it with the cache on as well.
func TestRefuteAUniqueValueAnswersAfterTruncationErasesTheLaterClaim(t *testing.T) {
	code := datalog.NewKeyword(":item/code")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: code, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne, Unique: schema.UniqueIdentity})
	holder := datalog.NewIdentity("item:holder")
	claimant := datalog.NewIdentity("item:claimant")
	seeds := map[datalog.Identity]string{holder: "item:holder", claimant: "item:claimant"}

	for _, mode := range legsFor(optimizerModes, fusionOption) {
		for _, cacheMode := range cacheTestModes {
			t.Run(mode.name+"/"+cacheMode.name, func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: cacheMode.disableCache})
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(holder, code, "y"))
				_, err := tx.Commit()
				require.NoError(t, err)
				_, err = db.Snapshot("before the claim")
				require.NoError(t, err)

				asserted, err := executor.CollectTuples(db.History().Query(`[:find ?t :where [#id "item:holder" :item/code "y" ?t]]`))
				require.NoError(t, err)
				require.Len(t, asserted, 1)
				holderTx, ok := datalog.DerefElementID(asserted[0][0])
				require.True(t, ok)

				tx2 := db.NewTransaction()
				require.NoError(t, tx2.Add(claimant, code, "y"))
				_, err = tx2.Commit()
				require.NoError(t, err)

				render := func(tuples [][]interface{}) []string {
					out := []string{}
					for _, tuple := range tuples {
						line := ""
						for _, v := range tuple {
							switch x := v.(type) {
							case datalog.Identity:
								line += seeds[x] + " "
							default:
								if id, isTx := datalog.DerefElementID(v); isTx {
									if id == holderTx {
										line += "holder's assertion "
									} else {
										line += fmt.Sprintf("tx%d ", id.Lamport)
									}
								} else {
									line += fmt.Sprintf("%v ", v)
								}
							}
						}
						out = append(out, line)
					}
					return out
				}
				forms := []struct {
					name string
					text string
					args []interface{}
					want []string
				}{
					{"the attribute scan", `[:find ?e ?v ?t :where [?e :item/code ?v ?t]]`, nil, []string{"item:holder y holder's assertion "}},
					{"the bound attribute, the value constant", `[:find ?e ?t :where [?e :item/code "y" ?t]]`, nil, []string{"item:holder holder's assertion "}},
					{"the entity from an input", `[:find ?v ?t :in $ ?e :where [?e :item/code ?v ?t]]`, []interface{}{holder}, []string{"y holder's assertion "}},
					{"the entity constant", `[:find ?v ?t :where [#id "item:holder" :item/code ?v ?t]]`, nil, []string{"y holder's assertion "}},
					{"the attribute free, the value constant", `[:find ?e ?a ?t :where [?e ?a "y" ?t]]`, nil, []string{"item:holder :item/code holder's assertion "}},
					{"the attribute free, the value from an input", `[:find ?e ?a ?t :in $ ?v :where [?e ?a ?v ?t]]`, []interface{}{"y"}, []string{"item:holder :item/code holder's assertion "}},
					{"the attribute free, the transaction from an input", `[:find ?e ?a ?v :in $ ?t :where [?e ?a ?v ?t]]`, []interface{}{holderTx}, []string{"item:holder :item/code y "}},
					{"the bound attribute, the transaction from an input", `[:find ?e ?v :in $ ?t :where [?e :item/code ?v ?t]]`, []interface{}{holderTx}, []string{"item:holder y "}},
					{"the attribute free, the value from a collection", `[:find ?e ?a ?t :in $ [?v ...] :where [?e ?a ?v ?t]]`, []interface{}{[]interface{}{"y", "w"}}, []string{"item:holder :item/code holder's assertion "}},
					{"the attribute free, the transaction from a collection", `[:find ?e ?a ?v :in $ [?t ...] :where [?e ?a ?v ?t]]`, []interface{}{[]interface{}{holderTx}}, []string{"item:holder :item/code y "}},
					{"the bound attribute, the transaction from a collection", `[:find ?e ?v :in $ [?t ...] :where [?e :item/code ?v ?t]]`, []interface{}{[]interface{}{holderTx}}, []string{"item:holder y "}},
				}
				// Asked once before the truncation: the cache holds what the
				// walk settles with claimant's claim in place.
				for _, f := range forms {
					_, err := executor.CollectTuples(db.Query(f.text, f.args...))
					require.NoError(t, err, f.text)
				}
				pulled, err := db.Pull(holder, "[:item/code]")
				require.NoError(t, err)
				require.Nil(t, pulled["item/code"], "with claimant's later claim holder holds no value")

				require.NoError(t, db.TruncateTo("before the claim"))

				pulled, err = db.Pull(holder, "[:item/code]")
				require.NoError(t, err)
				assert.Equal(t, "y", pulled["item/code"], "pull of holder after the truncation")
				for _, f := range forms {
					got, err := executor.CollectTuples(db.Query(f.text, f.args...))
					require.NoError(t, err, f.text)
					assert.ElementsMatch(t, f.want, render(got), "after the truncation, %s: %s", f.name, f.text)
				}
			})
		}
	}
}

