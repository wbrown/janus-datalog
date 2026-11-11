package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A cardinality-many member's transaction position in a resolving view is the
// Tx of the member's own newest assertion, as History reads the assertion and
// as the join routes bind it: through the set under a bound entity, a member's
// membership, every entity's set and every entity holding a value, with the
// cache on and off, on every leg. A bound T admits the member whose assertion
// it is and no other member of the set.
func TestAManyMembersTransactionPositionIsItsOwnAssertions(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":bindery/quires").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	quires := datalog.NewKeyword(":bindery/quires")
	caches := []struct {
		name    string
		disable bool
	}{{"cache on", false}, {"cache off", true}}
	for _, mode := range legsFor(optimizerModes) {
		for _, cache := range caches {
			t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{DisableCache: cache.disable})
				require.NoError(t, d.SetSchema(s))
				press := datalog.NewIdentity("bindery:press")
				loft := datalog.NewIdentity("bindery:loft")
				assert := func(entity datalog.Identity, value string) {
					tx := d.NewTransaction()
					require.NoError(t, tx.Add(entity, quires, value))
					_, err := tx.Commit()
					require.NoError(t, err)
				}
				assert(press, "folio")
				assert(press, "octavo")
				assert(loft, "folio")

				// History reads each assertion as written, with its own Tx.
				written, err := executor.CollectTuples(d.History().Query(`[:find ?e ?v ?t :where [?e :bindery/quires ?v ?t]]`))
				require.NoError(t, err)
				require.Len(t, written, 3, "three assertions were written")
				type member struct {
					entity datalog.Identity
					value  string
				}
				at := map[member]*datalog.ElementID{}
				for _, tuple := range written {
					at[member{tuple[0].(datalog.Identity), tuple[1].(string)}] = tuple[2].(*datalog.ElementID)
				}
				pressFolio := at[member{press, "folio"}]
				pressOctavo := at[member{press, "octavo"}]
				loftFolio := at[member{loft, "folio"}]
				require.NotNil(t, pressFolio)
				require.NotNil(t, pressOctavo)
				require.NotNil(t, loftFolio)
				eid := func(id *datalog.ElementID) string { return fmt.Sprintf("#eid [%d %d]", id.Lamport, id.ReplicaID) }

				cases := []struct {
					name string
					text string
					want [][]interface{}
				}{
					{"the set under a bound entity", `[:find ?v ?t :where [#id "bindery:press" :bindery/quires ?v ?t]]`,
						[][]interface{}{{"folio", pressFolio}, {"octavo", pressOctavo}}},
					{"a member's membership", `[:find ?t :where [#id "bindery:press" :bindery/quires "folio" ?t]]`,
						[][]interface{}{{pressFolio}}},
					{"every entity's set", `[:find ?e ?v ?t :where [?e :bindery/quires ?v ?t]]`,
						[][]interface{}{{press, "folio", pressFolio}, {press, "octavo", pressOctavo}, {loft, "folio", loftFolio}}},
					{"every entity holding a value", `[:find ?e ?t :where [?e :bindery/quires "folio" ?t]]`,
						[][]interface{}{{press, pressFolio}, {loft, loftFolio}}},
					{"a bound T admits the member it asserted", `[:find ?v :where [#id "bindery:press" :bindery/quires ?v ` + eid(pressFolio) + `]]`,
						[][]interface{}{{"folio"}}},
					{"a bound T admits no other member", `[:find ?added :where [#id "bindery:press" :bindery/quires "folio" ` + eid(pressOctavo) + ` ?added]]`,
						[][]interface{}{}},
					{"a bound T on the member's own assertion", `[:find ?added :where [#id "bindery:press" :bindery/quires "folio" ` + eid(pressFolio) + ` ?added]]`,
						[][]interface{}{{true}}},
				}
				for _, c := range cases {
					t.Run(c.name, func(t *testing.T) {
						got, err := executor.CollectTuples(d.Query(c.text))
						require.NoError(t, err)
						require.ElementsMatch(t, c.want, got, "a member's T is its own newest assertion's")
					})
				}

				t.Run("the join route binds the same", func(t *testing.T) {
					got, err := executor.CollectTuples(d.Query(`[:find ?v ?t :in $ ?e :where [?e :bindery/quires ?v ?t]]`, press))
					require.NoError(t, err)
					require.ElementsMatch(t, [][]interface{}{{"folio", pressFolio}, {"octavo", pressOctavo}}, got)
				})
			})
		}
	}
}

// A cardinality-many cache entry carries, under each member's key, the Tx of
// the member's own newest assertion beside the resolution's version. The
// press asserts "folio" and then "octavo" in two commits: "folio" carries its
// own assertion's Tx, and the version is the group's highest operation,
// "octavo"'s assertion (docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md,
// T11).
func TestACardinalityManyEntryCarriesEachMembersAssertionTx(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":bindery/quires").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	quires := datalog.NewKeyword(":bindery/quires")
	press := datalog.NewIdentity("bindery:press")
	for _, mode := range legsFor(optimizerModes) {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
			assert := func(value string) datalog.ElementID {
				tx := d.NewTransaction()
				require.NoError(t, tx.Add(press, quires, value))
				_, err := tx.Commit()
				require.NoError(t, err)
				got, err := executor.CollectTuples(d.History().Query(fmt.Sprintf(`[:find ?t :where [#id "bindery:press" :bindery/quires %q ?t]]`, value)))
				require.NoError(t, err)
				require.Len(t, got, 1, "%q is one assertion", value)
				id, ok := datalog.DerefElementID(got[0][0])
				require.True(t, ok, "the transaction position binds an ElementID, got %T", got[0][0])
				return id
			}
			folioTx := assert("folio")
			octavoTx := assert("octavo")

			matcher := d.Matcher().(*PatternMatcher)
			require.NotNil(t, matcher.cache, "the database runs with its EA cache")
			entry, err := matcher.cache.GetOrResolve(CacheKey{E: press, A: quires}, matcher.schema.GetAttribute(quires), matcher, matcher.cache.ReadPoint(), nil, DiscardIntake)
			require.NoError(t, err)
			require.NotNil(t, entry, "the pair carries datoms")

			memberTx := entry.ManyTx()
			require.Equal(t, folioTx, memberTx[memberKey("folio")], "a member's Tx is its own assertion's")
			require.Equal(t, octavoTx, memberTx[memberKey("octavo")], "a member's Tx is its own assertion's")
			require.Equal(t, octavoTx, entry.Version(), "the version is the group's highest operation")
		})
	}
}
