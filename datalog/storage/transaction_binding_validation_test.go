package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// The transaction position of a data pattern is inhabited by ElementID alone,
// as the entity position is by Identity and the attribute position by Keyword.
// A value of another type is refused at the two user boundaries, a query-text
// constant at the match's entry and an :in input at the query's entry. A value
// joined into the position from elsewhere is the equality join's typed
// non-match: it contributes no tuple, never an error and never a panic, in
// every view and on every join route.

// openTransactionTypingDB opens a database under mode, cache on or off,
// holding first's :item/one "x" and :item/count 5 and second's :item/count 7,
// and answers first's assertion's ElementID.
func openTransactionTypingDB(t *testing.T, mode optimizerMode, disableCache bool) (*Database, datalog.ElementID) {
	t.Helper()
	one := datalog.NewKeyword(":item/one")
	count := datalog.NewKeyword(":item/count")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: one, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})

	db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: disableCache})
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(datalog.NewIdentity("item:first"), one, "x"))
	require.NoError(t, tx.Add(datalog.NewIdentity("item:first"), count, int64(5)))
	require.NoError(t, tx.Add(datalog.NewIdentity("item:second"), count, int64(7)))
	_, err := tx.Commit()
	require.NoError(t, err)

	asserted, err := executor.CollectTuples(db.Query(`[:find ?t :where [#id "item:first" :item/one "x" ?t]]`))
	require.NoError(t, err)
	require.Len(t, asserted, 1)
	at, ok := datalog.DerefElementID(asserted[0][0])
	require.True(t, ok, "the transaction position binds an ElementID, got %T", asserted[0][0])
	return db, at
}

// A constant at the transaction position that is not an ElementID is refused
// at the match's entry, whatever the other positions bind and in every view,
// and the refusal names the #eid literal. An integer is such a constant: the
// Lamport is a component of an ElementID, and an integer names no
// transaction. The #eid constant answers the assertion it names.
func TestATransactionConstantOfAnotherTypeIsRefusedAtTheMatchsEntry(t *testing.T) {
	first := datalog.NewIdentity("item:first")
	for _, mode := range legsFor(optimizerModes) {
		t.Run(mode.name, func(t *testing.T) {
			db, at := openTransactionTypingDB(t, mode, false)
			eid := fmt.Sprintf("#eid [%d %d]", at.Lamport, at.ReplicaID)

			for _, view := range []struct {
				name string
				db   *Database
			}{{"latest", db}, {"History", db.History()}} {
				answered, err := executor.CollectTuples(view.db.Query(fmt.Sprintf(`[:find ?e :where [?e ?a "x" %s]]`, eid)))
				require.NoError(t, err, "%s: the #eid constant", view.name)
				require.Equal(t, [][]interface{}{{first}}, answered, "%s: the #eid constant answers the assertion it names", view.name)

				for _, c := range []struct {
					name  string
					query string
				}{
					{"value constant, the integer", fmt.Sprintf(`[:find ?e :where [?e ?a "x" %d]]`, at.Lamport)},
					{"entity constant, the integer", fmt.Sprintf(`[:find ?v :where [#id "item:first" :item/one ?v %d]]`, at.Lamport)},
					{"attribute constant, the integer", fmt.Sprintf(`[:find ?e :where [?e :item/one ?v %d]]`, at.Lamport)},
					{"every other position free, the integer", fmt.Sprintf(`[:find ?e :where [?e ?a ?v %d]]`, at.Lamport)},
					{"every other position free, a string", `[:find ?e :where [?e ?a ?v "16"]]`},
				} {
					_, err := executor.CollectTuples(view.db.Query(c.query))
					require.Error(t, err, "%s, %s: a constant that is no ElementID at the transaction position is refused", view.name, c.name)
					assert.ErrorContains(t, err, "transaction position", "%s, %s", view.name, c.name)
					assert.ErrorContains(t, err, "#eid", "%s, %s: the refusal names the literal", view.name, c.name)
				}
			}
		})
	}
}

// An :in input bound to a variable at the transaction position, directly, as
// a collection or through a subquery's argument, is refused at the query's
// entry when it is not an ElementID, and an ElementID is accepted as a value
// or through a pointer.
func TestATransactionInputOfAnotherTypeIsRefusedAtTheQuerysEntry(t *testing.T) {
	first := datalog.NewIdentity("item:first")
	for _, mode := range legsFor(optimizerModes) {
		t.Run(mode.name, func(t *testing.T) {
			db, at := openTransactionTypingDB(t, mode, false)

			for _, c := range []struct {
				name string
				arg  interface{}
			}{{"a value", at}, {"a pointer", &at}} {
				answered, err := executor.CollectTuples(db.Query(`[:find ?e :in $ ?t :where [?e ?a "x" ?t]]`, c.arg))
				require.NoError(t, err, "an ElementID given as %s", c.name)
				require.Equal(t, [][]interface{}{{first}}, answered, "an ElementID given as %s answers the assertion it names", c.name)
			}

			for _, c := range []struct {
				name  string
				query string
				arg   interface{}
			}{
				{"a scalar input", `[:find ?e :in $ ?t :where [?e ?a "x" ?t]]`, int64(at.Lamport)},
				{"a collection input", `[:find ?e :in $ [?t ...] :where [?e ?a "x" ?t]]`, []interface{}{at, int64(at.Lamport)}},
				{"a subquery's argument", `[:find ?n :in $ ?t :where [(q [:find (count ?x) :in $ ?tx :where [?x ?a "x" ?tx]] $ ?t) [[?n]]]]`, int64(at.Lamport)},
			} {
				_, err := executor.CollectTuples(db.Query(c.query, c.arg))
				require.Error(t, err, "%s that is no ElementID at the transaction position is refused", c.name)
				assert.ErrorContains(t, err, "input ?t", c.name)
				assert.ErrorContains(t, err, "transaction position", c.name)
			}
		})
	}
}

// A value joined into the transaction position from another position is the
// equality join's typed non-match. The count 5 joined from the value position
// names no transaction, so a pattern reading it at the transaction position
// answers nothing, with no error and no panic, in every view, with the cache
// on and off. A binding holding an ElementID beside an integer and a string
// at the position joins by the ElementID alone, on the validating route of a
// resolving view and on the join route of History.
func TestAValueJoinedIntoTheTransactionPositionIsATypedNonMatch(t *testing.T) {
	first := datalog.NewIdentity("item:first")
	one := datalog.NewKeyword(":item/one")
	for _, mode := range legsFor(optimizerModes) {
		for _, cacheMode := range cacheTestModes {
			t.Run(mode.name+"/"+cacheMode.name, func(t *testing.T) {
				db, at := openTransactionTypingDB(t, mode, cacheMode.disableCache)

				for _, view := range []struct {
					name string
					db   *Database
				}{{"latest", db}, {"as of the assertion", db.AsOf(at)}, {"History", db.History()}} {
					joined, err := executor.CollectTuples(view.db.Query(`[:find ?b :where [?a :item/count 5] [?a :item/count ?i] [?b :item/count _ ?i]]`))
					require.NoError(t, err, "%s: a value joined into the transaction position is no error", view.name)
					assert.Empty(t, joined, "%s: the count 5 names no transaction", view.name)

					pattern := &query.DataPattern{Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?b")},
						query.Constant{Value: one},
						query.Blank{},
						query.Variable{Name: datalog.NewSymbol("?i")},
					}}
					mixed := executor.NewMaterializedRelation(
						[]query.Symbol{datalog.NewSymbol("?i")},
						[]executor.Tuple{{at}, {int64(at.Lamport)}, {"5"}},
					)
					tuples, err := executor.CollectTuples(view.db.Match(query.PatternQuery(pattern), executor.Relations{mixed}))
					require.NoError(t, err, "%s: a mixed binding at the transaction position is no error", view.name)
					require.Len(t, tuples, 1, "%s: the ElementID alone joins, answered %v", view.name, tuples)
					require.Equal(t, first, tuples[0][0], "%s: the entity whose assertion the ElementID names", view.name)
					bound, ok := datalog.DerefElementID(tuples[0][1])
					require.True(t, ok, "%s: the transaction position binds an ElementID, got %T", view.name, tuples[0][1])
					require.Equal(t, at, bound, "%s: the assertion's own ElementID", view.name)
				}
			})
		}
	}
}
