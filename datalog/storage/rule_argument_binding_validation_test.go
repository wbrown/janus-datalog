package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// An :in input bound to a variable that reaches a typed position through a
// rule argument alone, the entity, attribute or transaction position of a
// pattern in the rule's body, of a rule that body invokes, directly or
// recursively, or of a body under a subquery's own %, is refused at the
// query's entry when its value is of another type, as an input reaching the
// position directly is, and an ElementID reaching the transaction position
// that way answers the assertion it names. An input reaching a body's value
// position alone is not refused: the pattern's typed non-match answers
// nothing.
func TestAnInputReachingATypedPositionThroughARuleArgumentIsRefused(t *testing.T) {
	first := datalog.NewIdentity("item:first")
	for _, mode := range legsFor(optimizerModes) {
		t.Run(mode.name, func(t *testing.T) {
			db, at := openTransactionTypingDB(t, mode, false)

			for _, c := range []struct {
				name     string
				query    string
				rules    string
				arg      interface{}
				position string
			}{
				{"the transaction position", `[:find ?e :in $ % ?t :where (at ?e ?t)]`, `[[(at ?e ?t) [?e :item/one "x" ?t]]]`, int64(at.Lamport), "transaction position"},
				{"the entity position", `[:find ?v :in $ % ?e :where (one ?e ?v)]`, `[[(one ?e ?v) [?e :item/one ?v]]]`, "item:first", "entity position"},
				{"the attribute position", `[:find ?v :in $ % ?a :where (has ?a ?v)]`, `[[(has ?a ?v) [?e ?a ?v]]]`, "item/one", "attribute position"},
				{"the transaction position of a rule the body invokes", `[:find ?e :in $ % ?t :where (at ?e ?t)]`, `[[(at ?e ?t) (atx ?e ?t)] [(atx ?e ?t) [?e :item/one "x" ?t]]]`, int64(at.Lamport), "transaction position"},
				{"the entity position of a recursive rule", `[:find ?b :in $ % ?x :where (reach ?x ?b)]`, `[[(reach ?a ?b) [?a :item/next ?b]] [(reach ?a ?c) [?a :item/next ?b] (reach ?b ?c)]]`, "item:first", "entity position"},
				{"the transaction position under a subquery's own %", `[:find ?e :in $ % ?t :where [(q [:find ?x :in $ % ?tx :where (at ?x ?tx)] $ % ?t) [[?e]]]]`, `[[(at ?e ?t) [?e :item/one "x" ?t]]]`, int64(at.Lamport), "transaction position"},
			} {
				_, err := executor.CollectTuples(db.Query(c.query, c.rules, c.arg))
				require.Error(t, err, "%s: an input of another type reaching it through a rule argument is refused", c.name)
				assert.ErrorContains(t, err, c.position, c.name)
			}

			answered, err := executor.CollectTuples(db.Query(`[:find ?e :in $ % ?t :where (at ?e ?t)]`, `[[(at ?e ?t) [?e :item/one "x" ?t]]]`, at))
			require.NoError(t, err, "an ElementID reaching the transaction position through a rule argument")
			require.Equal(t, [][]interface{}{{first}}, answered, "an ElementID reaching the transaction position through a rule argument answers the assertion it names")

			valued, err := executor.CollectTuples(db.Query(`[:find ?e :in $ % ?v :where (one ?e ?v)]`, `[[(one ?e ?v) [?e :item/one ?v]]]`, int64(5)))
			require.NoError(t, err, "an input reaching a value position alone is not refused")
			assert.Empty(t, valued, "the integer matches no :item/one value")
		})
	}
}
