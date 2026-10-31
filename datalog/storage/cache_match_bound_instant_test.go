package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// An instant is one value whatever zone or clock reading carries it
// (datalog.ValuesEqual; TestValuesEqualIsAnEquivalenceAndEqualValuesHashAlike),
// so a binding of an entity and an instant joins the entity's datom holding
// that instant. Under a constant attribute the cache arm reads the binding's
// value at the value position (docs/proposals/BOUND_VALUE_CACHE_MATCH_PROOF.md,
// T1, T2), and the query's answer is the one it gave with the arm reading the
// entity alone: the entities whose datom holds the bound instant, on a
// cardinality-many attribute as on a cardinality-one one.
func TestABindingOfAnInstantJoinsTheDatomHoldingIt(t *testing.T) {
	name := datalog.NewKeyword(":press/name")
	sewnAt := datalog.NewKeyword(":press/sewn-at")
	foldedAt := datalog.NewKeyword(":press/folded-at")
	declared := schema.NewBuilder().
		Attribute(":press/name").Type(schema.TypeString).One().Add().
		Attribute(":press/sewn-at").Type(schema.TypeInstant).Many().Add().
		Attribute(":press/folded-at").Type(schema.TypeInstant).One().Add().
		MustBuild()

	noon := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	dusk := time.Date(2026, 10, 29, 18, 0, 0, 0, time.UTC)
	east := time.FixedZone("east", 3*60*60)
	reading := time.Now()

	quire := datalog.NewIdentity("press:quire")
	sheet := datalog.NewIdentity("press:sheet")
	facts := []datalog.Datom{
		{E: quire, A: name, V: "quire"},
		{E: sheet, A: name, V: "sheet"},
		{E: quire, A: sewnAt, V: noon},
		{E: quire, A: sewnAt, V: reading},
		{E: sheet, A: sewnAt, V: dusk},
		{E: quire, A: foldedAt, V: noon.In(east)},
		{E: sheet, A: foldedAt, V: noon},
	}

	cases := []struct {
		name   string
		text   string
		inputs []interface{}
		want   []datalog.Identity
	}{
		{
			name:   "a relation input binding a cardinality-many instant in its own zone",
			text:   `[:find ?e :in $ [[?e ?at]] :where [?e :press/sewn-at ?at]]`,
			inputs: []interface{}{[][]interface{}{{quire, noon}, {sheet, noon}}},
			want:   []datalog.Identity{quire},
		},
		{
			name:   "a relation input binding a cardinality-many instant in another zone",
			text:   `[:find ?e :in $ [[?e ?at]] :where [?e :press/sewn-at ?at]]`,
			inputs: []interface{}{[][]interface{}{{quire, noon.In(east)}, {sheet, noon.In(east)}}},
			want:   []datalog.Identity{quire},
		},
		{
			name:   "a relation input binding a cardinality-many instant with the clock reading it was written with",
			text:   `[:find ?e :in $ [[?e ?at]] :where [?e :press/sewn-at ?at]]`,
			inputs: []interface{}{[][]interface{}{{quire, reading}, {sheet, reading}}},
			want:   []datalog.Identity{quire},
		},
		{
			name:   "a collection input of entities and a scalar instant in another zone",
			text:   `[:find ?e :in $ [?e ...] ?at :where [?e :press/sewn-at ?at]]`,
			inputs: []interface{}{[]interface{}{quire, sheet}, noon.In(east)},
			want:   []datalog.Identity{quire},
		},
		{
			name:   "a tuple input binding an entity and a cardinality-many instant in another zone",
			text:   `[:find ?e :in $ [?e ?at] :where [?e :press/sewn-at ?at]]`,
			inputs: []interface{}{[]interface{}{quire, noon.In(east)}},
			want:   []datalog.Identity{quire},
		},
		{
			name:   "a scalar entity and a scalar cardinality-many instant in another zone",
			text:   `[:find ?e :in $ ?e ?at :where [?e :press/sewn-at ?at]]`,
			inputs: []interface{}{quire, noon.In(east)},
			want:   []datalog.Identity{quire},
		},
		{
			name: "a cardinality-one instant written in another zone joined to the cardinality-many instant",
			text: `[:find ?e :where [?e :press/folded-at ?at] [?e :press/sewn-at ?at]]`,
			want: []datalog.Identity{quire},
		},
		{
			name:   "a relation input binding a cardinality-one instant in another zone",
			text:   `[:find ?e :in $ [[?e ?at]] :where [?e :press/folded-at ?at]]`,
			inputs: []interface{}{[][]interface{}{{quire, noon.In(east)}, {sheet, dusk}}},
			want:   []datalog.Identity{quire},
		},
	}

	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: declared})
			tx := db.NewTransaction()
			for _, fact := range facts {
				require.NoError(t, tx.Add(fact.E, fact.A, fact.V))
			}
			_, err := tx.Commit()
			require.NoError(t, err)

			names := func(entities []datalog.Identity) []string {
				var rendered []string
				for _, e := range entities {
					tuples, err := executor.CollectTuples(db.Query(`[:find ?n :in $ ?e :where [?e :press/name ?n]]`, e))
					require.NoError(t, err)
					require.Len(t, tuples, 1)
					rendered = append(rendered, tuples[0][0].(string))
				}
				return rendered
			}

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					tuples, err := executor.CollectTuples(db.Query(tc.text, tc.inputs...))
					require.NoError(t, err)
					var got []datalog.Identity
					for _, tuple := range tuples {
						got = append(got, tuple[0].(datalog.Identity))
					}
					if !sameIdentities(tc.want, got) {
						t.Errorf("want %v, got %v", names(tc.want), names(got))
					}
				})
			}
		})
	}
}

// sameIdentities reports whether two lists hold the same identities, each as
// many times.
func sameIdentities(want, got []datalog.Identity) bool {
	if len(want) != len(got) {
		return false
	}
	held := make(map[datalog.Identity]int, len(want))
	for _, e := range want {
		held[e]++
	}
	for _, e := range got {
		if held[e] == 0 {
			return false
		}
		held[e]--
	}
	return true
}
