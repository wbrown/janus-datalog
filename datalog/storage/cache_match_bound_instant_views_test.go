package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A binding of an entity and an instant joins the entity's datom holding that
// instant under every view of the database: an instant is one value whatever
// zone carries it (datalog.ValuesEqual), and the views differ only in which
// datoms they resolve (docs/reference/CRDT.md, "Three database modes").
func TestABindingOfAnInstantJoinsTheDatomHoldingItUnderEveryView(t *testing.T) {
	name := datalog.NewKeyword(":press/name")
	sewnAt := datalog.NewKeyword(":press/sewn-at")
	declared := schema.NewBuilder().
		Attribute(":press/name").Type(schema.TypeString).One().Add().
		Attribute(":press/sewn-at").Type(schema.TypeInstant).Many().Add().
		MustBuild()

	noon := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	dusk := time.Date(2026, 10, 29, 18, 0, 0, 0, time.UTC)
	east := time.FixedZone("east", 3*60*60)

	quire := datalog.NewIdentity("press:quire")
	sheet := datalog.NewIdentity("press:sheet")
	first := []datalog.Datom{
		{E: quire, A: name, V: "quire"},
		{E: sheet, A: name, V: "sheet"},
		{E: quire, A: sewnAt, V: noon},
		{E: sheet, A: sewnAt, V: dusk},
	}
	second := []datalog.Datom{
		{E: sheet, A: sewnAt, V: noon},
	}

	text := `[:find ?e :in $ [[?e ?at]] :where [?e :press/sewn-at ?at]]`
	bound := [][]interface{}{{quire, noon.In(east)}, {sheet, noon.In(east)}}

	cases := []struct {
		name string
		view func(db *Database, t1 datalog.ElementID) *Database
		want []datalog.Identity
	}{
		{
			name: "the current view",
			view: func(db *Database, _ datalog.ElementID) *Database { return db },
			want: []datalog.Identity{quire, sheet},
		},
		{
			name: "as of the first transaction",
			view: func(db *Database, t1 datalog.ElementID) *Database { return db.AsOf(t1) },
			want: []datalog.Identity{quire},
		},
		{
			name: "the history",
			view: func(db *Database, _ datalog.ElementID) *Database { return db.History() },
			want: []datalog.Identity{quire, sheet},
		},
	}

	for _, mode := range legsFor(backendModes) {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: declared})
			tx := db.NewTransaction()
			for _, fact := range first {
				require.NoError(t, tx.Add(fact.E, fact.A, fact.V))
			}
			t1, err := tx.Commit()
			require.NoError(t, err)
			tx = db.NewTransaction()
			for _, fact := range second {
				require.NoError(t, tx.Add(fact.E, fact.A, fact.V))
			}
			_, err = tx.Commit()
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
					tuples, err := executor.CollectTuples(tc.view(db, t1).Query(text, bound))
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
