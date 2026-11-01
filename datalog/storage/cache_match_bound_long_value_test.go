package storage

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A string or byte slice at or past the compression threshold is one value
// under datalog.ValuesEqual however the store encodes it, so a binding of an
// entity and such a value joins the entity's cardinality-many datom holding
// it, as a binding of a short value does (BOUND_VALUE_CACHE_MATCH_PROOF.md T1,
// T2, P6: one key per value under ValuesEqual).
func TestABindingOfALongValueJoinsTheManyDatomHoldingIt(t *testing.T) {
	declared := schema.NewBuilder().
		Attribute(":folio/name").Type(schema.TypeString).One().Add().
		Attribute(":folio/note").Type(schema.TypeString).Many().Add().
		Attribute(":folio/lead").Type(schema.TypeString).One().Add().
		Attribute(":folio/blob").Type(schema.TypeBytes).Many().Add().
		MustBuild()

	name := datalog.NewKeyword(":folio/name")
	note := datalog.NewKeyword(":folio/note")
	lead := datalog.NewKeyword(":folio/lead")
	blob := datalog.NewKeyword(":folio/blob")

	short := "a short note"
	long := strings.Repeat("the quire is sewn and folded. ", 40) // compressed into the key
	var huge strings.Builder                                    // compressed past the key, into a blob
	for i := 0; huge.Len() < 400000; i++ {
		fmt.Fprintf(&huge, "%d:%d;", i, (i*2654435761)%1000003)
	}
	hugeText := huge.String()
	longBytes := []byte(long)
	hugeBytes := []byte(hugeText)
	// noise is a byte slice past the threshold that compression does not
	// shrink, so the encoder stores it raw; the precondition below holds the
	// case to that tier.
	noise := make([]byte, 1024)
	x := uint64(0x9E3779B97F4A7C15)
	for i := range noise {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		noise[i] = byte(x)
	}
	noiseTag, _, _ := datalog.EncodeValue(noise, 512)
	require.Equal(t, datalog.TypeBytes, noiseTag, "the noise must be stored raw for its case to carry the fallback tier")

	quire := datalog.NewIdentity("folio:quire")
	sheet := datalog.NewIdentity("folio:sheet")
	facts := []datalog.Datom{
		{E: quire, A: name, V: "quire"},
		{E: sheet, A: name, V: "sheet"},
		{E: quire, A: note, V: short},
		{E: quire, A: note, V: long},
		{E: quire, A: note, V: hugeText},
		{E: sheet, A: note, V: "another note"},
		{E: quire, A: lead, V: long},
		{E: sheet, A: lead, V: long},
		{E: quire, A: blob, V: longBytes},
		{E: quire, A: blob, V: hugeBytes},
		{E: quire, A: blob, V: noise},
	}

	cases := []struct {
		name   string
		text   string
		inputs []interface{}
		want   []datalog.Identity
	}{
		{"a relation input binding a short member", `[:find ?e :in $ [[?e ?v]] :where [?e :folio/note ?v]]`,
			[]interface{}{[][]interface{}{{quire, short}, {sheet, short}}}, []datalog.Identity{quire}},
		{"a relation input binding a member compressed into the key", `[:find ?e :in $ [[?e ?v]] :where [?e :folio/note ?v]]`,
			[]interface{}{[][]interface{}{{quire, long}, {sheet, long}}}, []datalog.Identity{quire}},
		{"a relation input binding a member held in a blob", `[:find ?e :in $ [[?e ?v]] :where [?e :folio/note ?v]]`,
			[]interface{}{[][]interface{}{{quire, hugeText}, {sheet, hugeText}}}, []datalog.Identity{quire}},
		{"a relation input binding a byte-slice member compressed into the key", `[:find ?e :in $ [[?e ?v]] :where [?e :folio/blob ?v]]`,
			[]interface{}{[][]interface{}{{quire, longBytes}, {sheet, longBytes}}}, []datalog.Identity{quire}},
		{"a relation input binding a byte-slice member held in a blob", `[:find ?e :in $ [[?e ?v]] :where [?e :folio/blob ?v]]`,
			[]interface{}{[][]interface{}{{quire, hugeBytes}, {sheet, hugeBytes}}}, []datalog.Identity{quire}},
		{"a relation input binding a byte-slice member compression does not shrink", `[:find ?e :in $ [[?e ?v]] :where [?e :folio/blob ?v]]`,
			[]interface{}{[][]interface{}{{quire, noise}, {sheet, noise}}}, []datalog.Identity{quire}},
		{"a scalar entity and a scalar member compressed into the key", `[:find ?e :in $ ?e ?v :where [?e :folio/note ?v]]`,
			[]interface{}{quire, long}, []datalog.Identity{quire}},
		{"a tuple input binding an entity and a member compressed into the key", `[:find ?e :in $ [?e ?v] :where [?e :folio/note ?v]]`,
			[]interface{}{[]interface{}{quire, long}}, []datalog.Identity{quire}},
		{"a collection of entities with a scalar member compressed into the key", `[:find ?e :in $ [?e ...] ?v :where [?e :folio/note ?v]]`,
			[]interface{}{[]interface{}{quire, sheet}, long}, []datalog.Identity{quire}},
		{"a cardinality-one value joined to the member compressed into the key", `[:find ?e :where [?e :folio/lead ?v] [?e :folio/note ?v]]`,
			nil, []datalog.Identity{quire}},
		{"the members read whole", `[:find ?e :in $ ?v :where [?e :folio/note ?v]]`,
			[]interface{}{long}, []datalog.Identity{quire}},
	}

	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: declared, CompressionThreshold: 512, DisableCache: false})
			tx := db.NewTransaction()
			for _, fact := range facts {
				require.NoError(t, tx.Add(fact.E, fact.A, fact.V))
			}
			_, err := tx.Commit()
			require.NoError(t, err)

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					tuples, err := executor.CollectTuples(db.Query(tc.text, tc.inputs...))
					require.NoError(t, err)
					var got []datalog.Identity
					for _, tuple := range tuples {
						got = append(got, tuple[0].(datalog.Identity))
					}
					require.ElementsMatch(t, tc.want, got)
				})
			}
		})
	}
}

// The same binding under every view: the views differ only in which datoms
// they resolve (docs/reference/CRDT.md, "Three database modes").
func TestABindingOfALongValueJoinsTheManyDatomHoldingItUnderEveryView(t *testing.T) {
	declared := schema.NewBuilder().
		Attribute(":folio/note").Type(schema.TypeString).Many().Add().
		MustBuild()
	note := datalog.NewKeyword(":folio/note")
	long := strings.Repeat("the quire is sewn and folded. ", 40)
	quire := datalog.NewIdentity("folio:quire")
	sheet := datalog.NewIdentity("folio:sheet")

	text := `[:find ?e :in $ [[?e ?v]] :where [?e :folio/note ?v]]`
	bound := [][]interface{}{{quire, long}, {sheet, long}}

	cases := []struct {
		name string
		view func(db *Database, t1 datalog.ElementID) *Database
		want []datalog.Identity
	}{
		{"the current view", func(db *Database, _ datalog.ElementID) *Database { return db }, []datalog.Identity{quire, sheet}},
		{"as of the first transaction", func(db *Database, t1 datalog.ElementID) *Database { return db.AsOf(t1) }, []datalog.Identity{quire}},
		{"the history", func(db *Database, _ datalog.ElementID) *Database { return db.History() }, []datalog.Identity{quire, sheet}},
	}

	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: declared, CompressionThreshold: 512})
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(quire, note, long))
			require.NoError(t, tx.Add(sheet, note, "short"))
			t1, err := tx.Commit()
			require.NoError(t, err)
			tx = db.NewTransaction()
			require.NoError(t, tx.Add(sheet, note, long))
			_, err = tx.Commit()
			require.NoError(t, err)
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					tuples, err := executor.CollectTuples(tc.view(db, t1).Query(text, bound))
					require.NoError(t, err)
					var got []datalog.Identity
					for _, tuple := range tuples {
						got = append(got, tuple[0].(datalog.Identity))
					}
					require.ElementsMatch(t, tc.want, got)
				})
			}
		})
	}
}
