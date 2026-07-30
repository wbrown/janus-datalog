package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Pins for the scan-boundary set invariant: a pattern scan whose datom→tuple
// projection drops part of the emitted stream's candidate key must restore
// set semantics at birth (restoreScanSetSemantics). The candidate key is
// {E, A} for effective cardinality-one and vector resolution, {E, A, V} for
// declared cardinality-many or non-constant A, and {Tx} in history mode
// (every datom carries its own ElementID). Historically these duplicates were
// laundered by the find projection's unconditional dedup; projections that
// provably preserve set-ness no longer dedup, so the scan must be a set.

func TestScanProjectionPreservesSet(t *testing.T) {
	eVar := query.Variable{Name: datalog.NewSymbol("?e")}
	aVar := query.Variable{Name: datalog.NewSymbol("?a")}
	vVar := query.Variable{Name: datalog.NewSymbol("?v")}
	txVar := query.Variable{Name: datalog.NewSymbol("?tx")}
	oneAttr := datalog.NewKeyword(":scan/one")
	manyAttr := datalog.NewKeyword(":scan/many")
	oneConst := query.Constant{Value: oneAttr}
	manyConst := query.Constant{Value: manyAttr}

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       oneAttr,
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})
	s.Add(&schema.AttributeDefinition{
		Ident:       manyAttr,
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityMany,
	})

	pattern := func(elems ...query.PatternElement) *query.DataPattern {
		return &query.DataPattern{Elements: elems}
	}

	// Current/as-of mode: resolution emits one tuple per (E, A) group for
	// effective cardinality-one, one per (E, A, V) for declared many.
	require.True(t, scanProjectionPreservesSet(pattern(eVar, oneConst, vVar), s, false),
		"declared one, E and V variables: {E, A} covered")
	require.True(t, scanProjectionPreservesSet(pattern(eVar, oneConst, query.Blank{}), s, false),
		"declared one, V wildcard: LWW emits one tuple per (E, A)")
	require.False(t, scanProjectionPreservesSet(pattern(query.Blank{}, oneConst, vVar), s, false),
		"E wildcard drops a key component")
	require.False(t, scanProjectionPreservesSet(pattern(eVar, manyConst, query.Blank{}), s, false),
		"declared many, V wildcard: several members project to one (E) tuple")
	require.True(t, scanProjectionPreservesSet(pattern(eVar, manyConst, vVar), s, false),
		"declared many with V variable: {E, A, V} covered")
	require.False(t, scanProjectionPreservesSet(pattern(eVar, aVar, query.Blank{}), s, false),
		"A variable: cardinality varies per attribute, the conservative key includes V")
	require.True(t, scanProjectionPreservesSet(pattern(eVar, aVar, vVar), s, false),
		"A variable with V covered: {E, A, V} is a superkey in every cardinality")
	require.True(t, scanProjectionPreservesSet(pattern(eVar, oneConst, query.Blank{}), nil, false),
		"schemaless: the resolver defaults to LWW, one tuple per (E, A)")

	// History mode: raw operation records, each with its own ElementID —
	// Tx alone is a candidate key.
	require.True(t, scanProjectionPreservesSet(pattern(eVar, oneConst, vVar, txVar), s, true),
		"history with Tx variable: every datom has its own ElementID")
	require.False(t, scanProjectionPreservesSet(pattern(eVar, oneConst, vVar), s, true),
		"history without Tx: re-assertions of (E, A, V) project to identical tuples")
	require.False(t, scanProjectionPreservesSet(pattern(eVar, oneConst, vVar, query.Blank{}), s, true),
		"history with Tx wildcard: same as absent")
}

// TestScanSetSemantics_DeclaredManyValueWildcard pins the {E, A, V} key leg:
// [?e :attr _] on a declared cardinality-many attribute projects the member
// set away, and an entity with several members must appear once.
func TestScanSetSemantics_DeclaredManyValueWildcard(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, nil)
			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":scan/tags"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityMany,
			})
			db.SetSchema(s)

			tagged := datalog.NewIdentity("scan:tagged")
			single := datalog.NewIdentity("scan:single")
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(tagged, datalog.NewKeyword(":scan/tags"), "alpha"))
			require.NoError(t, tx.Add(tagged, datalog.NewKeyword(":scan/tags"), "beta"))
			require.NoError(t, tx.Add(single, datalog.NewKeyword(":scan/tags"), "gamma"))
			_, err := tx.Commit()
			require.NoError(t, err)

			rel, err := db.Query(`[:find ?e :where [?e :scan/tags _]]`)
			require.NoError(t, err)
			tuples, err := executor.CollectTuples(rel, nil)
			require.NoError(t, err)
			require.Len(t, tuples, 2,
				"one tuple per entity: a multi-member set must not duplicate its entity")
		})
	}
}

// TestScanSetSemantics_HistoryReassertedValue pins the history {Tx} key:
// re-asserting the same (E, A, V) in a later transaction is a second
// operation record; the (E, V) projection is one binding, and adding ?tx
// distinguishes the two records.
func TestScanSetSemantics_HistoryReassertedValue(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, nil)
			e := datalog.NewIdentity("scan:reasserted")
			attr := datalog.NewKeyword(":scan/value")

			for i := 0; i < 2; i++ {
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(e, attr, "constant"))
				_, err := tx.Commit()
				require.NoError(t, err)
			}

			hist := db.History()
			rel, err := hist.Query(`[:find ?e ?v :where [?e :scan/value ?v]]`)
			require.NoError(t, err)
			tuples, err := executor.CollectTuples(rel, nil)
			require.NoError(t, err)
			require.Len(t, tuples, 1,
				"two operation records project to one (E, V) binding — a relation is a set")

			withTx, err := hist.Query(`[:find ?e ?v ?tx :where [?e :scan/value ?v ?tx]]`)
			require.NoError(t, err)
			txTuples, err := executor.CollectTuples(withTx, nil)
			require.NoError(t, err)
			require.Len(t, txTuples, 2, "Tx distinguishes the two operation records")
		})
	}
}
