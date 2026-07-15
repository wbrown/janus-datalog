//go:build !(js && wasm)

package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func vBoundMatchCountWithAnnotations(t *testing.T, db *Database, a datalog.Keyword, v interface{}) int {
	t.Helper()
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(db.Schema())

	matcher.SetHandler(func(event annotations.Event) {
		t.Logf("EVENT: %s  data=%v", event.Name, event.Data)
	})

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: a},
			query.Constant{Value: v},
			query.Blank{},
		},
	}

	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	count := 0
	iter := results.Iterator()
	for iter.Next() {
		t.Logf("RESULT tuple: (got a result)")
		count++
	}
	iter.Close()
	t.Logf("TOTAL results: %d", count)
	return count
}

func TestDiag_VBound_AfterOverwrite(t *testing.T) {
	dir, err := os.MkdirTemp("", "vbound-diag-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/name"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})
	db.SetSchema(s)

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	tx1, err := tx.Commit()
	require.NoError(t, err)
	t.Logf("TX1 (Add Alice): Lamport=%d", tx1)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(e, a, "Bob"))
	tx2id, err := tx2.Commit()
	require.NoError(t, err)
	t.Logf("TX2 (Add Bob): Lamport=%d", tx2id)

	tx3 := db.NewTransaction()
	require.NoError(t, tx3.Remove(e, a, "Bob"))
	tx3id, err := tx3.Commit()
	require.NoError(t, err)
	t.Logf("TX3 (Remove Bob): Lamport=%d", tx3id)

	t.Log("--- Query V=Bob (expect 0) ---")
	bobCount := vBoundMatchCountWithAnnotations(t, db, a, "Bob")
	t.Logf("Bob count: %d (expected 0)", bobCount)

	t.Log("--- Query V=Alice (expect 0) ---")
	aliceCount := vBoundMatchCountWithAnnotations(t, db, a, "Alice")
	t.Logf("Alice count: %d (expected 0)", aliceCount)

	if bobCount != 0 {
		t.Errorf("V-bound: Bob should not match after Remove, got %d", bobCount)
	}
	if aliceCount != 0 {
		t.Errorf("V-bound: Alice should not match after Remove, got %d", aliceCount)
	}
}

func TestDiag_VBound_VIsIrrelevant(t *testing.T) {
	dir, err := os.MkdirTemp("", "vbound-diag-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/name"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})
	db.SetSchema(s)

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	tx1, err := tx.Commit()
	require.NoError(t, err)
	t.Logf("TX1 (Add Alice): Lamport=%d", tx1)

	// Remove with different V
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Bob"))
	tx2id, err := tx2.Commit()
	require.NoError(t, err)
	t.Logf("TX2 (Remove Bob): Lamport=%d", tx2id)

	t.Log("--- Query V=Alice (expect 0, attribute tombstoned) ---")
	aliceCount := vBoundMatchCountWithAnnotations(t, db, a, "Alice")
	t.Logf("Alice count: %d (expected 0)", aliceCount)

	if aliceCount != 0 {
		t.Errorf("V-bound: Alice should not match — attribute tombstoned regardless of Remove V, got %d", aliceCount)
	}

	// Also dump a raw EATV scan for this (E, A) to see what's in storage
	t.Log("--- Raw EATV scan for (alice, :person/name) ---")
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(db.Schema())
	sd := ToStorageDatom(datalog.Datom{E: e, A: a})
	encoder := db.Store().Encoder()
	start, end := encoder.EncodePrefixRange(EATV, sd.E[:], sd.A[:])
	iter, err := db.Store().Scan(EATV, start, end)
	require.NoError(t, err)
	defer iter.Close()
	i := 0
	for iter.Next() {
		d, err := iter.Datom()
		if err != nil {
			t.Logf("  EATV[%d]: decode error: %v", i, err)
			continue
		}
		t.Logf("  EATV[%d]: E=%s A=%s V=%v Tx={L:%d,R:%d} Op=%d",
			i, d.E.String(), d.A.String(), d.V, d.Tx.Lamport, d.Tx.ReplicaID, d.Op)
		i++
	}

	// Also dump raw AVET scan for (A, V=Alice)
	t.Log("--- Raw AVET scan for (:person/name, Alice) ---")
	dummyDatom := ToStorageDatom(datalog.Datom{E: e, A: a, V: "Alice"})
	vType := byte(datalog.Type(dummyDatom.V))
	vData := datalog.ValueBytes(dummyDatom.V)
	vBytes := append([]byte{vType}, vData...)
	start2, end2 := encoder.EncodePrefixRange(AVET, sd.A[:], vBytes)
	iter2, err := db.Store().Scan(AVET, start2, end2)
	require.NoError(t, err)
	defer iter2.Close()
	i = 0
	for iter2.Next() {
		d, err := iter2.Datom()
		if err != nil {
			t.Logf("  AVET[%d]: decode error: %v", i, err)
			continue
		}
		t.Logf("  AVET[%d]: E=%s A=%s V=%v Tx={L:%d,R:%d} Op=%d",
			i, d.E.String(), d.A.String(), d.V, d.Tx.Lamport, d.Tx.ReplicaID, d.Op)
		i++
	}

	// Also dump raw AVET scan for (A, V=Bob) to see tombstone
	t.Log("--- Raw AVET scan for (:person/name, Bob) ---")
	dummyDatom2 := ToStorageDatom(datalog.Datom{E: e, A: a, V: "Bob"})
	vType2 := byte(datalog.Type(dummyDatom2.V))
	vData2 := datalog.ValueBytes(dummyDatom2.V)
	vBytes2 := append([]byte{vType2}, vData2...)
	start3, end3 := encoder.EncodePrefixRange(AVET, sd.A[:], vBytes2)
	iter3, err := db.Store().Scan(AVET, start3, end3)
	require.NoError(t, err)
	defer iter3.Close()
	i = 0
	for iter3.Next() {
		d, err := iter3.Datom()
		if err != nil {
			t.Logf("  AVET[%d]: E=%s A=%s V=%v Tx={L:%d,R:%d} Op=%d",
				i, d.E.String(), d.A.String(), d.V, d.Tx.Lamport, d.Tx.ReplicaID, d.Op)
			continue
		}
		t.Logf("  AVET[%d]: E=%s A=%s V=%v Tx={L:%d,R:%d} Op=%d",
			i, d.E.String(), d.A.String(), d.V, d.Tx.Lamport, d.Tx.ReplicaID, d.Op)
		i++
	}

	_ = fmt.Sprintf("done") // keep fmt import
}
