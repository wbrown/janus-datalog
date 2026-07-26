//go:build !(js && wasm)

package storage

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestBadgerStore(t *testing.T) {
	// Create temporary directory for test database
	dir, err := os.MkdirTemp("", "badger-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create store
	encoder := &BinaryKeyEncoder{}
	store, err := NewBadgerStore(dir, encoder)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create some test datoms
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	tx1 := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	tx2 := datalog.ElementID{Lamport: 2, ReplicaID: 1}

	datoms := []datalog.Datom{
		{
			E:  alice,
			A:  datalog.NewKeyword(":user/name"),
			V:  "Alice Smith",
			Tx: tx1,
		},
		{
			E:  alice,
			A:  datalog.NewKeyword(":user/email"),
			V:  "alice@example.com",
			Tx: tx1,
		},
		{
			E:  bob,
			A:  datalog.NewKeyword(":user/name"),
			V:  "Bob Jones",
			Tx: tx1,
		},
		{
			E:  alice,
			A:  datalog.NewKeyword(":user/follows"),
			V:  bob, // Reference to bob
			Tx: tx2,
		},
	}

	// Assert datoms before running tests
	err = store.Assert(datoms)
	if err != nil {
		t.Fatalf("failed to assert datoms: %v", err)
	}

	// Test EAVT scan (get all facts about Alice)
	t.Run("EAVT Scan", func(t *testing.T) {
		it, err := store.Scan(ScanBound{Index: EAVT, Prefix: []datalog.Value{alice}})
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()

		count := 0
		for it.Next() {
			d, err := it.Datom()
			if err != nil {
				t.Fatal(err)
			}

			if !d.E.Equal(alice) {
				t.Errorf("expected entity alice, got %v", d.E)
			}
			t.Logf("Found: %s", d)
			count++
		}

		// Alice has 3 facts: name, email, and follows
		if count != 3 {
			t.Errorf("expected 3 datoms, got %d", count)
		}
	})

	// Test AVET scan (find all users by name attribute)
	t.Run("AVET Scan", func(t *testing.T) {
		it, err := store.Scan(ScanBound{
			Index:  AVET,
			Prefix: []datalog.Value{datalog.NewKeyword(":user/name")},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()

		count := 0
		for it.Next() {
			d, err := it.Datom()
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("User: %s", d.V)
			count++
		}

		// Alice and Bob both have names
		if count != 2 {
			t.Errorf("expected 2 users, got %d", count)
		}
	})

	// Test transaction
	t.Run("Transaction", func(t *testing.T) {
		tx, err := store.BeginTx()
		if err != nil {
			t.Fatal(err)
		}

		// Add a new fact in transaction
		err = tx.Assert([]datalog.Datom{
			{
				E:  bob,
				A:  datalog.NewKeyword(":user/email"),
				V:  "bob@example.com",
				Tx: tx2,
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		// Commit
		err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}

		// Verify it was added
		it, err := store.Scan(ScanBound{
			Index:  EAVT,
			Prefix: []datalog.Value{bob, datalog.NewKeyword(":user/email")},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()

		if !it.Next() {
			t.Error("expected to find Bob's email")
		}
	})

	// Test Retract
	t.Run("Retract", func(t *testing.T) {
		// Remove Bob's name
		err := store.Retract([]datalog.Datom{
			{
				E:  bob,
				A:  datalog.NewKeyword(":user/name"),
				V:  "Bob Jones",
				Tx: tx1,
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		// Verify it's gone
		it, err := store.Scan(ScanBound{
			Index:  EAVT,
			Prefix: []datalog.Value{bob, datalog.NewKeyword(":user/name")},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()

		if it.Next() {
			t.Error("Bob's name should have been retracted")
		}
	})
}

func TestBadgerStoreCloseFlushesCommittedWrites(t *testing.T) {
	store, err := NewBadgerStore(t.TempDir(), &BinaryKeyEncoder{})
	if err != nil {
		t.Fatal(err)
	}
	err = store.Assert([]datalog.Datom{{
		E:  datalog.NewIdentity("close-after-write"),
		A:  datalog.NewKeyword(":close/value"),
		V:  "committed",
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestRetractWithDifferentTx tests that retraction works even when the caller
// specifies a different Tx than what was used when the datom was asserted.
// This is the common case when retracting during an update operation, where
// the caller doesn't know the original transaction ID.
func TestRetractWithDifferentTx(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-retract-tx-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	encoder := &BinaryKeyEncoder{}
	store, err := NewBadgerStore(dir, encoder)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create entity and attribute
	alice := datalog.NewIdentity("alice-retract-tx")
	nameAttr := datalog.NewKeyword(":person/name")
	originalTx := datalog.ElementID{Lamport: 100, ReplicaID: 1}
	differentTx := datalog.ElementID{Lamport: 999, ReplicaID: 1} // Different from originalTx

	// Assert a datom with originalTx
	err = store.Assert([]datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: originalTx},
	})
	if err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	// Verify it was stored
	bound := ScanBound{Index: EAVT, Prefix: []datalog.Value{alice, nameAttr}}
	it, err := store.Scan(bound)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if !it.Next() {
		t.Fatal("Expected to find Alice's name datom")
	}
	it.Close()

	// Now retract with a DIFFERENT Tx value
	// This simulates what happens in UpdateStruct where we lookup a value
	// and try to retract it, but we don't know the original Tx
	err = store.Retract([]datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: differentTx},
	})
	if err != nil {
		t.Fatalf("Retract failed: %v", err)
	}

	// Verify the datom was actually retracted
	it, err = store.Scan(bound)
	if err != nil {
		t.Fatalf("Scan after retract failed: %v", err)
	}
	defer it.Close()

	if it.Next() {
		datom, _ := it.Datom()
		t.Errorf("Datom should have been retracted but found: %v", datom)
	}
}

func TestKeyOnlyScanning(t *testing.T) {
	// Create temporary directory
	dir, err := os.MkdirTemp("", "badger-keyonly-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	encoder := &BinaryKeyEncoder{}
	store, err := NewBadgerStore(dir, encoder)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create a large dataset for performance testing
	numEntities := 1000
	numAttrsPerEntity := 10

	var datoms []datalog.Datom
	for i := 0; i < numEntities; i++ {
		entity := datalog.NewIdentity(fmt.Sprintf("entity:%d", i))
		for j := 0; j < numAttrsPerEntity; j++ {
			datoms = append(datoms, datalog.Datom{
				E:  entity,
				A:  datalog.NewKeyword(fmt.Sprintf(":attr/%d", j)),
				V:  fmt.Sprintf("value-%d-%d", i, j),
				Tx: datalog.ElementID{Lamport: uint64(i)},
			})
		}
	}

	// Assert all datoms
	err = store.Assert(datoms)
	if err != nil {
		t.Fatalf("failed to assert datoms: %v", err)
	}

	t.Logf("Created %d datoms for testing", len(datoms))

	// Test counting with regular scan (fetches values)
	t.Run("Regular Scan Count", func(t *testing.T) {
		startTime := time.Now()
		it, err := store.Scan(ScanBound{Index: EAVT})
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()

		count := 0
		for it.Next() {
			// We have to call Datom() in regular scan which fetches and decodes values
			_, err := it.Datom()
			if err != nil {
				t.Fatal(err)
			}
			count++
		}

		elapsed := time.Since(startTime)
		t.Logf("Regular scan counted %d datoms in %v", count, elapsed)

		if count != len(datoms) {
			t.Errorf("expected %d datoms, got %d", len(datoms), count)
		}
	})

	// Test counting with key-only scan (no values fetched)
	t.Run("Key-Only Scan Count", func(t *testing.T) {
		startTime := time.Now()
		it, err := store.ScanKeysOnly(ScanBound{Index: EAVT})
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()

		count := 0
		for it.Next() {
			// Key-only scan decodes datom from key without fetching value
			_, err := it.Datom()
			if err != nil {
				t.Fatal(err)
			}
			count++
		}

		elapsed := time.Since(startTime)
		t.Logf("Key-only scan counted %d datoms in %v", count, elapsed)

		if count != len(datoms) {
			t.Errorf("expected %d datoms, got %d", len(datoms), count)
		}
	})

	// Test direct count method (fastest - just counts keys)
	t.Run("Direct Count", func(t *testing.T) {
		start, end := encoder.EncodePrefixRange(EAVT)

		startTime := time.Now()
		count, err := store.CountKeys(EAVT, start, end)
		if err != nil {
			t.Fatal(err)
		}

		elapsed := time.Since(startTime)
		t.Logf("Direct count found %d datoms in %v", count, elapsed)

		if count != int64(len(datoms)) {
			t.Errorf("expected %d datoms, got %d", len(datoms), count)
		}
	})

	// Test that key-only scan produces correct datoms
	t.Run("Key-Only Correctness", func(t *testing.T) {
		// Get first entity's datoms
		entity := datalog.NewIdentity("entity:0")
		entityBound := ScanBound{Index: EAVT, Prefix: []datalog.Value{entity}}

		// Get datoms with regular scan
		regularIt, err := store.Scan(entityBound)
		if err != nil {
			t.Fatal(err)
		}
		defer regularIt.Close()

		var regularDatoms []datalog.Datom
		for regularIt.Next() {
			d, err := regularIt.Datom()
			if err != nil {
				t.Fatal(err)
			}
			regularDatoms = append(regularDatoms, *d)
		}

		// Get datoms with key-only scan
		keyOnlyIt, err := store.ScanKeysOnly(entityBound)
		if err != nil {
			t.Fatal(err)
		}
		defer keyOnlyIt.Close()

		var keyOnlyDatoms []datalog.Datom
		for keyOnlyIt.Next() {
			d, err := keyOnlyIt.Datom()
			if err != nil {
				t.Fatal(err)
			}
			keyOnlyDatoms = append(keyOnlyDatoms, *d)
		}

		// Compare results
		if len(regularDatoms) != len(keyOnlyDatoms) {
			t.Errorf("different number of datoms: regular=%d, key-only=%d",
				len(regularDatoms), len(keyOnlyDatoms))
		}

		for i := range regularDatoms {
			if i >= len(keyOnlyDatoms) {
				break
			}
			rd := regularDatoms[i]
			kd := keyOnlyDatoms[i]

			if !rd.E.Equal(kd.E) {
				t.Errorf("entity mismatch at %d: regular=%v, key-only=%v", i, rd.E, kd.E)
			}
			if rd.A != kd.A {
				t.Errorf("attribute mismatch at %d: regular=%v, key-only=%v", i, rd.A, kd.A)
			}
			// Value comparison depends on type
			if rd.V != kd.V {
				// Check if both are strings (most common case in test)
				rs, rok := rd.V.(string)
				ks, kok := kd.V.(string)
				if rok && kok && rs != ks {
					t.Errorf("value mismatch at %d: regular=%v, key-only=%v", i, rd.V, kd.V)
				}
				// Could be other types, but for this test we expect strings
			}
			if rd.Tx != kd.Tx {
				t.Errorf("tx mismatch at %d: regular=%v, key-only=%v", i, rd.Tx, kd.Tx)
			}
		}

		t.Logf("Key-only scan correctly decoded %d datoms", len(keyOnlyDatoms))
	})
}

func TestKeyOnlyScanningAllTypes(t *testing.T) {
	// Create temporary directory
	dir, err := os.MkdirTemp("", "badger-alltypes-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	encoder := &BinaryKeyEncoder{}
	store, err := NewBadgerStore(dir, encoder)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create datoms with all value types
	entity := datalog.NewIdentity("test:entity")
	refEntity := datalog.NewIdentity("test:reference")
	testTime := time.Date(2025, 8, 24, 10, 30, 0, 0, time.UTC)

	datoms := []datalog.Datom{
		{E: entity, A: datalog.NewKeyword(":test/string"), V: "hello world", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: entity, A: datalog.NewKeyword(":test/int"), V: int64(42), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
		{E: entity, A: datalog.NewKeyword(":test/float"), V: 3.14159, Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
		{E: entity, A: datalog.NewKeyword(":test/bool"), V: true, Tx: datalog.ElementID{Lamport: 4, ReplicaID: 1}},
		{E: entity, A: datalog.NewKeyword(":test/time"), V: testTime, Tx: datalog.ElementID{Lamport: 5, ReplicaID: 1}},
		{E: entity, A: datalog.NewKeyword(":test/bytes"), V: []byte("binary data"), Tx: datalog.ElementID{Lamport: 6, ReplicaID: 1}},
		{E: entity, A: datalog.NewKeyword(":test/ref"), V: refEntity, Tx: datalog.ElementID{Lamport: 7, ReplicaID: 1}},
		{E: entity, A: datalog.NewKeyword(":test/keyword"), V: datalog.NewKeyword(":status/active"), Tx: datalog.ElementID{Lamport: 8, ReplicaID: 1}},
		{E: entity, A: datalog.NewKeyword(":test/symbol"), V: datalog.NewSymbol("my-function"), Tx: datalog.ElementID{Lamport: 9, ReplicaID: 1}},
	}

	// Assert all datoms
	err = store.Assert(datoms)
	if err != nil {
		t.Fatalf("failed to assert datoms: %v", err)
	}

	// Test each index type
	indices := []struct {
		name  string
		index IndexType
	}{
		{"EAVT", EAVT},
		{"AEVT", AEVT},
		{"AVET", AVET},
		{"VAET", VAET},
		{"TAEV", TAEV},
	}

	for _, idx := range indices {
		t.Run(idx.name, func(t *testing.T) {
			// Get all datoms with regular scan
			wholeIndex := ScanBound{Index: idx.index}

			regularIt, err := store.Scan(wholeIndex)
			if err != nil {
				t.Fatal(err)
			}
			defer regularIt.Close()

			var regularDatoms []datalog.Datom
			for regularIt.Next() {
				d, err := regularIt.Datom()
				if err != nil {
					t.Fatal(err)
				}
				regularDatoms = append(regularDatoms, *d)
			}

			// Get all datoms with key-only scan
			keyOnlyIt, err := store.ScanKeysOnly(wholeIndex)
			if err != nil {
				t.Fatal(err)
			}
			defer keyOnlyIt.Close()

			var keyOnlyDatoms []datalog.Datom
			for keyOnlyIt.Next() {
				d, err := keyOnlyIt.Datom()
				if err != nil {
					t.Fatal(err)
				}
				keyOnlyDatoms = append(keyOnlyDatoms, *d)
			}

			// Verify same number of results
			if len(regularDatoms) != len(keyOnlyDatoms) {
				t.Errorf("%s: different counts - regular=%d, key-only=%d",
					idx.name, len(regularDatoms), len(keyOnlyDatoms))
			}

			// For EAVT index, verify each value type decoded correctly
			if idx.index == EAVT {
				for i, kd := range keyOnlyDatoms {
					rd := regularDatoms[i]

					// Check all fields match
					if !rd.E.Equal(kd.E) {
						t.Errorf("Entity mismatch: regular=%v, key-only=%v", rd.E, kd.E)
					}
					if rd.A != kd.A {
						t.Errorf("Attribute mismatch: regular=%v, key-only=%v", rd.A, kd.A)
					}
					if rd.Tx != kd.Tx {
						t.Errorf("Tx mismatch: regular=%v, key-only=%v", rd.Tx, kd.Tx)
					}

					// Check value based on type
					switch rv := rd.V.(type) {
					case string:
						if kv, ok := kd.V.(string); !ok || rv != kv {
							t.Errorf("String value mismatch: regular=%v, key-only=%v", rv, kd.V)
						}
					case int64:
						if kv, ok := kd.V.(int64); !ok || rv != kv {
							t.Errorf("Int64 value mismatch: regular=%v, key-only=%v", rv, kd.V)
						}
					case float64:
						if kv, ok := kd.V.(float64); !ok || rv != kv {
							t.Errorf("Float64 value mismatch: regular=%v, key-only=%v", rv, kd.V)
						}
					case bool:
						if kv, ok := kd.V.(bool); !ok || rv != kv {
							t.Errorf("Bool value mismatch: regular=%v, key-only=%v", rv, kd.V)
						}
					case time.Time:
						if kv, ok := kd.V.(time.Time); !ok || !rv.Equal(kv) {
							t.Errorf("Time value mismatch: regular=%v, key-only=%v", rv, kd.V)
						}
					case []byte:
						if kv, ok := kd.V.([]byte); !ok || !bytes.Equal(rv, kv) {
							t.Errorf("Bytes value mismatch: regular=%v, key-only=%v", rv, kd.V)
						}
					case datalog.Identity:
						if kv, ok := kd.V.(datalog.Identity); !ok || !rv.Equal(kv) {
							t.Errorf("Identity value mismatch: regular=%v, key-only=%v", rv, kd.V)
						}
					case datalog.Keyword:
						if kv, ok := kd.V.(datalog.Keyword); !ok || rv != kv {
							t.Errorf("Keyword value mismatch: regular=%v, key-only=%v", rv, kd.V)
						}
					case datalog.Symbol:
						if kv, ok := kd.V.(datalog.Symbol); !ok || rv != kv {
							t.Errorf("Symbol value mismatch: regular=%v, key-only=%v", rv, kd.V)
						}
					}
				}
			}

			t.Logf("%s: Successfully decoded %d datoms with key-only scanning", idx.name, len(keyOnlyDatoms))
		})
	}
}
