package storage

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

var exportInstantPattern = regexp.MustCompile(`#inst "[^"]+"`)

func stabilizeExport(dump string) string {
	return exportInstantPattern.ReplaceAllString(dump, `#inst "stable"`)
}

func TestStoreBackendContract(t *testing.T) {
	for _, testCase := range storeContractCases() {
		t.Run(testCase.name, func(t *testing.T) {
			encoder := &BinaryKeyEncoder{CompressionThreshold: 64}
			store := testCase.open(t, encoder)
			defer store.Close()
			require.Same(t, encoder, store.Encoder())

			entity := datalog.NewIdentity("contract:entity")
			attr := datalog.NewKeyword(":contract/value")
			payload := make([]byte, 80*1024)
			_, err := rand.Read(payload)
			require.NoError(t, err)
			payload = append(payload, make([]byte, 80*1024)...)
			datoms := []datalog.Datom{
				{E: entity, A: attr, V: int64(1), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 9}},
				{E: entity, A: attr, V: payload, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 9}},
			}

			tx, err := store.BeginTx()
			require.NoError(t, err)
			require.NoError(t, tx.Assert(datoms))
			require.NoError(t, tx.Rollback())
			require.Zero(t, countStoreIndex(t, store, EAVT))

			tx, err = store.BeginTx()
			require.NoError(t, err)
			require.NoError(t, tx.Assert(datoms))
			require.NoError(t, tx.Commit())
			for _, index := range Indices {
				require.Equal(t, len(datoms), countStoreIndex(t, store, index), "index %v", index)
			}
			maxID, err := store.MaxElementID()
			require.NoError(t, err)
			require.Equal(t, datoms[1].Tx, maxID)
			require.NoError(t, store.SetMetadataUint64("contract", 42))
			value, found, err := store.GetMetadataUint64("contract")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, uint64(42), value)
			after, err := store.DatomsAfter(datoms[0].Tx)
			require.NoError(t, err)
			require.Len(t, after, 1)
			require.Equal(t, payload, after[0].V)
			deleted, err := store.DeleteDatoms(after)
			require.NoError(t, err)
			require.Equal(t, 1, deleted)
			require.Equal(t, 1, countStoreIndex(t, store, EAVT))
			require.NoError(t, store.Close())
			require.NoError(t, store.Close())
		})
	}
}

func TestStoreBackendOrderedScanAndSeek(t *testing.T) {
	for _, testCase := range storeContractCases() {
		t.Run(testCase.name, func(t *testing.T) {
			store := testCase.open(t, &BinaryKeyEncoder{})
			defer store.Close()
			first := datalog.NewIdentity("seek:aaa")
			second := datalog.NewIdentity("seek:zzz")
			attr := datalog.NewKeyword(":seek/value")
			require.NoError(t, store.Assert([]datalog.Datom{
				{E: second, A: attr, V: "second", Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
				{E: first, A: attr, V: "first", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			}))

			for _, index := range Indices {
				iter, err := store.ScanKeysOnly(index, []byte{byte(index)}, []byte{byte(index) + 1})
				require.NoError(t, err, "index %v", index)
				var keys [][]byte
				require.True(t, iter.Next(), "index %v", index)
				firstDatom, err := iter.Datom()
				require.NoError(t, err)
				_ = firstDatom
				require.True(t, iter.Next(), "index %v", index)
				require.False(t, iter.Next(), "index %v", index)
				require.NoError(t, iter.Error())
				require.NoError(t, iter.Close())

				// Seek past the first key on EAVT using a fresh iterator.
				if index != EAVT {
					continue
				}
				iter, err = store.ScanKeysOnly(EAVT, []byte{byte(EAVT)}, []byte{byte(EAVT) + 1})
				require.NoError(t, err)
				require.True(t, iter.Next())
				datom, err := iter.Datom()
				require.NoError(t, err)
				seekKey := store.Encoder().EncodeKey(EAVT, datom)
				keys = append(keys, append([]byte(nil), seekKey...))
				require.True(t, iter.Next())
				secondDatom, err := iter.Datom()
				require.NoError(t, err)
				secondKey := store.Encoder().EncodeKey(EAVT, secondDatom)
				iter.Seek(secondKey)
				require.True(t, iter.Next())
				afterSeek, err := iter.Datom()
				require.NoError(t, err)
				require.True(t, afterSeek.E.Equal(secondDatom.E))
				require.NoError(t, iter.Close())
				_ = keys
			}
		})
	}
}

func TestScanAndScanKeysOnlyShareWorkspaceContract(t *testing.T) {
	for _, testCase := range storeContractCases() {
		t.Run(testCase.name, func(t *testing.T) {
			store := testCase.open(t, &BinaryKeyEncoder{})
			defer store.Close()
			entity := datalog.NewIdentity("workspace:entity")
			attr := datalog.NewKeyword(":workspace/value")
			require.NoError(t, store.Assert([]datalog.Datom{
				{E: entity, A: attr, V: []byte("alpha"), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
				{E: entity, A: attr, V: []byte("bravo"), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
			}))

			scan := collectIndexDatoms(t, store, true)
			keysOnly := collectIndexDatoms(t, store, false)
			require.Equal(t, scan, keysOnly)

			iter, err := store.Scan(EAVT, []byte{byte(EAVT)}, []byte{byte(EAVT) + 1})
			require.NoError(t, err)
			defer iter.Close()
			require.True(t, iter.Next())
			first, err := iter.Datom()
			require.NoError(t, err)
			require.True(t, iter.Next())
			second, err := iter.Datom()
			require.NoError(t, err)
			require.Same(t, first, second, "Scan must reuse one datom workspace")
			require.NoError(t, iter.Close())
			require.NoError(t, iter.Close())
		})
	}
}

func TestStoreBackendsRetainByteValuesAndStickyBlobErrors(t *testing.T) {
	for _, testCase := range storeContractCases() {
		t.Run(testCase.name, func(t *testing.T) {
			encoder := &BinaryKeyEncoder{CompressionThreshold: 64}
			store := testCase.open(t, encoder)
			defer store.Close()

			entity := datalog.NewIdentity("bytes:entity")
			inlineAttr := datalog.NewKeyword(":bytes/a-inline")
			blobAttr := datalog.NewKeyword(":bytes/z-blob")
			payload := make([]byte, 80*1024)
			_, err := rand.Read(payload)
			require.NoError(t, err)
			payload = append(payload, make([]byte, 80*1024)...)

			require.NoError(t, store.Assert([]datalog.Datom{
				{E: entity, A: inlineAttr, V: []byte("first-value"), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
				{E: entity, A: inlineAttr, V: []byte("second-value"), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
				{E: entity, A: blobAttr, V: payload, Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
			}))

			start, end := encoder.EncodePrefixRange(EAVT, entity.Bytes())
			iter, err := store.ScanKeysOnly(EAVT, start, end)
			require.NoError(t, err)
			var retained [][]byte
			for iter.Next() {
				datom, err := iter.Datom()
				require.NoError(t, err)
				if datom.A == inlineAttr {
					retained = append(retained, datom.V.([]byte))
				}
			}
			require.NoError(t, iter.Error())
			require.NoError(t, iter.Close())
			require.Len(t, retained, 2)
			values := map[string]bool{string(retained[0]): true, string(retained[1]): true}
			require.True(t, values["first-value"])
			require.True(t, values["second-value"])
			secondBefore := string(retained[1])
			retained[0][0] ^= 0x20
			require.Equal(t, secondBefore, string(retained[1]))

			require.Greater(t, deleteStoreBlobs(t, store), 0)

			iter, err = store.ScanKeysOnly(EAVT, start, end)
			require.NoError(t, err)
			defer iter.Close()
			seenInline := 0
			var firstErr error
			for iter.Next() {
				datom, err := iter.Datom()
				if err != nil {
					firstErr = err
					break
				}
				if datom.A == inlineAttr {
					seenInline++
				}
			}
			require.Equal(t, 2, seenInline, "valid rows before a corrupt blob must remain visible")
			if firstErr == nil {
				firstErr = iter.Error()
			}
			require.ErrorContains(t, firstErr, "blob")
			require.False(t, iter.Next())
			require.ErrorIs(t, iter.Error(), firstErr)
		})
	}
}

func TestDatabaseBackendsPublicSemantics(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":item/name").Type(schema.TypeString).One().Add().
		Attribute(":item/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		Attribute(":item/tags").Type(schema.TypeString).Many().Add().
		Attribute(":item/steps").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	type backendResult struct {
		latest   [][]interface{}
		asOf     [][]interface{}
		history  int
		queryInto []string
		unique   datalog.Identity
		pulled   map[datalog.Keyword]interface{}
		exported string
		afterTruncate [][]interface{}
	}
	results := make(map[string]backendResult)

	for _, testCase := range storeContractCases() {
		t.Run(testCase.name, func(t *testing.T) {
			database := openContractDatabase(t, testCase, DatabaseOptions{
				Schema:       s,
				DisableCache: true,
				ReplicaID:    42,
			})
			entity := datalog.NewIdentity("backend:item")
			name := datalog.NewKeyword(":item/name")
			email := datalog.NewKeyword(":item/email")
			tags := datalog.NewKeyword(":item/tags")
			steps := datalog.NewKeyword(":item/steps")

			tx := database.NewTransaction()
			require.NoError(t, tx.Set(entity, name, "first"))
			require.NoError(t, tx.Set(entity, email, "item@example.com"))
			require.NoError(t, tx.Add(entity, tags, "a"))
			require.NoError(t, tx.Add(entity, tags, "b"))
			require.NoError(t, tx.Add(entity, steps, "one"))
			require.NoError(t, tx.Add(entity, steps, "two"))
			firstTx, err := tx.Commit()
			require.NoError(t, err)

			_, err = database.Snapshot("base")
			require.NoError(t, err)

			tx = database.NewTransaction()
			require.NoError(t, tx.Set(entity, name, "second"))
			_, err = tx.Commit()
			require.NoError(t, err)

			latest, err := executor.CollectTuples(database.Query(
				`[:find ?name :where [?entity :item/name ?name]]`,
			))
			require.NoError(t, err)
			asOf, err := executor.CollectTuples(database.AsOf(firstTx).Query(
				`[:find ?name :where [?entity :item/name ?name]]`,
			))
			require.NoError(t, err)
			history, err := executor.CollectTuples(database.History().Query(
				`[:find ?name ?tx :where [?entity :item/name ?name ?tx]]`,
			))
			require.NoError(t, err)

			var names []string
			require.NoError(t, database.QueryInto(
				&names,
				`[:find ?name :where [?entity :item/name ?name]]`,
			))
			owner, err := database.LookupByUnique(email, "item@example.com")
			require.NoError(t, err)
			require.True(t, owner.Equal(entity))

			pulled, err := database.ResolveAllAttributesMany([]datalog.Identity{entity})
			require.NoError(t, err)

			var dump bytes.Buffer
			require.NoError(t, database.Export(&dump))
			imported := openContractDatabase(t, testCase, DatabaseOptions{})
			require.NoError(t, imported.Import(bytes.NewReader(dump.Bytes())))
			importedRows, err := executor.CollectTuples(imported.Query(
				`[:find ?name :where [?entity :item/name ?name]]`,
			))
			require.NoError(t, err)
			require.Equal(t, latest, importedRows)

			require.NoError(t, database.TruncateTo("base"))
			afterTruncate, err := executor.CollectTuples(database.Query(
				`[:find ?name :in $ ?entity :where [?entity :item/name ?name]]`,
				entity,
			))
			require.NoError(t, err)

			got := backendResult{
				latest:        latest,
				asOf:          asOf,
				history:       len(history),
				queryInto:     names,
				unique:        owner,
				pulled:        pulled[0],
				exported:      dump.String(),
				afterTruncate: afterTruncate,
			}
			results[testCase.name] = got
			require.Equal(t, [][]interface{}{{"second"}}, got.latest)
			require.Equal(t, [][]interface{}{{"first"}}, got.asOf)
			require.GreaterOrEqual(t, got.history, 2)
			require.Equal(t, []string{"second"}, got.queryInto)
			require.Equal(t, "second", got.pulled[name])
			require.ElementsMatch(t, []interface{}{"a", "b"}, got.pulled[tags])
			require.Equal(t, []string{"one", "two"}, got.pulled[steps])
			require.Equal(t, [][]interface{}{{"first"}}, got.afterTruncate)
		})
	}

	if _, ok := results["badger"]; ok {
		require.Equal(t, results["badger"].latest, results["memory"].latest)
		require.Equal(t, results["badger"].asOf, results["memory"].asOf)
		require.Equal(t, results["badger"].history, results["memory"].history)
		require.Equal(t, results["badger"].queryInto, results["memory"].queryInto)
		require.True(t, results["badger"].unique.Equal(results["memory"].unique))
		require.Equal(t, results["badger"].pulled[datalog.NewKeyword(":item/name")], results["memory"].pulled[datalog.NewKeyword(":item/name")])
		require.ElementsMatch(t,
			results["badger"].pulled[datalog.NewKeyword(":item/tags")],
			results["memory"].pulled[datalog.NewKeyword(":item/tags")],
		)
		require.Equal(t,
			results["badger"].pulled[datalog.NewKeyword(":item/steps")],
			results["memory"].pulled[datalog.NewKeyword(":item/steps")],
		)
		require.Equal(t, results["badger"].afterTruncate, results["memory"].afterTruncate)
		require.Equal(t,
			stabilizeExport(results["badger"].exported),
			stabilizeExport(results["memory"].exported),
		)
	}
}

func TestStoreBackendTransactionOrderedScan(t *testing.T) {
	for _, testCase := range storeContractCases() {
		t.Run(testCase.name, func(t *testing.T) {
			store := testCase.open(t, &BinaryKeyEncoder{})
			defer store.Close()
			first := datalog.NewIdentity("memory:first")
			second := datalog.NewIdentity("memory:second")
			attr := datalog.NewKeyword(":memory/value")
			datoms := []datalog.Datom{
				{E: second, A: attr, V: "second", Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
				{E: first, A: attr, V: "first", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			}

			tx, err := store.BeginTx()
			require.NoError(t, err)
			require.NoError(t, tx.Assert(datoms))
			require.NoError(t, tx.Rollback())
			iter, err := store.ScanKeysOnly(EAVT, []byte{byte(EAVT)}, []byte{byte(EAVT) + 1})
			require.NoError(t, err)
			require.False(t, iter.Next())
			require.NoError(t, iter.Close())

			tx, err = store.BeginTx()
			require.NoError(t, err)
			require.NoError(t, tx.Assert(datoms))
			require.NoError(t, tx.Commit())
			iter, err = store.ScanKeysOnly(EAVT, []byte{byte(EAVT)}, []byte{byte(EAVT) + 1})
			require.NoError(t, err)
			var got []datalog.Datom
			for iter.Next() {
				datom, err := iter.Datom()
				require.NoError(t, err)
				got = append(got, *datom)
			}
			require.NoError(t, iter.Error())
			require.NoError(t, iter.Close())
			require.Len(t, got, 2)
			require.LessOrEqual(t, bytes.Compare(got[0].E.Bytes(), got[1].E.Bytes()), 0)

			// Multi-key ordering must be deterministic and strictly sorted by
			// encoded EAVT key — not a two-element flake check.
			extra := make([]datalog.Datom, 0, 5)
			for i := 0; i < 5; i++ {
				extra = append(extra, datalog.Datom{
					E:  datalog.NewIdentity(fmt.Sprintf("memory:order:%d", i)),
					A:  attr,
					V:  fmt.Sprintf("v%d", i),
					Tx: datalog.ElementID{Lamport: uint64(10 + i), ReplicaID: 1},
				})
			}
			require.NoError(t, store.Assert(extra))
			iter, err = store.ScanKeysOnly(EAVT, []byte{byte(EAVT)}, []byte{byte(EAVT) + 1})
			require.NoError(t, err)
			var keys [][]byte
			for iter.Next() {
				keyed, ok := iter.(interface{ Key() []byte })
				require.True(t, ok, "store iterators must expose Key() for order checks")
				key := keyed.Key()
				require.NotNil(t, key)
				keys = append(keys, append([]byte(nil), key...))
			}
			require.NoError(t, iter.Error())
			require.NoError(t, iter.Close())
			require.GreaterOrEqual(t, len(keys), 7)
			for i := 1; i < len(keys); i++ {
				require.Equal(t, -1, bytes.Compare(keys[i-1], keys[i]))
			}
		})
	}
}

func collectIndexDatoms(t *testing.T, store Store, useScan bool) []datalog.Datom {
	t.Helper()
	var (
		iter Iterator
		err  error
	)
	if useScan {
		iter, err = store.Scan(EAVT, []byte{byte(EAVT)}, []byte{byte(EAVT) + 1})
	} else {
		iter, err = store.ScanKeysOnly(EAVT, []byte{byte(EAVT)}, []byte{byte(EAVT) + 1})
	}
	require.NoError(t, err)
	defer iter.Close()
	var datoms []datalog.Datom
	for iter.Next() {
		datom, err := iter.Datom()
		require.NoError(t, err)
		copied := *datom
		if bytesValue, ok := copied.V.([]byte); ok {
			copied.V = append([]byte(nil), bytesValue...)
		}
		datoms = append(datoms, copied)
	}
	require.NoError(t, iter.Error())
	return datoms
}

func countStoreIndex(t *testing.T, store Store, index IndexType) int {
	t.Helper()
	iterator, err := store.ScanKeysOnly(index, []byte{byte(index)}, []byte{byte(index) + 1})
	require.NoError(t, err)
	defer iterator.Close()
	count := 0
	for iterator.Next() {
		datom, err := iterator.Datom()
		require.NoError(t, err)
		require.NotNil(t, datom.E, fmt.Sprintf("index %v", index))
		count++
	}
	require.NoError(t, iterator.Error())
	return count
}
