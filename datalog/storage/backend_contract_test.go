package storage

import (
	"bytes"
	"crypto/rand"
	"errors"
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

// TestStoreBackendAssertEach pins the streaming write path across backends.
//
// Three properties, each of which a backend can break on its own.
//
// Parity: what AssertEach stores is what Assert stores — every index, every
// datom, same order.
//
// Workspace: the producer yields one reused datom, which is the shape a decoder
// has — binaryChunkCursor.Datom returns the same address for every record. A
// backend that keeps the pointer rather than the datom stores the last record
// N times over, collapsing every index to a single entry. The tree store holds
// *Datom in its nodes and is where this bites.
//
// Abort: a producer that fails stores nothing and its error reaches the caller
// intact.
func TestStoreBackendAssertEach(t *testing.T) {
	entity := datalog.NewIdentity("asserteach:entity")
	attr := datalog.NewKeyword(":asserteach/value")
	datoms := make([]datalog.Datom, 16)
	for i := range datoms {
		datoms[i] = datalog.Datom{
			E:  entity,
			A:  attr,
			V:  int64(i),
			Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 3},
		}
	}
	yieldThroughWorkspace := func(add func(*datalog.Datom) error) error {
		var workspace datalog.Datom
		for i := range datoms {
			workspace = datoms[i]
			if err := add(&workspace); err != nil {
				return err
			}
		}
		return nil
	}

	for _, testCase := range storeContractCases() {
		t.Run(testCase.name, func(t *testing.T) {
			streamed := testCase.open(t, &BinaryKeyEncoder{})
			defer streamed.Close()
			require.NoError(t, streamed.AssertEach(yieldThroughWorkspace))
			// A backend may leave the run open, so the run has to end before what
			// it wrote can be counted.
			require.NoError(t, streamed.FinishBatch())

			gathered := testCase.open(t, &BinaryKeyEncoder{})
			defer gathered.Close()
			require.NoError(t, gathered.Assert(datoms))

			for _, index := range Indices {
				require.Equal(t, len(datoms), countStoreIndex(t, streamed, index),
					"index %v: AssertEach stored the wrong number of datoms", index)
			}
			require.Equal(t,
				collectIndexDatoms(t, gathered, true),
				collectIndexDatoms(t, streamed, true),
				"AssertEach and Assert disagree on what they stored")

			aborted := testCase.open(t, &BinaryKeyEncoder{})
			defer aborted.Close()
			producerFailed := errors.New("producer failed")
			err := aborted.AssertEach(func(add func(*datalog.Datom) error) error {
				if err := add(&datoms[0]); err != nil {
					return err
				}
				return producerFailed
			})
			require.ErrorIs(t, err, producerFailed)
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
				iter, err := store.ScanKeysOnly(ScanBound{Index: index})
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
				iter, err = store.ScanKeysOnly(ScanBound{Index: EAVT})
				require.NoError(t, err)
				require.True(t, iter.Next())
				datom, err := iter.Datom()
				require.NoError(t, err)
				seekKey := store.Encoder().EncodeKey(EAVT, datom)
				keys = append(keys, append([]byte(nil), seekKey...))
				require.True(t, iter.Next())
				secondDatom, err := iter.Datom()
				require.NoError(t, err)
				// Binding all four orderable components names a run holding
				// exactly this datom, which is how a bound reaches one key.
				iter.Seek(ScanBound{
					Index:  EAVT,
					Prefix: []datalog.Value{secondDatom.E, secondDatom.A, secondDatom.V, secondDatom.Tx},
				})
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

			// Whether Datom reuses one address is the backend's to choose;
			// requiring either answer forbids the other. What a caller relies on
			// is that the two spellings give the same one.
			require.Equal(t,
				datomPointerReused(t, store, true),
				datomPointerReused(t, store, false),
				"Scan and ScanKeysOnly must agree on whether Datom reuses one address")

			iter, err := store.Scan(ScanBound{Index: EAVT})
			require.NoError(t, err)
			require.True(t, iter.Next())
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

			entityBound := ScanBound{Index: EAVT, Prefix: []datalog.Value{entity}}
			iter, err := store.ScanKeysOnly(entityBound)
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

			// With no blob tier there is no out-of-line value to lose, so
			// retention above is the whole of this contract for that backend.
			deleted, hasBlobTier := deleteStoreBlobs(t, store)
			if !hasBlobTier {
				return
			}
			require.Greater(t, deleted, 0)

			iter, err = store.ScanKeysOnly(entityBound)
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
			require.Equal(t, 2, seenInline, "valid datoms before a corrupt blob must remain visible")
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
		latest        [][]interface{}
		asOf          [][]interface{}
		history       int
		queryInto     []string
		unique        datalog.Identity
		pulled        map[datalog.Keyword]interface{}
		exported      string
		afterTruncate [][]interface{}
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			results := make(map[string]backendResult)

			for _, testCase := range storeContractCases() {
				t.Run(testCase.name, func(t *testing.T) {
					database := openContractDatabase(t, testCase, DatabaseOptions{
						Schema:         s,
						DisableCache:   true,
						ReplicaID:      42,
						PlannerOptions: &popts,
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
					imported := openContractDatabase(t, testCase, DatabaseOptions{PlannerOptions: &popts})
					require.NoError(t, imported.Import(bytes.NewReader(dump.Bytes())))
					importedTuples, err := executor.CollectTuples(imported.Query(
						`[:find ?name :where [?entity :item/name ?name]]`,
					))
					require.NoError(t, err)
					require.Equal(t, latest, importedTuples)

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

			// Every backend answers against badger, the on-disk format. Naming
			// the other side instead of deriving it from storeContractCases left
			// a backend collecting results nothing compared.
			reference, hasReference := results["badger"]
			if !hasReference {
				return
			}
			itemName := datalog.NewKeyword(":item/name")
			itemTags := datalog.NewKeyword(":item/tags")
			itemSteps := datalog.NewKeyword(":item/steps")
			for _, testCase := range storeContractCases() {
				if testCase.name == "badger" {
					continue
				}
				t.Run("badger vs "+testCase.name, func(t *testing.T) {
					other, ran := results[testCase.name]
					require.True(t, ran, "%s produced no result to compare", testCase.name)
					require.Equal(t, reference.latest, other.latest)
					require.Equal(t, reference.asOf, other.asOf)
					require.Equal(t, reference.history, other.history)
					require.Equal(t, reference.queryInto, other.queryInto)
					require.True(t, reference.unique.Equal(other.unique))
					require.Equal(t, reference.pulled[itemName], other.pulled[itemName])
					require.ElementsMatch(t, reference.pulled[itemTags], other.pulled[itemTags])
					require.Equal(t, reference.pulled[itemSteps], other.pulled[itemSteps])
					require.Equal(t, reference.afterTruncate, other.afterTruncate)
					require.Equal(t,
						stabilizeExport(reference.exported),
						stabilizeExport(other.exported),
					)
				})
			}
		})
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
			iter, err := store.ScanKeysOnly(ScanBound{Index: EAVT})
			require.NoError(t, err)
			require.False(t, iter.Next())
			require.NoError(t, iter.Close())

			tx, err = store.BeginTx()
			require.NoError(t, err)
			require.NoError(t, tx.Assert(datoms))
			require.NoError(t, tx.Commit())
			iter, err = store.ScanKeysOnly(ScanBound{Index: EAVT})
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

			// Multi-key ordering must be deterministic and strictly ascending in
			// EAVT order — not a two-element flake check. Asserted with
			// compareDatoms because an encoded key is one backend's
			// representation of that order, not the order itself.
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
			iter, err = store.ScanKeysOnly(ScanBound{Index: EAVT})
			require.NoError(t, err)
			var ordered []datalog.Datom
			for iter.Next() {
				datom, err := iter.Datom()
				require.NoError(t, err)
				ordered = append(ordered, *datom)
			}
			require.NoError(t, iter.Error())
			require.NoError(t, iter.Close())
			require.GreaterOrEqual(t, len(ordered), 7)
			for i := 1; i < len(ordered); i++ {
				require.Less(t, compareDatoms(EAVT, &ordered[i-1], &ordered[i]), 0,
					"scan emitted %v before %v", ordered[i-1], ordered[i])
			}
		})
	}
}

// TestStoreBackendVBoundRunExcludesValueExtensions is the backend-parity pin
// the typed bound owed. A V payload carries no length, so a byte range whose
// last bound component is a variable-length V is a prefix range: the keys for
// "abcd" sort inside the range for "abc", interleaved with them, and no range
// can separate the two. Narrowing the range to the run the bound names is the
// store's job, and both stores must do it the same way.
//
// Op is load-bearing here: an RGA datom's key carries an AfterRef ahead of Op
// and is 16 bytes longer for the same value, so a store that measures a key
// without reading Op drops every RGA datom from a V-bound scan.
func TestStoreBackendVBoundRunExcludesValueExtensions(t *testing.T) {
	for _, testCase := range storeContractCases() {
		t.Run(testCase.name, func(t *testing.T) {
			store := testCase.open(t, &BinaryKeyEncoder{})
			defer store.Close()

			attr := datalog.NewKeyword(":run/tag")
			other := datalog.NewKeyword(":run/other")
			short := datalog.NewIdentity("run:short")
			long := datalog.NewIdentity("run:long")
			empty := datalog.NewIdentity("run:empty")
			rga := datalog.NewIdentity("run:rga")
			rgaLong := datalog.NewIdentity("run:rga-long")
			raw := datalog.NewIdentity("run:raw")
			rawLong := datalog.NewIdentity("run:raw-long")
			// Keyword and Symbol are variable-width too — PayloadIsFixedWidth
			// classifies both false, and their payload is []byte(String()), so
			// :status/act is a byte prefix of :status/active exactly as "abc"
			// is of "abcd".
			kw := datalog.NewIdentity("run:kw")
			kwLong := datalog.NewIdentity("run:kw-long")
			sym := datalog.NewIdentity("run:sym")
			symLong := datalog.NewIdentity("run:sym-long")

			tx := func(l uint64) datalog.ElementID {
				return datalog.ElementID{Lamport: l, ReplicaID: 1}
			}
			after := datalog.ElementID{Lamport: 99, ReplicaID: 1}

			require.NoError(t, store.Assert([]datalog.Datom{
				{E: short, A: attr, V: "abc", Tx: tx(1)},
				{E: long, A: attr, V: "abcd", Tx: tx(2)},
				{E: empty, A: attr, V: "", Tx: tx(3)},
				{E: rga, A: attr, V: "abc", Tx: tx(4),
					Op: datalog.OpRGAInsert, AfterRef: after},
				{E: rgaLong, A: attr, V: "abcd", Tx: tx(5),
					Op: datalog.OpRGAInsert, AfterRef: after},
				{E: raw, A: other, V: []byte("xy"), Tx: tx(6)},
				{E: rawLong, A: other, V: []byte("xyz"), Tx: tx(7)},
				{E: kw, A: attr, V: datalog.NewKeyword(":status/act"), Tx: tx(8)},
				{E: kwLong, A: attr, V: datalog.NewKeyword(":status/active"), Tx: tx(9)},
				{E: sym, A: attr, V: datalog.NewSymbol("run"), Tx: tx(10)},
				{E: symLong, A: attr, V: datalog.NewSymbol("running"), Tx: tx(11)},
			}))

			for _, run := range []struct {
				name  string
				bound ScanBound
				want  []datalog.Identity
			}{
				{"AVET/string", ScanBound{Index: AVET,
					Prefix: []datalog.Value{attr, "abc"}},
					[]datalog.Identity{short, rga}},
				{"AVET/empty-string", ScanBound{Index: AVET,
					Prefix: []datalog.Value{attr, ""}},
					[]datalog.Identity{empty}},
				{"AVET/bytes", ScanBound{Index: AVET,
					Prefix: []datalog.Value{other, []byte("xy")}},
					[]datalog.Identity{raw}},
				{"VAET/value-first", ScanBound{Index: VAET,
					Prefix: []datalog.Value{"abc"}},
					[]datalog.Identity{short, rga}},
				{"EAVT/value-last", ScanBound{Index: EAVT,
					Prefix: []datalog.Value{short, attr, "abc"}},
					[]datalog.Identity{short}},
				{"EAVT/value-belongs-to-another-entity", ScanBound{Index: EAVT,
					Prefix: []datalog.Value{long, attr, "abc"}},
					nil},
				{"AVET/keyword", ScanBound{Index: AVET,
					Prefix: []datalog.Value{attr, datalog.NewKeyword(":status/act")}},
					[]datalog.Identity{kw}},
				{"AVET/symbol", ScanBound{Index: AVET,
					Prefix: []datalog.Value{attr, datalog.NewSymbol("run")}},
					[]datalog.Identity{sym}},
			} {
				t.Run(run.name, func(t *testing.T) {
					iter, err := store.ScanKeysOnly(run.bound)
					require.NoError(t, err)
					defer iter.Close()

					var got []datalog.Identity
					for iter.Next() {
						datom, err := iter.Datom()
						require.NoError(t, err)
						got = append(got, datom.E)
					}
					require.NoError(t, iter.Error())
					require.ElementsMatch(t, run.want, got,
						"the run must hold exactly the datoms carrying the bound value")
				})
			}
		})
	}
}

// TestStoreBackendCompressedVBoundRun covers the two variable-width value types
// the plain V-bound parity test cannot reach. TypeCompressedString and
// TypeCompressedBytes exist only when the encoder carries a compression
// threshold, and that test opens its store with the zero-threshold encoder, so
// its strings and bytes stay uncompressed.
//
// PayloadIsFixedWidth classifies both compressed types false, so both take the
// inexact arm and rest on the same key-length arithmetic. Getting that
// arithmetic wrong for a compressed value fails closed — every key is excluded
// and the scan returns nothing — which is what these assertions catch.
func TestStoreBackendCompressedVBoundRun(t *testing.T) {
	for _, testCase := range storeContractCases() {
		t.Run(testCase.name, func(t *testing.T) {
			const threshold = 32
			store := testCase.open(t, &BinaryKeyEncoder{CompressionThreshold: threshold})
			defer store.Close()

			text := string(bytes.Repeat([]byte("compressible-"), 8))
			otherText := string(bytes.Repeat([]byte("distinct-value-"), 8))
			raw := bytes.Repeat([]byte("0123456789"), 8)
			otherRaw := bytes.Repeat([]byte("abcdefghij"), 8)

			// Tier 2 is where these values have to land for the test to mean
			// anything: below the threshold they stay plain, above the key-size
			// ceiling they move out of line and become fixed-width hashes.
			// Assert it rather than skip, so a fixture that stops exercising
			// the tier says so.
			for _, v := range []datalog.Value{text, otherText} {
				vType, _, blob := datalog.EncodeValue(v, threshold)
				require.Equal(t, datalog.TypeCompressedString, vType,
					"fixture must reach Tier 2 compressed-string, got %v", vType)
				require.Nil(t, blob, "Tier 2 stays in the key")
			}
			for _, v := range []datalog.Value{raw, otherRaw} {
				vType, _, blob := datalog.EncodeValue(v, threshold)
				require.Equal(t, datalog.TypeCompressedBytes, vType,
					"fixture must reach Tier 2 compressed-bytes, got %v", vType)
				require.Nil(t, blob, "Tier 2 stays in the key")
			}

			strAttr := datalog.NewKeyword(":zip/text")
			bytesAttr := datalog.NewKeyword(":zip/raw")
			wantText := datalog.NewIdentity("zip:text")
			otherTextE := datalog.NewIdentity("zip:text-other")
			wantRaw := datalog.NewIdentity("zip:raw")
			otherRawE := datalog.NewIdentity("zip:raw-other")

			tx := func(l uint64) datalog.ElementID {
				return datalog.ElementID{Lamport: l, ReplicaID: 1}
			}
			require.NoError(t, store.Assert([]datalog.Datom{
				{E: wantText, A: strAttr, V: text, Tx: tx(1)},
				{E: otherTextE, A: strAttr, V: otherText, Tx: tx(2)},
				{E: wantRaw, A: bytesAttr, V: raw, Tx: tx(3)},
				{E: otherRawE, A: bytesAttr, V: otherRaw, Tx: tx(4)},
			}))

			for _, run := range []struct {
				name  string
				bound ScanBound
				want  datalog.Identity
			}{
				{"AVET/compressed-string", ScanBound{Index: AVET,
					Prefix: []datalog.Value{strAttr, text}}, wantText},
				{"AVET/compressed-bytes", ScanBound{Index: AVET,
					Prefix: []datalog.Value{bytesAttr, raw}}, wantRaw},
			} {
				t.Run(run.name, func(t *testing.T) {
					iter, err := store.ScanKeysOnly(run.bound)
					require.NoError(t, err)
					defer iter.Close()

					var got []datalog.Identity
					for iter.Next() {
						datom, err := iter.Datom()
						require.NoError(t, err)
						got = append(got, datom.E)
					}
					require.NoError(t, iter.Error())
					require.Equal(t, []datalog.Identity{run.want}, got,
						"the run must hold exactly the datom carrying the bound value")
				})
			}
		})
	}
}

// TestStoreBackendsBinaryRoundTrip carries each backend's datoms out through
// JDZL and back. A store holding typed datoms encodes only at this boundary, so
// a format the contract leaves out is one only the byte-key backends are known
// to produce.
func TestStoreBackendsBinaryRoundTrip(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":item/name").Type(schema.TypeString).One().Add().
		Attribute(":item/tags").Type(schema.TypeString).Many().Add().
		Attribute(":item/steps").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	for _, testCase := range storeContractCases() {
		t.Run(testCase.name, func(t *testing.T) {
			source := openContractDatabase(t, testCase, DatabaseOptions{Schema: s, ReplicaID: 7})

			entity := datalog.NewIdentity("binary:item")
			name := datalog.NewKeyword(":item/name")
			tags := datalog.NewKeyword(":item/tags")
			steps := datalog.NewKeyword(":item/steps")

			tx := source.NewTransaction()
			require.NoError(t, tx.Set(entity, name, "first"))
			require.NoError(t, tx.Add(entity, tags, "a"))
			require.NoError(t, tx.Add(entity, tags, "b"))
			require.NoError(t, tx.Add(entity, steps, "one"))
			require.NoError(t, tx.Add(entity, steps, "two"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// A second transaction so the export carries a superseded value and a
			// tombstone, not one flat generation.
			tx = source.NewTransaction()
			require.NoError(t, tx.Set(entity, name, "second"))
			require.NoError(t, tx.Remove(entity, tags, "a"))
			_, err = tx.Commit()
			require.NoError(t, err)

			var encoded seekBuffer
			require.NoError(t, source.ExportBinary(&encoded))

			restored := openContractDatabase(t, testCase, DatabaseOptions{Schema: s, ReplicaID: 7})
			require.NoError(t, restored.ImportBinary(&encoded))

			var before, after bytes.Buffer
			require.NoError(t, source.Export(&before))
			require.NoError(t, restored.Export(&after))
			require.Equal(t, stabilizeExport(before.String()), stabilizeExport(after.String()),
				"the binary round trip lost or altered datoms")

			resolved, err := executor.CollectTuples(restored.Query(
				`[:find ?name :where [?entity :item/name ?name]]`,
			))
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"second"}}, resolved)

			liveTags, err := restored.GetStrings(entity, tags)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"b"}, liveTags,
				"the tombstone did not survive the round trip")

			stepCount, err := restored.GetVectorLength(entity, steps)
			require.NoError(t, err)
			require.Equal(t, int64(2), stepCount)
			first, err := restored.GetVectorNth(entity, steps, 0)
			require.NoError(t, err)
			require.Equal(t, "one", first)
			second, err := restored.GetVectorNth(entity, steps, 1)
			require.NoError(t, err)
			require.Equal(t, "two", second)
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
		iter, err = store.Scan(ScanBound{Index: EAVT})
	} else {
		iter, err = store.ScanKeysOnly(ScanBound{Index: EAVT})
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

// datomPointerReused reports whether two successive Datom calls on one scan
// hand back the same address.
func datomPointerReused(t *testing.T, store Store, useScan bool) bool {
	t.Helper()
	var (
		iter Iterator
		err  error
	)
	if useScan {
		iter, err = store.Scan(ScanBound{Index: EAVT})
	} else {
		iter, err = store.ScanKeysOnly(ScanBound{Index: EAVT})
	}
	require.NoError(t, err)
	defer iter.Close()

	require.True(t, iter.Next())
	first, err := iter.Datom()
	require.NoError(t, err)
	require.True(t, iter.Next())
	second, err := iter.Datom()
	require.NoError(t, err)
	require.NoError(t, iter.Error())
	return first == second
}

func countStoreIndex(t *testing.T, store Store, index IndexType) int {
	t.Helper()
	iterator, err := store.ScanKeysOnly(ScanBound{Index: index})
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
