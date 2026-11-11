package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

var errStoreAssert = errors.New("the store refuses the write")

// assertFailingStore is a store that, while failing holds, refuses with
// errStoreAssert every write after the one carrying holder's datoms, so an
// import ends with holder's batch or chunk in the store and the rest refused.
type assertFailingStore struct {
	Store
	holder  datalog.Identity
	failing *atomic.Bool
	landed  *atomic.Bool
}

func (s assertFailingStore) Assert(datoms []datalog.Datom) error {
	if s.failing.Load() && s.landed.Load() {
		return errStoreAssert
	}
	if err := s.Store.Assert(datoms); err != nil {
		return err
	}
	for i := range datoms {
		if datoms[i].E.Equal(s.holder) {
			s.noteLanded()
			break
		}
	}
	return nil
}

func (s assertFailingStore) AssertEach(produce func(add func(*datalog.Datom) error) error) error {
	if s.failing.Load() && s.landed.Load() {
		return errStoreAssert
	}
	carries := false
	err := s.Store.AssertEach(func(add func(*datalog.Datom) error) error {
		return produce(func(d *datalog.Datom) error {
			if d.E.Equal(s.holder) {
				carries = true
			}
			return add(d)
		})
	})
	if err == nil && carries {
		s.noteLanded()
	}
	return err
}

// noteLanded records that a write carrying holder landed while failing holds.
func (s assertFailingStore) noteLanded() {
	if s.failing.Load() {
		s.landed.Store(true)
	}
}

// An import that fails after a batch leaves that batch in the store, and a
// read after the failure answers the store. Holder's name is imported and
// read, so the cache holds it. A second dump carries holder's later name and a
// thousand filler entities sorting after holder, and the store refuses every
// write after the one carrying holder, so the import fails with holder's later
// name in the store. Holder's name then reads as the store holds it, through
// every entry point, with the cache on as with it off.
func TestAReadAfterAFailedImportAnswersTheStore(t *testing.T) {
	name := datalog.NewKeyword(":item/name")
	holder := datalog.NewIdentity("item:holder")
	for _, entry := range importEntryPoints {
		for _, mode := range legsFor(optimizerModes, fusionOption) {
			for _, cacheMode := range cacheTestModes {
				t.Run(entry.name+"/"+mode.name+"/"+cacheMode.name, func(t *testing.T) {
					var failing, landed atomic.Bool
					opts := mode.plannerOptions()
					opts.Handler = recordingReach(t, nil)
					target, err := NewDatabaseWithOptions(DatabaseOptions{
						Store:          assertFailingStore{Store: NewBadgerEmulatorStore(nil), holder: holder, failing: &failing, landed: &landed},
						DisableCache:   cacheMode.disableCache,
						PlannerOptions: &opts,
					})
					require.NoError(t, err)
					defer func() { require.NoError(t, target.Close()) }()

					first, second := twoBatchDumps(t, entry.binary, holder, name)
					require.NoError(t, entry.load(target, first))
					var names []string
					require.NoError(t, target.QueryInto(&names, `[:find ?v :in $ ?e :where [?e :item/name ?v]]`, holder))
					require.Equal(t, []string{"first"}, names, "holder's name after the first import")

					failing.Store(true)
					err = entry.load(target, second)
					failing.Store(false)
					require.ErrorIs(t, err, errStoreAssert, "the write after holder's is refused, so the import fails")
					require.Equal(t, "second", storedName(t, target.Store(), holder, name), "holder's later name is in the store after the failure")

					names = nil
					require.NoError(t, target.QueryInto(&names, `[:find ?v :in $ ?e :where [?e :item/name ?v]]`, holder))
					require.Equal(t, []string{"second"}, names, "holder's name after the failed import answers the store")
				})
			}
		}
	}
}

// twoBatchDumps are two dumps of one database in the format binary names: the
// first written after holder's name "first", the second after holder's name
// "second" and a thousand filler entities whose identities sort after
// holder's, so holder's datoms precede the fillers' in every index order the
// dumps are written in. The binary second dump is exported under a soft
// budget that closes a chunk at every entity.
func twoBatchDumps(t *testing.T, binary bool, holder datalog.Identity, name datalog.Keyword) (first, second []byte) {
	t.Helper()
	source, err := NewDatabaseWithOptions(DatabaseOptions{Store: NewBadgerEmulatorStore(nil)})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	tx := source.NewTransaction()
	require.NoError(t, tx.Add(holder, name, "first"))
	_, err = tx.Commit()
	require.NoError(t, err)
	first = exportDumpUnder(t, source, binary, BinaryExportOptions{})
	tx = source.NewTransaction()
	require.NoError(t, tx.Add(holder, name, "second"))
	for _, filler := range fillersAfter(holder, 1000) {
		require.NoError(t, tx.Add(filler, name, "filler"))
	}
	_, err = tx.Commit()
	require.NoError(t, err)
	return first, exportDumpUnder(t, source, binary, BinaryExportOptions{SoftBudget: 1})
}

// fillersAfter is n entities whose identities sort after holder's in the
// entity-first index orders.
func fillersAfter(holder datalog.Identity, n int) []datalog.Identity {
	fillers := make([]datalog.Identity, 0, n)
	for i := 0; len(fillers) < n; i++ {
		filler := datalog.NewIdentity(fmt.Sprintf("item:filler-%d", i))
		if bytes.Compare(filler.Bytes(), holder.Bytes()) > 0 {
			fillers = append(fillers, filler)
		}
	}
	return fillers
}

// exportDumpUnder writes d as EDN, or as JDZL under opts when binary, and
// returns the bytes.
func exportDumpUnder(t *testing.T, d *Database, binary bool, opts BinaryExportOptions) []byte {
	t.Helper()
	if !binary {
		var buf bytes.Buffer
		require.NoError(t, d.Export(&buf))
		return buf.Bytes()
	}
	path := filepath.Join(t.TempDir(), "dump.jdzl")
	file, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, d.ExportBinary(file, opts))
	require.NoError(t, file.Close())
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return raw
}

// storedName is the name the store resolves for holder: the first datom of
// holder's name in EATV, whose transactions descend.
func storedName(t *testing.T, store Store, holder datalog.Identity, name datalog.Keyword) string {
	t.Helper()
	iter, err := OpenScan(store, DiscardIntake, ScanBound{Index: EATV, Prefix: []datalog.Value{holder, name}})
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()
	require.True(t, iter.Next(), "the store holds a name for holder")
	datom, err := iter.Datom()
	require.NoError(t, err)
	return datom.V.(string)
}
