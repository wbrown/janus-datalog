package db_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/internal/errjoin"
	dlreflect "github.com/wbrown/janus-datalog/datalog/reflect"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

var (
	acrossName  = datalog.NewKeyword(":across/name")
	acrossCells = datalog.NewKeyword(":across/cells")
	acrossOrder = datalog.NewKeyword(":across/order")
)

type acrossCellsRecord struct {
	ID    datalog.Identity `datalog:"-,id"`
	Cells []string         `datalog:"across/cells"`
}

// acrossGateStore is a store a caller opens and hands to db.Open. Once armed,
// its next read session is opened on the inner store and then held until
// release closes: the schedule in which the goroutine that asked for it is
// descheduled between the open and its return.
type acrossGateStore struct {
	storage.Store
	armed   *atomic.Bool
	opened  chan struct{}
	release chan struct{}
}

func (s acrossGateStore) NewReadSession() (storage.ReadSession, error) {
	session, err := s.Store.NewReadSession()
	if s.armed.CompareAndSwap(true, false) {
		close(s.opened)
		<-s.release
	}
	return session, err
}

// commitAcrossWrite commits tx after a write that answered err, and rolls it
// back when err is set, so a drain never waits on it.
func commitAcrossWrite(tx *db.Transaction, err error) error {
	if err != nil {
		return errjoin.Join(err, tx.Rollback())
	}
	_, err = tx.Commit()
	return err
}

// TestAWriteThroughDbOpenOverlappingARewindIsDrained pins, through db.Open of
// a caller's store, the writers a caller reaches without holding the
// transaction and those that read its view: a write whose transaction is
// opening its read session when a TruncateTo is called is admitted before the
// rewind, which drains it and erases its commit. The snapshot holds name
// "Alice", cells [a] and order [a]. After it, name "Bob", order [a b] and
// cells [a b a] are committed. A write opening no read session is made again
// after the rewind.
func TestAWriteThroughDbOpenOverlappingARewindIsDrained(t *testing.T) {
	e := datalog.NewIdentity("across:e")
	cases := []struct {
		name  string
		attr  datalog.Keyword
		write func(d *db.DB, s schema.SchemaProvider) error
		// drained is the database's value when the rewind drains the write,
		// and after its value when the write is made after the rewind.
		drained, after string
	}{
		{"Database.Assert of an ordered-set element the rewind erased", acrossOrder, func(d *db.DB, s schema.SchemaProvider) error {
			return d.Assert([]datalog.Datom{{E: e, A: acrossOrder, V: "b"}})
		}, "[a] true", "[a b] true"},
		{"StructWriter.UpdateAuto of a vector field with the transaction as its lookup", acrossCells, func(d *db.DB, s schema.SchemaProvider) error {
			tx := d.NewTransaction()
			record := &acrossCellsRecord{ID: e, Cells: []string{"a", "b", "c"}}
			sw, err := dlreflect.NewStructWriter(record, s)
			if err != nil {
				return commitAcrossWrite(tx, err)
			}
			_, err = sw.UpdateAuto(tx, tx, record, dlreflect.UpdateModeReplace)
			return commitAcrossWrite(tx, err)
		}, "[a] true", "[a b c] true"},
		{"Transaction.Remove of a vector value the rewind erased an element of", acrossCells, func(d *db.DB, s schema.SchemaProvider) error {
			tx := d.NewTransaction()
			return commitAcrossWrite(tx, tx.Remove(e, acrossCells, "a"))
		}, "[a] true", "[] true"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := schema.NewBuilder().
				Attribute(acrossName.String()).Type(schema.TypeString).Add().
				Attribute(acrossCells.String()).Type(schema.TypeString).Vector().Add().
				Attribute(acrossOrder.String()).Type(schema.TypeString).OrderedSet().Add().
				Build()
			require.NoError(t, err)
			gate := acrossGateStore{Store: storage.NewBadgerEmulatorStore(nil), armed: &atomic.Bool{}, opened: make(chan struct{}), release: make(chan struct{})}
			d, err := db.Open(gate, db.WithSchema(s))
			require.NoError(t, err)
			defer func() { require.NoError(t, d.Close()) }()

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(e, acrossName, "Alice"))
			require.NoError(t, tx.Add(e, acrossCells, "a"))
			require.NoError(t, tx.Add(e, acrossOrder, "a"))
			_, err = tx.Commit()
			require.NoError(t, err)
			_, err = d.Snapshot("before-bob")
			require.NoError(t, err)
			tx = d.NewTransaction()
			require.NoError(t, tx.Add(e, acrossName, "Bob"))
			require.NoError(t, tx.Add(e, acrossOrder, "b"))
			require.NoError(t, tx.Set(e, acrossCells, []string{"a", "b", "a"}))
			_, err = tx.Commit()
			require.NoError(t, err)

			gate.armed.Store(true)
			done := make(chan error, 1)
			go func() { done <- c.write(d, s) }()
			var want string
			select {
			case <-gate.opened:
				// The write's transaction is opening its read session. The
				// rewind waits for its admission and drains it.
				rewound := make(chan error, 1)
				go func() { rewound <- d.TruncateTo("before-bob") }()
				close(gate.release)
				require.NoError(t, <-done)
				require.NoError(t, <-rewound)
				want = c.drained
			case err := <-done:
				// The write's transaction opened no read session, so it is
				// admitted before the rewind or after it. It is made again
				// after the rewind.
				require.NoError(t, err)
				gate.armed.Store(false)
				require.NoError(t, d.TruncateTo("before-bob"))
				require.NoError(t, c.write(d, s))
				want = c.after
			}

			v, found, err := d.LookupAttribute(e, c.attr)
			require.NoError(t, err)
			assert.Equal(t, want, fmt.Sprint(v, " ", found), "the write, on the side of the rewind it is admitted on")
		})
	}
}
