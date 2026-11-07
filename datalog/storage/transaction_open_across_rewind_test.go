package storage

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/internal/errjoin"
	dlreflect "github.com/wbrown/janus-datalog/datalog/reflect"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

var (
	rewindName  = datalog.NewKeyword(":rewind/name")
	rewindCells = datalog.NewKeyword(":rewind/cells")
	rewindOrder = datalog.NewKeyword(":rewind/order")
)

type rewindRecord struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name string           `datalog:"rewind/name"`
}

type rewindCellsRecord struct {
	ID    datalog.Identity `datalog:"-,id"`
	Cells []string         `datalog:"rewind/cells"`
}

// rewindGateStore is a caller-supplied store whose next read session, once
// armed, is opened on the inner store and then held until release closes:
// the schedule in which the goroutine that asked for it is descheduled
// between the open and its return.
type rewindGateStore struct {
	Store
	armed   *atomic.Bool
	opened  chan struct{}
	release chan struct{}
}

func (s rewindGateStore) NewReadSession() (ReadSession, error) {
	session, err := s.Store.NewReadSession()
	if s.armed.CompareAndSwap(true, false) {
		close(s.opened)
		<-s.release
	}
	return session, err
}

// commitRewindWrite commits tx after a write that answered err, and rolls it
// back when err is set, so a drain never waits on it.
func commitRewindWrite(tx *Transaction, err error) error {
	if err != nil {
		return errjoin.Join(err, tx.Rollback())
	}
	_, err = tx.Commit()
	return err
}

// TestAWriteOverlappingARewindLandsOnTheSideItIsAdmittedOn pins that a write
// whose NewTransaction is opening its read session when a TruncateTo is
// called is made on one side of the rewind: its view and its admission are
// taken together. Admitted before the rewind, the rewind drains it: it writes
// against the value before the rewind, and the rewind erases its commit.
// Admitted after the rewind, it writes against the value the rewind leaves.
// The snapshot holds name "Alice", cells [a] and order [a]; after it, name
// "Bob", order [a b] and cells [a b a] are committed. A write opening no read
// session is admitted before the rewind or after it, and is made again after
// the rewind.
func TestAWriteOverlappingARewindLandsOnTheSideItIsAdmittedOn(t *testing.T) {
	cases := []struct {
		name  string
		attr  datalog.Keyword
		write func(d *Database, s schema.SchemaProvider, e datalog.Identity) error
		// after is the database's value when the write is admitted after the
		// rewind, and drained its value when the write is admitted before it.
		after, drained string
	}{
		{"Transaction.SaveStruct of the name the rewind erased", rewindName, func(d *Database, s schema.SchemaProvider, e datalog.Identity) error {
			tx := d.NewTransaction()
			_, err := tx.SaveStruct(&rewindRecord{ID: e, Name: "Bob"})
			return commitRewindWrite(tx, err)
		}, "Bob", "Alice"},
		{"Transaction.Set of a vector holding elements the rewind erased", rewindCells, func(d *Database, s schema.SchemaProvider, e datalog.Identity) error {
			tx := d.NewTransaction()
			return commitRewindWrite(tx, tx.Set(e, rewindCells, []string{"a", "b", "c"}))
		}, "[a b c]", "[a]"},
		{"Database.Assert of an ordered-set element the rewind erased", rewindOrder, func(d *Database, s schema.SchemaProvider, e datalog.Identity) error {
			return d.Assert([]datalog.Datom{{E: e, A: rewindOrder, V: "b"}})
		}, "[a b]", "[a]"},
		{"StructWriter.UpdateAuto of a vector field with the transaction as its lookup", rewindCells, func(d *Database, s schema.SchemaProvider, e datalog.Identity) error {
			tx := d.NewTransaction()
			record := &rewindCellsRecord{ID: e, Cells: []string{"a", "b", "c"}}
			sw, err := dlreflect.NewStructWriter(record, s)
			if err != nil {
				return commitRewindWrite(tx, err)
			}
			_, err = sw.UpdateAuto(tx, tx, record, dlreflect.UpdateModeReplace)
			return commitRewindWrite(tx, err)
		}, "[a b c]", "[a]"},
		{"Transaction.Remove of a vector value the rewind erased an element of", rewindCells, func(d *Database, s schema.SchemaProvider, e datalog.Identity) error {
			tx := d.NewTransaction()
			return commitRewindWrite(tx, tx.Remove(e, rewindCells, "a"))
		}, "[]", "[a]"},
	}
	stores := []struct {
		name string
		open func() Store
	}{
		{"badger-emulator", func() Store { return NewBadgerEmulatorStore(nil) }},
		{"memory-trees", func() Store { return NewMemoryTreeStore(nil) }},
	}
	for _, c := range cases {
		for _, st := range stores {
			for _, disableCache := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/disable-cache=%v", c.name, st.name, disableCache), func(t *testing.T) {
					s, err := schema.NewBuilder().
						Attribute(rewindName.String()).Type(schema.TypeString).Add().
						Attribute(rewindCells.String()).Type(schema.TypeString).Vector().Add().
						Attribute(rewindOrder.String()).Type(schema.TypeString).OrderedSet().Add().
						Build()
					require.NoError(t, err)
					gate := rewindGateStore{Store: st.open(), armed: &atomic.Bool{}, opened: make(chan struct{}), release: make(chan struct{})}
					d, err := NewDatabaseWithOptions(DatabaseOptions{Store: gate, Schema: s, DisableCache: disableCache})
					require.NoError(t, err)
					defer func() { require.NoError(t, d.Close()) }()

					e := datalog.NewIdentity("rewind:e")
					tx := d.NewTransaction()
					require.NoError(t, tx.Add(e, rewindName, "Alice"))
					require.NoError(t, tx.Add(e, rewindCells, "a"))
					require.NoError(t, tx.Add(e, rewindOrder, "a"))
					_, err = tx.Commit()
					require.NoError(t, err)
					_, err = d.Snapshot("before-bob")
					require.NoError(t, err)
					tx = d.NewTransaction()
					require.NoError(t, tx.Add(e, rewindName, "Bob"))
					require.NoError(t, tx.Add(e, rewindOrder, "b"))
					require.NoError(t, tx.Set(e, rewindCells, []string{"a", "b", "a"}))
					_, err = tx.Commit()
					require.NoError(t, err)

					gate.armed.Store(true)
					done := make(chan error, 1)
					go func() { done <- c.write(d, s, e) }()
					var want string
					select {
					case <-gate.opened:
						if d.mu.TryLock() {
							// The write's transaction opened its read session
							// outside the section that admits it, so the rewind
							// runs to its end before the admission.
							d.mu.Unlock()
							require.NoError(t, d.TruncateTo("before-bob"))
							close(gate.release)
							require.NoError(t, <-done)
							want = c.after
						} else {
							// The admission holds the database's lock while the
							// read session opens, so the rewind waits for it and
							// drains the write.
							rewound := make(chan error, 1)
							go func() { rewound <- d.TruncateTo("before-bob") }()
							close(gate.release)
							require.NoError(t, <-done)
							require.NoError(t, <-rewound)
							want = c.drained
						}
					case err := <-done:
						// The write's transaction opened no read session, so it
						// is admitted before the rewind or after it. It is made
						// again after the rewind.
						require.NoError(t, err)
						gate.armed.Store(false)
						require.NoError(t, d.TruncateTo("before-bob"))
						require.NoError(t, c.write(d, s, e))
						want = c.after
					}

					got, found, err := d.LookupAttribute(e, c.attr)
					require.NoError(t, err)
					require.True(t, found)
					assert.Equal(t, want, fmt.Sprint(got), "the write, on the side of the rewind it is admitted on")
				})
			}
		}
	}
}
