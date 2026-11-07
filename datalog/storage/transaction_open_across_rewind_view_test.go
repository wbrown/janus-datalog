package storage

import (
	"fmt"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

var (
	rewindViewName  = datalog.NewKeyword(":rewindview/name")
	rewindViewTags  = datalog.NewKeyword(":rewindview/tags")
	rewindViewOrder = datalog.NewKeyword(":rewindview/order")
)

// TestATransactionOverlappingARewindHoldsTheValueOnTheSideItIsAdmittedOn
// pins, for a transaction's lookups and for the writers that read its view,
// what TestAWriteOverlappingARewindLandsOnTheSideItIsAdmittedOn pins for the
// writes: a transaction whose NewTransaction is opening its read session when
// a TruncateTo is called holds the value on the side of the rewind it is
// admitted on. The snapshot holds name "Alice", tags [t1] and order [a]; after
// it, name "Bob", tag t2 and order element b are committed.
func TestATransactionOverlappingARewindHoldsTheValueOnTheSideItIsAdmittedOn(t *testing.T) {
	e := datalog.NewIdentity("rewindview:e")
	cases := []struct {
		name string
		// act runs the transaction's operation and answers what it reads. A
		// case with attr set is a write: it answers nothing, the transaction
		// commits, and the answer is the database's value of attr once the
		// rewind has returned.
		act  func(tx *Transaction) (string, error)
		attr datalog.Keyword
		// after is the answer when the transaction is admitted after the
		// rewind, and drained the answer when it is admitted before it.
		after, drained string
	}{
		{"Transaction.LookupAttribute of the name", func(tx *Transaction) (string, error) {
			v, found, err := tx.LookupAttribute(e, rewindViewName)
			return fmt.Sprintf("%v %v", v, found), err
		}, nil, "Alice true", "Bob true"},
		{"Transaction.LookupAllAttributes of the tags", func(tx *Transaction) (string, error) {
			members, err := tx.LookupAllAttributes(e, rewindViewTags)
			rendered := make([]string, len(members))
			for i, m := range members {
				rendered[i] = fmt.Sprint(m)
			}
			sort.Strings(rendered)
			return fmt.Sprint(rendered), err
		}, nil, "[t1]", "[t1 t2]"},
		{"Transaction.Set of the tags the rewind erased one of", func(tx *Transaction) (string, error) {
			return "", tx.Set(e, rewindViewTags, []string{"t1", "t2"})
		}, rewindViewTags, "[t1 t2]", "[t1]"},
		{"an ordered set's Add of the element the rewind erased", func(tx *Transaction) (string, error) {
			return "", tx.Add(e, rewindViewOrder, "b")
		}, rewindViewOrder, "[a b]", "[a]"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := schema.NewBuilder().
				Attribute(rewindViewName.String()).Type(schema.TypeString).Add().
				Attribute(rewindViewTags.String()).Type(schema.TypeString).Many().Add().
				Attribute(rewindViewOrder.String()).Type(schema.TypeString).OrderedSet().Add().
				Build()
			require.NoError(t, err)
			gate := rewindGateStore{Store: NewBadgerEmulatorStore(nil), armed: &atomic.Bool{}, opened: make(chan struct{}), release: make(chan struct{})}
			d, err := NewDatabaseWithOptions(DatabaseOptions{Store: gate, Schema: s})
			require.NoError(t, err)
			defer func() { require.NoError(t, d.Close()) }()

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(e, rewindViewName, "Alice"))
			require.NoError(t, tx.Add(e, rewindViewTags, "t1"))
			require.NoError(t, tx.Add(e, rewindViewOrder, "a"))
			_, err = tx.Commit()
			require.NoError(t, err)
			_, err = d.Snapshot("before-bob")
			require.NoError(t, err)
			tx = d.NewTransaction()
			require.NoError(t, tx.Add(e, rewindViewName, "Bob"))
			require.NoError(t, tx.Add(e, rewindViewTags, "t2"))
			require.NoError(t, tx.Add(e, rewindViewOrder, "b"))
			_, err = tx.Commit()
			require.NoError(t, err)

			gate.armed.Store(true)
			opening := make(chan *Transaction, 1)
			go func() { opening <- d.NewTransaction() }()
			var raced *Transaction
			// rewound carries the rewind's answer when it waits on the raced
			// transaction, and is nil when the rewind ran to its end first.
			var rewound chan error
			want := c.after
			select {
			case <-gate.opened:
				if d.mu.TryLock() {
					// The read session opened outside the section that admits
					// the transaction, so the rewind runs to its end before the
					// admission.
					d.mu.Unlock()
					require.NoError(t, d.TruncateTo("before-bob"))
					close(gate.release)
					raced = <-opening
				} else {
					// The admission holds the database's lock while the read
					// session opens, so the rewind waits for it and drains the
					// transaction.
					rewound = make(chan error, 1)
					go func() { rewound <- d.TruncateTo("before-bob") }()
					close(gate.release)
					raced = <-opening
					want = c.drained
				}
			case opened := <-opening:
				// NewTransaction opened no read session, so it is admitted
				// before the rewind or after it. The transaction is opened
				// again after the rewind.
				gate.armed.Store(false)
				require.NoError(t, opened.Rollback())
				require.NoError(t, d.TruncateTo("before-bob"))
				raced = d.NewTransaction()
			}

			answer, err := c.act(raced)
			require.NoError(t, err)
			if c.attr == nil {
				require.NoError(t, raced.Rollback())
			} else {
				_, err = raced.Commit()
				require.NoError(t, err)
			}
			if rewound != nil {
				require.NoError(t, <-rewound)
			}
			if c.attr != nil {
				v, found, err := d.LookupAttribute(e, c.attr)
				require.NoError(t, err)
				require.True(t, found)
				if members, ok := v.([]interface{}); ok {
					rendered := make([]string, len(members))
					for i, m := range members {
						rendered[i] = fmt.Sprint(m)
					}
					sort.Strings(rendered)
					answer = fmt.Sprint(rendered)
				} else {
					answer = fmt.Sprint(v)
				}
			}
			assert.Equal(t, want, answer, "the transaction, on the side of the rewind it is admitted on")
		})
	}
}
