package storage

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// resolutionHeld is an attribute's value as a lookup answers it: whether it
// holds one, and the value, a set's members in sorted order.
type resolutionHeld struct {
	Found bool
	Value interface{}
}

func resolutionHeldOf(attr datalog.Keyword, value interface{}, found bool) resolutionHeld {
	if !found {
		return resolutionHeld{}
	}
	if attr == resolutionTags {
		members := make([]string, 0)
		for _, member := range value.([]interface{}) {
			members = append(members, member.(string))
		}
		sort.Strings(members)
		return resolutionHeld{true, members}
	}
	return resolutionHeld{true, value}
}

// TestTransactionViewIsTheValueItOpenedOnWithItsOwnWrites pins, over generated
// histories, that what a transaction's LookupAttribute answers for each (E, A)
// just before it commits is the value it opened on with its own writes
// applied. A second transaction, opened before it and writing only before it
// opens, commits between its writes, and its datoms are not in the view. The
// reference is a database the second never writes: it commits the same
// history and then the first's writes, and its LookupAttribute answers the
// view.
func TestTransactionViewIsTheValueItOpenedOnWithItsOwnWrites(t *testing.T) {
	profiles := []struct {
		name       string
		entities   int
		attributes []datalog.Keyword
	}{
		{"every attribute of two entities", 2, resolutionAttributes},
		{"the vectors of one entity", 1, []datalog.Keyword{resolutionCells, resolutionOrder}},
	}
	for _, profile := range profiles {
		for _, disableCache := range []bool{false, true} {
			for seed := int64(1); seed <= 400; seed++ {
				t.Run(fmt.Sprintf("%s/cache-off=%v/seed=%d", profile.name, disableCache, seed), func(t *testing.T) {
					r := rand.New(rand.NewSource(seed))
					open := func() *Database {
						db, err := NewDatabaseWithOptions(DatabaseOptions{Store: NewBadgerEmulatorStore(nil), Schema: resolutionSchema(t), DisableCache: disableCache})
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, db.Close()) })
						return db
					}
					db := open()

					first := datalog.NewIdentity("resolution:first")
					second := datalog.NewIdentity("resolution:second")
					entities := []datalog.Identity{first, second}
					written := entities[:profile.entities]
					var log []string

					names := []resolutionWrite{
						{"e0 Add name first", func(tx *Transaction) error { return tx.Add(first, resolutionName, "first") }},
						{"e1 Add name second", func(tx *Transaction) error { return tx.Add(second, resolutionName, "second") }},
					}
					committed := [][]resolutionWrite{names}
					for commits := r.Intn(3); commits > 0; commits-- {
						var batch []resolutionWrite
						for n := 1 + r.Intn(5); n > 0; n-- {
							w := drawResolutionWrite(r, written, profile.attributes)
							log = append(log, "committed: "+w.text)
							batch = append(batch, w)
						}
						committed = append(committed, batch)
					}
					for _, batch := range committed {
						tx := db.NewTransaction()
						for _, w := range batch {
							require.NoError(t, w.apply(tx))
						}
						_, err := tx.Commit()
						require.NoError(t, err)
					}

					earlier := db.NewTransaction()
					for n := r.Intn(4); n > 0; n-- {
						w := drawResolutionWrite(r, written, profile.attributes)
						log = append(log, "earlier: "+w.text)
						require.NoError(t, w.apply(earlier))
					}
					later := db.NewTransaction()
					var laterWrites []resolutionWrite
					for n := r.Intn(4); n > 0; n-- {
						w := drawResolutionWrite(r, written, profile.attributes)
						log = append(log, "later: "+w.text)
						laterWrites = append(laterWrites, w)
						require.NoError(t, w.apply(later))
					}
					_, err := earlier.Commit()
					require.NoError(t, err)
					log = append(log, "earlier commits")
					for n := r.Intn(4); n > 0; n-- {
						w := drawResolutionWrite(r, written, profile.attributes)
						log = append(log, "later: "+w.text)
						laterWrites = append(laterWrites, w)
						require.NoError(t, w.apply(later))
					}

					type held struct {
						e    datalog.Identity
						attr datalog.Keyword
					}
					var keys []held
					views := map[held]resolutionHeld{}
					for _, e := range entities {
						for _, attr := range resolutionAttributes {
							value, found, err := later.LookupAttribute(e, attr)
							require.NoError(t, err)
							keys = append(keys, held{e, attr})
							views[held{e, attr}] = resolutionHeldOf(attr, value, found)
						}
					}
					_, err = later.Commit()
					require.NoError(t, err)

					reference := open()
					for _, batch := range append(committed, laterWrites) {
						tx := reference.NewTransaction()
						for _, w := range batch {
							require.NoError(t, w.apply(tx))
						}
						_, err := tx.Commit()
						require.NoError(t, err)
					}
					for _, key := range keys {
						value, found, err := reference.LookupAttribute(key.e, key.attr)
						require.NoError(t, err)
						name, _, err := reference.LookupAttribute(key.e, resolutionName)
						require.NoError(t, err)
						require.Equal(t, resolutionHeldOf(key.attr, value, found), views[key],
							"%v %s: the transaction held another value than the one it opened on with its own writes\n%s", name, key.attr, strings.Join(log, "\n"))
					}
				})
			}
		}
	}
}
