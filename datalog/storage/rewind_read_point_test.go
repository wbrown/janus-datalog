package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

var (
	rewindReadTags  = datalog.NewKeyword(":rewindread/tags")
	rewindReadOther = datalog.NewKeyword(":rewindread/other")
)

// rewindReadStores are the stores the rewind read-point tests run on.
var rewindReadStores = []struct {
	name string
	open func() Store
}{
	{"badger-emulator", func() Store { return NewBadgerEmulatorStore(nil) }},
	{"memory-trees", func() Store { return NewMemoryTreeStore(nil) }},
}

// rewindReadDatabase opens a database over store with a set of tags and a
// value declared.
func rewindReadDatabase(t *testing.T, store Store, disableCache bool) *Database {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(rewindReadTags.String()).Type(schema.TypeString).Many().Add().
		Attribute(rewindReadOther.String()).Type(schema.TypeString).Add().
		Build()
	require.NoError(t, err)
	d, err := NewDatabaseWithOptions(DatabaseOptions{Store: store, Schema: s, DisableCache: disableCache})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	return d
}

// rewindReadCommit commits e's a as v in a transaction of its own.
func rewindReadCommit(t *testing.T, d *Database, e datalog.Identity, a datalog.Keyword, v interface{}) {
	t.Helper()
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(e, a, v))
	_, err := tx.Commit()
	require.NoError(t, err)
}

// rewindReadValues renders the sorted ?v of every tuple of rel.
func rewindReadValues(t *testing.T, rel executor.Relation, err error) string {
	t.Helper()
	tuples, err := executor.CollectTuples(rel, err)
	require.NoError(t, err)
	values := make([]string, 0, len(tuples))
	for _, tuple := range tuples {
		values = append(values, fmt.Sprint(tuple[len(tuple)-1]))
	}
	sort.Strings(values)
	return fmt.Sprint(values)
}

// A query whose read session opened before a TruncateTo, read after a commit
// that follows the rewind, answers as the same query does with the cache
// disabled. The commits the rewind erases put the query's read point above
// the arrivals drawn from the restored clock.
func TestAQueryOpenAcrossARewindAnswersAsTheUncachedQuery(t *testing.T) {
	for _, st := range rewindReadStores {
		t.Run(st.name, func(t *testing.T) {
			run := func(t *testing.T, disableCache bool) string {
				d := rewindReadDatabase(t, st.open(), disableCache)
				e := datalog.NewIdentity("rewind-read:e")
				other := datalog.NewIdentity("rewind-read:other")
				rewindReadCommit(t, d, e, rewindReadTags, "a")
				_, err := d.Snapshot("s")
				require.NoError(t, err)
				rewindReadCommit(t, d, e, rewindReadTags, "b")
				for i := 0; i < 8; i++ {
					rewindReadCommit(t, d, other, rewindReadOther, fmt.Sprint(i))
				}
				open, openErr := d.Query(`[:find ?v :where [?e :rewindread/tags ?v]]`)
				require.NoError(t, d.TruncateTo("s"))
				rewindReadCommit(t, d, e, rewindReadTags, "c")
				latestRel, latestErr := d.Query(`[:find ?v :in $ ?e :where [?e :rewindread/tags ?v]]`, e)
				require.Equal(t, "[a c]", rewindReadValues(t, latestRel, latestErr), "the latest view after the rewind and the commit")
				return rewindReadValues(t, open, openErr)
			}
			uncached := run(t, true)
			cached := run(t, false)
			require.Equal(t, uncached, cached, "the cached query answers as the uncached one")
		})
	}
}

// A resolution a query reads from a session opened before a TruncateTo, when
// the query is read after the rewind, is not served to a reader that opens
// after the rewind. The commits after the rewind advance the clock past the
// removed datom's ElementID before the later reader opens.
func TestAResolutionReadBeforeARewindIsNotServedAfterIt(t *testing.T) {
	for _, st := range rewindReadStores {
		t.Run(st.name, func(t *testing.T) {
			run := func(t *testing.T, disableCache bool) string {
				d := rewindReadDatabase(t, st.open(), disableCache)
				e := datalog.NewIdentity("rewind-read:e")
				other := datalog.NewIdentity("rewind-read:other")
				rewindReadCommit(t, d, e, rewindReadTags, "a")
				_, err := d.Snapshot("s")
				require.NoError(t, err)
				rewindReadCommit(t, d, e, rewindReadTags, "b")
				open, openErr := d.Query(`[:find ?v :where [?e :rewindread/tags ?v]]`)
				require.NoError(t, d.TruncateTo("s"))
				t.Logf("disable-cache=%v: the query opened before the rewind answers %s", disableCache, rewindReadValues(t, open, openErr))
				for i := 0; i < 8; i++ {
					rewindReadCommit(t, d, other, rewindReadOther, fmt.Sprint(i))
				}
				laterRel, laterErr := d.Query(`[:find ?v :in $ ?e :where [?e :rewindread/tags ?v]]`, e)
				return rewindReadValues(t, laterRel, laterErr)
			}
			uncached := run(t, true)
			cached := run(t, false)
			require.Equal(t, "[a]", uncached, "the uncached reader after the rewind answers the rewound value")
			require.Equal(t, uncached, cached, "the cached reader answers as the uncached one")
		})
	}
}
