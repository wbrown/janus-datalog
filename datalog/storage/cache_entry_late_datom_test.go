package storage

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// lateDatomTarget is an attribute whose resolution a datom arriving below the
// highest ElementID among its datoms changes without raising that ElementID.
type lateDatomTarget struct {
	name string
	attr datalog.Keyword
	// values is a LookupAttribute answer in the order the attribute keeps:
	// a set's members sorted, a vector's elements as they stand.
	values func(t *testing.T, value interface{}) []string
	// tuplePerMember is whether a match of the attribute binds one tuple per
	// member, as a set's does, where a vector's binds one tuple holding it.
	tuplePerMember bool
}

var lateDatomTargets = []lateDatomTarget{
	{
		name:           "set",
		attr:           datalog.NewKeyword(":late/tags"),
		tuplePerMember: true,
		values: func(t *testing.T, value interface{}) []string {
			members, ok := value.([]interface{})
			require.Truef(t, ok, "a set answers []interface{}, got %T", value)
			out := make([]string, 0, len(members))
			for _, member := range members {
				s, ok := member.(string)
				require.Truef(t, ok, "a member of :late/tags is a string, got %T", member)
				out = append(out, s)
			}
			sort.Strings(out)
			return out
		},
	},
	{
		name: "vector",
		attr: datalog.NewKeyword(":late/cells"),
		values: func(t *testing.T, value interface{}) []string {
			elements, ok := value.([]string)
			require.Truef(t, ok, "a string vector answers []string, got %T", value)
			return elements
		},
	},
}

// addedBeneathACommit opens a transaction that adds "y" to the target, then
// commits another that adds "z", and returns the first uncommitted. Its datom
// is drawn below the other's, so when it commits it arrives below the highest
// ElementID among the target's datoms. Both elements follow HEAD, so the
// vector holds y before z.
func addedBeneathACommit(t *testing.T, d *Database, e datalog.Identity, target lateDatomTarget) *Transaction {
	t.Helper()
	early := d.NewTransaction()
	require.NoError(t, early.Add(e, target.attr, "y"))
	later := d.NewTransaction()
	require.NoError(t, later.Add(e, target.attr, "z"))
	_, err := later.Commit()
	require.NoError(t, err)
	return early
}

// openSessionMatcher is the matcher a query reads through, with its session
// open from now until the test ends.
func openSessionMatcher(t *testing.T, d *Database) *PatternMatcher {
	t.Helper()
	matcher, session, err := d.sessionMatcher(d.effectivePlannerOptions())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close()) })
	return matcher.(*PatternMatcher)
}

// lookedUpValues is the target's value for e through the matcher's
// LookupAttribute, which reads the cache first.
func lookedUpValues(t *testing.T, m *PatternMatcher, e datalog.Identity, target lateDatomTarget) []string {
	t.Helper()
	value, found, err := m.LookupAttribute(e, target.attr)
	require.NoError(t, err)
	require.True(t, found, "the target has been written")
	return target.values(t, value)
}

// matchedValues is the target's value through the matcher's match of
// [?e attr ?v], the scan of every entity carrying the attribute, where e is
// the one entity that carries it.
func matchedValues(t *testing.T, m *PatternMatcher, target lateDatomTarget) []string {
	t.Helper()
	v := query.Variable{Name: datalog.NewSymbol("?v")}
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: datalog.NewSymbol("?e")},
		query.Constant{Value: target.attr},
		v,
	}}
	rel, err := m.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)
	position := query.SymbolIndex(rel.Symbols(), v.Name)
	tuples, err := executor.CollectTuples(rel, nil)
	require.NoError(t, err)
	matched := make([]interface{}, 0, len(tuples))
	for _, tuple := range tuples {
		matched = append(matched, tuple[position])
	}
	if target.tuplePerMember {
		return target.values(t, matched)
	}
	require.Len(t, matched, 1, "a match of a vector binds one tuple holding it")
	return target.values(t, matched[0])
}

// committedValue commits a transaction that adds value to the target and
// returns its mark, the ElementID a view as of the commit reads to.
func committedValue(t *testing.T, d *Database, e datalog.Identity, target lateDatomTarget, value string) datalog.ElementID {
	t.Helper()
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(e, target.attr, value))
	mark, err := tx.Commit()
	require.NoError(t, err)
	return mark
}

// commitAfterScanReader reads through its StoreReader and runs commit once,
// when the first scan it opened closes: after a resolution has read its datoms
// and before the cache stores what it resolved.
type commitAfterScanReader struct {
	StoreReader
	commit func()
	fired  bool
}

func (r *commitAfterScanReader) Scan(bound ScanBound) (Iterator, error) {
	iter, err := r.StoreReader.Scan(bound)
	if err != nil {
		return nil, err
	}
	return &commitOnCloseIterator{Iterator: iter, reader: r}, nil
}

func (r *commitAfterScanReader) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	iter, err := r.StoreReader.ScanKeysOnly(bound)
	if err != nil {
		return nil, err
	}
	return &commitOnCloseIterator{Iterator: iter, reader: r}, nil
}

type commitOnCloseIterator struct {
	Iterator
	reader *commitAfterScanReader
}

func (it *commitOnCloseIterator) Close() error {
	err := it.Iterator.Close()
	if !it.reader.fired {
		it.reader.fired = true
		it.reader.commit()
	}
	return err
}

// TestCacheEntryIsTheResolutionOfEveryDatomOfItsKey pins the cache's contract
// with the datoms of a key: an entry serves a reader only when no datom
// arrived on its key after the reader's read point, so a session is answered
// the value it opened on, and the cache stores an entry as fresh only when it
// is the resolution of every datom of its key the store holds. A case either
// brings in a datom that arrives below the highest ElementID among the
// target's datoms, which changes the resolution and leaves that ElementID
// where it was, or reads as of a commit before the target's last.
func TestCacheEntryIsTheResolutionOfEveryDatomOfItsKey(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":late/tags").Type(schema.TypeString).Many().Add().
		Attribute(":late/cells").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	cases := []struct {
		name string
		run  func(t *testing.T, d *Database, e datalog.Identity, target lateDatomTarget)
	}{
		{
			name: "a session is not served an entry holding a datom that arrived after it opened",
			run: func(t *testing.T, d *Database, e datalog.Identity, target lateDatomTarget) {
				early := addedBeneathACommit(t, d, e, target)
				sessioned := openSessionMatcher(t, d)
				_, err := early.Commit()
				require.NoError(t, err)

				require.Equal(t, []string{"y", "z"}, lookedUpValues(t, d.Matcher().(*PatternMatcher), e, target),
					"the latest view resolves every datom of the key")
				require.Equal(t, []string{"z"}, lookedUpValues(t, sessioned, e, target),
					"y arrived after the session opened, so the session's value lacks it")
			},
		},
		{
			name: "a session's resolution without a datom that arrived after it opened is not stored as fresh",
			run: func(t *testing.T, d *Database, e datalog.Identity, target lateDatomTarget) {
				early := addedBeneathACommit(t, d, e, target)
				sessioned := openSessionMatcher(t, d)
				_, err := early.Commit()
				require.NoError(t, err)

				require.Equal(t, []string{"z"}, lookedUpValues(t, sessioned, e, target),
					"the session's resolution lacks y, which arrived after the session opened")
				require.Equal(t, []string{"y", "z"}, lookedUpValues(t, d.Matcher().(*PatternMatcher), e, target),
					"the latest view resolves every datom of the key")
			},
		},
		{
			name: "a session's prefetch without a datom that arrived after it opened is not stored as fresh",
			run: func(t *testing.T, d *Database, e datalog.Identity, target lateDatomTarget) {
				early := addedBeneathACommit(t, d, e, target)
				sessioned := openSessionMatcher(t, d)
				_, err := early.Commit()
				require.NoError(t, err)

				require.NoError(t, sessioned.PrefetchEntities([]datalog.Identity{e}))
				require.Equal(t, []string{"y", "z"}, lookedUpValues(t, d.Matcher().(*PatternMatcher), e, target),
					"the latest view resolves every datom of the key")
			},
		},
		{
			name: "a session's resolution after the cache is cleared, without a datom that arrived after it opened, is not stored as fresh",
			run: func(t *testing.T, d *Database, e datalog.Identity, target lateDatomTarget) {
				early := addedBeneathACommit(t, d, e, target)
				sessioned := openSessionMatcher(t, d)
				_, err := early.Commit()
				require.NoError(t, err)
				d.Cache().Clear()

				require.Equal(t, []string{"z"}, lookedUpValues(t, sessioned, e, target),
					"the session's resolution lacks y, which arrived after the session opened")
				require.Equal(t, []string{"y", "z"}, lookedUpValues(t, d.Matcher().(*PatternMatcher), e, target),
					"the latest view resolves every datom of the key")
			},
		},
		{
			name: "a resolution a datom arrives during is not stored as fresh",
			run: func(t *testing.T, d *Database, e datalog.Identity, target lateDatomTarget) {
				early := addedBeneathACommit(t, d, e, target)
				hooked := d.Matcher().(*PatternMatcher)
				hooked.reader = &commitAfterScanReader{
					StoreReader: hooked.reader,
					commit: func() {
						_, err := early.Commit()
						require.NoError(t, err)
					},
				}

				require.Equal(t, []string{"z"}, lookedUpValues(t, hooked, e, target),
					"the resolution read its datoms before y arrived, so it lacks y")
				require.Equal(t, []string{"y", "z"}, lookedUpValues(t, d.Matcher().(*PatternMatcher), e, target),
					"the latest view resolves every datom of the key")
			},
		},
		{
			name: "a matcher's as-of copy looks up the view as of its target after the latest view of its key is cached",
			run: func(t *testing.T, d *Database, e datalog.Identity, target lateDatomTarget) {
				mark := committedValue(t, d, e, target, "y")
				committedValue(t, d, e, target, "z")
				require.Equal(t, []string{"y", "z"}, lookedUpValues(t, d.Matcher().(*PatternMatcher), e, target),
					"the latest view resolves every datom of the key")

				require.Equal(t, []string{"y"}, lookedUpValues(t, d.Matcher().(*PatternMatcher).AsOf(mark), e, target),
					"the as-of copy reads the datoms at or below its target")
			},
		},
		{
			name: "a matcher's as-of copy scans the view as of its target after the latest view of its key is cached",
			run: func(t *testing.T, d *Database, e datalog.Identity, target lateDatomTarget) {
				mark := committedValue(t, d, e, target, "y")
				committedValue(t, d, e, target, "z")
				require.Equal(t, []string{"y", "z"}, lookedUpValues(t, d.Matcher().(*PatternMatcher), e, target),
					"the latest view resolves every datom of the key")

				require.Equal(t, []string{"y"}, matchedValues(t, d.Matcher().(*PatternMatcher).AsOf(mark), target),
					"the as-of copy's scan reads the datoms at or below its target")
			},
		},
		{
			name: "a matcher's as-of copy does not store its view as the latest view where a commit left the key's slot without an entry",
			run: func(t *testing.T, d *Database, e datalog.Identity, target lateDatomTarget) {
				mark := committedValue(t, d, e, target, "y")
				committedValue(t, d, e, target, "z")

				require.Equal(t, []string{"y"}, lookedUpValues(t, d.Matcher().(*PatternMatcher).AsOf(mark), e, target),
					"the as-of copy reads the datoms at or below its target")
				require.Equal(t, []string{"y", "z"}, lookedUpValues(t, d.Matcher().(*PatternMatcher), e, target),
					"the latest view resolves every datom of the key")
			},
		},
		{
			name: "a matcher's as-of copy on a database whose cache holds no slot for the key does not store its view as the latest view",
			run: func(t *testing.T, d *Database, e datalog.Identity, target lateDatomTarget) {
				mark := committedValue(t, d, e, target, "y")
				committedValue(t, d, e, target, "z")
				reopened, err := NewDatabaseWithOptions(DatabaseOptions{Store: d.store, PlannerOptions: d.plannerOptions})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, reopened.Close()) })

				require.Equal(t, []string{"y"}, lookedUpValues(t, reopened.Matcher().(*PatternMatcher).AsOf(mark), e, target),
					"the as-of copy reads the datoms at or below its target")
				require.Equal(t, []string{"y", "z"}, lookedUpValues(t, reopened.Matcher().(*PatternMatcher), e, target),
					"the latest view resolves every datom of the key")
			},
		},
		{
			name: "an as-of handle's view holds a datom at or below its target that arrives after the handle read the key",
			run: func(t *testing.T, d *Database, e datalog.Identity, target lateDatomTarget) {
				early := d.NewTransaction()
				require.NoError(t, early.Add(e, target.attr, "y"))
				mark := committedValue(t, d, e, target, "z")
				handle := d.AsOf(mark).Matcher().(*PatternMatcher)
				require.Equal(t, []string{"z"}, lookedUpValues(t, handle, e, target),
					"the handle reads before y arrives, so its resolution lacks y")
				_, err := early.Commit()
				require.NoError(t, err)

				require.Equal(t, []string{"y", "z"}, lookedUpValues(t, handle, e, target),
					"y was drawn below the handle's target, so the view as of the target holds it once it arrives")
			},
		},
	}

	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, target := range lateDatomTargets {
				t.Run(target.name, func(t *testing.T) {
					for _, tc := range cases {
						t.Run(tc.name, func(t *testing.T) {
							d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
							tc.run(t, d, datalog.NewIdentity("late:e"), target)
						})
					}
				})
			}
		})
	}
}

// TestCacheServesAUniqueEntryOnlyToAViewHoldingEveryClaimItRead pins the
// cache's contract on a unique attribute, whose resolution for one entity
// reads the other entities' claims to its value: an entry is served to a
// reader only when every datom its resolution read, those claims included,
// lies at or below the reader's read point. In each case the second entity's
// claim supersedes the first entity's in the latest view, and the reader's
// view holds the first entity's claim alone.
func TestCacheServesAUniqueEntryOnlyToAViewHoldingEveryClaimItRead(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":late/handle").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		Build()
	require.NoError(t, err)
	handle := datalog.NewKeyword(":late/handle")
	first := datalog.NewIdentity("late:first")
	second := datalog.NewIdentity("late:second")

	// claim commits e's claim to value and returns its mark.
	claim := func(t *testing.T, d *Database, e datalog.Identity, value string) datalog.ElementID {
		t.Helper()
		tx := d.NewTransaction()
		require.NoError(t, tx.Add(e, handle, value))
		mark, err := tx.Commit()
		require.NoError(t, err)
		return mark
	}

	cases := []struct {
		name string
		run  func(t *testing.T, d *Database)
	}{
		{
			name: "an as-of view holding the first claim is not served the latest view's absence",
			run: func(t *testing.T, d *Database) {
				mark := claim(t, d, first, "y")
				claim(t, d, second, "y")
				_, found, err := d.Matcher().(*PatternMatcher).LookupAttribute(first, handle)
				require.NoError(t, err)
				require.False(t, found, "in the latest view the second entity's claim supersedes the first's")

				value, found, err := d.AsOf(mark).Matcher().(*PatternMatcher).LookupAttribute(first, handle)
				require.NoError(t, err)
				require.True(t, found, "as of the first claim, the first entity holds the value")
				require.Equal(t, "y", value)
			},
		},
		{
			name: "an as-of view holding the first entity's later claim is not served the value the latest view falls back to",
			run: func(t *testing.T, d *Database) {
				claim(t, d, first, "x")
				mark := claim(t, d, first, "y")
				claim(t, d, second, "y")
				value, found, err := d.Matcher().(*PatternMatcher).LookupAttribute(first, handle)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, "x", value, "in the latest view the first entity falls back past its superseded claim")

				value, found, err = d.AsOf(mark).Matcher().(*PatternMatcher).LookupAttribute(first, handle)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, "y", value, "as of its later claim, the first entity holds that claim's value")
			},
		},
		{
			name: "a session opened before the second claim is not served the latest view's absence",
			run: func(t *testing.T, d *Database) {
				claim(t, d, first, "y")
				sessioned := openSessionMatcher(t, d)
				claim(t, d, second, "y")
				_, found, err := d.Matcher().(*PatternMatcher).LookupAttribute(first, handle)
				require.NoError(t, err)
				require.False(t, found, "in the latest view the second entity's claim supersedes the first's")

				value, found, err := sessioned.LookupAttribute(first, handle)
				require.NoError(t, err)
				require.True(t, found, "the session's view holds the first entity's claim alone")
				require.Equal(t, "y", value)
			},
		},
	}

	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
					tc.run(t, d)
				})
			}
		})
	}
}

// TestAnAsOfViewKeepsItsResolutionOfAKey pins the cache's contract with a view
// as of a target: the view keeps its resolution of a key, whatever was written
// after the target, so a repeated read of the key reads no index, and a drop
// of the key's entries reaches the view.
func TestAnAsOfViewKeepsItsResolutionOfAKey(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":late/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	set := lateDatomTargets[0]
	e := datalog.NewIdentity("late:e")

	cases := []struct {
		name string
		// scanned is the index intake of the last LookupAttribute.
		run func(t *testing.T, d *Database, scanned func() int)
	}{
		{
			name: "an as-of handle's second read of a key written after its target reads no index",
			run: func(t *testing.T, d *Database, scanned func() int) {
				mark := committedValue(t, d, e, set, "y")
				committedValue(t, d, e, set, "z")
				view := d.AsOf(mark)

				require.Equal(t, []string{"y"}, lookedUpValues(t, view.Matcher().(*PatternMatcher), e, set))
				require.Positive(t, scanned(), "the first read resolves the view from the index")
				require.Equal(t, []string{"y"}, lookedUpValues(t, view.Matcher().(*PatternMatcher), e, set))
				require.Zero(t, scanned(), "the view keeps its resolution of the key")
			},
		},
		{
			name: "a matcher's as-of copy's second read of a key written after its target reads no index",
			run: func(t *testing.T, d *Database, scanned func() int) {
				mark := committedValue(t, d, e, set, "y")
				committedValue(t, d, e, set, "z")
				copied := d.Matcher().(*PatternMatcher).AsOf(mark)

				require.Equal(t, []string{"y"}, lookedUpValues(t, copied, e, set))
				require.Positive(t, scanned(), "the first read resolves the view from the index")
				require.Equal(t, []string{"y"}, lookedUpValues(t, copied, e, set))
				require.Zero(t, scanned(), "the copy keeps its resolution of the key")
			},
		},
		{
			name: "a latest reader and an as-of handle of one key each keep their resolution",
			run: func(t *testing.T, d *Database, scanned func() int) {
				mark := committedValue(t, d, e, set, "y")
				committedValue(t, d, e, set, "z")
				view := d.AsOf(mark)

				require.Equal(t, []string{"y", "z"}, lookedUpValues(t, d.Matcher().(*PatternMatcher), e, set))
				require.Equal(t, []string{"y"}, lookedUpValues(t, view.Matcher().(*PatternMatcher), e, set))
				require.Equal(t, []string{"y", "z"}, lookedUpValues(t, d.Matcher().(*PatternMatcher), e, set))
				require.Zero(t, scanned(), "the latest view's resolution stands beside the as-of view's")
				require.Equal(t, []string{"y"}, lookedUpValues(t, view.Matcher().(*PatternMatcher), e, set))
				require.Zero(t, scanned(), "the as-of view's resolution stands beside the latest view's")
			},
		},
		{
			name: "an as-of handle answers without a datom a rewind removed after it read the key",
			run: func(t *testing.T, d *Database, scanned func() int) {
				committedValue(t, d, e, set, "y")
				_, err := d.Snapshot("before-z")
				require.NoError(t, err)
				mark := committedValue(t, d, e, set, "z")
				view := d.AsOf(mark)
				require.Equal(t, []string{"y", "z"}, lookedUpValues(t, view.Matcher().(*PatternMatcher), e, set))

				require.NoError(t, d.TruncateTo("before-z"))
				require.Equal(t, []string{"y"}, lookedUpValues(t, view.Matcher().(*PatternMatcher), e, set),
					"the rewind removed z, so the view as of its target no longer holds it")
			},
		},
	}

	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					var last int
					d := createOptimizerModeDB(t, mode, DatabaseOptions{
						Schema: s,
						AnnotationHandler: func(event annotations.Event) {
							if event.Name == annotations.StorageResolveComplete {
								last = event.Data[annotations.KeyDatomsScanned].(int)
							}
						},
					})
					tc.run(t, d, func() int { return last })
				})
			}
		})
	}
}
