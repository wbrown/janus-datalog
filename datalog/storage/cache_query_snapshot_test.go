package storage

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// snapshotValues is the sorted ?v of every tuple of rel.
func snapshotValues(t *testing.T, rel executor.Relation, err error) []string {
	t.Helper()
	require.NoError(t, err)
	position := query.SymbolIndex(rel.Symbols(), datalog.NewSymbol("?v"))
	require.GreaterOrEqual(t, position, 0, "the query binds ?v")
	tuples, err := executor.CollectTuples(rel, nil)
	require.NoError(t, err)
	out := make([]string, 0, len(tuples))
	for _, tuple := range tuples {
		s, ok := tuple[position].(string)
		require.Truef(t, ok, "?v is a string, got %T", tuple[position])
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// snapshotWrite is one write of a generated run: e's a takes v, or loses it.
type snapshotWrite struct {
	entity int
	many   bool
	value  string
	remove bool
}

// snapshotRun is a generated run: committed transactions, a transaction open
// across the queries' opening, and transactions committed between its writes
// and the queries' opening.
type snapshotRun struct {
	committed [][]snapshotWrite
	open      []snapshotWrite
	between   [][]snapshotWrite
}

// snapshotAnswer is a query's answer as a set of (entity, value) pairs.
type snapshotAnswer map[struct {
	E [20]byte
	V string
}]bool

func generateSnapshotRun(r *rand.Rand) snapshotRun {
	write := func() snapshotWrite {
		w := snapshotWrite{entity: r.Intn(3), many: r.Intn(2) == 0}
		if w.many {
			w.value = []string{"a", "b", "c", "d"}[r.Intn(4)]
			w.remove = r.Intn(4) == 0
		} else {
			w.value = []string{"s0", "s1", "s2"}[r.Intn(3)]
		}
		return w
	}
	txs := func(n int) [][]snapshotWrite {
		out := make([][]snapshotWrite, n)
		for i := range out {
			for j := 0; j < 1+r.Intn(3); j++ {
				out[i] = append(out[i], write())
			}
		}
		return out
	}
	run := snapshotRun{committed: txs(1 + r.Intn(3)), between: txs(r.Intn(2))}
	for j := 0; j < 1+r.Intn(3); j++ {
		run.open = append(run.open, write())
	}
	return run
}

// TestAQueryOpenAcrossACommitAnswersAsTheUncachedQueryOnGeneratedRuns is the
// generated form of the pinned cases: on each run, the queries opened before
// the open transaction commits answer, once consumed after a latest reader has
// read every key, as the same queries do on a database with the cache
// disabled.
func TestAQueryOpenAcrossACommitAnswersAsTheUncachedQueryOnGeneratedRuns(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":gen/st").Type(schema.TypeString).Add().
		Attribute(":gen/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	st := datalog.NewKeyword(":gen/st")
	tags := datalog.NewKeyword(":gen/tags")
	entities := []datalog.Identity{
		datalog.NewIdentity("gen:e0"),
		datalog.NewIdentity("gen:e1"),
		datalog.NewIdentity("gen:e2"),
	}
	queries := []struct {
		name  string
		text  string
		value bool
	}{
		{name: "a set scan of every entity", text: `[:find ?e ?v :where [?e :gen/tags ?v]]`, value: true},
		{name: "a value-bound match", text: `[:find ?e :where [?e :gen/st "s1"]]`},
	}

	apply := func(t *testing.T, tx *Transaction, writes []snapshotWrite) {
		for _, w := range writes {
			a := st
			if w.many {
				a = tags
			}
			if w.remove {
				require.NoError(t, tx.Remove(entities[w.entity], a, w.value))
			} else {
				require.NoError(t, tx.Add(entities[w.entity], a, w.value))
			}
		}
	}
	answerOf := func(t *testing.T, rel executor.Relation, err error, value bool) snapshotAnswer {
		require.NoError(t, err)
		ePos := query.SymbolIndex(rel.Symbols(), datalog.NewSymbol("?e"))
		vPos := query.SymbolIndex(rel.Symbols(), datalog.NewSymbol("?v"))
		tuples, err := executor.CollectTuples(rel, nil)
		require.NoError(t, err)
		out := snapshotAnswer{}
		for _, tuple := range tuples {
			k := struct {
				E [20]byte
				V string
			}{E: tuple[ePos].(datalog.Identity).Hash()}
			if value {
				k.V = tuple[vPos].(string)
			}
			out[k] = true
		}
		return out
	}
	runOn := func(t *testing.T, run snapshotRun, disableCache bool) []snapshotAnswer {
		d, err := NewDatabaseWithOptions(DatabaseOptions{Store: NewMemoryTreeStore(nil), Schema: s, DisableCache: disableCache})
		require.NoError(t, err)
		defer func() { require.NoError(t, d.Close()) }()
		for _, writes := range run.committed {
			tx := d.NewTransaction()
			apply(t, tx, writes)
			_, err := tx.Commit()
			require.NoError(t, err)
		}
		open := d.NewTransaction()
		apply(t, open, run.open)
		for _, writes := range run.between {
			tx := d.NewTransaction()
			apply(t, tx, writes)
			_, err := tx.Commit()
			require.NoError(t, err)
		}
		type opened struct {
			rel executor.Relation
			err error
		}
		pending := make([]opened, len(queries))
		for i, q := range queries {
			rel, err := d.Query(q.text)
			pending[i] = opened{rel, err}
		}
		_, err = open.Commit()
		require.NoError(t, err)
		for _, e := range entities {
			for _, text := range []string{
				`[:find ?v :in $ ?e :where [?e :gen/st ?v]]`,
				`[:find ?v :in $ ?e :where [?e :gen/tags ?v]]`,
			} {
				rel, err := d.Query(text, e)
				require.NoError(t, err)
				_, err = executor.CollectTuples(rel, nil)
				require.NoError(t, err)
			}
		}
		answers := make([]snapshotAnswer, len(queries))
		for i, q := range queries {
			answers[i] = answerOf(t, pending[i].rel, pending[i].err, q.value)
		}
		return answers
	}

	const runs = 300
	diverged, divergedWithNoneBetween := 0, 0
	var first string
	for seed := int64(1); seed <= runs; seed++ {
		run := generateSnapshotRun(rand.New(rand.NewSource(seed)))
		cached := runOn(t, run, false)
		uncached := runOn(t, run, true)
		for i, q := range queries {
			if !reflect.DeepEqual(cached[i], uncached[i]) {
				diverged++
				if len(run.between) == 0 {
					divergedWithNoneBetween++
				}
				if first == "" {
					first = fmt.Sprintf("seed %d, %s: run %+v: cached %d pairs, uncached %d pairs", seed, q.name, run, len(cached[i]), len(uncached[i]))
				}
			}
		}
	}
	require.Zerof(t, diverged, "%d of %d query answers diverge from the uncached query, %d on runs with no transaction committed between; first: %s", diverged, runs*len(queries), divergedWithNoneBetween, first)
}

// TestAQueryOpenAcrossACommitAnswersItsSnapshotWhateverTheCacheHolds pins that
// a query answers the state its read session holds: a query opened before a
// transaction commits answers without that transaction's datoms, whether or
// not a later reader has cached the key, and answers as the same query does on
// a database with the cache disabled.
func TestAQueryOpenAcrossACommitAnswersItsSnapshotWhateverTheCacheHolds(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":snap/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	tags := datalog.NewKeyword(":snap/tags")
	unboundScan := `[:find ?v :where [?e :snap/tags ?v]]`
	boundEntity := `[:find ?v :in $ ?e :where [?e :snap/tags ?v]]`

	cases := []struct {
		name string
		// earlier is committed before the open transaction begins.
		earlier []string
		// open is added by a transaction begun before the query opens.
		open string
		// between is committed by another transaction after open is added and
		// before the query opens.
		between []string
		// want is the query's answer: the state its session opened on.
		want []string
	}{
		{
			name:    "the open transaction's member is the key's highest",
			earlier: []string{"a"},
			open:    "b",
			want:    []string{"a"},
		},
		{
			name:    "the open transaction's member lies below a member committed between",
			earlier: []string{"a"},
			open:    "b",
			between: []string{"c"},
			want:    []string{"a", "c"},
		},
	}

	for _, store := range []struct {
		name string
		make func() Store
	}{
		{"memory-trees", func() Store { return NewMemoryTreeStore(nil) }},
		{"badger-emulator", func() Store { return NewBadgerEmulatorStore(nil) }},
	} {
		t.Run(store.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					run := func(t *testing.T, disableCache bool) []string {
						d, err := NewDatabaseWithOptions(DatabaseOptions{Store: store.make(), Schema: s, DisableCache: disableCache})
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, d.Close()) })
						e := datalog.NewIdentity("snap:e")

						tx := d.NewTransaction()
						for _, v := range tc.earlier {
							require.NoError(t, tx.Add(e, tags, v))
						}
						_, err = tx.Commit()
						require.NoError(t, err)

						open := d.NewTransaction()
						require.NoError(t, open.Add(e, tags, tc.open))
						if len(tc.between) > 0 {
							between := d.NewTransaction()
							for _, v := range tc.between {
								require.NoError(t, between.Add(e, tags, v))
							}
							_, err = between.Commit()
							require.NoError(t, err)
						}

						rel, queryErr := d.Query(unboundScan)
						_, err = open.Commit()
						require.NoError(t, err)

						latestRel, latestErr := d.Query(boundEntity, e)
						latest := snapshotValues(t, latestRel, latestErr)
						require.Contains(t, latest, tc.open, "the latest view holds the committed member")

						return snapshotValues(t, rel, queryErr)
					}
					cached := run(t, false)
					uncached := run(t, true)
					require.Equal(t, tc.want, uncached, "the uncached query answers its session's state")
					require.Equal(t, uncached, cached, "the cached query answers as the uncached one")
				})
			}
		})
	}
}
