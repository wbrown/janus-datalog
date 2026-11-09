package storage

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// Every datom a run yields is the full decode of its own key, the components
// the run's prefix fixes and the rest alike, over every index and prefix
// length. A bound may hold a value in another Go form than the store holds,
// an int for an int64 or a time outside UTC, and the run's datoms still carry
// the store's.
func TestARunsDatomsAreTheFullDecodeOfTheirKeys(t *testing.T) {
	kw := datalog.NewKeyword(":run/kw")
	str := datalog.NewKeyword(":run/str")
	num := datalog.NewKeyword(":run/num")
	when := datalog.NewKeyword(":run/when")
	long := datalog.NewKeyword(":run/long")
	first, second := datalog.NewIdentity("run:first"), datalog.NewIdentity("run:second")
	kwValue := datalog.NewKeyword(":run.value/one")
	instant := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	longValue := strings.Repeat("a long value the store compresses ", 64)

	type run struct {
		name  string
		bound ScanBound
		count int // the datoms the run holds, 0 for any positive number
	}
	runs := []run{
		{"AVET keyword value", ScanBound{Index: AVET, Prefix: []datalog.Value{kw, kwValue}}, 2},
		{"AVET string value", ScanBound{Index: AVET, Prefix: []datalog.Value{str, "hello"}}, 2},
		{"AVET int bound for an int64", ScanBound{Index: AVET, Prefix: []datalog.Value{num, int(5)}}, 2},
		{"AVET time bound outside UTC", ScanBound{Index: AVET, Prefix: []datalog.Value{when, instant.In(time.FixedZone("east", 3*3600))}}, 2},
		{"AVET compressed value", ScanBound{Index: AVET, Prefix: []datalog.Value{long, longValue}}, 2},
		{"AVET attribute", ScanBound{Index: AVET, Prefix: []datalog.Value{kw}}, 2},
		{"VAET value", ScanBound{Index: VAET, Prefix: []datalog.Value{"hello"}}, 2},
		{"VAET value and attribute", ScanBound{Index: VAET, Prefix: []datalog.Value{int(5), num}}, 2},
		{"EAVT entity", ScanBound{Index: EAVT, Prefix: []datalog.Value{first}}, 5},
		{"EAVT entity and attribute", ScanBound{Index: EAVT, Prefix: []datalog.Value{first, str}}, 1},
		{"EAVT entity, attribute and value", ScanBound{Index: EAVT, Prefix: []datalog.Value{first, when, instant.In(time.FixedZone("west", -5*3600))}}, 1},
		{"EATV entity and attribute", ScanBound{Index: EATV, Prefix: []datalog.Value{second, long}}, 1},
		{"AEVT attribute and entity", ScanBound{Index: AEVT, Prefix: []datalog.Value{num, second}}, 1},
		{"AEVT attribute, entity and value", ScanBound{Index: AEVT, Prefix: []datalog.Value{kw, first, kwValue}}, 1},
		{"AETV attribute", ScanBound{Index: AETV, Prefix: []datalog.Value{when}}, 2},
		{"ATEV attribute", ScanBound{Index: ATEV, Prefix: []datalog.Value{str}}, 2},
		{"TAEV whole index", ScanBound{Index: TAEV}, 0},
	}

	for _, mode := range byteKeyBackends(t) {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := db.NewTransaction()
			for _, e := range []datalog.Identity{first, second} {
				require.NoError(t, tx.Add(e, kw, kwValue))
				require.NoError(t, tx.Add(e, str, "hello"))
				require.NoError(t, tx.Add(e, num, int64(5)))
				require.NoError(t, tx.Add(e, when, instant))
				require.NoError(t, tx.Add(e, long, longValue))
			}
			_, err := tx.Commit()
			require.NoError(t, err)

			session, err := db.Store().(interface{ NewReadSession() (ReadSession, error) }).NewReadSession()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, session.Close()) })
			readers := []struct {
				name   string
				reader StoreReader
			}{
				{"store", db.Store()},
				{"read session", session},
			}

			encoder := &BinaryKeyEncoder{}
			for _, reader := range readers {
				for _, r := range runs {
					it, err := reader.reader.ScanKeysOnly(r.bound)
					require.NoError(t, err, "%s, %s", reader.name, r.name)
					keyed, ok := it.(interface{ Key() []byte })
					require.True(t, ok, "%s, %s: the %s iterator exposes its key", reader.name, r.name, mode.backend.Name)
					count := 0
					for it.Next() {
						datom, err := it.Datom()
						require.NoError(t, err, "%s, %s", reader.name, r.name)
						full, err := DatomFromKey(r.bound.Index, keyed.Key(), encoder, nil)
						require.NoError(t, err, "%s, %s", reader.name, r.name)
						require.Equal(t, full, *datom, "%s, %s: a run's datom is the full decode of its key", reader.name, r.name)
						count++
					}
					require.NoError(t, it.Error(), "%s, %s", reader.name, r.name)
					require.NoError(t, it.Close(), "%s, %s", reader.name, r.name)
					if r.count == 0 {
						require.Positive(t, count, "%s, %s: the run holds datoms", reader.name, r.name)
					} else {
						require.Equal(t, r.count, count, "%s, %s: the datoms the run holds", reader.name, r.name)
					}
				}
			}
		})
	}
}
