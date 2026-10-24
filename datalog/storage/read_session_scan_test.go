package storage

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// A read session opened on a store, with no write after it, scans a bound as
// the store does: the same datoms in the same order, under the same seeks,
// with the same intake. The sizes straddle the tree store's chunk boundaries
// and the seeks land across them, on every backend.
func TestReadSessionScanYieldsWhatTheStoreScanYields(t *testing.T) {
	attrs := []datalog.Keyword{datalog.NewKeyword(":scan/a"), datalog.NewKeyword(":scan/b")}
	const distinctEntities = 37
	for _, testCase := range storeContractCases() {
		for _, n := range []int{1, 255, 256, 257, 511, 512, 513, 1000} {
			t.Run(fmt.Sprintf("%s/%d", testCase.name, n), func(t *testing.T) {
				store := testCase.open(t, &BinaryKeyEncoder{})
				defer store.Close()
				entities := make([]datalog.Identity, 0, n)
				datoms := make([]datalog.Datom, 0, n)
				for i := 0; i < n; i++ {
					e := datalog.NewIdentity(fmt.Sprintf("scan:%d", i%distinctEntities))
					entities = append(entities, e)
					datoms = append(datoms, datalog.Datom{
						E: e, A: attrs[i%2], V: int64(i),
						Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 1},
					})
				}
				require.NoError(t, store.Assert(datoms))
				session, err := store.NewReadSession()
				require.NoError(t, err)
				defer session.Close()

				var bounds []ScanBound
				for _, index := range Indices {
					bounds = append(bounds, ScanBound{Index: index})
				}
				for i := 0; i < len(entities) && i < distinctEntities; i++ {
					bounds = append(bounds, ScanBound{Index: EAVT, Prefix: []datalog.Value{entities[i]}})
				}
				for _, a := range attrs {
					bounds = append(bounds, ScanBound{Index: AEVT, Prefix: []datalog.Value{a}})
				}

				r := rand.New(rand.NewSource(int64(n)))
				for _, bound := range bounds {
					for trial := 0; trial < 40; trial++ {
						seeks := map[int]ScanBound{}
						if trial > 0 {
							for s := 0; s < 3; s++ {
								var seek ScanBound
								switch bound.Index {
								case EAVT:
									seek = ScanBound{Index: EAVT, Prefix: []datalog.Value{entities[r.Intn(len(entities))]}}
									if len(bound.Prefix) > 0 {
										seek = ScanBound{Index: EAVT, Prefix: []datalog.Value{bound.Prefix[0], attrs[r.Intn(2)]}}
									}
								case AEVT:
									seek = ScanBound{Index: AEVT, Prefix: []datalog.Value{attrs[r.Intn(2)], entities[r.Intn(len(entities))]}}
								default:
									continue
								}
								seeks[1+r.Intn(n+1)] = seek
							}
						}
						fromStore, err := store.Scan(bound)
						require.NoError(t, err)
						fromSession, err := session.Scan(bound)
						require.NoError(t, err)
						storeDatoms, storeIntake := collectSeekingScan(t, fromStore, seeks)
						sessionDatoms, sessionIntake := collectSeekingScan(t, fromSession, seeks)
						require.Equal(t, storeDatoms, sessionDatoms,
							"bound %v with seeks %v: the session yields what the store yields", bound, seeks)
						require.Equal(t, storeIntake, sessionIntake,
							"bound %v with seeks %v: the session's intake is the store's", bound, seeks)
					}
				}
			})
		}
	}
}

// collectSeekingScan copies the datoms an iterator yields, seeking to the
// bound seeks names after the yield it keys, and returns them with the
// iterator's intake.
func collectSeekingScan(t *testing.T, it Iterator, seeks map[int]ScanBound) ([]datalog.Datom, int) {
	t.Helper()
	var out []datalog.Datom
	yielded := 0
	for it.Next() {
		d, err := it.Datom()
		require.NoError(t, err)
		out = append(out, *d)
		yielded++
		if bound, ok := seeks[yielded]; ok {
			it.Seek(bound)
		}
	}
	require.NoError(t, it.Error())
	require.NoError(t, it.Close())
	return out, it.Scanned()
}
