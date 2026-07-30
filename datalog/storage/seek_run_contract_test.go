package storage

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TestSeekHonoursTheRunItNames pins that a seek names a complete run, not just
// where to start it.
//
// A ScanBound names a run — EncodeScanBound returns its start, its end and the
// membership rule that narrows it — and Seek takes a ScanBound. Honouring the
// start and the membership rule while discarding the end leaves the caller
// holding an iterator that walks off the end of the run it asked for, with no
// way to say where it stops. The caller then re-derives the end itself, and the
// only material it has is the encoded key: that is how key[1:21] came to be
// sliced in pull_batch.go, above the seam whose purpose is that callers never
// hold a key layout.
//
// The shape under test is the one that path uses and the one the shared-scan
// optimisation depends on: open a scan wide, seek within it repeatedly, and get
// each sought run and nothing else. The second seek is not decoration — an
// iterator that stopped by exhausting itself could pass the first assertion and
// fail every caller that reuses it.
func TestSeekHonoursTheRunItNames(t *testing.T) {
	for _, testCase := range storeContractCases() {
		t.Run(testCase.name, func(t *testing.T) {
			store := testCase.open(t, &BinaryKeyEncoder{})
			defer store.Close()

			// Two entities, three attributes each, so a run that overruns its
			// entity lands in the next one rather than off the end of the index
			// — the failure a whole-index scan would otherwise hide.
			entities := []datalog.Identity{
				datalog.NewIdentity("seek:alice"),
				datalog.NewIdentity("seek:bob"),
			}
			attrs := []datalog.Keyword{
				datalog.NewKeyword(":seek/one"),
				datalog.NewKeyword(":seek/two"),
				datalog.NewKeyword(":seek/three"),
			}
			var datoms []datalog.Datom
			lamport := uint64(1)
			for _, e := range entities {
				for _, a := range attrs {
					datoms = append(datoms, datalog.Datom{
						E: e, A: a, V: int64(lamport),
						Tx: datalog.ElementID{Lamport: lamport, ReplicaID: 1},
					})
					lamport++
				}
			}
			tx, err := store.BeginTx()
			require.NoError(t, err)
			require.NoError(t, tx.Assert(datoms))
			require.NoError(t, tx.Commit())

			// EATV keys order by entity hash, so seek in that order: this is the
			// shared-scan pattern, one forward pass with a seek per entity.
			sort.Slice(entities, func(i, j int) bool {
				return string(entities[i].Bytes()) < string(entities[j].Bytes())
			})

			iterator, err := store.ScanKeysOnly(ScanBound{Index: EATV})
			require.NoError(t, err)
			defer iterator.Close()

			for _, e := range entities {
				iterator.Seek(ScanBound{Index: EATV, Prefix: []datalog.Value{e}})
				var seen []datalog.Keyword
				for iterator.Next() {
					datom, err := iterator.Datom()
					require.NoError(t, err)
					require.True(t, datom.E.Equal(e),
						"the run is bound to %v; %v is outside it", e, datom.E)
					seen = append(seen, datom.A)
				}
				require.NoError(t, iterator.Error())
				require.Len(t, seen, len(attrs),
					"the sought run holds this entity's datoms and stops")

				// The run is exhausted, so nothing is positioned. Datom() answers
				// that way already; ElementID() must agree rather than reading
				// whichever key the cursor stopped on, which lies past the run's
				// end — the next entity's first key, not off the end of the index.
				require.Equal(t, datalog.ElementID{}, iterator.ElementID(),
					"an exhausted run positions nothing")
			}
		})
	}
}
