package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

func versionOf(t *testing.T, datoms []*datalog.Datom) *storeVersion {
	t.Helper()
	b := emptyStoreVersion().transient()
	for _, d := range datoms {
		b.addDatom(d)
	}
	return b.commit()
}

func drainScan(t *testing.T, it Iterator) []*datalog.Datom {
	t.Helper()
	var out []*datalog.Datom
	for it.Next() {
		d, err := it.Datom()
		require.NoError(t, err)
		out = append(out, d)
	}
	require.NoError(t, it.Error())
	require.NoError(t, it.Close())
	return out
}

// datomsMatchingBound is the contract stated directly: the datoms whose bound
// components equal the bound's values. The scan must yield exactly these.
func datomsMatchingBound(t *testing.T, all []*datalog.Datom, bound ScanBound) []*datalog.Datom {
	t.Helper()
	order, err := componentOrder(bound.Index)
	require.NoError(t, err)
	probe, err := boundProbeDatom(order, bound)
	require.NoError(t, err)

	var want []*datalog.Datom
	for _, d := range all {
		if comparePrefixInOrder(order, len(bound.Prefix), probe, d) == 0 {
			want = append(want, d)
		}
	}
	sortDatomsInOrder(order, want)
	return want
}

func sortDatomsInOrder(order [componentsPerIndex]keyComponent, datoms []*datalog.Datom) {
	for i := 1; i < len(datoms); i++ {
		for j := i; j > 0 && compareDatomsInOrder(order, datoms[j-1], datoms[j]) > 0; j-- {
			datoms[j-1], datoms[j] = datoms[j], datoms[j-1]
		}
	}
}

func requireScanYieldsExactly(t *testing.T, v *storeVersion, all []*datalog.Datom, bound ScanBound) {
	t.Helper()
	it, err := v.scan(bound)
	require.NoError(t, err)
	got := drainScan(t, it)

	want := datomsMatchingBound(t, all, bound)
	require.Len(t, got, len(want), "scan on %v yielded the wrong count", bound.Index)
	for i := range want {
		require.Same(t, want[i], got[i], "scan on %v diverges at %d", bound.Index, i)
	}
}

func TestMemoryScanWholeIndex(t *testing.T) {
	for _, index := range Indices {
		t.Run(fmt.Sprintf("%v", index), func(t *testing.T) {
			all := sortedTreeDatoms(index, branchingFactor*2+5)
			v := versionOf(t, all)
			requireScanYieldsExactly(t, v, all, ScanBound{Index: index})
		})
	}
}

func TestMemoryScanEmptyVersion(t *testing.T) {
	v := emptyStoreVersion()
	it, err := v.scan(ScanBound{Index: EAVT})
	require.NoError(t, err)
	require.False(t, it.Next())
	require.NoError(t, it.Error())
	require.Zero(t, it.Scanned())

	_, err = it.Datom()
	require.Error(t, err)
	require.NoError(t, it.Close())
}

// TestMemoryScanBoundYieldsExactlyItsRun walks every prefix length of every
// index against a fixture with heavy repetition, so a bound that over-covered
// or under-covered would show up as a count mismatch.
func TestMemoryScanBoundYieldsExactlyItsRun(t *testing.T) {
	attrs := []datalog.Keyword{
		datalog.NewKeyword(":scan/alpha"),
		datalog.NewKeyword(":scan/beta"),
	}
	entities := []datalog.Identity{
		datalog.NewIdentity("scan-entity-1"),
		datalog.NewIdentity("scan-entity-2"),
		datalog.NewIdentity("scan-entity-3"),
	}
	values := []datalog.Value{int64(1), int64(2), "shared"}

	var all []*datalog.Datom
	lamport := uint64(1)
	for _, e := range entities {
		for _, a := range attrs {
			for _, val := range values {
				all = append(all, &datalog.Datom{
					E: e, A: a, V: val,
					Tx: datalog.ElementID{Lamport: lamport, ReplicaID: 1},
				})
				lamport++
			}
		}
	}
	v := versionOf(t, all)

	for _, index := range Indices {
		t.Run(fmt.Sprintf("%v", index), func(t *testing.T) {
			order, err := componentOrder(index)
			require.NoError(t, err)

			// Bind each prefix length using values drawn from a real datom, so
			// every bound names a run that exists.
			sample := all[len(all)/2]
			for n := 1; n <= len(order); n++ {
				prefix := make([]datalog.Value, n)
				for i := 0; i < n; i++ {
					switch order[i] {
					case componentE:
						prefix[i] = sample.E
					case componentA:
						prefix[i] = sample.A
					case componentV:
						prefix[i] = sample.V
					case componentTx:
						prefix[i] = sample.Tx
					}
				}
				requireScanYieldsExactly(t, v, all, ScanBound{Index: index, Prefix: prefix})
			}
		})
	}
}

// TestMemoryScanDoesNotOverCoverPrefixValues is the case the byte-key backend
// cannot answer without a membership filter. An encoded payload carries no
// length, so keys for "abcd" fall inside the range for "abc" and must be
// filtered out afterward (BUG_V_PAYLOAD_NOT_PREFIX_FREE). Comparing typed
// components, a bound on "abc" simply does not reach "abcd" — the run is
// contiguous and exact.
func TestMemoryScanDoesNotOverCoverPrefixValues(t *testing.T) {
	attr := datalog.NewKeyword(":scan/text")
	var all []*datalog.Datom
	lamport := uint64(1)
	for i, text := range []string{"ab", "abc", "abc", "abcd", "abcde", "abd"} {
		all = append(all, &datalog.Datom{
			E:  datalog.NewIdentity(fmt.Sprintf("text-entity-%d", i)),
			A:  attr,
			V:  text,
			Tx: datalog.ElementID{Lamport: lamport, ReplicaID: 1},
		})
		lamport++
	}
	v := versionOf(t, all)

	// AVET orders [A][V][E][Tx], so binding A and V names one value's run.
	bound := ScanBound{Index: AVET, Prefix: []datalog.Value{attr, "abc"}}
	it, err := v.scan(bound)
	require.NoError(t, err)
	got := drainScan(t, it)

	require.Len(t, got, 2, "a bound on \"abc\" must not reach \"abcd\" or \"abcde\"")
	for _, d := range got {
		require.Equal(t, "abc", d.V)
	}
}

// TestMemoryScanCountsIntake: a tree's run is contiguous, so it rejects nothing
// inside the run and does not count the entry that ends it. Intake equals what
// the scan yields.
func TestMemoryScanCountsIntake(t *testing.T) {
	attr := datalog.NewKeyword(":scan/text")
	var all []*datalog.Datom
	for i, text := range []string{"aaa", "bbb", "bbb", "ccc"} {
		all = append(all, &datalog.Datom{
			E:  datalog.NewIdentity(fmt.Sprintf("intake-%d", i)),
			A:  attr,
			V:  text,
			Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 1},
		})
	}
	v := versionOf(t, all)

	it, err := v.scan(ScanBound{Index: AVET, Prefix: []datalog.Value{attr, "bbb"}})
	require.NoError(t, err)
	require.Len(t, drainScan(t, it), 2)
	require.Equal(t, 2, it.Scanned(),
		"the two matches; the entry whose value ended the run is the terminator")

	whole, err := v.scan(ScanBound{Index: AVET})
	require.NoError(t, err)
	require.Len(t, drainScan(t, whole), len(all))
	require.Equal(t, len(all), whole.Scanned(),
		"a whole-index scan ends by exhausting the tree, not by rejecting an entry")
}

// TestMemoryScanSeekAdoptsTheWholeBound: a Seek supplies where the new run ends
// as well as where it starts. An implementation taking only the start would run
// on into whatever the tree still holds.
func TestMemoryScanSeekAdoptsTheWholeBound(t *testing.T) {
	attr := datalog.NewKeyword(":scan/text")
	var all []*datalog.Datom
	for i, text := range []string{"aaa", "bbb", "bbb", "ccc", "ddd"} {
		all = append(all, &datalog.Datom{
			E:  datalog.NewIdentity(fmt.Sprintf("seek-%d", i)),
			A:  attr,
			V:  text,
			Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 1},
		})
	}
	v := versionOf(t, all)

	it, err := v.scan(ScanBound{Index: AVET, Prefix: []datalog.Value{attr, "aaa"}})
	require.NoError(t, err)
	require.True(t, it.Next())

	it.Seek(ScanBound{Index: AVET, Prefix: []datalog.Value{attr, "bbb"}})
	got := drainScan(t, it)
	require.Len(t, got, 2, "seek must stop at the end of the run it named")
	for _, d := range got {
		require.Equal(t, "bbb", d.V)
	}
}

func TestMemoryScanRejectsBadBounds(t *testing.T) {
	v := versionOf(t, sortedTreeDatoms(EAVT, 4))

	t.Run("prefix longer than the order", func(t *testing.T) {
		_, err := v.scan(ScanBound{
			Index:  EAVT,
			Prefix: []datalog.Value{int64(1), int64(2), int64(3), int64(4), int64(5)},
		})
		require.ErrorContains(t, err, "index orders")
	})

	t.Run("wrong type for a position", func(t *testing.T) {
		_, err := v.scan(ScanBound{Index: EAVT, Prefix: []datalog.Value{"not an identity"}})
		require.ErrorContains(t, err, "Identity")
	})

	t.Run("seek to another index is a sticky error", func(t *testing.T) {
		it, err := v.scan(ScanBound{Index: EAVT})
		require.NoError(t, err)
		it.Seek(ScanBound{Index: AVET})
		require.ErrorContains(t, it.Error(), "one index")
		require.False(t, it.Next())
	})
}

// TestMemoryScanWalksItsOwnVersion: an open scan is reading a retained version,
// so commits landing mid-iteration are invisible to it.
func TestMemoryScanWalksItsOwnVersion(t *testing.T) {
	h := newVersionHolder()
	base := sortedTreeDatoms(EAVT, 64)
	seed := h.begin()
	for _, d := range base {
		seed.addDatom(d)
	}
	h.publish(seed)

	it, err := h.read().scan(ScanBound{Index: EAVT})
	require.NoError(t, err)
	require.True(t, it.Next())

	later := h.begin()
	for i := 0; i < 32; i++ {
		later.addDatom(&datalog.Datom{
			E:  datalog.NewIdentity(fmt.Sprintf("mid-scan-%d", i)),
			A:  datalog.NewKeyword(":tree/value"),
			V:  int64(i),
			Tx: datalog.ElementID{Lamport: uint64(50_000 + i), ReplicaID: 1},
		})
	}
	h.publish(later)

	seen := 1
	for it.Next() {
		seen++
	}
	require.NoError(t, it.Error())
	require.Equal(t, len(base), seen, "the scan saw datoms committed after it opened")
	require.Equal(t, len(base)+32, h.read().datomCount())
}
