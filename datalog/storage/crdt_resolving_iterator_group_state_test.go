package storage

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

var groupStateTag = datalog.NewKeyword(":bindery/tag")

// resolvedMembers reads every datom one AEVT scan of :bindery/tag resolves,
// grouped by entity, through the resolving iterator the scan arms wrap their
// sources in.
func resolvedMembers(t *testing.T, db *Database) map[datalog.Identity][]interface{} {
	t.Helper()
	m := db.Matcher().(*PatternMatcher)
	source, err := OpenScan(m.reader, DiscardIntake, ScanBound{Index: AEVT, Prefix: []datalog.Value{groupStateTag}})
	require.NoError(t, err)
	it := NewCRDTResolvingIterator(source, m.schema, m, nil)
	members := map[datalog.Identity][]interface{}{}
	for it.Next() {
		d, err := it.Datom()
		require.NoError(t, err)
		members[d.E] = append(members[d.E], d.V)
	}
	require.NoError(t, it.Error())
	require.NoError(t, it.Close())
	return members
}

// Each group of one scan resolves from its own datoms
// (docs/proposals/RESOLVING_ITERATOR_GROUP_STATE_PROOF.md, T1, T3): a value an
// entity withdrew is not emitted for it and is emitted for the entity that
// holds it, a value one entity holds is emitted for every entity that holds
// it, and a group of one member after a group wider than a small map emits
// its one member. The entities are named in the order the scan reads them,
// by their hashes, so the withdrawal precedes the holder and the wide group
// precedes the lone one.
func TestCRDTResolvingIterator_ResolvesEachGroupFromItsOwnDatoms(t *testing.T) {
	declared := schema.NewBuilder().Attribute(":bindery/tag").Type(schema.TypeString).Many().Add().MustBuild()
	db := createOptimizerModeDB(t, optimizerModes[0], DatabaseOptions{Schema: declared})
	quires := make([]datalog.Identity, 4)
	for i := range quires {
		quires[i] = datalog.NewIdentity(fmt.Sprintf("bindery:quire%d", i))
	}
	sort.Slice(quires, func(i, j int) bool {
		a, b := quires[i].Hash(), quires[j].Hash()
		return bytes.Compare(a[:], b[:]) < 0
	})
	withdrawing, holder, wide, lone := quires[0], quires[1], quires[2], quires[3]
	// More members than the eight entries a small map holds.
	const wideCount = 12
	wideMembers := make([]interface{}, wideCount)
	for i := range wideMembers {
		wideMembers[i] = fmt.Sprintf("sheet%d", i)
	}

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(withdrawing, groupStateTag, "folded"))
	require.NoError(t, tx.Add(withdrawing, groupStateTag, "sewn"))
	require.NoError(t, tx.Add(holder, groupStateTag, "folded"))
	require.NoError(t, tx.Add(holder, groupStateTag, "sewn"))
	for _, member := range wideMembers {
		require.NoError(t, tx.Add(wide, groupStateTag, member))
	}
	require.NoError(t, tx.Add(lone, groupStateTag, "folded"))
	_, err := tx.Commit()
	require.NoError(t, err)
	tx = db.NewTransaction()
	require.NoError(t, tx.Remove(withdrawing, groupStateTag, "sewn"))
	_, err = tx.Commit()
	require.NoError(t, err)

	members := resolvedMembers(t, db)
	assert.ElementsMatch(t, []interface{}{"folded"}, members[withdrawing], "the withdrawn value is not emitted for the entity that withdrew it")
	assert.ElementsMatch(t, []interface{}{"folded", "sewn"}, members[holder], "the value another entity withdrew is emitted for the entity that holds it")
	assert.ElementsMatch(t, wideMembers, members[wide], "every member of the wide group")
	assert.ElementsMatch(t, []interface{}{"folded"}, members[lone], "the one member of the group after the wide one")
	assert.Len(t, members, 4)
}

// groupStateAllowance bounds what one scan over many entities may allocate
// beyond one scan over one entity. It is below one group's cost before the
// change, two maps and a slot group, times the groups beyond the first.
const groupStateAllowance = 4 << 10

// One scan over many entities each holding one member allocates no more
// bytes than one scan over one entity, within the allowance: the resolving
// iterator keeps its set state across groups
// (docs/proposals/RESOLVING_ITERATOR_GROUP_STATE_PROOF.md, T2).
func TestCRDTResolvingIterator_KeepsItsSetStateAcrossGroups(t *testing.T) {
	bytesPerScan := func(entities int) int64 {
		declared := schema.NewBuilder().Attribute(":bindery/tag").Type(schema.TypeString).Many().Add().MustBuild()
		db := createOptimizerModeDB(t, optimizerModes[0], DatabaseOptions{Schema: declared})
		tx := db.NewTransaction()
		for i := 0; i < entities; i++ {
			require.NoError(t, tx.Add(datalog.NewIdentity(fmt.Sprintf("bindery:quire%d", i)), groupStateTag, "folded"))
		}
		_, err := tx.Commit()
		require.NoError(t, err)
		m := db.Matcher().(*PatternMatcher)

		var scanErr error
		resolved := 0
		scan := func() {
			source, err := OpenScan(m.reader, DiscardIntake, ScanBound{Index: AEVT, Prefix: []datalog.Value{groupStateTag}})
			if err != nil {
				scanErr = err
				return
			}
			it := NewCRDTResolvingIterator(source, m.schema, m, nil)
			resolved = 0
			for it.Next() {
				resolved++
			}
			if err := it.Error(); err != nil {
				scanErr = err
			}
			if err := it.Close(); err != nil && scanErr == nil {
				scanErr = err
			}
		}
		scan()
		require.NoError(t, scanErr)
		require.Equal(t, entities, resolved, "one member per entity")
		perScan := testing.Benchmark(func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				scan()
			}
		}).AllocedBytesPerOp()
		require.NoError(t, scanErr)
		return perScan
	}

	one, many := bytesPerScan(1), bytesPerScan(1000)
	assert.LessOrEqual(t, many-one, int64(groupStateAllowance), "bytes a scan over a thousand entities allocates beyond a scan over one: %d", many-one)
}
