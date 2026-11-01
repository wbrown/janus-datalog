package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// An add-wins resolution holds every surviving member under its memberKey,
// whatever form the index entry's value span took: raw, compressed into the
// key, or a blob's hash. The resolution reads the spans and decodes only the
// survivors, so the key is computed from the decoded value, never from the
// span (BOUND_VALUE_CACHE_MATCH_PROOF.md P6).
func TestResolveAddWinsSet_HoldsASpanObservedMemberUnderItsMemberKey(t *testing.T) {
	declared := schema.NewBuilder().
		Attribute(":folio/note").Type(schema.TypeString).Many().Add().
		Attribute(":folio/blob").Type(schema.TypeBytes).Many().Add().
		MustBuild()
	note := datalog.NewKeyword(":folio/note")
	blob := datalog.NewKeyword(":folio/blob")
	quire := datalog.NewIdentity("folio:quire")

	short := "a short note"
	long := strings.Repeat("the quire is sewn and folded. ", 40)
	// letters is text past what fits in a key once compressed, so the encoder
	// stores its hash in the key and the bytes in a blob; noise is bytes
	// compression does not shrink, which the encoder stores raw.
	letters := make([]byte, 200000)
	noise := make([]byte, 1024)
	x := uint64(0x9E3779B97F4A7C15)
	next := func() byte {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		return byte(x)
	}
	for i := range letters {
		letters[i] = 'a' + next()%26
	}
	for i := range noise {
		noise[i] = next()
	}
	hugeText := string(letters)
	longBytes := []byte(long)

	tiers := []struct {
		name  string
		value interface{}
		tag   datalog.ValueType
	}{
		{"a short string, stored raw", short, datalog.TypeString},
		{"a string compressed into the key", long, datalog.TypeCompressedString},
		{"a string held in a blob", hugeText, datalog.TypeHashedString},
		{"a byte slice compressed into the key", longBytes, datalog.TypeCompressedBytes},
		{"a byte slice held in a blob", letters, datalog.TypeHashedBytes},
		{"a byte slice compression does not shrink, stored raw", noise, datalog.TypeBytes},
	}
	for _, tier := range tiers {
		tag, _, _ := datalog.EncodeValue(tier.value, 512)
		require.Equal(t, tier.tag, tag, "%s must take the tier its name claims", tier.name)
	}

	attributes := []struct {
		attr    datalog.Keyword
		members []interface{}
	}{
		{note, []interface{}{short, long, hugeText}},
		{blob, []interface{}{longBytes, letters, noise}},
	}

	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: declared, CompressionThreshold: 512})
			tx := db.NewTransaction()
			for _, a := range attributes {
				for _, member := range a.members {
					require.NoError(t, tx.Add(quire, a.attr, member))
				}
			}
			_, err := tx.Commit()
			require.NoError(t, err)

			matcher := NewPatternMatcher(db.Store())
			matcher.SetSchema(db.Schema())
			var eBytes [20]byte
			copy(eBytes[:], quire.Bytes())
			for _, a := range attributes {
				t.Run(a.attr.String(), func(t *testing.T) {
					var aBytes [32]byte
					copy(aBytes[:], a.attr.String())
					result, err := matcher.resolveAddWinsSet(eBytes[:], aBytes[:], DiscardIntake)
					require.NoError(t, err)
					require.Len(t, result.Members, len(a.members), "one member per value written, each under one key")
					for _, member := range a.members {
						held, ok := result.Members[memberKey(member)]
						require.True(t, ok, "the member is held under its memberKey; keys held: %d", len(result.Members))
						require.True(t, datalog.ValuesEqual(member, held), "the value held under the key is the member")
					}
				})
			}
		})
	}
}
