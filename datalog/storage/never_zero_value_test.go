package storage

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func neverZeroSchema(t *testing.T) *schema.Schema {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(":test/string").Type(schema.TypeString).NeverZeroValue().Add().
		Attribute(":test/long").Type(schema.TypeLong).NeverZeroValue().Add().
		Attribute(":test/double").Type(schema.TypeDouble).NeverZeroValue().Add().
		Attribute(":test/boolean").Type(schema.TypeBoolean).NeverZeroValue().Add().
		Attribute(":test/instant").Type(schema.TypeInstant).NeverZeroValue().Add().
		Attribute(":test/bytes").Type(schema.TypeBytes).NeverZeroValue().Add().
		Attribute(":test/tx").Type(schema.TypeTx).NeverZeroValue().Add().
		Attribute(":test/tags").Type(schema.TypeString).Many().NeverZeroValue().Add().
		Attribute(":test/steps").Type(schema.TypeString).Vector().NeverZeroValue().Add().
		Build()
	require.NoError(t, err)
	return s
}

// The many and vector arms of Set validate each element before either
// diffs against the stored collection, so a collection holding a zero is
// rejected whole and the prior collection stands.
func TestNeverZeroValue_SetCollectionRejectsZeroElementAndBuffersNothing(t *testing.T) {
	// A many attribute binds one member per tuple; a vector binds the whole
	// vector as one value, so a buffered zero would change the value, not the
	// tuple count.
	cases := []struct {
		attr string
		want interface{}
	}{
		{":test/tags", "ok"},
		{":test/steps", []string{"ok"}},
	}
	for _, tc := range cases {
		for _, mode := range optimizerModes {
			t.Run(tc.attr+"/"+mode.name, func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: neverZeroSchema(t)})
				e := datalog.NewIdentity("e")
				attr := datalog.NewKeyword(tc.attr)

				tx := db.NewTransaction()
				require.NoError(t, tx.Set(e, attr, []string{"ok"}))
				_, err := tx.Commit()
				require.NoError(t, err)

				tx = db.NewTransaction()
				err = tx.Set(e, attr, []string{"fine", ""})
				require.Error(t, err)
				require.Contains(t, err.Error(), "schema validation failed")
				_, err = tx.Commit()
				require.NoError(t, err)

				got, err := executor.CollectTuples(db.Query(
					`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`, e, attr))
				require.NoError(t, err)
				require.Len(t, got, 1, "the rejected collection must buffer neither its removals nor its adds")
				require.True(t, datalog.ValuesEqual(got[0][0], tc.want), "got %#v want %#v", got[0][0], tc.want)
			})
		}
	}
}

func TestNeverZeroValue_AddAndSetRejectZeroAndBufferNothing(t *testing.T) {
	nonzeroInstant := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	cases := []struct {
		attr    string
		zero    interface{}
		nonzero interface{}
	}{
		{":test/string", "", "hello"},
		{":test/long", int64(0), int64(1)},
		{":test/double", 0.0, 1.5},
		{":test/boolean", false, true},
		{":test/instant", time.Time{}, nonzeroInstant},
		{":test/bytes", []byte{}, []byte{1}},
		{":test/tx", datalog.ElementID{}, datalog.ElementID{Lamport: 1}},
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: neverZeroSchema(t)})
			e := datalog.NewIdentity("e")

			for _, tc := range cases {
				attr := datalog.NewKeyword(tc.attr)

				tx := db.NewTransaction()
				require.NoError(t, tx.Add(e, attr, tc.nonzero))
				err := tx.Add(e, attr, tc.zero)
				require.Error(t, err, "Add zero %s", tc.attr)
				require.Contains(t, err.Error(), "schema validation failed")
				_, err = tx.Commit()
				require.NoError(t, err)

				got, err := executor.CollectTuples(db.Query(
					`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`, e, attr))
				require.NoError(t, err)
				require.Len(t, got, 1, "zero Add must not buffer a datom for %s", tc.attr)
				require.True(t, datalog.ValuesEqual(got[0][0], tc.nonzero), "got %#v want %#v", got[0][0], tc.nonzero)

				tx = db.NewTransaction()
				err = tx.Set(e, attr, tc.zero)
				require.Error(t, err, "Set zero %s", tc.attr)
				require.Contains(t, err.Error(), "schema validation failed")
			}

			// Whitespace is a value.
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":test/string"), " "))
			_, err := tx.Commit()
			require.NoError(t, err)
		})
	}
}

func TestNeverZeroValue_RemoveIsUnconstrained(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			src := createOptimizerModeDB(t, mode, DatabaseOptions{})
			e := datalog.NewIdentity("e")
			attr := datalog.NewKeyword(":test/string")
			tx := src.NewTransaction()
			require.NoError(t, tx.Add(e, attr, ""))
			_, err := tx.Commit()
			require.NoError(t, err)

			var buf bytes.Buffer
			require.NoError(t, src.Export(&buf))

			dst := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: neverZeroSchema(t)})
			require.NoError(t, dst.Import(strings.NewReader(buf.String())))

			tx = dst.NewTransaction()
			require.NoError(t, tx.Remove(e, attr, ""))
			_, err = tx.Commit()
			require.NoError(t, err)

			got, err := executor.CollectTuples(dst.Query(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`, e, attr))
			require.NoError(t, err)
			require.Empty(t, got)
		})
	}
}

func TestNeverZeroValue_ImportBypassesConstraint(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			src := createOptimizerModeDB(t, mode, DatabaseOptions{})
			e := datalog.NewIdentity("e")
			attr := datalog.NewKeyword(":test/string")
			tx := src.NewTransaction()
			require.NoError(t, tx.Add(e, attr, ""))
			_, err := tx.Commit()
			require.NoError(t, err)

			var buf bytes.Buffer
			require.NoError(t, src.Export(&buf))

			dst := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: neverZeroSchema(t)})
			require.NoError(t, dst.Import(strings.NewReader(buf.String())))

			got, err := executor.CollectTuples(dst.Query(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`, e, attr))
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Equal(t, "", got[0][0])
		})
	}
}
