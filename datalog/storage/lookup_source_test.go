package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A database, and the matcher it builds, are each one source: the lookup of
// the source under any name reads that database.
func TestLookupSource_OfOneSourceIsThatSourceUnderAnyName(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			one := datalog.NewIdentity("lookup-source:one")
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(one, kw(":t/a"), "held"))
			_, err := tx.Commit()
			require.NoError(t, err)

			for name, m := range map[string]executor.PatternMatcher{"the database": d, "its matcher": d.Matcher()} {
				elm, ok := m.(executor.EntityLookupMatcher)
				require.True(t, ok, "%s is an entity lookup matcher", name)
				for _, source := range []datalog.Symbol{datalog.SymDollar, datalog.NewSymbol("$other")} {
					lookup, err := elm.LookupSource(source)
					require.NoError(t, err)
					value, found, err := lookup.LookupAttribute(one, kw(":t/a"))
					require.NoError(t, err)
					require.True(t, found)
					require.Equal(t, "held", value, "the lookup of %s from %s reads the database", source, name)
				}
			}
		})
	}
}
