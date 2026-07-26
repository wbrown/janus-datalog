package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// TestScanStatisticsCarryTheirDuration pins that a scan's statistics event says
// how long the scan took. The events are emitted from Close, so their timing is
// the iterator's lifetime — open to exhaustion — and it belongs in the Event's
// own Latency field, the mechanism every other timed event uses and the one the
// output formatter renders as the line's prefix.
//
// Before this, emitIteratorStatistics left Latency zero and no emitter wrote the
// "scan.duration" key the formatter reads, so every scan line claimed "[0µs]"
// and ended "in <nil>" — the same dead-contract shape as the bound field.
func TestScanStatisticsCarryTheirDuration(t *testing.T) {
	var events []annotations.Event
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:              t.TempDir(),
		AnnotationHandler: func(e annotations.Event) { events = append(events, e) },
	})
	require.NoError(t, err)
	defer db.Close()

	name := datalog.NewKeyword(":person/name")
	tx := db.NewTransaction()
	for _, who := range []string{"Alice", "Bob", "Carol"} {
		require.NoError(t, tx.Add(datalog.NewIdentity("person:"+who), name, who))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	result, err := db.Query(`[:find ?e ?n :where [?e :person/name ?n]]`)
	require.NoError(t, err)
	rows, err := executor.CollectTuples(result, nil)
	require.NoError(t, err)
	require.Len(t, rows, 3)

	var scan *annotations.Event
	for i := range events {
		if events[i].Name == annotations.PatternStorageScan {
			scan = &events[i]
		}
	}
	require.NotNil(t, scan, "the unbound scan path must report its statistics")
	require.Positive(t, scan.Latency,
		"a scan that returned rows took measurable time; a zero latency is an unset field, not a fast scan")
	require.NotContains(t, scan.Data, "scan.duration",
		"timing rides the Event's Latency, not a bespoke data key")
}
