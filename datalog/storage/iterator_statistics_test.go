package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestScanReportsIntakeAndResolution pins the two counts a scan owes its
// reader, and the fact that they differ.
//
// datoms.scanned is intake from the index; datoms.resolved is what CRDT
// resolution produced from it. Their gap is history the query paid to read and
// did not use — for a cardinality-one attribute written N times, the index
// holds N datoms and resolution emits one. Reporting only the second number
// makes a scan over deep history indistinguishable from a scan over none.
func TestScanReportsIntakeAndResolution(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)

			var events []annotations.Event
			opts := mode.plannerOptions()
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:              t.TempDir(),
				Schema:            s,
				PlannerOptions:    &opts,
				AnnotationHandler: func(e annotations.Event) { events = append(events, e) },
			})
			require.NoError(t, err)
			defer db.Close()

			person := datalog.NewIdentity("person:alice")
			name := datalog.NewKeyword(":person/name")
			for _, v := range []string{"Alice", "Alicia", "Alys"} {
				tx := db.NewTransaction()
				require.NoError(t, tx.Set(person, name, v))
				_, err = tx.Commit()
				require.NoError(t, err)
			}

			events = nil
			result, err := db.Query(`[:find ?e ?n :where [?e :person/name ?n]]`)
			require.NoError(t, err)
			tuples, err := executor.CollectTuples(result, nil)
			require.NoError(t, err)
			require.Len(t, tuples, 1, "last write wins, so one tuple survives")

			var scan *annotations.Event
			for i := range events {
				if events[i].Name == annotations.PatternStorageScan {
					scan = &events[i]
				}
			}
			require.NotNil(t, scan, "the unbound scan path must report its statistics")

			resolved, ok := scan.Data[annotations.KeyDatomsResolved].(int)
			require.True(t, ok, "the scan must report what resolution produced")
			scanned, ok := scan.Data[annotations.KeyDatomsScanned].(int)
			require.True(t, ok, "the scan must report what it took in from the index")

			require.Equal(t, 1, resolved, "resolution emits the LWW winner alone")
			require.Equal(t, 3, scanned,
				"three writes are three datoms under this attribute, and the scan read them all")
			require.Greater(t, scanned, resolved,
				"the history the index charged for is only visible as the gap between the two")

			// The producer and the formatter are pinned separately elsewhere;
			// here they meet. Two pins can agree on a payload shape that
			// neither side actually produces — only running them together
			// catches a formatter reading keys no emitter writes.
			var rendered bytes.Buffer
			formatter := annotations.NewPlainTextFormatter(&rendered)
			for _, e := range events {
				formatter.Handle(e)
			}
			require.Contains(t, rendered.String(), "(3 scanned)",
				"the amplification must survive from the scan to the -verbose line a human reads")
		})
	}
}

// TestScanStatisticsCarryTheirDuration pins that a scan's statistics event says
// how long the scan took. The events are emitted from Close, so their timing is
// the iterator's lifetime — open to exhaustion — and it belongs in the Event's
// own Latency field, the mechanism every other timed event uses and the one the
// output formatter renders as the line's prefix.
//
// A duration carried in a bespoke data key instead would have to be written by
// every emitter and read by the formatter, and neither half fails visibly when
// the other is missing: the line renders "[0µs]" and trails "in <nil>".
func TestScanStatisticsCarryTheirDuration(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var events []annotations.Event
			opts := mode.plannerOptions()
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:              t.TempDir(),
				PlannerOptions:    &opts,
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
			tuples, err := executor.CollectTuples(result, nil)
			require.NoError(t, err)
			require.Len(t, tuples, 3)

			var scan *annotations.Event
			for i := range events {
				if events[i].Name == annotations.PatternStorageScan {
					scan = &events[i]
				}
			}
			require.NotNil(t, scan, "the unbound scan path must report its statistics")
			require.Positive(t, scan.Latency,
				"a scan that returned tuples took measurable time; a zero latency is an unset field, not a fast scan")
			require.NotContains(t, scan.Data, "scan.duration",
				"timing rides the Event's Latency, not a bespoke data key")
		})
	}
}
