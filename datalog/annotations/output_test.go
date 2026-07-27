package annotations

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRenderBoundPositionsCompactsComponentOrder pins the compact rendering of
// a scan bound's positions: the names arrive in the index's component order and
// concatenate, matching the index-name idiom on the same output line.
func TestRenderBoundPositionsCompactsComponentOrder(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field interface{}
		want  string
	}{
		{"two positions", []string{"A", "V"}, "AV"},
		{"one position", []string{"E"}, "E"},
		{"multi-character position", []string{"A", "Tx"}, "ATx"},
		{"all four", []string{"E", "A", "V", "Tx"}, "EAVTx"},
		// A whole-index scan binds nothing. It is a real answer, distinct from
		// "no selection event was seen" — which the scan line renders "?".
		{"whole index, nil slice", []string(nil), "none"},
		{"whole index, empty slice", []string{}, "none"},
		{"field absent", nil, "none"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, renderBoundPositions(tc.field))
		})
	}
}

// TestScanLineReportsBoundFromIndexSelection is the pairing pin: the scan line's
// index and bound come from the preceding pattern/index-selection event, so the
// two events must agree on the payload's shape.
//
// Nothing else checks that agreement. A formatter reading a key no emitter
// writes still compiles, still runs, and renders "bound: ?" on every line —
// a wrong answer that looks like missing instrumentation.
func TestScanLineReportsBoundFromIndexSelection(t *testing.T) {
	var out bytes.Buffer
	f := NewPlainTextFormatter(&out)

	require.Empty(t, f.Format(Event{
		Name: PatternIndexSelection,
		Data: map[string]interface{}{
			"pattern":      "[?e :task/scenario ?s]",
			"index":        "AVET",
			"bound":        []string{"A", "V"},
			"bound.values": []string{":task/scenario", "scenario-alpha"},
		},
	}), "index selection is folded into the following scan line, not printed itself")

	line := f.Format(Event{
		Name:    PatternStorageScan,
		Latency: 2 * time.Millisecond,
		Data: map[string]interface{}{
			"pattern":         "[?e :task/scenario ?s]",
			"datoms.scanned":  10,
			"datoms.resolved": 10,
			"scan.duration":   2 * time.Millisecond,
		},
	})
	require.Contains(t, line, "Scan([[?e :task/scenario ?s]], AVET, bound: AV)")
	require.Contains(t, line, "10 datoms")
	require.NotContains(t, line, "scanned",
		"a scan that returned everything it read says one number, not two")
	// The scan's duration is the line's latency prefix, the mechanism every
	// other timed event uses. It must not also be appended from a data key —
	// nothing writes one, so the line read "in <nil>".
	require.Contains(t, line, "[2.0ms]")
	require.NotContains(t, line, "<nil>")
	require.NotContains(t, line, " in ")
}

// TestScanLineShowsIntakeWhenItExceedsOutput pins the amplification the typed
// bound exists to control: a scan that read ten datoms to return one must say
// so on the line a human reads, or a bound that narrowed and a bound that did
// nothing render identically.
func TestScanLineShowsIntakeWhenItExceedsOutput(t *testing.T) {
	var out bytes.Buffer
	f := NewPlainTextFormatter(&out)

	line := f.Format(Event{
		Name:    PatternStorageScan,
		Latency: time.Millisecond,
		Data: map[string]interface{}{
			"pattern":         "[?e :person/name ?n]",
			"datoms.scanned":  10,
			"datoms.resolved": 1,
		},
	})
	require.Contains(t, line, "1 datoms")
	require.Contains(t, line, "(10 scanned)",
		"the index charged ten reads for one datom and the line must show it")
}

// TestScanLineWholeIndexBoundIsNotUnknown separates the two ways a scan line can
// fail to name a bound: a selection that bound nothing (a whole-index scan) is
// "none", while no selection event at all is "?". Collapsing them would hide a
// full scan behind the same rendering as missing instrumentation.
func TestScanLineWholeIndexBoundIsNotUnknown(t *testing.T) {
	scan := Event{
		Name: PatternStorageScan,
		Data: map[string]interface{}{
			"pattern":         "[?e ?a ?v]",
			"datoms.resolved": 500,
			"scan.duration":   time.Millisecond,
		},
	}

	var withSelection bytes.Buffer
	f := NewPlainTextFormatter(&withSelection)
	f.Format(Event{
		Name: PatternIndexSelection,
		Data: map[string]interface{}{
			"pattern":      "[?e ?a ?v]",
			"index":        "EATV",
			"bound":        []string(nil),
			"bound.values": []string(nil),
		},
	})
	require.Contains(t, f.Format(scan), "EATV, bound: none")

	var noSelection bytes.Buffer
	bare := NewPlainTextFormatter(&noSelection)
	require.Contains(t, bare.Format(scan), "?, bound: ?")
}

// TestFormatterHandleWritesScanLine pins that the paired events reach the
// writer through Handle, not only through Format: the index-selection event
// must write nothing while still setting up the scan line that follows.
func TestFormatterHandleWritesScanLine(t *testing.T) {
	var out bytes.Buffer
	f := NewPlainTextFormatter(&out)

	f.Handle(Event{
		Name: PatternIndexSelection,
		Data: map[string]interface{}{
			"pattern": "[?e :person/name ?n]",
			"index":   "AETV",
			"bound":   []string{"A"},
		},
	})
	require.Empty(t, out.String())

	f.Handle(Event{
		Name: PatternStorageScan,
		Data: map[string]interface{}{
			"pattern":         "[?e :person/name ?n]",
			"datoms.resolved": 3,
			"scan.duration":   time.Millisecond,
		},
	})
	require.Equal(t, 1, strings.Count(out.String(), "\n"))
	require.Contains(t, out.String(), "AETV, bound: A")
}
